"""
Extracts all products for ETM calibration runs

Tuomas Karna 2014-09-11
"""
from batchScriptLib import *


def processRun(runTag, parentJob=None, parentMode=None, testOnly=True,
               verbose=False):
    print 'Setting up', runTag

    queue = 'normal'

    shortname = runTag.replace('refine', 'ref')
    # --- run model ---
    name = shortname
    nproc = 128
    runDir = runTag
    cmd = 'ibrun tacc_affinity ./pelfe -ksp_type cg -pc_type mg\n'
    log = 'log_run'
    job = batchJob(name, queue, nproc, timeRequest(10, 00, 0), log,
                   runDirectory=runDir, command=cmd)
    runID = launchJob(job, testOnly, verbose)
    print 'submitted run job:', runTag, runID

    # --- combine ---
    name = 'c_'+shortname
    nproc = 48
    threads = 24
    first_stack = 1
    last_stack = 12
    log = os.path.join(logFileDir, 'log_comb_'+shortname)
    t1 = taskThreadedCombine(runTag, threads, 1, 4)
    t2 = taskThreadedCombine(runTag, threads, 5, 8)
    t3 = taskThreadedCombine(runTag, threads, 9, 12)
    t1.cmd = 'ibrun -n 1 -o 0  ' + t1.cmd
    t2.cmd = 'ibrun -n 1 -o 16 ' + t2.cmd
    t3.cmd = 'ibrun -n 1 -o 32 ' + t3.cmd
    combJob = batchJob(name, queue, nproc, timeRequest(1, 30, 0), log,
                       parentJob=runID, parentMode='any')
    combJob.appendTask(t1, threaded=True)
    combJob.appendTask(t2, threaded=True)
    combJob.appendTask(t3, threaded=True)
    combID = launchJob(combJob, testOnly, verbose)
    print 'submitted comb job:', runTag, combID

    # --- extract ---
    name = 'e_'+shortname
    log = os.path.join(logFileDir, 'log_extr_'+shortname)
    nproc = 8
    masterJob = batchJob(name, queue, nproc, timeRequest(5, 30, 0), log,
                         parentJob=combID, parentMode='any')

    st = datetime.datetime(2012, 10, 21)
    et = datetime.datetime(2012, 11, 1)

    masterJob.appendTask(taskSkillExtract(runTag, st, et), threaded=True)

    # North channel profiles
    command = "time extractStation -r {runTag} -d {runTag}/combined/ -C -s {start} -e {end} -v salt,temp,hvel,kine,vdff,tdff,mixl -p -n -t nc_etm_extra_stations.csv\n"
    cmdStr = command.format(runTag=runTag, start=st.strftime('%Y-%m-%d'),
                            end=et.strftime('%Y-%m-%d'))
    name = 'extrProf_'+runTag
    log = os.path.join(logFileDir, 'log_'+name)
    t = task(cmdStr, name, log)
    masterJob.appendTask(t, threaded=True)

    masterJob.appendTask(taskExtractWP(runTag), threaded=True)

    masterJob.appendTask(taskExtractSlab(runTag, st, et,
                                        ['elev'], k=0), threaded=True)
    masterJob.appendTask(taskExtractSlab(runTag, st, et,
                                        ['salt', 'temp'], k=1), threaded=True)
    masterJob.appendTask(taskExtractSlab(runTag, st, et,
                                        ['hvel'], k=2), threaded=True)

    # AUV extraction cannot be threaded (tasks must be run in order), append last
    masterJob.appendTask(taskExtractAUV(runTag), threaded=False)

    extrID = launchJob(masterJob, testOnly=testOnly, verbose=verbose)
    print 'submitted extr job:', runTag, extrID

# -----------------------------------------------------------------------------
# Command line interface
# -----------------------------------------------------------------------------

if __name__=='__main__' :

    from optparse import OptionParser
    usage = ('Usage: %prog [options] runTag\n')

    parser = OptionParser(usage=usage)
    parser.add_option('-p', '--parentJob', action='store', type='string',
                      dest='parentJob', help='Add job dependency; this job'+
                      ' will only be executed after the parent has finished')
    parser.add_option('-m', '--parentMode', action='store', type='string',
                      dest='parentMode', help='', default='ok')
    parser.add_option('-t', '--test', action='store_true',
                      dest='testOnly', help='do not launch anything, just print submission script on stdout')
    parser.add_option('-v', '--verbose', action='store_true',
                      dest='verbose', help='print submission script on stdout')

    (options, args) = parser.parse_args()
    if len(args) < 1:
        parser.print_help()
        parser.error('runTag missing')

    runTag = args[0]

    processRun(runTag, testOnly=options.testOnly, parentJob=options.parentJob,
               parentMode=options.parentMode ,verbose=options.verbose)

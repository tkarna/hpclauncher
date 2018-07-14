"""
Extracts all products for ETM calibration runs

Tuomas Karna 2014-09-11
"""
from batchScriptLib import *


def processRun(runTag, parentjob=None, parentmode=None, testonly=True,
               verbose=False):
    print('Setting up {:}'.format(runTag))

    queue = 'normal'

    shortname = runTag.replace('refine', 'ref')

    # --- combine ---
    name = 'c_'+shortname
    nproc = 48
    threads = 24
    first_stack = 1
    last_stack = 12
    log = os.path.join(logfiledir, 'log_comb_'+shortname)
    t1 = taskThreadedCombine(runTag, threads, 1, 4)
    t2 = taskThreadedCombine(runTag, threads, 5, 8)
    t3 = taskThreadedCombine(runTag, threads, 9, 12)
    t1.cmd = 'ibrun -n 1 -o 0  ' + t1.cmd
    t2.cmd = 'ibrun -n 1 -o 16 ' + t2.cmd
    t3.cmd = 'ibrun -n 1 -o 32 ' + t3.cmd
    combJob = batchJob(name, queue, nproc, timerequest(1, 30, 0), log,
                       parentjob=parentjob, parentmode='any')
    combJob.appendTask(t1, threaded=True)
    combJob.appendTask(t2, threaded=True)
    combJob.appendTask(t3, threaded=True)
    combID = launchJob(combJob, testonly, verbose)
    print('submitted comb job: {:} {:}'.format(runTag, combID))

    # --- extract ---
    name = 'e_'+shortname
    log = os.path.join(logfiledir, 'log_extr_'+shortname)
    nproc = 8
    masterJob = batchJob(name, queue, nproc, timerequest(5, 30, 0), log,
                         parentjob=combID, parentmode='any')

    st = datetime.datetime(2012, 10, 21)
    et = datetime.datetime(2012, 11, 1)

    masterJob.appendTask(taskSkillExtract(runTag, st, et), threaded=True)

    # North channel profiles
    command = "time extractStation -r {runTag} -d {runTag}/combined/ -C -s {start} -e {end} -v salt,temp,hvel,kine,vdff,tdff,mixl -p -n -t nc_etm_extra_stations.csv\n"
    cmdStr = command.format(runTag=runTag, start=st.strftime('%Y-%m-%d'),
                            end=et.strftime('%Y-%m-%d'))
    name = 'extrProf_'+runTag
    log = os.path.join(logfiledir, 'log_'+name)
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

    extrID = launchJob(masterJob, testonly=testonly, verbose=verbose)
    print('submitted extr job: {:} {:}'.format(runTag, extrID))

# -----------------------------------------------------------------------------
# Command line interface
# -----------------------------------------------------------------------------

if __name__=='__main__' :

    from optparse import OptionParser
    usage = ('Usage: %prog [options] runTag\n')

    parser = OptionParser(usage=usage)
    parser.add_option('-p', '--parentjob', action='store', type='string',
                      dest='parentjob', help='Add job dependency; this job'+
                      ' will only be executed after the parent has finished')
    parser.add_option('-m', '--parentmode', action='store', type='string',
                      dest='parentmode', help='', default='ok')
    parser.add_option('-t', '--test', action='store_true',
                      dest='testonly', help='do not launch anything, just print submission script on stdout')
    parser.add_option('-v', '--verbose', action='store_true',
                      dest='verbose', help='print submission script on stdout')

    (options, args) = parser.parse_args()
    if len(args) < 1:
        parser.print_help()
        parser.error('runTag missing')

    runTag = args[0]

    processRun(runTag, testonly=options.testonly, parentjob=options.parentjob,
               parentmode=options.parentmode ,verbose=options.verbose)

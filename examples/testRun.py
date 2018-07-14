"""
Extracts all products for ETM calibration runs

Tuomas Karna 2014-09-11
"""
from batchScriptLib import *


def processRun(runTag, parentjob=None, parentmode=None, testonly=True,
               verbose=False):
    print('Setting up {:}'.format(runTag))

    queue = 'development'

    shortname = runTag.replace('refine', 'ref')
    # --- run model ---
    name = shortname
    nproc = 32
    rundir = runTag
    cmd = 'ibrun tacc_affinity ./pelfe -ksp_type cg -pc_type mg\n'
    log = 'log_run'
    job = batchJob(name, queue, nproc, timerequest(0, 15, 0), log,
                   rundirectory=rundir, command=cmd)
    runID = launchJob(job, testonly, verbose)
    print('submitted run job: {:} {:}'.format(runTag, runID))


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

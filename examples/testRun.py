"""
Extracts all products for ETM calibration runs

Tuomas Karna 2014-09-11
"""
from batchScriptLib import *


def processRun(runTag, parentJob=None, parentMode=None, testOnly=True,
               verbose=False):
    print 'Setting up', runTag

    queue = 'development'

    shortname = runTag.replace('refine', 'ref')
    # --- run model ---
    name = shortname
    nproc = 32
    runDir = runTag
    cmd = 'ibrun tacc_affinity ./pelfe -ksp_type cg -pc_type mg\n'
    log = 'log_run'
    job = batchJob(name, queue, nproc, timeRequest(0, 15, 0), log,
                   runDirectory=runDir, command=cmd)
    runID = launchJob(job, testOnly, verbose)
    print 'submitted run job:', runTag, runID


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

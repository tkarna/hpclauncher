"""
An example python script that launces the following command
'{mpiExec} python runTestSuite.py {resolution} -Re {Re} -p {p} -j {jFactor}'

The parameters are read from command line. If multiple values are given, will
loop over the values and lauch a separate job for each case.

Usage:

# launch a single run with parameters
python paramSweep.py coarse -Re 0.1 -j 1.0

# test only (print script content on stdout)
python paramSweep.py coarse -Re 0.1 -j 1.0 -t

# multiple values, run all combinations as independent jobs
python paramSweep.py coarse,medium -Re 0.1,1.0,2.5 -j 1.0
python paramSweep.py coarse -Re 0.1,1.0,2.5 -j 1.0,2.0

"""
from batchScriptLib import *

duration = {
    'coarse':  timeRequest( 4, 0, 0),
    'medium':  timeRequest( 8, 0, 0),
    'fine':    timeRequest(24, 0, 0),
    }

processes = {
    'coarse':  2,
    'medium':  8,
    'fine':    32,
    }


def processArgs(reso, reynoldsNumber, jFactor, polyOrder,
                queue='normal', testOnly=False, verbose=False):
    """
    runs the following command
    """
    cmd = '{mpiExec} python runTestSuite.py {reso} -Re {Re} -p {p} -j {jFactor}'

    t = duration[reso]
    nproc = processes[reso]
    jobName = 'sweep_{:}_p{:}_Re{:}_j{:}'.format(reso, polyOrder, reynoldsNumber,
                                                 jFactor)
    # create a job
    j = batchJob(jobName=jobName, queue=queue,
                 nproc=nproc, timeReq=t, logFile='log_'+jobName)
    # add the command
    j.appendNewTask(cmd, reso=reso, Re=reynoldsNumber,
                    p=polyOrder, jFactor=jFactor)
    # submit to queue manager
    submitJobs(j, testOnly=testOnly, verbose=verbose)


def parseCommandLine():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('reso_str', type=str,
                        help='resolution string (coarse, medium, fine)')
    parser.add_argument('-j', '--jFactor', default=1.0,
                        help='factor j')
    parser.add_argument('-p', '--polyOrder', default=1, type=int,
                        help='order of finite element space (0|1)')
    parser.add_argument('-q', '--queue', default='normal', type=str,
                        help='cluster job queue')
    parser.add_argument('-Re', '--reynoldsNumber', default=2.0,
                        help='mesh Reynolds number')
    parser.add_argument('-t', '--testOnly', action='store_true', default=False,
                        help=('do not launch anything, just print submission '
                              'script on stdout'))
    parser.add_argument('-v', '--verbose', action='store_true', default=False,
                        help='print submission script on stdout')
    args = parser.parse_args()

    # parse lists (if any)
    if isinstance(args.reso_str, str):
        ResoList = [s.strip() for s in args.reso_str.split(',')]
    else:
        ResoList = [args.reso_str]
    if isinstance(args.reynoldsNumber, str):
        ReList = [float(v) for v in args.reynoldsNumber.split(',')]
    else:
        ReList = [args.reynoldsNumber]
    if isinstance(args.jFactor, str):
        jList = [float(v) for v in args.jFactor.split(',')]
    else:
        jList = [args.jFactor]

    # loop over all parameter combinations, launcing a job for each case
    for reso in ResoList:
        for reynoldsNumber in ReList:
            for jFactor in jList:
                processArgs(reso, reynoldsNumber, jFactor,
                            args.polyOrder, queue=args.queue,
                            testOnly=args.testOnly, verbose=args.verbose)


if __name__ == '__main__':
    parseCommandLine()

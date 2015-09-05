#!/usr/bin/env python
"""
Submits jobs defined in yaml file.

Tuomas Karna 2015-09-03
"""
from batchScriptLib import *
import argparse


def submitYamlJobs(jobFile, clusterParamsFile, testOnly=False, verbose=False):
    """
    Submits jobs defined in the jobFile.
    """
    if clusterParamsFile is not None:
        clusterParams.initializeFromFile(clusterParamsFile)

    jobs = parseJobsFromYAML(jobFile)
    submitJobs(jobs, testOnly=testOnly, verbose=verbose)


def parseCommandLine():
    parser = argparse.ArgumentParser()
    parser.add_argument('jobFile', help='yaml job file')
    parser.add_argument('-c', '--clusterParamsFile', help='Custom yaml '
                        'cluster parameter file. By default user config file '
                        'is used (if present).')
    parser.add_argument('-t', '--testOnly', action='store_true', default=False,
                        help=('Do not launch anything, just print submission '
                              'script on stdout'))
    parser.add_argument('-v', '--verbose', action='store_true', default=False,
                        help='Print submission script on stdout')
    args = parser.parse_args()

    submitYamlJobs(args.jobFile, args.clusterParamsFile,
                   testOnly=args.testOnly, verbose=args.verbose)


if __name__ == '__main__':
    parseCommandLine()

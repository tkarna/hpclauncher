#!/usr/bin/env python
"""
Submits jobs defined in yaml file.

Tuomas Karna 2015-09-03
"""
from hpclauncher import *
import argparse


def submitYamlJobs(jobfile, clusterparamsfile, testonly=False, verbose=False):
    """
    Submits jobs defined in the jobfile.
    """
    if clusterparamsfile is not None:
        clusterparams.initialize_from_file(clusterparamsfile)

    jobs = parse_jobs_from_yaml(jobfile)
    submit_jobs(jobs, testonly=testonly, verbose=verbose)


def parseCommandLine():
    parser = argparse.ArgumentParser()
    parser.add_argument('jobfile', help='yaml job file')
    parser.add_argument('-c', '--clusterparamsfile', help='Custom yaml '
                        'cluster parameter file. By default user config file '
                        'is used (if present).')
    parser.add_argument('-t', '--testonly', action='store_true', default=False,
                        help=('Do not launch anything, just print submission '
                              'script on stdout'))
    parser.add_argument('-v', '--verbose', action='store_true', default=False,
                        help='Print submission script on stdout')
    args = parser.parse_args()

    submitYamlJobs(args.jobfile, args.clusterparamsfile,
                   testonly=args.testonly, verbose=args.verbose)


if __name__ == '__main__':
    parseCommandLine()

"""
A collection of routines for generating and launching processes on clusters.

Job dependencies can be handled by chaining tasks:

runID = launchRun(nproc, timeReq, queue, runTag,
                  parentJob, testOnly, verbose=verbose)

Tuomas Karna 2014-09-11
"""

import datetime
from dateutil import relativedelta
import subprocess
import os
import sys

import job
import task
import yamlInterface
import clusterParameters


def getClusterParametersFromYAML(yamlFile):
    """
    Parses a yaml file and returns a clusterSetup object
    """
    kwargs = yamlInterface.readYamlFile(yamlFile)
    c = clusterParameters.clusterSetup(**kwargs)
    return c


def parseJobsFromYAML(yamlFile):
    """
    Parses a yaml file and returns a list of task objects
    """
    raise NotImplementedError('this feature has not been implemented yet')
    # TODO must parse jobs, tasks, populate commands with kwargs, handle deps
    kwargs = yamlInterface.readYamlFile(yamlFile)
    t = task.batchJob(**kwargs)


class timeRequest(relativedelta.relativedelta):
    """
    Simple object that represents requested duration of a batch job.
    """
    def __init__(self, hours, minutes, seconds):
        relativedelta.relativedelta.__init__(self,
                                             hours=hours,
                                             minutes=minutes,
                                             seconds=seconds)

    @classmethod
    def fromString(cls, hhmmss):
        h, m, s = hhmmss.split(':')
        return cls(int(h), int(m), int(s))

    def getString(self):
        day_hours = self.days*24
        return '{0:02d}:{1:02d}:{2:02d}'.format(
            self.hours + day_hours, self.minutes, self.seconds)

    def getHourString(self):
        day_hours = self.days*24
        hours = '{0:02d}'.format(self.hours + day_hours)
        return hours

    def getMinuteString(self):
        minutes = '{0:02d}'.format(self.minutes)
        return minutes

    def getSecondString(self):
        seconds = '{0:02d}'.format(self.seconds)
        return seconds


def _parseJobID(launcher_output):
    """
    Returns jobID of the launched runs by parsing submission exec output.
    Output needs to be grabbed from stdout.
    """
    lines = launcher_output.split('\n')
    target = 'Submitted batch job '
    for line in lines:
        if line.find(target) == 0:
            return int(line.split()[-1])
    raise Exception('Could not parse sbatch output for job id')


def _launchJob(name, content, submitExec, directory=None,
               testOnly=False, verbose=False):
    """
    Writes given batch script content to a temp file and launches the run.
    Returns jobID of the started job.
    If directory given, starts job in that directory.
    """
    if testOnly:
        # print to stdout and return
        print content
        return 0
    elif verbose:
        print content
    if directory:
        # change to runDir, store current dir
        if verbose:
            print 'chdir', directory
        curDir = os.getcwd()
        os.chdir(directory)
    # write out temp submission file
    subfile = 'batch_' + name + '.sub'
    if verbose:
        print 'writing to', subfile
    fid = open(subfile, 'w')
    fid.write(content)
    fid.close()
    # submit file
    try:
        if verbose:
            print 'launching', submitExec, subfile
        output = subprocess.check_output([submitExec, subfile])
    except Exception as e:
        print e
        raise e
    if directory:
        # change back to currect directory
        if verbose:
            print 'chdir', curDir
        os.chdir(curDir)
    # print launcher output to stdout
    print output
    jobID = _parseJobID(output)
    print 'Parsed Job ID:', jobID
    return jobID


def launchJob(job, testOnly=False, verbose=False):
    """
    Lauches given job and returns the jobID number.
    """
    name = job.kwargs['jobName']
    content = job.generateScript()
    directory = job.kwargs['jobName']
    return _launchJob(name, content, directory, testOnly, verbose)

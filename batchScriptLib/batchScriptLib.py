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

import batchScriptLib.job as job


def getJobForSingleTask(t, queue, nproc, timeReq, jobName=None, logFile=None,
                        runDirectory='', parentJob=None, parentMode='ok'):
    """
    Returns a batch job that only excecutes this task.
    """
    if jobName is None:
        if t.jobName is None:
            raise Exception('jobName must be given')
        else:
            jobName = t.jobName
    if logFile is None:
        if t.logFile is None:
            raise Exception('logFile must be given')
        else:
            logFile = t.logFile
    dir = str(runDirectory)
    return job.batchJob(jobName, queue, nproc, timeReq,
                        logFile, command=t.getCommand(), runDirectory=dir,
                        parentJob=parentJob, parentMode=parentMode)


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


def _parseJobID(sbatch_output):
    """
    Returns jobID of the launched runs by parsing sbatch output.
    Output needs to be grabbed from stdout.
    """
    lines = sbatch_output.split('\n')
    target = 'Submitted batch job '
    for line in lines:
        if line.find(target) == 0:
            return int(line.split()[-1])
    raise Exception('Could not parse sbatch output for job id')


def _launchJob(name, content, directory=None, testOnly=False, verbose=False):
    """
    Writes given batch script content to a temp file and launches the run.
    Returns jobID of the started job.
    If directory given, starts job in that directory.
    """
    if testOnly:
        print content
        return 0
    elif verbose:
        print content
    if directory:
        if verbose:
            print 'chdir', directory
        curDir = os.getcwd()
        os.chdir(directory)
    subfile = 'batch_' + name + '.sub'
    if verbose:
        print 'writing to', subfile
    fid = open(subfile, 'w')
    fid.write(content)
    fid.close()
    try:
        if verbose:
            print 'launching', launcher, subfile
        output = subprocess.check_output([launcher, subfile])
    except Exception as e:
        print e.output
        raise e
    if directory:
        if verbose:
            print 'chdir', curDir
        os.chdir(curDir)
    print output
    jobID = _parseJobID(output)
    print 'Parsed Job ID:', jobID
    return jobID


def launchJob(job, testOnly=False, verbose=False):
    """
    Lauches given job and returns the jobID number.
    """
    name = job.jobName
    content = job.generateScript()
    directory = job.runDirectory
    return _launchJob(name, content, directory, testOnly, verbose)

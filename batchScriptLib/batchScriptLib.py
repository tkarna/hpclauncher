"""
A collection of routines for generating and launching processes on clusters.

Job dependencies can be handled by chaining tasks:

runID = launchRun(nproc, timeReq, queue, runTag,
                  parentJob, testOnly, verbose=verbose)

Tuomas Karna 2014-09-11
"""
from dateutil import relativedelta
from collections import OrderedDict

import clusterParameters
from clusterParameters import clusterParams
from clusterParameters import CLUSTERPARAM_ENV_VAR, CLUSTERPARAM_USERFILE

import job
import yamlInterface
import launcher


def parseJobsFromYAML(yamlFile):
    """
    Parses a yaml file and returns a list of job objects
    """
    # read file to nested OrderedDict
    kwargs = yamlInterface.readYamlFile(yamlFile)
    # global tags: at highest level, if not starting with job_
    globKeys = [k for k in kwargs if k[:4] != 'job_']
    globals = OrderedDict([(k, kwargs[k]) for k in kwargs if k in globKeys])
    # global tags may be used in job or task defs, store in clusterParams
    clusterParams.getArgs().update(globals)
    # all other sub-dicts are jobs
    jobs = OrderedDict([(k, kwargs[k]) for k in kwargs if k not in globKeys])
    # parse dict to jobs
    jobList = []
    for jobKey in jobs:
        j = _parseJobFromDict(jobKey, jobs[jobKey])
        jobList.append(j)
    return jobList


def submitJobs(jobList, testOnly=False, verbose=False):
    """
    Submits the given list of jobs.
    """
    if not isinstance(jobList, list):
        jobList = [jobList]
    # keep track of launched job names and ids
    parentTags = ['parentJobOk', 'parentJobAny']
    parentJobIDs = {}
    # launch jobs
    for j in jobList:
        # substitute parentJob with actual job id
        for tag in parentTags:
            # parentJobs can only be defined in batchJob
            parentName = j.kwargs.get(tag)
            if parentName is not None:
                if parentName in parentJobIDs:
                    j.kwargs[tag] = parentJobIDs[parentName]
                else:
                    raise Exception('unknown parentJob: ' + parentName)
        id = launcher.launchJob(j, testOnly=testOnly, verbose=verbose)
        parentJobIDs[j['jobName']] = id


def _parseJobFromDict(jobKey, d):
    """
    Creates a job from a nested OrderedDict
    """
    # parse jobName from the key
    jobName = '_'.join(jobKey.split('_')[1:])
    # parse timeRequest if any
    timeReq = d.pop('time')
    if timeReq is not None:
        timeReq = timeRequest(timeReq['hours'],
                              timeReq['minutes'],
                              timeReq['seconds'])
    # all keys starting with task_ indicate a task spec
    taskKeys = [k for k in d if k[:5] == 'task_']
    tasks = dict([(k, d[k]) for k in taskKeys])
    # all remaining keys are job arguments
    job_kwargs = dict([(k, d[k]) for k in d if k not in taskKeys])
    job_kwargs['jobName'] = jobName
    # create job
    j = job.batchJob(timeReq, **job_kwargs)
    for tkey in tasks:
        task_kwargs = tasks[tkey]
        # create task
        command = task_kwargs.pop('command')
        j.appendNewTask(command, **task_kwargs)
    return j


class timeRequest(relativedelta.relativedelta):
    """
    Simple object that represents requested duration of a batch job.
    """
    def __init__(self, hours=0, minutes=0, seconds=0):
        relativedelta.relativedelta.__init__(self,
                                             hours=hours,
                                             minutes=minutes,
                                             seconds=seconds)

    @classmethod
    def fromString(cls, hhmmss):
        h, m, s = hhmmss.split(':')
        return cls(int(h), int(m), int(s))

    def __str__(self):
        return self.getString()

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



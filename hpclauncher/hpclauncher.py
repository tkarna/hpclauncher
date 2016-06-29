"""
A collection of routines for generating and launching processes on clusters.

Job dependencies can be handled by chaining tasks:

runID = launchRun(nproc, timereq, queue, runTag,
                  parentjob, testonly, verbose=verbose)

Tuomas Karna 2014-09-11
"""
from __future__ import absolute_import
from dateutil import relativedelta
from collections import OrderedDict

from . import clusterparameters
from . import job
from . import yaml_interface
from . import launcher
from .clusterparameters import clusterparams


def parse_jobs_from_yaml(yamlfile):
    """
    Parses a yaml file and returns a list of job objects
    """
    # read file to nested OrderedDict
    kwargs = yaml_interface.read_yaml_file(yamlfile)
    # global tags: at highest level, if not starting with job_
    global_keys = [k for k in kwargs if k[:4] != 'job_']
    globals = OrderedDict([(k, kwargs[k]) for k in kwargs if k in global_keys])
    # global tags may be used in job or task defs, store in clusterParams
    clusterparams.getArgs().update(globals)
    # all other sub-dicts are jobs
    jobs = OrderedDict([(k, kwargs[k]) for k in kwargs if k not in global_keys])
    # parse dict to jobs
    job_list = []
    for job_key in jobs:
        j = _parse_job_from_dict(job_key, jobs[job_key])
        job_list.append(j)
    return job_list


def submit_jobs(job_list, testonly=False, verbose=False):
    """
    Submits the given list of jobs.
    """
    if not isinstance(job_list, list):
        job_list = [job_list]
    # keep track of launched job names and ids
    parent_tags = ['parentjobOk', 'parentjobAny']
    parent_job_ids = {}
    # launch jobs
    for j in job_list:
        # substitute parentjob with actual job id
        for tag in parent_tags:
            # parent jobs can only be defined in BatchJob
            parent_name = j.kwargs.get(tag)
            if parent_name is not None:
                if parent_name in parent_job_ids:
                    j.kwargs[tag] = parent_job_ids[parent_name]
                else:
                    # raise Exception('unknown parentjob: ' + parentName)
                    # assume that user has given a valid parent job ID
                    pass
        id = launcher.launch_job(j, testonly=testonly, verbose=verbose)
        parent_job_ids[j['jobname']] = id


def _parse_job_from_dict(jobkey, d):
    """
    Creates a job from a nested OrderedDict
    """
    # parse jobname from the key
    jobname = '_'.join(jobkey.split('_')[1:])
    # parse timerequest if any
    timereq = d.pop('time')
    if timereq is not None:
        timereq = TimeRequest(timereq['hours'],
                              timereq['minutes'],
                              timereq['seconds'])
    # all keys starting with task_ indicate a task spec
    task_keys = [k for k in d if k[:5] == 'task_']
    tasks = dict([(k, d[k]) for k in task_keys])
    # all remaining keys are job arguments
    job_kwargs = dict([(k, d[k]) for k in d if k not in task_keys])
    job_kwargs['jobname'] = jobname
    # create job
    j = job.BatchJob(timereq, **job_kwargs)
    for tkey in tasks:
        task_kwargs = tasks[tkey]
        # create task
        command = task_kwargs.pop('command')
        j.append_new_task(command, **task_kwargs)
    return j


class TimeRequest(relativedelta.relativedelta):
    """
    Simple object that represents requested duration of a batch job.
    """
    def __init__(self, hours=0, minutes=0, seconds=0):
        relativedelta.relativedelta.__init__(self,
                                             hours=hours,
                                             minutes=minutes,
                                             seconds=seconds)

    @classmethod
    def from_string(cls, hhmmss):
        h, m, s = hhmmss.split(':')
        return cls(int(h), int(m), int(s))

    def __str__(self):
        return self.get_string()

    def get_string(self):
        day_hours = self.days*24
        return '{0:02d}:{1:02d}:{2:02d}'.format(
            self.hours + day_hours, self.minutes, self.seconds)

    def get_hour_string(self):
        day_hours = self.days*24
        hours = '{0:02d}'.format(self.hours + day_hours)
        return hours

    def get_minute_string(self):
        minutes = '{0:02d}'.format(self.minutes)
        return minutes

    def get_second_string(self):
        seconds = '{0:02d}'.format(self.seconds)
        return seconds

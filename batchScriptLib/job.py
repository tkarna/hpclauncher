"""
Representation of a job than can be submitted to target machine queue manager.

Job contains a clusterSetup object and a list of tasks.

Tuomas Karna 2015-09-02
"""
from __future__ import absolute_import
from .clusterparameters import clusterparams
from . import task

import os
from string import Template


def create_directory(path):
    """
    Creates given directory if it does not exist already.

    Raises an exception if a file with the same name exists.
    """
    if os.path.exists(path):
        if not os.path.isdir(path):
            raise Exception('file with same name exists', path)
    else:
        os.makedirs(path)
    return path


class BatchJob(object):
    """
    An object that represents a batch job, that can contain multiple tasks.
    """
    def __init__(self, timereq=None, **kwargs):
        """
        Arguments
        ---------
        clusterParams : clusterSetup object
                settings for the HPC cluster
        kwargs  : keyword arguments
                rest of arguments reguired to fill submission script header
        """
        self.necessary_parameters = [
            'jobname',
            'queue',
            'nproc',
        ]
        # merge kwargs, ensures propagation of common params
        kw = {}
        kw.update(clusterparams.get_args())
        kw.update(kwargs)
        for k in self.necessary_parameters:
            if kw.get(k) is None:
                raise Exception('missing job parameter: ' + k)
        self.kwargs = kw
        if timereq:
            self.kwargs['hours'] = timereq.get_hour_string()
            self.kwargs['minutes'] = timereq.get_minute_string()
            self.kwargs['seconds'] = timereq.get_second_string()
        self.tasks = []

    def __getitem__(self, key):
        return self.kwargs.get(key)

    def append_task(self, task, threaded=False):
        """
        Appends given task in the current batch job.
        If threaded=True, the bash command will be lauched with a new thread.
        """
        if threaded:
            task = task.copy()
            task.threaded = True
        self.tasks.append(task)

    def append_new_task(self, *args, **kwargs):
        """
        Creates a new task using args and kwargs and append to this job
        """
        t = task.BatchTask(*args, **kwargs)
        self.append_task(t)

    def generate_script(self):
        """
        Generates content of the batch script.
        """
        header = clusterparams.generate_script_header(**self.kwargs)
        all_args = {}
        all_args.update(clusterparams.get_args())
        all_args.update(self.kwargs)
        logdir = all_args.get('logfiledir')
        all_args.pop('logfile')
        # prepend logfile with logfiledir
        if logdir is not None:
            # ensure logfiledir exists
            create_directory(logdir)
        footer = ''
        for t in self.tasks:
            # all possible kwargs
            d = dict(all_args)
            d.update(t.kwargs)
            # use 'nproc' by default if 'nthread' is not defined
            if 'nproc' in d:
                d.setdefault('nthread', d['nproc'])
            if logdir is not None:
                # update task logfile
                if d['logfile'] is not None and logdir+'/' not in d['logfile']:
                    d['logfile'] = os.path.join(logdir, d['logfile'])

            # substitute to command, twice to allow tags in tags
            cmd = t.get_command() + '\n'
            cmd = cmd.format(**d)
            cmd = cmd.format(**d)
            footer += cmd
        content = header + footer
        content += 'wait\n'
        return content


# def _gen_job_script_header(jobname, logfile, nproc, timereq, queue, pattern,
#                            email, accountnb=None,
#                            parentjob=None, parentmode='ok'):
#     """
#     Generates the header for SLURM job submission script.
#     The actual job command is not included.
#     """
#     day_hours = timereq.days*24
#     hours = '{0:02d}'.format(timereq.hours+day_hours)
#     minutes = '{0:02d}'.format(timereq.minutes)
#     seconds = '{0:02d}'.format(timereq.seconds)
#     s = Template(pattern)
#     content = s.substitute(jobname=jobname, logfile=logfile,
#                            queue=queue, nproc=nproc,
#                            minutes=minutes, hours=hours, seconds=seconds,
#                            email=email, accountnb=accountnb)
#     if accountnb is not None:
#         content += accountnbEntry.format(accountnb=accountnb)
#     if parentjob is not None:
#         content += dependencyEntry[parentmode].format(parentjob=parentjob)
#     return content

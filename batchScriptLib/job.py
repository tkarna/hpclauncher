"""
Representation of a job than can be submitted to target machine queue manager.

Job contains a clusterSetup object and a list of tasks.

Tuomas Karna 2015-09-02
"""
from clusterParameters import clusterParams
import task
import os


def createDirectory(path):
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


class batchJob(object):
    """
    An object that represents a batch job, that can contain multiple tasks.
    """
    def __init__(self, timeReq=None, **kwargs):
        """
        Arguments
        ---------
        clusterParams : clusterSetup object
                settings for the HPC cluster
        kwargs  : keyword arguments
                rest of arguments reguired to fill submission script header
        """
        self.necessaryParameters = ['jobName',
                                    'queue',
                                    'nproc',
                                    ]
        # merge kwargs, ensures propagation of common params
        kw = {}
        kw.update(clusterParams.getArgs())
        kw.update(kwargs)
        for k in self.necessaryParameters:
            if kw.get(k) is None:
                raise Exception('missing job parameter: ' + k)
        self.kwargs = kw
        if timeReq:
            self.kwargs['hours'] = timeReq.getHourString()
            self.kwargs['minutes'] = timeReq.getMinuteString()
            self.kwargs['seconds'] = timeReq.getSecondString()
        self.tasks = []

    def __getitem__(self, key):
        return self.kwargs.get(key)

    def appendTask(self, task, threaded=False):
        """
        Appends given task in the current batch job.
        If threaded=True, the bash command will be lauched with a new thread.
        """
        if threaded:
            task = task.copy()
            task.threaded = True
        self.tasks.append(task)

    def appendNewTask(self, *args, **kwargs):
        """
        Creates a new task using args and kwargs and append to this job
        """
        t = task.batchTask(*args, **kwargs)
        self.appendTask(t)

    def generateScript(self):
        """
        Generates content of the batch script.
        """
        header = clusterParams.generateScriptHeader(**self.kwargs)
        allArgs = {}
        allArgs.update(clusterParams.getArgs())
        allArgs.update(self.kwargs)
        logdir = allArgs.get('logFileDir')
        # prepend logFile with logFileDir
        if logdir is not None:
            # ensure logFileDir exists
            createDirectory(logdir)
            if allArgs.get('logFile') is not None:
                # fix update job logfile
                allArgs['logFile'] = os.path.join(logdir, allArgs['logFile'])
        footer = ''
        for t in self.tasks:
            # all possible kwargs
            d = dict(allArgs)
            d.update(t.kwargs)
            # use 'nproc' by default if 'nthread' is not defined
            if 'nproc' in d:
                d.setdefault('nthread', d['nproc'])
            if logdir is not None:
                # update task logfile
                if t.logFile is not None and logdir not in t.logFile:
                    t.logFile = os.path.join(logdir, t.logFile)
            # substitute to command, twice to allow tags in tags
            cmd = t.getCommand() + '\n'
            cmd = cmd.format(**d)
            cmd = cmd.format(**d)
            footer += cmd
        content = header + footer
        content += 'wait\n'
        return content


def _genJobScriptHeader(jobName, logFile, nproc, timeReq, queue, pattern,
                        email, accountNb=None,
                        parentJob=None, parentMode='ok'):
    """
    Generates the header for SLURM job submission script.
    The actual job command is not included.
    """
    day_hours = timeReq.days*24
    hours = '{0:02d}'.format(timeReq.hours+day_hours)
    minutes = '{0:02d}'.format(timeReq.minutes)
    seconds = '{0:02d}'.format(timeReq.seconds)
    s = Template(pattern)
    content = s.substitute(jobName=jobName, logFile=logFile,
                           queue=queue, nproc=nproc,
                           minutes=minutes, hours=hours, seconds=seconds,
                           email=email, accountNb=accountNb)
    if accountNb is not None:
        content += accountNbEntry.format(accountNb=accountNb)
    if parentJob is not None:
        content += dependencyEntry[parentMode].format(parentJob=parentJob)
    return content

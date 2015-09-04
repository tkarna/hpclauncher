"""
Representation of a job than can be submitted to target machine queue manager.

Job contains a clusterSetup object and a list of tasks.

Tuomas Karna 2015-09-02
"""
import task
import os


class batchJob(object):
    """
    An object that represents a batch job, that can contain multiple tasks.
    """
    def __init__(self, clusterParams, timeReq=None, **kwargs):
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
        kw.update(clusterParams.kwargs)
        kw.update(kwargs)
        for k in self.necessaryParameters:
            if kw.get(k) is None:
                raise Exception('missing job parameter: ' + k)
        self.kwargs = kw
        if timeReq:
            self.kwargs['hours'] = timeReq.getHourString()
            self.kwargs['minutes'] = timeReq.getMinuteString()
            self.kwargs['seconds'] = timeReq.getSecondString()
        self.clusterParams = clusterParams
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
        header = self.clusterParams.generateScriptHeader(**self.kwargs)
        allArgs = {}
        allArgs.update(self.clusterParams.kwargs)
        allArgs.update(self.kwargs)
        footer = ''
        for c in self.tasks:
            # all possible kwargs
            d = dict(allArgs)
            d.update(c.kwargs)
            # use 'nproc' by default if 'nthread' is not defined
            if 'nproc' in d:
                d.setdefault('nthread', d['nproc'])
            # prepend logFile with logFileDir
            if d.get('logFile') is not None and d.get('logFileDir') is not None:
                d['logFile'] = os.path.join(d['logFileDir'], d['logFile'])
            # substitute to command, twice to allow tags in tags
            s = c.getCommand() + '\n'
            s = s.format(**d)
            s = s.format(**d)
            footer += s
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

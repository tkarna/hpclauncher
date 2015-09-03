"""
Representation of a job than can be submitted to job managers.
Job contains multiple tasks.

Tuomas Karna 2015-09-02
"""
import task

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
        for k in self.necessaryParameters:
            if k not in kwargs or kwargs.get(k) is None:
                raise Exception('missing job parameter: ' + k)
        self.kwargs = kwargs
        if timeReq:
            self.kwargs['hours'] = timeReq.getHourString()
            self.kwargs['minutes'] = timeReq.getMinuteString()
            self.kwargs['seconds'] = timeReq.getSecondString()
        self.clusterParams = clusterParams
        self.commands = []

    def appendTask(self, task, threaded=False):
        """
        Appends given task in the current batch job.
        If threaded=True, the bash command will be lauched with a new thread.
        """
        if threaded:
            task = task.copy()
            task.threaded = True
        self.commands.append(task)

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
        for c in self.commands:
            # all possible kwargs
            d = dict(allArgs)
            d.update(c.kwargs)
            # substitute to command, twice to allow tags in tags
            s = c.getCommand() + '\n'
            s = s.format(**d)
            s = s.format(**d)
            footer += s
        content = header + '\n' + footer
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

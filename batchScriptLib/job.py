"""
Representation of a job than can be submitted to job managers.
Job contains multiple tasks.

Tuomas Karna 2015-09-02
"""
from string import Template


class batchJob(object):
    """
    An object that represents a batch job, that can contain multiple tasks.
    """
    def __init__(self, jobName, queue, nproc, timeReq, logFile,
                 runDirectory='', command='', parentJob=None, parentMode='ok'):
        """
        Arguments
        ---------
        jobName : string
                descriptive name of the job, e.g. run003 or extractTrans_run004
        queue   : string
                name of the queue where job will be submitted to
        nproc   : int
                number of processes to request
        timeReq : timeRequest
                Requested wall-clock time
        logFile : string
                File where job stdout will be redirected to
        runDirectory : string
                Directory where the job should be executed.
                Default: currect directory.
        command : string
                bash command to run
        parentJob : integer
                ID of the job that this task depends on. This task will only
                begin after the parent has finished.
        parentMode : string
                Indicates the condition for the dependency:
                'ok' : this task will start only if parent ends succesfully
                'any' : this task will always start after the parent finishes
        """
        self.jobName = jobName
        self.command = command
        self.logFile = logFile
        self.nproc = nproc
        self.timeReq = timeReq
        self.queue = queue
        self.runDirectory = runDirectory
        self.parentJob = parentJob
        self.parentMode = parentMode

    def generateScript(self):
        """
        Generates content of the batch script.
        """
        content = _genJobScriptHeader(self.jobName, self.logFile, self.nproc,
                                      self.timeReq, self.queue,
                                      emailAddress, accountNb,
                                      self.parentJob, self.parentMode)
        content += self.command
        content += 'wait\n'
        return content

    def appendTask(self, task, threaded=False):
        """
        Appends given task in the current batch job.
        If threaded=True, the bash command will be lauched with a new thread.
        """
        cmd = str(task.getCommand())
        if task.logFile:
            cmd = cmd.replace('\n', ' &>> '+task.logFile+'\n')
        if threaded:
            # TODO find a better way of doing this
            cmd = cmd.replace('\n', ' &\n')
        self.command += cmd


def _genJobScriptHeader(jobName, logFile, nproc, timeReq, queue,
                       email, accountNb=None, parentJob=None, parentMode='ok'):
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

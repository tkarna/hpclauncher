"""
Representation of a single command.

Tuomas Karna 2015-09-02
"""


class batchTask(object):
    """
    A single task, representable as a bash command.
    Tasks can be added to batchJob objects.
    """
    def __init__(self, command, jobName=None, logFile=None):
        self.cmd = command
        self.jobName = jobName
        self.logFile = logFile

    def getCommand(self):
        """Returns the batch command of this task."""
        return self.cmd

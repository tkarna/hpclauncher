"""
Representation of a single command that can be added to a batchJob.

Tuomas Karna 2015-09-02
"""
import copy


class batchTask(object):
    """
    A single task, representable as a bash command.
    Tasks can be added to batchJob objects.
    """
    def __init__(self, command, threaded=False, logFile=None,
                 redirectMode='append', **kwargs):
        # rm trailing whitespace
        if command is None:
            raise Exception('missing task parameter: command')
        self.cmd = command.strip()
        self.logFile = logFile
        self.threaded = bool(threaded)
        self.redirectMode = redirectMode
        self.kwargs = kwargs
        self.kwargs['logFile'] = self.logFile

    def __getitem__(self, key):
        return self.kwargs.get(key)

    def copy(self):
        """Get a deep copy of this task"""
        return copy.deepcopy(self)

    def getCommand(self):
        """
        Returns the command of this task.

        Appends redirection to log file and/or ampersand for threading
        if needed.
        """
        full_cmd = self.cmd
        if self.redirectMode == 'append':
            redirOp = '&>>'
        elif self.redirectMode == 'replace':
            redirOp = '&>'
        if self.logFile:
            full_cmd += ' ' + redirOp + ' ' + '{logFile}'
        if self.threaded:
            full_cmd += ' &'
        return full_cmd

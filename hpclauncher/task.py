"""
Representation of a single command that can be added to a batchJob.

Tuomas Karna 2015-09-02
"""
import copy


class BatchTask(object):
    """
    A single task, representable as a bash command.
    Tasks can be added to batchJob objects.
    """
    def __init__(self, command, threaded=False, logfile=None,
                 redirmode='append', **kwargs):
        # rm trailing whitespace
        if command is None:
            raise Exception('missing task parameter: command')
        self.cmd = command.strip()
        self.logfile = logfile
        self.threaded = bool(threaded)
        self.redirmode = redirmode
        self.kwargs = kwargs
        self.kwargs['logfile'] = self.logfile

    def __getitem__(self, key):
        return self.kwargs.get(key)

    def copy(self):
        """Get a deep copy of this task"""
        return copy.deepcopy(self)

    def get_command(self):
        """
        Returns the command of this task.

        Appends redirection to log file and/or ampersand for threading
        if needed.
        """
        full_cmd = self.cmd
        if self.redirmode == 'append':
            redir_op = '&>>'
        elif self.redirmode == 'replace':
            redir_op = '&>'
        if self.logfile:
            full_cmd += ' ' + redir_op + ' ' + '{logfile}'
        if self.threaded:
            full_cmd += ' &'
        return full_cmd

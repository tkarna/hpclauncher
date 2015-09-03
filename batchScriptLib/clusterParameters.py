"""
HPC cluster setup.

Tuomas Karna 2015-09-02
"""
import os


class clusterSetup(object):
    """
    Object that represents a HPC cluster setup with fields like
    job manager, user email address etc.
    """
    def __init__(self, **kwargs):
        """
        Creates a generic cluster parameter object
        """
        # check for necessary parameters
        self.necessaryParameters = ['submitExec',
                                    'mpiExec',
                                    'scriptPattern',]
        for k in self.necessaryParameters:
            if k not in kwargs or kwargs.get(k) is None:
                raise Exception('missing cluster parameter: ' + k)
        self.scriptPattern = kwargs.pop('scriptPattern')
        self.kwargs = kwargs

    def generateScriptHeader(self, **kwargs):
        """
        Returns submission script filled with metadata
        """
        # parse all keywords from the pattern
        keywords = self.scriptPattern.split('{')[1:]
        keywords = [ p.split('}')[0] for p in keywords]
        # make dict with all keywords set to placeholder
        missingTag = '--missing--'
        metadata = dict(zip(keywords, [missingTag]*len(keywords)))
        # update from self
        metadata.update(self.kwargs)
        # update with user input
        metadata.update(kwargs)
        # prepend logFile with logFileDir
        if 'logFile' in metadata and 'logFileDir' in metadata:
            metadata['logFile'] = os.path.join(metadata['logFileDir'],
                                               metadata['logFile'])

        content = self.scriptPattern.format(**metadata)
        # do again in case some tags are made out of other tags
        content = content.format(**metadata)
        # silently remove lines that contain missing parameters
        # NOTE necessary parameters must be checked elsewhere
        lines = content.split('\n')
        new_content = ''
        for l in lines:
            if len(l) > 0 and missingTag not in l:
                new_content += l + '\n'
        return new_content


class slurmSetup(clusterSetup):
    """
    Setup for slurm resourceManager
    """
    def __init__(self, mpiExec, scriptPattern=None, userEmail=None,
                 userAccountNb=None, logFileDir='log', npPrefix=None):
        """
        Creates cluster config for slurm systems
        """
        # NOTE submission pattern should contain all possible entries
        defaultPattern = """#!/bin/bash
#SBATCH -J {jobName}
#SBATCH -o {logFile}.o%j
#SBATCH -N {nnode}
#SBATCH -n {nproc}
#SBATCH -p {queue}
#SBATCH -t {hours}:{minutes}:{seconds}
#SBATCH --mail-user={userEmail}
#SBATCH --mail-type=begin
#SBATCH --mail-type=end
#SBATCH -A {userAccountNb}
#SBATCH --dependency=afterany:{parentJobAny}
#SBATCH --dependency=afterok:{parentJobOk}
"""
        if scriptPattern is None:
            scriptPattern = defaultPattern
        resoMan = 'slurm'
        submitExec = 'sbatch'
        super(slurmSetup, self).__init__(resourceManager=resoMan,
                                         submitExec=submitExec,
                                         mpiExec=mpiExec,
                                         scriptPattern=defaultPattern,
                                         userEmail=userEmail,
                                         userAccountNb=userAccountNb,
                                         npPrefix=npPrefix,
                                         logFileDir=logFileDir)

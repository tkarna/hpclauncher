"""
HPC cluster setup.

Tuomas Karna 2015-09-02
"""
import os
import yamlInterface

# constants for fiding cluster param file
CLUSTERPARAM_ENV_VAR = 'BATCHSCRIPTLIB_CLUSTER'
CLUSTERPARAM_USERFILE = '~/.batchScriptLib/local_cluster.yaml'


class clusterSetup(object):
    """
    Object that represents a HPC cluster setup with fields like
    job manager, user email address etc.
    """
    def __init__(self):
        """
        Creates an empty defunct object.
        """
        self._initialized = False

    def initializeWithArgs(self, **kwargs):
        """
        Set parameters. Will raise exeption if necessary parameters are missing.
        """
        self.necessaryParameters = ['submitExec',
                                    'mpiExec',
                                    'scriptPattern',
                                    'resourceManager',
                                    ]
        for k in self.necessaryParameters:
            if k not in kwargs or kwargs.get(k) is None:
                raise Exception('missing cluster parameter: ' + k)
        self.scriptPattern = kwargs.pop('scriptPattern')
        self.kwargs = kwargs
        self._initialized = True

    def initializeFromFile(self, yamlFile):
        """
        Parses a yaml file and returns a clusterSetup object
        """
        kwargs = yamlInterface.readYamlFile(yamlFile)
        self.initializeWithArgs(**kwargs)

    def _checkInitialized(self):
        if not self._initialized:
            msg = """
clusterParams object is not initialized.

You can set global cluster parameters by defining environment variable
{env_var}=path/to/your/file.yaml

Or by adding the following file
{user_file}

In python scripts cluster parameters can be read from yaml file with
clusterParams.initializeFromFile('path/to/your/file.yaml')

or by setting the parameters directly
clusterParams.initializeWithArgs(submitExec='sbatch', ...)

"""
            print msg.format(env_var=CLUSTERPARAM_ENV_VAR,
                             user_file=CLUSTERPARAM_USERFILE)
            raise Exception('clusterParams must be initialized')

    def __getitem__(self, key):
        self._checkInitialized()
        return self.kwargs.get(key)

    def keys(self):
        self._checkInitialized()
        return self.kwargs.keys()

    def getArgs(self):
        self._checkInitialized()
        return self.kwargs

    def generateScriptHeader(self, **kwargs):
        """
        Returns submission script filled with metadata
        """
        self._checkInitialized()
        # parse all keywords from the pattern
        keywords = self.scriptPattern.split('{')[1:]
        keywords = [p.split('}')[0] for p in keywords]
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


# create global cluster setup object
clusterParams = clusterSetup()

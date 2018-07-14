"""
HPC cluster setup.

Tuomas Karna 2015-09-02
"""
from __future__ import absolute_import
import os
from . import yaml_interface

# constants for fiding cluster param file
CLUSTERPARAM_ENV_VAR = 'HPCLAUNCHER_CLUSTER'
CLUSTERPARAM_USERFILE = '~/.hpclauncher/local_cluster.yaml'


class ClusterSetup(object):
    """
    Object that represents a HPC cluster setup with fields like
    job manager, user email address etc.
    """
    def __init__(self):
        """
        Creates an empty defunct object.
        """
        self._initialized = False

    def initialize_with_args(self, **kwargs):
        """
        Set parameters. Will raise exeption if necessary parameters are missing.
        """
        self.necessaryParameters = ['submitexec',
                                    'mpiexec',
                                    'scriptpattern',
                                    'resourcemanager',
                                    ]
        for k in self.necessaryParameters:
            if k not in kwargs or kwargs.get(k) is None:
                raise Exception('missing cluster parameter: ' + k)
        self.scriptpattern = kwargs.pop('scriptpattern')
        self.kwargs = kwargs
        self._initialized = True

    def initialize_from(self, other):
        """
        Set parameters from another instance.
        """
        self.initialize_with_args(scriptpattern=other.scriptpattern, **other.kwargs)

    def initialize_from_file(self, yamlfile):
        """
        Parses a yaml file and returns a clusterSetup object
        """
        kwargs = yaml_interface.read_yaml_file(yamlfile)
        self.initialize_with_args(**kwargs)

    def _check_initialized(self):
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
clusterParams.initializeWithArgs(submitexec='sbatch', ...)

"""
            print(msg.format(env_var=CLUSTERPARAM_ENV_VAR,
                             user_file=CLUSTERPARAM_USERFILE))
            raise Exception('clusterparams must be initialized')

    def __getitem__(self, key):
        self._check_initialized()
        return self.kwargs.get(key)

    def keys(self):
        self._check_initialized()
        return self.kwargs.keys()

    def get_args(self):
        self._check_initialized()
        return self.kwargs

    def generate_script_header(self, **kwargs):
        """
        Returns submission script filled with metadata
        """
        self._check_initialized()
        # parse all keywords from the pattern
        keywords = self.scriptpattern.split('{')[1:]
        keywords = [p.split('}')[0] for p in keywords]
        # make dict with all keywords set to placeholder
        missing_tag = '--missing--'
        metadata = dict(zip(keywords, [missing_tag]*len(keywords)))
        # update from self
        metadata.update(self.kwargs)
        # update with user input
        metadata.update(kwargs)
        # prepend logfile with logfiledir
        if 'logfile' in metadata and 'logfiledir' in metadata:
            metadata['logfile'] = os.path.join(metadata['logfiledir'],
                                               metadata['logfile'])

        content = self.scriptpattern.format(**metadata)
        # do again in case some tags are made out of other tags
        content = content.format(**metadata)
        # silently remove lines that contain missing parameters
        # NOTE necessary parameters must be checked elsewhere
        lines = content.split('\n')
        new_content = ''
        for l in lines:
            if len(l) > 0 and missing_tag not in l:
                new_content += l + '\n'
        return new_content


class SlurmSetup(ClusterSetup):
    """
    Setup for slurm resourcemanager
    """
    def __init__(self, mpiexec, scriptpattern=None, useremail=None,
                 useraccountnb=None, logfiledir='log', procprefix=None):
        """
        Creates cluster config for slurm systems
        """
        # NOTE submission pattern should contain all possible entries
        default_pattern = """#!/bin/bash
#SBATCH -J {jobname}
#SBATCH -o {logfile}.o%j
#SBATCH -N {nnode}
#SBATCH -n {nproc}
#SBATCH -p {queue}
#SBATCH -t {hours}:{minutes}:{seconds}
#SBATCH --mail-user={useremail}
#SBATCH --mail-type=begin
#SBATCH --mail-type=end
#SBATCH -A {useraccountnb}
#SBATCH --dependency=afterany:{parentjobany}
#SBATCH --dependency=afterok:{parentjobok}
"""
        if scriptpattern is None:
            scriptpattern = default_pattern
        resoman = 'slurm'
        submitexec = 'sbatch'
        super(SlurmSetup, self).initialize_with_args(resourcemanager=resoman,
                                                     submitexec=submitexec,
                                                     mpiexec=mpiexec,
                                                     scriptpattern=default_pattern,
                                                     useremail=useremail,
                                                     useraccountnb=useraccountnb,
                                                     nprocprefix=procprefix,
                                                     logfiledir=logfiledir)


# create global cluster setup object
clusterparams = ClusterSetup()

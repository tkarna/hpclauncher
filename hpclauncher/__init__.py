
import os
from .hpclauncher import *
from .clusterparameters import CLUSTERPARAM_ENV_VAR, CLUSTERPARAM_USERFILE  # NOQA
from .task import BatchTask  # NOQA
from .job import BatchJob  # NOQA
from os.path import expanduser  # NOQA

# initialize cluster parameters
envfile = os.environ.get(CLUSTERPARAM_ENV_VAR)
locfile = expanduser(CLUSTERPARAM_USERFILE)
if envfile is not None:
    # from env variable
    clusterparams.initialize_from_file(envfile)
elif os.path.isfile(locfile):
    # from default user file
    clusterparams.initialize_from_file(locfile)
# leave uninitialized (and defunct)

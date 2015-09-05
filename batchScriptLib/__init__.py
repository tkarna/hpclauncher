
import os
from batchScriptLib import *
from task import batchTask
from job import batchJob
from os.path import expanduser

# initialize cluster parameters
envfile = os.environ.get(CLUSTERPARAM_ENV_VAR)
locfile = expanduser(CLUSTERPARAM_USERFILE)
if envfile is not None:
    # from env variable
    clusterParams.initializeFromFile(envfile)
elif os.path.isfile(locfile):
    # from default user file
    clusterParams.initializeFromFile(locfile)
# leave uninitialized (and defunct)


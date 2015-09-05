# batchScriptLib

Generic interface for submitting jobs to super computer queue managers.

## Installation and short introduction

Login to cluster and install batchScriptLib as user

    python setup.py install --user

Add `~/.local/bin` to your `PATH`

    export PATH=~/.local/bin:$PATH

Define a yaml cluster configure file and copy it under

    ~/.batchScriptLib/local_cluster.yaml

For example cluster parameter files see [examples/cluster_config](https://bitbucket.org/tkarna/batchscriplib/src/HEAD/examples/cluster_config/?at=master).
Cluster configure file can also be overriden with `BATCHSCRIPTLIB_CLUSTER` environment variable.

You can now submit jobs from yaml files with

    submitYAMLJob.py myjob.yaml

For example job files see [examples/job_config](https://bitbucket.org/tkarna/batchscriplib/src/HEAD/examples/job_config/?at=master).

For testing purposes, adding `-t` will only print the submission script without submitting it.

    submitYAMLJob.py myjob.yaml -t

Jobs can also be created and launched from python:

    from batchScriptLib import *
    # create job. Jobs can be submitted to queue managers
    j = batchJob(jobName='somename', queue='normal',
                 nproc=12, timeReq=timeRequest(10, 30, 0), logFile='log_somelog')
    # job can contain multiple tasks (commands)
    j.appendNewTask('echo {message}', message='hello')
    submitJobs(j, testOnly=True, verbose=False)

For python examples see [examples/python](https://bitbucket.org/tkarna/batchscriplib/src/HEAD/examples/python/?at=master).

## List of common keywords

### Cluster parameters

- __scriptPattern__: job script header filled with keyword placeholders '{nprocs}'
- __submitExec__: executable used to submit jobs
- __mpiExec__: executable for running parallel jobs, e.g. 'ibrun' or 'mpiexec -n {nproc}'
- userEmail: email address where notifications will be sent
- userAccountNb: user allocation number (if needed)
- resourceManager: string identifying the manager: 'slurm'|'sge'|'pge'

Parameters marked in __bold__ are required to initialize `clusterSetup` object.

### Job parameters

- __jobName__: job name
- __queue__: job queue where job will be submitted
- __nproc__: number of processes to allocate (in header)
- logFileDir: directory where all log files will be stored
- nnode: number of nodes to allocate (if needed)
- nthread: number of threads to launch (for each command)

Parameters marked in __bold__ are required to initialize `job` object.

For example, to allocate 3 nodes and total 24 processes, and running a task with 12 threads could be set with

    mpiExec: mpiexec -n {nthread}
    nnode: 3
    nproc: 24
    nthread: 12

Assuming we wish to run task `{mpiExec} myprogram -a`, on a slurm system this would translate the following entries in the job submission script:

    #SBATCH -N 3
    #SBATCH -n 24
    mpiexec -n 12 myprogram -a

All keywords are read hierarchically from the `clusterSetup`, `job` and `task` objects.

## Roadmap

- update selfe example(s)
- clean up old examples


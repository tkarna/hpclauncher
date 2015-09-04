# batchScriptLib

Generic interface for submitting jobs to super computer queue managers.

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

For example, to allocate 3 nodes for total 24 processes, and running a threaded task with 12 threads could be set with

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

- implement and test job dependencies
- test runDir
- add scripts dir, add executables, install under bin/
- add python examples with loops



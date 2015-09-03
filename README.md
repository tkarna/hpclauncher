batchScriptLib
--------------

Generic interface for submitting jobs to super computer queue managers.

## List of common keywords

### Cluster parameters

- userEmail: email address where notifications will be sent
- userAccountNb: user allocation number (if needed)
- submitExec: executable used to submit jobs
- mpiExec: executable for running parallel jobs, e.g. 'ibrun' or 'mpiexec -n {nproc}'
- resourceManager: string identifying the manager: 'slurm'|'sge'|'pge'
- scriptPattern: job script header filled with keyword placeholders '{nprocs}'

### Job parameters
- logFileDir: directory where all log files will be stored
- jobName: job name
- queue: job queue where job will be submitted
- nproc: number of processes to allocate (in header)
- nnode: number of nodes to allocate (if needed)
- nthread: number of threads to launch (for each command)

For example to allocate 3 nodes for total 24 processes, and running a threaded task with 12 threads would be

mpiExec: mpiexec -n {nthread}
nnode: 3
nproc: 24
nthread: 12

Assuming we wish to run task "{mpiExec} myprogram -a", on a slurm system this would translate to a job script:

    #!/bin/bash
    #SBATCH -N {nnode}
    #SBATCH -n {nproc}
    mpiexec -n {nthread} myprogram -a

## Roadmap

- implement and test job dependencies
- implement reading job yaml files
    - globalArgs dict from high level
    - jobsDict for all the rest (starting with job_*)
    - implement launcher(globalArgs, jobsdict)
      that handles dependencies
- add scripts dir, add executables, install under bin/
- add python examples with loops



batchScriptLib
--------------

Generic interface for submitting jobs to super computer queue managers.

Roadmap
-------

- test running scripts with slurm
- implement and test job dependencies
- implement reading job yaml files
    - globalArgs dict from high level
    - jobsDict for all the rest (starting with job_*)
    - implement launcher(globalArgs, jobsdict)
      that handles dependencies
- add examples for other clusters, test
- add scripts dir, add executables, install under bin/
- add python examples with loops



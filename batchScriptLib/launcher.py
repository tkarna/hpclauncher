"""
Routines for launching jobs on target system.

Tuomas Karna 2015-09-03
"""
from __future__ import absolute_import
import os
import subprocess
from .clusterparameters import clusterparams


def launch_job(job, testonly=False, verbose=False):
    """
    Lauches given job and returns the jobID number.
    """
    name = job['jobname']
    content = job.generateScript()
    submitexec = clusterparams['submitexec']
    managertype = clusterparams['resourcemanager']
    rundir = job['rundir']
    logfile = job['logfile']
    if rundir is not None and not os.path.isdir(rundir):
        raise IOError('rundir does not exist: ' + rundir)
    return _launch_job(name, content, submitexec, managertype, rundir,
                       logfile, testonly, verbose)


def _launch_job(name, content, submitexec, managertype, rundir=None,
                logfile=None,
                testonly=False, verbose=False):
    """
    Writes given batch script content to a temp file and launches the run.
    Returns jobID of the started job.
    If directory given, starts job in that directory.
    """
    if testonly:
        # print to stdout and return
        print content
        return 0
    elif verbose:
        print content
    if rundir:
        # change to rundir, store current dir
        if verbose:
            print 'chdir', rundir
        curdir = os.getcwd()
        os.chdir(rundir)
    # write out temp submission file
    subfile = 'batch_' + name + '.sub'
    _write_script_file(subfile, content, verbose=verbose)
    # submit file
    try:
        call = [submitexec, subfile]
        if verbose:
            print 'excecuting {:}'.format(' '.join(call))
        if managertype == 'bash' and logfile is not None:
            with open(logfile, 'w') as logstream:
                output = subprocess.check_call(call, stdout=logstream,
                                               stderr=subprocess.STDOUT)
        else:
            output = subprocess.check_output(call)
    except Exception as e:
        print e
        raise e
    if rundir:
        # change back to currect directory
        if verbose:
            print 'chdir', curdir
        os.chdir(curdir)
    # print launcher output to stdout
    print output
    jobid = _parse_job_id(output, managertype)
    print 'Parsed Job ID:', jobid
    return jobid


def _write_script_file(subfile, content, verbose=False):
    """
    Stores content to a submission script file.
    """
    if verbose:
        print 'writing to', subfile
    fid = open(subfile, 'w')
    fid.write(content)
    fid.close()


def _parse_job_id(output, managertype):
    """
    Returns jobID of the launched runs by parsing submission exec output.
    Output needs to be grabbed from stdout.
    """
    if managertype == 'slurm':
        return _parse_job_id_slurm(output)
    if managertype == 'sge':
        return _parse_job_id_sge(output)
    # not implemented yet
    return 0


def _parse_job_id_slurm(output):
    lines = output.split('\n')
    target = 'Submitted batch job '
    for line in lines:
        if line.find(target) == 0:
            return int(line.split()[-1])
    raise Exception('Could not parse sbatch output for job id')


def _parse_job_id_sge(output):
    jobid = output.split(' ')[2]
    return jobid

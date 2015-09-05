"""
Routines for launching jobs on target system.

Tuomas Karna 2015-09-03
"""
import os
import subprocess


def launchJob(job, testOnly=False, verbose=False):
    """
    Lauches given job and returns the jobID number.
    """
    name = job['jobName']
    content = job.generateScript()
    submitExec = job.clusterParams['submitExec']
    managerType = job.clusterParams['resourceManager']
    runDir = job['runDir']
    if runDir is not None and not os.path.isdir(runDir):
        raise IOError('runDir does not exist: ' + runDir)
    return _launchJob(name, content, submitExec, managerType, runDir,
                      testOnly, verbose)


def _launchJob(name, content, submitExec, managerType, runDir=None,
               testOnly=False, verbose=False):
    """
    Writes given batch script content to a temp file and launches the run.
    Returns jobID of the started job.
    If directory given, starts job in that directory.
    """
    if testOnly:
        # print to stdout and return
        print content
        return 0
    elif verbose:
        print content
    if runDir:
        # change to runDir, store current dir
        if verbose:
            print 'chdir', runDir
        curDir = os.getcwd()
        os.chdir(runDir)
    # write out temp submission file
    subfile = 'batch_' + name + '.sub'
    _writeScriptFile(subfile, content, verbose=verbose)
    # submit file
    try:
        if verbose:
            print 'excecuting "{:} {:}"'.format(submitExec, subfile)
        output = subprocess.check_output([submitExec, subfile])
    except Exception as e:
        print e
        raise e
    if runDir:
        # change back to currect directory
        if verbose:
            print 'chdir', curDir
        os.chdir(curDir)
    # print launcher output to stdout
    print output
    jobID = _parseJobID(output, managerType)
    print 'Parsed Job ID:', jobID
    return jobID


def _writeScriptFile(subfile, content, verbose=False):
    """
    Stores content to a submission script file.
    """
    if verbose:
        print 'writing to', subfile
    fid = open(subfile, 'w')
    fid.write(content)
    fid.close()


def _parseJobID(output, managerType):
    """
    Returns jobID of the launched runs by parsing submission exec output.
    Output needs to be grabbed from stdout.
    """
    if managerType == 'slurm':
        return _parseJobID_SLURM(output)
    if managerType == 'sge':
        return _parseJobID_SGE(output)
    # not implemented yet
    return 0


def _parseJobID_SLURM(output):
    lines = output.split('\n')
    target = 'Submitted batch job '
    for line in lines:
        if line.find(target) == 0:
            return int(line.split()[-1])
    raise Exception('Could not parse sbatch output for job id')


def _parseJobID_SGE(output):
    jobID = output.split(' ')[2]
    return jobID

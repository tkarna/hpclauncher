"""
A collection of routines for generating and launching processes on clusters.

Job dependencies can be handled by chaining tasks:

runID = launchRun(nproc, timeReq, queue, runTag,
                  parentJob, testOnly, verbose=verbose)
combID = launchThreadedCombine(32, timeRequest(1, 30, 0), queue,
                               runTag, first_stack, last_stack,
                               parentJob=runID, parentMode='any',
                               testOnly=testOnly, verbose=verbose)
exrtID = launchSkillExtract(1, timeRequest(2, 10, 0),
                            'serial', runTag, extrStart, extrEnd,
                            parentJob=combID, parentMode='any',
                            testOnly=testOnly, verbose=verbose)

Tuomas Karna 2014-09-11
"""

from string import Template
import datetime
from dateutil import relativedelta
import subprocess
import os
import sys

# -------------------------------------------------------------------------
# Constants
# -------------------------------------------------------------------------

# user information
emailAddress = 'karnat@stccmop.org'
accountNb = 'TG-OCE130027'

# subdirectory where all log files will be stored
logFileDir = 'log'

# NOTE these depend on the job manager, these are for SLURM

# binary used to submit jobs
launcher = 'sbatch'
# header pattern for all batch scripts
pattern = """#!/bin/bash
#SBATCH -J ${jobName}
#SBATCH -o ${logFile}.o%j
#SBATCH -n ${nproc}
#SBATCH -p ${queue}
#SBATCH -t ${hours}:${minutes}:${seconds}
#SBATCH --mail-user=${email}
#SBATCH --mail-type=begin
#SBATCH --mail-type=end
"""
accountNbEntry = '#SBATCH -A {accountNb}\n'
dependencyEntry = {'any': '#SBATCH --dependency=afterany:{parentJob}\n',
                   'ok': '#SBATCH --dependency=afterok:{parentJob}\n'}

# -------------------------------------------------------------------------
# Generic routines
# -------------------------------------------------------------------------


class timeRequest(relativedelta.relativedelta):
    """
    Simple object that represents requested duration of a batch job.
    """
    def __init__(self, hours, minutes, seconds):
        relativedelta.relativedelta.__init__(self, hours = hours,minutes = minutes,
                               seconds = seconds)

    @classmethod
    def fromString(cls, hhmmss):
        h, m, s = hhmmss.split(':')
        return cls(int(h), int(m), int(s))

    def getString(self):
        day_hours = self.days*24
        return '{0:02d}:{1:02d}:{2:02d}'.format(
            self.hours+day_hours, self.minutes, self.seconds)


class batchJob(object):
    """
    An object that represents a batch job, that can contain multiple tasks.
    """
    def __init__(self, jobName, queue, nproc, timeReq, logFile,
                 runDirectory='', command='', parentJob=None, parentMode='ok'):
        """
        Arguments
        ---------
        jobName : string
                descriptive name of the job, e.g. run003 or extractTrans_run004
        queue   : string
                name of the queue where job will be submitted to
        nproc   : int
                number of processes to request
        timeReq : timeRequest
                Requested wall-clock time
        logFile : string
                File where job stdout will be redirected to
        runDirectory : string
                Directory where the job should be executed.
                Default: currect directory.
        command : string
                bash command to run
        parentJob : integer
                ID of the job that this task depends on. This task will only
                begin after the parent has finished.
        parentMode : string
                Indicates the condition for the dependency:
                'ok' : this task will start only if parent ends succesfully
                'any' : this task will always start after the parent finishes
        """
        self.jobName = jobName
        self.command = command
        self.logFile = logFile
        self.nproc = nproc
        self.timeReq = timeReq
        self.queue = queue
        self.runDirectory = runDirectory
        self.parentJob = parentJob
        self.parentMode = parentMode

    def generateScript(self):
        """
        Generates content of the batch script.
        """
        content = genJobScriptHeader(self.jobName, self.logFile, self.nproc,
                                     self.timeReq, self.queue,
                                     emailAddress, accountNb,
                                     self.parentJob, self.parentMode)
        content += self.command
        content += 'wait\n'
        return content

    def appendTask(self, task, threaded=False):
        """
        Appends given task in the current batch job.
        If threaded=True, the bash command will be lauched with a new thread.
        """
        cmd = str(task.getCommand())
        if task.logFile:
            cmd = cmd.replace('\n', ' &>> '+task.logFile+'\n')
        if threaded:
            # TODO find a better way of doing this
            cmd = cmd.replace('\n', ' &\n')
        self.command += cmd


def genJobScriptHeader(jobName, logFile, nproc, timeReq, queue,
                       email, accountNb=None, parentJob=None, parentMode='ok'):
    """
    Generates the header for SLURM job submission script.
    The actual job command is not included.
    """
    day_hours = timeReq.days*24
    hours = '{0:02d}'.format(timeReq.hours+day_hours)
    minutes = '{0:02d}'.format(timeReq.minutes)
    seconds = '{0:02d}'.format(timeReq.seconds)
    s = Template(pattern)
    content = s.substitute(jobName=jobName, logFile=logFile,
                           queue=queue, nproc=nproc,
                           minutes=minutes, hours=hours, seconds=seconds,
                           email=email, accountNb=accountNb)
    if accountNb is not None:
        content += accountNbEntry.format(accountNb=accountNb)
    if parentJob is not None:
        content += dependencyEntry[parentMode].format(parentJob=parentJob)
    return content


class task(object):
    """
    A single task, representable as a bash command.
    Tasks can be added to batchJob objects.
    """
    def __init__(self, command, jobName=None, logFile=None):
        self.cmd = command
        self.jobName = jobName
        self.logFile = logFile

    def getCommand(self):
        """Returns the batch command of this task."""
        return self.cmd

    def getBatchJob(self, queue, nproc, timeReq, jobName=None, logFile=None,
                    runDirectory='', parentJob=None, parentMode='ok'):
        """
        Returns a batch job that only excecutes this task.
        """
        if jobName is None:
            if self.jobName is None:
                raise Exception('jobName must be given')
            else:
                jobName = self.jobName
        if logFile is None:
            if self.logFile is None:
                raise Exception('logFile must be given')
            else:
                logFile = self.logFile
        dir = str(runDirectory)
        return batchJob(jobName, queue, nproc, timeReq,
                        logFile, command=self.getCommand(), runDirectory=dir,
                        parentJob=parentJob, parentMode=parentMode)


def parseJobID(sbatch_output):
    """
    Returns jobID of the launched runs by parsing sbatch output.
    Output needs to be grabbed from stdout.
    """
    lines = sbatch_output.split('\n')
    target = 'Submitted batch job '
    for line in lines:
        if line.find(target) == 0:
            return int(line.split()[-1])
    raise Exception('Could not parse sbatch output for job id')


def _launchJob(name, content, directory=None, testOnly=False, verbose=False):
    """
    Writes given batch script content to a temp file and launches the run.
    Returns jobID of the started job.
    If directory given, starts job in that directory.
    """
    if testOnly:
        print content
        return 0
    elif verbose:
        print content
    if directory:
        if verbose:
            print 'chdir', directory
        curDir = os.getcwd()
        os.chdir(directory)
    subfile = 'batch_' + name + '.sub'
    if verbose:
        print 'writing to', subfile
    fid = open(subfile, 'w')
    fid.write(content)
    fid.close()
    try:
        if verbose:
            print 'launching', launcher, subfile
        output = subprocess.check_output([launcher, subfile])
    except Exception as e:
        print e.output
        raise e
    if directory:
        if verbose:
            print 'chdir', curDir
        os.chdir(curDir)
    print output
    jobID = parseJobID(output)
    print 'Parsed Job ID:', jobID
    return jobID


def launchJob(job, testOnly=False, verbose=False):
    """
    Lauches given job and returns the jobID number.
    """
    name = job.jobName
    content = job.generateScript()
    directory = job.runDirectory
    return _launchJob(name, content, directory, testOnly, verbose)

# -------------------------------------------------------------------------
# Routines for specific tasks
# -------------------------------------------------------------------------

class taskSELFE(task):
    """Task for running selfe simulations"""
    def __init__(self, runTag):
        self.cmd = 'ibrun tacc_affinity ./pelfe -ksp_type cg -pc_type mg'
        name = runTag
        logFile = '_'.join(['log', name, runTag])
        self.logFile = os.path.join(logFileDir, logFile)
        self.jobName = '_'.join([name, runTag])
        self.runDirectory = runTag


class taskThreadedCombine(task):
    """Combines SELFE model outputs using multiple threads"""
    def __init__(self, runTag, nproc, first_stack, last_stack):
        cmd = 'autocombine.py -o {runTag}/combined -j {np:d} -d {runTag}/outputs {first:d} {last:d}\n'
        self.cmd = cmd.format(runTag=runTag, np=nproc,
                              first=first_stack, last=last_stack)
        name = 'combine'
        stackStr = '{0}-{1}'.format(first_stack, last_stack)
        logFile = '_'.join(['log', name, runTag, stackStr])
        self.logFile = os.path.join(logFileDir, logFile)
        self.jobName = '_'.join([name, runTag])


class taskSkillExtract(task):
    """task for running skillExtract"""
    def __init__(self, runTag, startTime, endTime, noProfiles=True):
        name = 'skillExtr'
        logFile = '_'.join(['log', name, runTag])
        logFile = os.path.join(logFileDir, logFile)
        jobName = '_'.join([name, runTag])
        # generate command string
        startStr = startTime.strftime('%Y-%m-%d')
        endStr = endTime.strftime('%Y-%m-%d')
        noProfStr = ' --no-profiles' if noProfiles else ''
        command = 'time skillExtract -r {runTag} -s {start} -e {end} -t real_stations.csv -d {runTag}/combined/ -C{noProfiles}\n'
        self.cmd = command.format(runTag=runTag, start=startStr, end=endStr,
                                  noProfiles=noProfStr)
        name = 'skillExtr'
        logFile = '_'.join(['log', name, runTag])
        self.logFile = os.path.join(logFileDir, logFile)
        self.jobName = '_'.join([name, runTag])


class taskExtractWP(task):
    """Extracts products related to winched profiler observations"""
    def __init__(self, runTag):
        # generate command string
        tracks = ['obs/data/track/oc2_salt_0_2012-05-03_2012-05-07.nc',
                  'obs/data/track/oc1_salt_0_2012-05-01_2012-05-02.nc',
                  'obs/data/track/oc2_salt_0_2012-10-29_2012-10-31.nc',
                  'obs/data/track/oc1_salt_0_2012-10-26_2012-10-28.nc']
        locs = ['oc2', 'oc1', 'oc2', 'oc1']
        command = 'extractTrack -r {runTag} -d {runTag}/combined/ -C -v salt,temp,hvel -n {loc} -i {track}\n'
        cmdStr = ''
        for loc, track in zip(locs, tracks):
            cmdStr += command.format(runTag=runTag, loc=loc, track=track)
        self.cmd = cmdStr
        name = 'extrWP'
        logFile = '_'.join(['log', name, runTag])
        self.logFile = os.path.join(logFileDir, logFile)
        self.jobName = '_'.join([name, runTag])


class taskExtractAUV(task):
    """Extracts products related to AUV observations"""
    def __init__(self, runTag):
        AUVIDs = [44, 45, 46, 47, 48, 49, 50, 51, 52, 53, 65, 66, 67, 68, 69,
                  70, 71, 72, 73, 74]
        varList = ['salt', 'temp']
        command = 'extractTrack -r {runTag} -d {runTag}/combined/ -C -v {var} -n AUV{AUVID} -i `ls AUV/data/track/AUV{AUVID}_{var}*`\n'
        cmdStr = ''
        for var in varList:
            for AUVID in AUVIDs:
                cmdStr += command.format(runTag=runTag, var=var, AUVID=AUVID)
        command = 'python splitAUVDataToLegs.py -r {runTag} -v {var} -i {AUVID} -d {runTag}/combined -C\n'
        for var in varList:
            for AUVID in AUVIDs:
                cmdStr += command.format(runTag=runTag, var=var, AUVID=AUVID)
        self.cmd = cmdStr
        name = 'extrAUV'
        logFile = '_'.join(['log', name, runTag])
        self.logFile = os.path.join(logFileDir, logFile)
        self.jobName = '_'.join([name, runTag])


class taskExtractSlab(task):
    """Extracts slab from model outputs"""
    def __init__(self, runTag, startTime, endTime, varList, k):
        startStr = startTime.strftime('%Y-%m-%d')
        endStr = endTime.strftime('%Y-%m-%d')
        varStr = ','.join(varList)
        command = 'time extractSlab -r {runTag} -s {start} -e {end} -d {runTag}/combined/ -C -v {var} -n slab -k {k}\n'
        cmdStr = command.format(runTag=runTag, start=startStr, end=endStr,
                                var=varStr, k=k)
        self.cmd = cmdStr
        name = 'extrSlab'
        logFile = '_'.join(['log', name, runTag])
        self.logFile = os.path.join(logFileDir, logFile)
        self.jobName = '_'.join([name, runTag])


class taskExtractSIL(task):
    """Extracts salinity intrusion length from model outputs"""
    def __init__(self, runTag, startTime, endTime):
        startStr = startTime.strftime('%Y-%m-%d')
        endStr = endTime.strftime('%Y-%m-%d')
        command = 'time generateSaltIntrusion -r {runTag} -s {start} -e {end} -d {runTag}/combined/ -t intrusion_length.bp -C\n'
        cmdStr = command.format(runTag=runTag, start=startStr, end=endStr)
        self.cmd = cmdStr
        name = 'extrSIL'
        logFile = '_'.join(['log', name, runTag])
        self.logFile = os.path.join(logFileDir, logFile)
        self.jobName = '_'.join([name, runTag])

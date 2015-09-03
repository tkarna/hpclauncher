from batchScriptLib import *
import unittest
import difflib


class testBase(unittest.TestCase):

    def assertStringEqual(self, first, second):
        try:
            a = first.splitlines()
            b = second.splitlines()
            diff = difflib.unified_diff(a, b,
                                        fromfile='correct',
                                        tofile='generated')
            diff = [p+'\n' for p in diff]
            self.assertTrue(len(diff) == 0)
        except AssertionError as e:
            print 'string mismatch:'
            print ''.join(diff)
            raise e


class test_clusterParamenters(testBase):

    def test_slurm_python(self):
        c = clusterParameters.slurmSetup(mpiExec='mpiexec -n {nproc}',
                                         userEmail='sir.john@yahoo.co.uk',
                                         userAccountNb='TG445')

        treq = timeRequest(12, 30, 10)
        j = batchJob(c, jobName='pyjob', queue='normal', nproc=12,
                     timeReq=treq, logFile='log', parentJobOk='1001')
        j.appendNewTask('{mpiExec} python runSome.py -r {runTag}', logFile='log_{jobName}', runTag='runLola')
        out = j.generateScript()
        correct_output = """#!/bin/bash
#SBATCH -J pyjob
#SBATCH -o log.o%j
#SBATCH -n 12
#SBATCH -p normal
#SBATCH -t 12:30:10
#SBATCH --mail-user=sir.john@yahoo.co.uk
#SBATCH --mail-type=begin
#SBATCH --mail-type=end
#SBATCH -A TG445
#SBATCH --dependency=afterok:1001
mpiexec -n 12 python runSome.py -r runLola &>> log_pyjob
wait"""
        self.assertStringEqual(correct_output, out)

    def test_yaml_slurm(self):
        c = getClusterParametersFromYAML('../examples/cluster_config/mike_stampede.yaml')

        treq = timeRequest(12, 30, 10)
        j = batchJob(c, jobName='yamljob', queue='normal', nproc=12, timeReq=treq, logFile='log')
        j.appendNewTask('{mpiExec} python runSome.py -r {runTag}', logFile='log_{jobName}', runTag='runLola')
        out = j.generateScript()
        correct_output = """#!/bin/bash
#SBATCH -J yamljob
#SBATCH -o log.o%j
#SBATCH -n 12
#SBATCH -p normal
#SBATCH -t 12:30:10
#SBATCH --mail-user=killaMike@stccmop.org
#SBATCH --mail-type=begin
#SBATCH --mail-type=end
#SBATCH -A TG-OCENNNNNN
ibrun tacc_affinity python runSome.py -r runLola &>> log_yamljob
wait"""
        self.assertStringEqual(correct_output, out)

    def test_yaml_sge(self):
        c = getClusterParametersFromYAML('../examples/cluster_config/joe_sirius.yaml')

        treq = timeRequest(12, 30, 10)
        j = batchJob(c, jobName='yamljob', queue='normal', nproc=12, timeReq=treq, logFile='log')
        j.appendNewTask('{mpiExec} python runSome.py -r {runTag}', logFile='log_{jobName}', runTag='runLola')
        out = j.generateScript()
        correct_output = """#!/bin/bash
#$ -cwd
#$ -N yamljob
#$ -o log.out
#$ -e log.err
#$ -q normal
#$ -M glassJoe@stccmop.org
#$ -A j007
#$ -m ea
#$ -j
#$ -V
mpirun -pe orte 12 python runSome.py -r runLola &>> log_yamljob
wait"""
        self.assertStringEqual(correct_output, out)

    def test_yaml_pbs(self):
        # --- yaml pbs ---

        c = getClusterParametersFromYAML('../examples/cluster_config/sara_edison.yaml')

        treq = timeRequest(12, 30, 10)
        j = batchJob(c, jobName='yamljob', queue='normal', nproc=12, timeReq=treq, logFile='log')
        j.appendNewTask('{mpiExec} python runSome.py -r {runTag}', logFile='log_{jobName}', runTag='runLola')
        out = j.generateScript()
        correct_output = """#!/bin/bash
#PBS -q normal
#PBS -l mppwidth=12
#PBS -l walltime=12:30:10
#PBS -N yamljob
#PBS -o log.$PBS_JOBID.out
#PBS -e log.$PBS_JOBID.err
#PBS -M SaraLee@stccmop.org
#PBS -A mNNNN
#PBS -m ea
#PBS -V
aprun -n 12 python runSome.py -r runLola &>> log_yamljob
wait"""
        self.assertStringEqual(correct_output, out)


if __name__ == '__main__':
    """Run all tests"""
    unittest.main()

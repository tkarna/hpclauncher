from batchScriptLib import *
import unittest
import difflib


class TestBase(unittest.TestCase):

    def assert_string_equal(self, first, second):
        try:
            a = first.splitlines()
            b = second.splitlines()
            diff = difflib.unified_diff(a, b,
                                        fromfile='correct',
                                        tofile='generated')
            diff = [p + '\n' for p in diff]
            self.assertTrue(len(diff) == 0)
        except AssertionError as e:
            print 'string mismatch:'
            print ''.join(diff)
            raise e


class TestClusterParameters(TestBase):

    def test_slurm_python(self):
        c = clusterparameters.SlurmSetup(mpiexec='mpiexec -n {nthread}',
                                         useremail='sir.john@yahoo.co.uk',
                                         useraccountnb='TG445')
        clusterparams.initialize_from(c)
        treq = TimeRequest(12, 30, 10)
        j = BatchJob(jobname='pyjob', queue='normal', nproc=12, nthread=8,
                     nnode=2, timereq=treq,
                     logfiledir='somedir', logfile='log', parentjobok='1001')
        j.append_new_task('{mpiexec} python runSome.py -r {runTag}', logfile='log_{jobname}', runTag='run_lola')
        out = j.generate_script()
        correct_output = """#!/bin/bash
#SBATCH -J pyjob
#SBATCH -o somedir/log.o%j
#SBATCH -N 2
#SBATCH -n 12
#SBATCH -p normal
#SBATCH -t 12:30:10
#SBATCH --mail-user=sir.john@yahoo.co.uk
#SBATCH --mail-type=begin
#SBATCH --mail-type=end
#SBATCH -A TG445
#SBATCH --dependency=afterok:1001
mpiexec -n 8 python runSome.py -r run_lola &>> somedir/log_pyjob
wait"""
        self.assert_string_equal(correct_output, out)

    def test_yaml_slurm(self):
        clusterparams.initialize_from_file('../examples/cluster_config/mike_stampede.yaml')
        treq = TimeRequest(12, 30, 10)
        j = BatchJob(jobname='yamljob', queue='normal', nproc=12, timereq=treq, logfile='log')
        j.append_new_task('{mpiexec} python runSome.py -r {runTag}', runTag='run_lola')
        out = j.generate_script()
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
ibrun tacc_affinity python runSome.py -r run_lola
wait"""
        self.assert_string_equal(correct_output, out)

    def test_yaml_sge(self):
        clusterparams.initialize_from_file('../examples/cluster_config/joe_sirius.yaml')
        treq = TimeRequest(12, 30, 10)
        j = BatchJob(jobname='yamljob', queue='normal', nproc=12, nthread=8,
                     timereq=treq, logfile='log')
        j.append_new_task('{mpiexec} python runSome.py -r {runTag}',
                          logfile='log_{jobname}', runTag='run_lola',
                          redirectMode='replace')
        out = j.generate_script()
        correct_output = """#!/bin/bash
#$ -cwd
#$ -j y
#$ -S /bin/bash
#$ -N yamljob
#$ -o /tmp/logs/log.out
#$ -e /tmp/logs/log.err
#$ -q normal
#$ -pe orte 12
#$ -M glassJoe@stccmop.org
#$ -m ea
#$ -A j007
#$ -V
mpirun -n 8 python runSome.py -r run_lola &>> /tmp/logs/log_yamljob
wait"""
        self.assert_string_equal(correct_output, out)

    def test_yaml_pbs(self):
        # --- yaml pbs ---

        clusterparams.initialize_from_file('../examples/cluster_config/sara_edison.yaml')

        treq = TimeRequest(12, 30, 10)
        j = BatchJob(jobname='yamljob', queue='normal', nproc=12, timereq=treq, logfile='log')
        j.append_new_task('{mpiexec} python runSome.py -r {runTag}', logfile='log_{jobname}', runTag='run_lola')
        out = j.generate_script()
        correct_output = """#!/bin/bash
#PBS -q normal
#PBS -l mppwidth=12
#PBS -l walltime=12:30:10
#PBS -N yamljob
#PBS -o log/log.$PBS_JOBID.out
#PBS -e log/log.$PBS_JOBID.err
#PBS -M SaraLee@stccmop.org
#PBS -A mNNNN
#PBS -m ea
#PBS -V
aprun -n 12 python runSome.py -r run_lola &>> log/log_yamljob
wait"""
        self.assert_string_equal(correct_output, out)


if __name__ == '__main__':
    """Run all tests"""
    unittest.main()

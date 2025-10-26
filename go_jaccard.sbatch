#!/bin/bash -l
#SBATCH -J jaccard-benchmark
#SBATCH -N 1
#SBATCH -n 1
#SBATCH -c 16
#SBATCH --time=07:30:00
#SBATCH -A g100-2238
#SBATCH -p topola
#SBATCH --mem-per-cpu=4G
#SBATCH -o output/slurm_jaccard_%j.out    # Slurm stdout
#SBATCH -e output/slurm_jaccard_%j.err    # Slurm stderr

# Ensure the output directory exists
mkdir -p output

# Run inside Apptainer and save results
srun install-dir/bin/apptainer exec --overlay overlay \
  docker://quay.io/biocontainers/adam:1.0.1--hdfd78af_0 \
  /usr/bin/time -v spark-submit --executor-cores $SLURM_CPUS_PER_TASK jaccard.py \
  > output/result_jaccard_${SLURM_CPUS_PER_TASK}.out


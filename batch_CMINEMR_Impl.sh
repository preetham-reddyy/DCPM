#!/bin/bash
#SBATCH -n 16
#SBATCH --gres=gpu:0
#SBATCH --mem-per-cpu=2048
#SBATCH --time=2-00:00:00
#SBATCH --mail-type=END
# minfreq mincs max_overlap_ratio
# echo "$1_output.txt"
#export http_proxy=''
#export https_proxy=''
minfreq=$1
mincs=0
max_overlap_ratio=0.25
export SPARK_WORKER_DIR='./tmp'
export SPARK_LOCAL_DIRS='./tmp'
export LOCAL_DIRS='./tmp'
# echo $1\_$minfreq\_$mincs\_$max_overlap_ratio\_output.txt
python old_final.py $minfreq $mincs $max_overlap_ratio 16 ../CMineMR-Mapreduce-algorithm-to-extract-coverage-patterns-master/dataset/synthetic.txt varying_minRF_synthetic_"$1"_prevalgodummy


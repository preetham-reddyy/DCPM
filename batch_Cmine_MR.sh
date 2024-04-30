#!/bin/bash
#SBATCH -n 8
#SBATCH --gres=gpu:1
#SBATCH --mem-per-cpu=2048
#SBATCH --time=2-00:00:00
#SBATCH --mail-type=END
# minfreq mincs max_overlap_ratio
# echo "$1_output.txt"
minfreq=0.05
mincs=0
max_overlap_ratio=$1
# echo $1\_$minfreq\_$mincs\_$max_overlap_ratio\_output.txt
python old_final.py $minfreq $mincs $max_overlap_ratio 8 ../CMineMR-Mapreduce-algorithm-to-extract-coverage-patterns-master/dataset/mushroom.txt varying_minrf_"$1"_prevalgo 


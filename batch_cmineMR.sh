#!/bin/bash
#SBATCH -n 40
#SBATCH --gres=gpu:1
#SBATCH --mem-per-cpu=3000
#SBATCH --time=4-00:00:00
#SBATCH --mail-type=END
# minfreq mincs max_overlap_ratio
# echo "$1_output.txt"
minfreq=0.0015
mincs=0.12
max_overlap_ratio=0.01
# echo $1\_$minfreq\_$mincs\_$max_overlap_ratio\_output.txt
python cmine_mapreduce.py $minfreq $mincs $max_overlap_ratio 40 $1 pre $1\_$minfreq\_$mincs\_$max_overlap_ratio\_output_1.txt

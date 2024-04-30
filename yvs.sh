#!/bin/bash
#SBATCH -n 30
#SBATCH --mem-per-cpu=2048
#SBATCH --time=2-00:00:00
#SBATCH --mail-type=END
# minfreq mincs max_overlap_ratio
# echo "$1_output.txt"
minfreq=0.0
mincs=0.0
max_overlap_ratio=0.01
# echo $1\_$minfreq\_$mincs\_$max_overlap_ratio\_output.txt
# python cmine_mapreduce.py $minfreq $mincs $max_overlap_ratio 40 $1 pre $1\_$minfreq\_$mincs\_$max_overlap_ratio\_output.txt
python or.py $minfreq $mincs $max_overlap_ratio $1 > $1\_$minfreq\_$mincs\_$max_overlap_ratio\_output.txt

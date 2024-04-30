#!/bin/bash
#SBATCH --gres=gpu:0
#SBATCH --time=2-00:00:00
#SBATCH --mail-type=END
# minfreq mincs max_overlap_ratio
# echo "$1_output.txt"
export http_proxy=''
export https_proxy=''
minfreq=0.04
mincs=0
max_overlap_ratio=0.25
# echo $1\_$minfreq\_$mincs\_$max_overlap_ratio\_output.txt
python old_improved.py $minfreq $mincs $max_overlap_ratio $1 ../CMineMR-Mapreduce-algorithm-to-extract-coverage-patterns-master/dataset/synthetic.txt varying_minRF_bmspos_"$1"_improvedalgodummy


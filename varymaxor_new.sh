#!/bin/bash
#SBATCH --gres=gpu:0
#SBATCH --time=2-00:00:00
#SBATCH --mail-type=END
# minfreq mincs max_overlap_ratio
# echo "$1_output.txt"
export http_proxy=''
export https_proxy=''
source ~/anaconda3/bin/activate py37
minfreq=$1
mincs=1
max_overlap_ratio=$2
# echo $1\_$minfreq\_$mincs\_$max_overlap_ratio\_output.txt
python our_newtest.py $minfreq $mincs $max_overlap_ratio $3 ../CMineMR-Mapreduce-algorithm-to-extract-coverage-patterns-master/dataset/"$4".txt varying_maxor_"$4"_"$3"



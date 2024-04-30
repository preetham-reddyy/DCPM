#!/bin/bash
#SBATCH -n 40
#SBATCH --gres=gpu:0
#SBATCH --mem-per-cpu=3000
#SBATCH --time=4-00:00:00
#SBATCH --mail-type=END
# minfreq mincs max_overlap_ratio
# echo "$1_output.txt"
source ~/anaconda2/bin/activate py37
minfreq=0.00126
maxcs=1
max_overlap_ratio=0.1
# echo $1\_$minfreq\_$mincs\_$max_overlap_ratio\_output.txt
#python our_mine_3.py $minfreq $maxcs $max_overlap_ratio 8 ../CMineMR-Mapreduce-algorithm-to-extract-coverage-patterns-master/dataset/synthetic.txt varying_minrf_"$1"_ouralgo
python our_mine_3.py $minfreq $maxcs $max_overlap_ratio 40 ./kosarak.dat kosarak_new_output

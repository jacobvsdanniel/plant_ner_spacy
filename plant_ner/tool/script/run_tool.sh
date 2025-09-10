#!/bin/bash

set -eux

src=/volume/penghsuanli-genome2-nas2/plant/dataset_20230926/sentence
tgt=/volume/penghsuanli-genome2-nas2/plant/dataset_20230926/sentence_ner
mkdir -p ${tgt}

i_l=${1}
i_r=${2}

for (( i=${i_l}; i<=${i_r}; i++ ))
do
python main.py \
--source_file ${src}/batch_${i}.json \
--target_dir ${tgt}/batch_${i} \
--indent -1 \
2>&1 | tee log/run_ner/batch_${i}.txt
done


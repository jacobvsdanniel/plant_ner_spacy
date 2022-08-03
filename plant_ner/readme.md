# Plant NER
Reference: https://github.com/dmis-lab/biobert-pytorch

## Installation
- Docker image: pytorch/pytorch:1.6.0-cuda10.1-cudnn7-runtime
```bash
cd named-entity-recognition
pip install -r requirements.txt
```

## Run train.sh
```bash
export SAVE_DIR=./output
export DATA_DIR=../datasets/NER

export MAX_LENGTH=192
export BATCH_SIZE=32
export NUM_EPOCHS=20
export SAVE_STEPS=1000
export ENTITY=plant
export SEED=1

echo $ENTITY

CUDA_VISIBLE_DEVICES=0 python3 run_ner.py \
    --data_dir ${DATA_DIR}/${ENTITY}/ \
    --labels ${DATA_DIR}/${ENTITY}/labels.txt \
    --model_name_or_path dmis-lab/biobert-base-cased-v1.1 \
    --output_dir ${SAVE_DIR}/${ENTITY} \
    --max_seq_length ${MAX_LENGTH} \
    --num_train_epochs ${NUM_EPOCHS} \
    --per_device_train_batch_size ${BATCH_SIZE} \
    --save_steps ${SAVE_STEPS} \
    --seed ${SEED} \
    --do_train \
    --do_eval \
    --do_predict \
    --overwrite_output_dir

```
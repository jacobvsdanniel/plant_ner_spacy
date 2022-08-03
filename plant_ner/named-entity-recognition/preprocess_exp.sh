#!/bin/bash
ENTITIES="plant"
MAX_LENGTH=128

# for NUM in 40000 30000 20000 10000 5000 2000 1000 500
# for NUM in 500 200 100 50
for NUM in 50
do

    for ENTITY in $ENTITIES
    do
        echo "***** " ${ENTITY}_${NUM} " Preprocessing Start *****"
        DATA_DIR=../datasets/NER/${ENTITY}_${NUM}

        # Replace tab to space
        cat $DATA_DIR/train.tsv | tr '\t' ' ' > $DATA_DIR/train.txt.tmp
        cat $DATA_DIR/devel.tsv | tr '\t' ' ' > $DATA_DIR/devel.txt.tmp
        cat $DATA_DIR/train_dev.tsv | tr '\t' ' ' > $DATA_DIR/train_dev.txt.tmp
        cat $DATA_DIR/test.tsv | tr '\t' ' ' > $DATA_DIR/test.txt.tmp
        echo "Replacing Done"

        # Preprocess for BERT-based models
        python3 scripts/preprocess.py $DATA_DIR/train.txt.tmp bert-base-cased $MAX_LENGTH > $DATA_DIR/train.txt
        python3 scripts/preprocess.py $DATA_DIR/devel.txt.tmp bert-base-cased $MAX_LENGTH > $DATA_DIR/devel.txt
        python3 scripts/preprocess.py $DATA_DIR/train_dev.txt.tmp bert-base-cased $MAX_LENGTH > $DATA_DIR/train_dev.txt
        python3 scripts/preprocess.py $DATA_DIR/test.txt.tmp bert-base-cased $MAX_LENGTH > $DATA_DIR/test.txt
        cat $DATA_DIR/train.txt $DATA_DIR/devel.txt $DATA_DIR/test.txt | cut -d " " -f 2 | grep -v "^$"| sort | uniq > $DATA_DIR/labels.txt
        echo "***** " ${ENTITY}_${NUM} " Preprocessing Done *****"
    done

done

#! /bin/bash


# Perform clean-up and preparation actions.
if [ ! -d "partitions/" ]; then
    mkdir partitions/
    python partition.py
fi
test -d 'results/' && rm -rf results/


# Submit locally.
spark-submit \
    --master 'local[*]' \
    --name 'EPA Data: Learning Mean' \
    --py-files kdtree.py,kfold.py,point.py \
    epa_data_job.py;


# Submit on "gsu-hue".
# spark-submit \
#     --master 'yarn' \
#     --name 'Experiment #04' \
#     --deploy-mode client \
#     --py-files kdtree.py,kfold.py,point.py \
#     --num-executors 28 \
#     --executor-cores 4 \
#     --executor-memory 4G \
#     experiment_04.py;

#! /bin/bash


# Perform clean-up and preparation actions.
if [ ! -d "partitions/" ]; then
    mkdir partitions/
    python partition.py
fi
# test -d 'results/' && rm -rf results/

hdfs dfs -rm -R results/ozone_max_results
hdfs dfs -rm -R results/pm25_max_results

# Submit locally.
# spark-submit \
    # --master 'local[*]' \
    # --name 'EPA Data: Learning Mean' \
    # --py-files kdtree.py,kfold.py,point.py \
    # epa_data_job.py;


# Submit on "gsu-hue".
spark-submit \
    --master 'yarn' \
    --name 'EPA Data: Learning the Max' \
    --deploy-mode client \
    --py-files kdtree.py,kfold.py,point.py \
    --num-executors 14 \
    --executor-cores 16 \
    --executor-memory 2G \
    epa_data_job.py;

#! /bin/bash


# Perform clean-up and preparation actions.
# if [ ! -d "partitions/" ]; then
#     mkdir partitions/
#     python partition.py
# fi
# test -d 'results/' && rm -rf results/

#hdfs dfs -rm -R results/ozone_max_results
#hdfs dfs -rm -R results/pm25_max_results

# local clean up code
if [ -d "ozone_interpolation_results.csv/" ]; then
    rm -rf ozone_interpolation_results.csv/
fi
if [ -d "pm25_interpolation_results.csv/" ]; then
    rm -rf pm25_interpolation_results.csv/
fi

# Submit locally.
spark-submit \
     --master 'local[*]' \
     --name 'Interpolation Testing' \
     --py-files kdtree.py,kfold.py,point.py \
     interpolation_job.py;


# Submit on "gsu-hue".
# spark-submit \
#     --master 'yarn' \
#     --name 'EPA Data: Learning the Max' \
#     --deploy-mode client \
#     --py-files kdtree.py,kfold.py,point.py \
#     --num-executors 14 \
#     --executor-cores 16 \
#     --executor-memory 2G \
#     epa_data_job.py;

#!/bin/bash

export YARN_NODEMANAGER_OPTS="-Dhadoop.tmp.dir=/s/${HOSTNAME}/a/nobackup/cs455/${USER}"
#export HADOOP_OPTS="-Djava.library.path=$HADOOP_HOME/lib"
export HADOOP_COMMON_LIB_NATIVE_DIR=${HADOOP_HOME}/lib/native
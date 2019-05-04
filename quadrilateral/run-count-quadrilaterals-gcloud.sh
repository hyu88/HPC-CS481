#!/bin/bash

export BUCKET=hpc-storage
export CLUSTER=shape-counter
export CLASS_NAME=CountQuadrilaterals
export JAR_NAME=gs://${BUCKET}/code/CountQuadrilaterals.jar
export INPUT=gs://${BUCKET}/data/small_graph.txt
export OUTPUT_DIR=gs://${BUCKET}/data/output

gsutil rm -r $OUTPUT_DIR
gcloud dataproc jobs submit spark \
	--cluster=$CLUSTER \
	--class $CLASS_NAME \
	--jars $JAR_NAME \
	-- $INPUT $OUTPUT_DIR

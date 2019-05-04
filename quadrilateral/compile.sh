#!/bin/bash

javac -classpath /usr/local/spark/jars/scala-library-2.11.12.jar:/usr/local/spark/jars/spark-core_2.11-2.4.0.jar CountQuadrilaterals.java --release 8
jar -cvf CountQuadrilaterals.jar META-INF CountQuadrilaterals*class

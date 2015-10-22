#!/bin/sh
export JAR="/Users/eugene/proj/geotrellis-etl/target/scala-2.10/geotrellis-etl-assembly-0.1-SNAPSHOT.jar"

INPUT=file:/Users/eugene/tmp/nlcd-stitch-12.tif

spark-submit \
--class GeotrellisEtl \
--master local[8] \
--driver-memory 4G \
$JAR \
--input hadoop --format geotiff --cache NONE -I path=$INPUT \
--output hadoop -O path=file:/Users/eugene/tmp/model-lab/catalog \
--layer nlcd --layoutScheme tms --pyramid --tileSize 256 --crs EPSG:3857 --histogram

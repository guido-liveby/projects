#!/bin/bash

source ./.env
rm ./data/example_geo*

echo 'output GeoJSON'
ogr2ogr -f "GeoJSON" ./data/example_geoJSON.geojson PG:"host=liveby-postgresql.cluster-custom-czzli0i2ycl6.us-east-2.rds.amazonaws.com user=prod dbname=census password=$pg_password" -makevalid -sql @wa_query.sql

echo 'output GeoJSONSeq'
ogr2ogr -f "GeoJSONSeq" ./data/example_geoJSONseq.geoJSONseq PG:"host=liveby-postgresql.cluster-custom-czzli0i2ycl6.us-east-2.rds.amazonaws.com user=prod dbname=census password=$pg_password" -makevalid -nln example_geoJSONSeq -sql @wa_query.sql

echo 'outputGPKG'
ogr2ogr -f "GPKG" ./data/example_geoPackage.gpkg PG:"host=liveby-postgresql.cluster-custom-czzli0i2ycl6.us-east-2.rds.amazonaws.com user=prod dbname=census password=$pg_password" -makevalid -sql @wa_query.sql
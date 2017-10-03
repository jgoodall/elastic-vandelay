elastic-vandelay
================

`elastic-vandelay` is an importer-exporter utility to import / export data to / from Elasticsearch.

To export data from Elasticsearch to a file (use the appropriate binary for your platform):

```
./bin/elastic-vandelay_darwin_amd64 \
-source http://localhost:9200 -source-index myindex \
-source-type mytype -source-time-field stime \
-source-time-start "2017.01.01 07:30:00" -source-time-end "2017.01.01 17:30:00" \
-dest ~/Desktop/myindex.json
```

`source-type` is optional, defaulting to all types. The `source-time-*` fields are optional, they can be specified to limit the data exported based on a time field in the data; the format for the times must be `YYYY.MM.DD HH:MM:SS`.


To import the data file into Elasticsearch (use the appropriate binary for your platform):


```
./bin/elastic-vandelay_darwin_amd64 \
-source ~/Desktop/myindex.json \
-dest http://localhost:9200 -dest-index mynewindex -dest-type mynewtype
```

In this example, `dest-index` and `dest-type` are optional; if they are left out, the same index name and type will be used as was in the original data.

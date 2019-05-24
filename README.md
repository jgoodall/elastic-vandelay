elastic-vandelay
================

`elastic-vandelay` is an importer-exporter utility to import / export data to / from Elasticsearch v7.


## Export

To export data from Elasticsearch to a file (use the appropriate binary for your platform):

```
./bin/elastic-vandelay_darwin_amd64 export --source-url=http://localhost:9200/ --source-index=index-to-export --dest-file=exported-index
```

The export will result in two files: `dest-file` will be the exported data and `dest-file-mapping.json` will be the mappings.

The `time-*` fields are optional, they can be specified to limit the data exported based on a time field in the data; the format for the times must be `YYYY.MM.DD HH:MM:SS`. 

If the `dest-file` name specified ends in `.gz`, the data file will be gzipped.


## Import

To import the data file into Elasticsearch (use the appropriate binary for your platform):

```
./bin/elastic-vandelay_darwin_amd64 import --source-file=./exported-index --dest-url=http://127.0.0.1:9200 --dest-index=new-index
```

If the source filename specified ends in `.gz`, the file will be gunzipped first.

# Hive Omniture Storage Handler

Hive storage handler and SerDe implementation intended for Omniture clickstream log transformation.
The handler creates all header and metadata information, including data file locations, based on the manifest file provided by Adobe
with logs and used in the HiveQL's TBLPROPERTIES.

This is an early alpha version based on the [omniture-clickstream](https://github.com/bartekdobija/omniture-clickstream) library.

###Configuration:

hdfs://, s3:// and file:// schemes are supported

```sql
CREATE EXTERNAL TABLE omniture_table
ROW FORMAT SERDE 'com.github.bartekdobija.omniture.serde.OmnitureSerDe'
STORED BY 'com.github.bartekdobija.omniture.handler.OmnitureStorageHandler'
TBLPROPERTIES (
  "omniture.manifest.file" = 'file://omniture-logs/manifest.txt'
  -- "omniture.manifest.file" = 'hdfs://namenode/omniture-logs/manifest.txt'
  -- "omniture.manifest.file" = 's3://bucket/omniture-logs/manifest.txt'
);
```
This first version does not implement partition handling.
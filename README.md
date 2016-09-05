# Hive Omniture Storage Handler

Create a table by using the below SQL statement:

```sql
CREATE EXTERNAL TABLE omniture_table
ROW FORMAT SERDE 'com.github.bartekdobija.omniture.serde.OmnitureSerDe'
STORED BY 'com.github.bartekdobija.omniture.handler.OmnitureStorageHandler'
TBLPROPERTIES (
  "omniture.manifest.file" = 'file://sample/manifest.txt'
);
```
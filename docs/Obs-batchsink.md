# Huawei Obs Batch Sink


Description
-----------
This sink is used whenever you need to write to Huawei Obs in various formats. For example,
you might want to create daily snapshots of a database by reading the entire contents of a
table, writing to this sink, and then other programs can analyze the contents of the
specified file.


Properties
----------
**Reference Name:** Name used to uniquely identify this sink for lineage, annotating metadata, etc.

**Path:** Path to write to. For example, obs://<bucket>/path/to/output

**Path Suffix:** Time format for the output directory that will be appended to the path.
For example, the format 'yyyy-MM-dd-HH-mm' will result in a directory of the form '2015-01-01-20-42'.
If not specified, nothing will be appended to the path."

**Format:** Format to write the records in.
The format must be one of 'json', 'avro', 'parquet', 'csv', 'tsv', or 'delimited'.

**Delimiter:** Delimiter to use if the format is 'delimited'.
The delimiter will be ignored if the format is anything other than 'delimited'.

**End Point:** End-Point to be used by the Obs Client.

**Access Key:** Amazon access ID required for authentication.

**Secret Key:** Amazon access key required for authentication.


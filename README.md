# aws_kinesis_firehose_s3_bucket
Kinesis to kinesis firehose to s3
<br>

customize the S3 prefix with dynamic expressions that are evaluated at runtime.

<br>

!{timestamp:yyyy}-!{timestamp:MM}-!{timestamp:dd}/!{timestamp:HH}/

Compression, file extension, and encryption<br>
Compression for data records<br>
ZIP<br>
File extension format<br>
.parquet<br>



<br>
BufferingHints.IntervalInSeconds must be at least 60 seconds when Dynamic Partitioning is enabled.

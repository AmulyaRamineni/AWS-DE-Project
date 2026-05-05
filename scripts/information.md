
Bronze Bucket name-  la-yt-data-bronze
Silver Bucket name- la-yt-data-silver
Gold Bucket name-  la-yt-data-gold

Script Bucket name- la-yt-data-script

Lifecycle configuration name for archiving oldd raw data of bronze layer is done for 100 days later: la-bronze-archive-raw-data


SNS ARN-  arn:aws:sns:us-east-1:043074500839:la-yt-data-pipeline-alerts:fed11bf6-9eaa-4d02-987d-faf5b279f631

AWS Glue Database-
la-yt-pipeline-bronze
la-yt-pipeline-gold
la-yt-pipeline-silver


--bronze_database la-yt-pipeline-bronze
--bronze_table raw_statistics
--silver_bucket la-yt-data-silver
--silver_database la-yt-pipeline-silver
--silver_table clean_statistics

 --silver_database la-yt-pipeline-silver
 --gold_bucket  la-yt-data-gold
 --gold_database la-yt-pipeline-gold

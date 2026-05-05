
aws s3 cp CAVideos.csv s3://la-yt-data-bronze/youtube/raw_statistics/region=ca/

aws s3 cp CA_category_id.json s3://la-yt-data-bronze/youtube/raw_statistics_reference_data/region=ca/


# Canada
aws s3 cp CAvideos.csv $BUCKET/raw_statistics/region=ca/
aws s3 cp CA_category_id.json $BUCKET/raw_statistics_reference_data/region=ca/

# Germany
aws s3 cp DEvideos.csv $BUCKET/raw_statistics/region=de/
aws s3 cp DE_category_id.json $BUCKET/raw_statistics_reference_data/region=de/

# France
aws s3 cp FRvideos.csv $BUCKET/raw_statistics/region=fr/
aws s3 cp FR_category_id.json $BUCKET/raw_statistics_reference_data/region=fr/

# Great Britain
aws s3 cp GBvideos.csv $BUCKET/raw_statistics/region=gb/
aws s3 cp GB_category_id.json $BUCKET/raw_statistics_reference_data/region=gb/

# India
aws s3 cp INvideos.csv $BUCKET/raw_statistics/region=in/
aws s3 cp IN_category_id.json $BUCKET/raw_statistics_reference_data/region=in/

# Japan
aws s3 cp JPvideos.csv $BUCKET/raw_statistics/region=jp/
aws s3 cp JP_category_id.json $BUCKET/raw_statistics_reference_data/region=jp/

# Korea
aws s3 cp KRvideos.csv $BUCKET/raw_statistics/region=kr/
aws s3 cp KR_category_id.json $BUCKET/raw_statistics_reference_data/region=kr/

# Mexico
aws s3 cp MXvideos.csv $BUCKET/raw_statistics/region=mx/
aws s3 cp MX_category_id.json $BUCKET/raw_statistics_reference_data/region=mx/

# Russia
aws s3 cp RUvideos.csv $BUCKET/raw_statistics/region=ru/
aws s3 cp RU_category_id.json $BUCKET/raw_statistics_reference_data/region=ru/

# United States
aws s3 cp USvideos.csv $BUCKET/raw_statistics/region=us/
aws s3 cp US_category_id.json $BUCKET/raw_statistics_reference_data/region=us/

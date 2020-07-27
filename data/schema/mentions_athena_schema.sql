CREATE EXTERNAL TABLE mentions (
globaleventid bigint,
eventtimedate bigint,
mentiontimedate bigint,
mentiontype bigint,
mentionsourcename string,
mentionidentifier string,
sentenceid bigint,
actor1charoffset bigint,
actor2charoffset bigint,
actioncharoffset bigint,
inrawtext string,
confidence bigint,
mentiondoclen bigint,
mentiondoctone double,
mentiondoctranslationinfo double,
extras double
)
--PARTITIONED BY (eventtimedate BIGINT)
STORED AS PARQUET
LOCATION 's3://cse6242-project-data/parquet/mentions/'
tblproperties ("parquet.compression"="SNAPPY");



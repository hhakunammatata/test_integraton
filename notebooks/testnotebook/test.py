# Databricks notebook source
# changing something

def mf(file):
  return file.name

files=sorted(dbutils.fs.ls("s3://skyscanner-data-platform-trusted-data-pipeline/grappler-parquet/prod.direct-booking.funnel_events.direct_booking.PriceChangeEvent"))

print(files)

# COMMAND ----------

df = spark.read.parquet("s3://skyscanner-data-platform-trusted-data-pipeline/grappler-parquet/prod.direct-booking.funnel_events.direct_booking_funnel.DBookingComplete/dt=2020-02-19")
display(df)

# COMMAND ----------

from pyspark.sql.functions import col, asc

def ip_is_missed(path):
  try:
    df = spark.read.parquet(path)
    return df.filter(df.header.client_ip == "").count() > 0
  except:
    print("NO IP in Header: {}".format(path))


def list(path):
  files=sorted(dbutils.fs.ls(path))
  if (files[len(files)-1].isDir()):
    return list(files[len(files)-1].path)
  else:
    return files[len(files)-1].path
  

  
all_topics =   dbutils.fs.ls("s3://skyscanner-data-platform-trusted-data-pipeline/grappler-parquet/")

for topic in all_topics:
  latest_file = list(topic.path) 
  if (ip_is_missed(latest_file)):
    print(topic.name)

# COMMAND ----------

from pyspark.sql.functions import col, asc


df = spark.read.parquet("s3://skyscanner-data-platform-trusted-data-pipeline/grappler-parquet/public.login.funnel_events.clients.View/dt=2019-12-21/hour=18/region=eu-west-1/jobname=TrustedDataArchiver-2019-12-23T16-28-44/part-2-191547")

#display(df)
#display(df.filter(df.header.client_ip == ""))
#row = df.filter("header.client_ip is null").count()
print(df.filter(df.header.client_ip == "").count())

# COMMAND ----------

from pyspark.sql.functions import col, asc

def ip_is_missed(path):
  df = spark.read.parquet(path)
  return df.filter(df.header.client_ip == "").count() > 0

def list(path):
 
  files=dbutils.fs.ls(path)
  if (files[len(files)-1].isDir()):
    return list(files[len(files)-1].path)
  else:
    return files[len(files)-1].path
  
  
file = list("s3://skyscanner-data-platform-trusted-data-pipeline/grappler-parquet/public.login.funnel_events.clients.View")

print(file)
print(ip_is_missed(file))

# COMMAND ----------



all_topics =   dbutils.fs.ls("s3://skyscanner-data-platform-trusted-data-pipeline/grappler-parquet/")

for topic in all_topics:
  if topic.name == "public-sandbox.longboat.applog.applog.Message/":
    print(topic.path)

# COMMAND ----------

from pyspark.sql.functions import col, asc

def ip_is_missed(path):
  df = spark.read.parquet(path)
  return df.filter(df.header.client_ip == "").count() > 0

def list(path):
 
  files=dbutils.fs.ls(path)
  if (files[len(files)-1].isDir()):
    return list(files[len(files)-1].path)
  else:
    return files[len(files)-1].path
  
#print(list("s3://skyscanner-data-platform-trusted-data-pipeline/grappler-parquet/public-sandbox.longboat.applog.applog.Message/"))
print(ip_is_missed("s3://skyscanner-data-platform-trusted-data-pipeline/grappler-parquet/public-sandbox.longboat.applog.applog.Message/dt=1970-01-01/hour=00/region=eu-west-1/jobname=TrustedDataArchiver-2019-12-24T19-29-20/part-3-348725"))
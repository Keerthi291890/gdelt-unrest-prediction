from pyspark.sql import SparkSession, functions as F

spark = (SparkSession.builder.appName("m2_verify")
         .config("spark.driver.memory", "6g").getOrCreate())

base = "data/PersonA_handoff/joined_daily"
cols = ["GlobalEventID","MentionIdentifier","MentionTimeDate",
        "ActionGeo_CountryCode","date","source_common_name"]

df = (spark.read.parquet(base + "/*")
      .select(*cols)   # also drops v2_gcam, the likely 20 GB culprit
      .withColumn("part_date", F.regexp_extract(F.input_file_name(), r"(\d{4}-\d{2}-\d{2})", 1)))

# 1. Partition coverage: expect 91 days, no outlier day
daily = df.groupBy("part_date").count().orderBy("part_date")
daily.show(100, truncate=False)
print("days:", daily.count())

# 2. Totals and key structure
tot = df.count()
ev = df.select("GlobalEventID").distinct().count()
ev_day = df.select("part_date","GlobalEventID").distinct().count()
print(tot, ev, ev_day)   # ev_day > ev  => events repeat across partitions

# 3. As-of alignment: mention date must equal partition date, every day
bad = df.filter(F.substring(F.col("MentionTimeDate").cast("string"),1,8)
                != F.regexp_replace("part_date","-",""))
print("mention/partition date mismatches:", bad.count())

# 4. Nulls on keys
df.select([F.sum(F.col(c).isNull().cast("int")).alias(c) for c in cols[:4]]).show()

# 5. Duplicate pairs: exact vs differing (full rows, per day)
key = ["part_date","GlobalEventID","MentionIdentifier"]
g = df.groupBy(*key).agg(F.count("*").alias("n"),
                         F.countDistinct("source_common_name","date").alias("n_gkg_variants"))
g.filter("n>1").groupBy("n","n_gkg_variants").count().show()
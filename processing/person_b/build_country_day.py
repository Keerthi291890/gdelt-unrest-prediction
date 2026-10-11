# processing/person_b/build_country_day.py
from datetime import date, timedelta
from pathlib import Path

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F

ROOT = Path(__file__).resolve().parents[2]
SRC = (ROOT / "data" / "PersonA_handoff" / "joined_daily").as_posix() + "/*"
OUT = ROOT / "data" / "processed" / "person_b"
OUT.mkdir(parents=True, exist_ok=True)
PAIRS = (OUT / "_pairs_clean").as_posix()

COUNTRIES = ["US", "IN", "NI", "UP", "SF"]            # FIPS, as in GDELT
START, END = date(2026, 4, 1), date(2026, 6, 30)

spark = (SparkSession.builder.appName("m2_country_day").master("local[2]")
         .config("spark.driver.memory", "6g")
         .config("spark.sql.shuffle.partitions", "16")
         .config("spark.sql.session.timeZone", "UTC").getOrCreate())
spark.sparkContext.setLogLevel("ERROR")

# ---------------- Stage A: clean, dedupe to event-mention pairs ----------------
raw = spark.read.parquet(SRC).select(
    "GlobalEventID", "Day", "EventRootCode", "QuadClass", "GoldsteinScale",
    "NumMentions", "NumSources", "NumArticles", "AvgTone",
    "ActionGeo_CountryCode", "MentionIdentifier", "MentionTimeDate",
    "MentionSourceName", "Confidence", "MentionDocLen", "MentionDocTone",
    "document_identifier", "v1_5_tone",
)

base = (raw.withColumnRenamed("ActionGeo_CountryCode", "country")
        .filter(F.col("country").isin(COUNTRIES))
        # as-of key: observation (mention) date; verified == partition date
        .withColumn("part_date", F.to_date(F.substring("MentionTimeDate", 1, 8), "yyyyMMdd")))

fi = lambda c: F.first(c, ignorenulls=True)
pairs = (base.groupBy("country", "part_date", "GlobalEventID", "MentionIdentifier")
         .agg(F.min("MentionTimeDate").alias("first_mt"),
              F.max(F.col("Confidence").cast("double")).alias("conf"),
              fi("MentionSourceName").alias("src"),
              fi(F.col("MentionDocTone").cast("double")).alias("doc_tone"),
              fi(F.col("MentionDocLen").cast("double")).alias("doc_len"),
              fi("document_identifier").alias("gkg_doc"),
              fi(F.split("v1_5_tone", ",").getItem(0).cast("double")).alias("gkg_tone"),
              fi("Day").alias("ev_day"),
              fi("EventRootCode").alias("root"),
              fi(F.col("QuadClass").cast("int")).alias("quad"),
              fi(F.col("GoldsteinScale").cast("double")).alias("gold"),
              fi(F.col("NumMentions").cast("double")).alias("n_mentions"),
              fi(F.col("NumSources").cast("double")).alias("n_sources"),
              fi(F.col("NumArticles").cast("double")).alias("n_articles"),
              fi(F.col("AvgTone").cast("double")).alias("ev_tone"))
         .withColumn("mt", F.to_timestamp("first_mt", "yyyyMMddHHmmss")))

pairs.write.mode("overwrite").parquet(PAIRS)
pairs = spark.read.parquet(PAIRS)

# ---------------- Stage B1: EVENT-level features (one row per event) ----------------
ev = pairs.dropDuplicates(["part_date", "GlobalEventID"])
ev_day = (ev.groupBy("country", "part_date").agg(
    F.count("*").alias("event_count"),
    F.mean("gold").alias("gold_mean"), F.stddev("gold").alias("gold_std"),
    F.min("gold").alias("gold_min"),
    F.mean((F.col("gold") < 0).cast("double")).alias("neg_event_ratio"),
    F.mean(F.col("quad").isin(3, 4).cast("double")).alias("conflict_quad_share"),
    F.sum(F.col("root").isin("18", "19", "20").cast("int")).alias("violent_event_count"),
    F.mean(F.col("root").isin("18", "19", "20").cast("double")).alias("violent_event_share"),
    F.mean("ev_tone").alias("ev_tone_mean"), F.stddev("ev_tone").alias("ev_tone_std"),
    F.sum("n_mentions").alias("sum_num_mentions"), F.mean("n_mentions").alias("avg_num_mentions"),
    F.mean("n_sources").alias("avg_num_sources"), F.mean("n_articles").alias("avg_num_articles"),
    # share of events whose own Day predates the observation date (resurfaced events)
    F.mean((F.col("ev_day") < F.date_format("part_date", "yyyyMMdd")).cast("double"))
     .alias("old_event_share")))

# ---------------- Stage B2: MENTION / propagation features ----------------
ev_prop = (pairs.groupBy("country", "part_date", "GlobalEventID").agg(
    F.count("*").alias("n_m"), F.countDistinct("src").alias("n_s"),
    F.min("mt").alias("t0"), F.max("mt").alias("t1"))
    .withColumn("spread_min", (F.unix_timestamp("t1") - F.unix_timestamp("t0")) / 60.0))
prop_day = ev_prop.groupBy("country", "part_date").agg(
    F.mean("n_m").alias("obs_mentions_per_event"),
    F.mean("n_s").alias("obs_sources_per_event"),
    F.mean("spread_min").alias("spread_mean_min"),
    F.expr("percentile_approx(spread_min, 0.5)").alias("spread_p50_min"),
    F.mean((F.col("n_s") >= 2).cast("double")).alias("multi_source_event_share"))

w = Window.partitionBy("country", "part_date")
src_day = (pairs.filter(F.col("src").isNotNull())
           .groupBy("country", "part_date", "src").agg(F.count("*").alias("n"))
           .withColumn("p", F.col("n") / F.sum("n").over(w))
           .groupBy("country", "part_date").agg(
               F.sum("n").alias("pair_count"),
               F.count("*").alias("unique_mention_sources"),
               F.sum(F.col("p") * F.col("p")).alias("source_hhi"),
               F.max("p").alias("top_source_share"),
               (-F.sum(F.col("p") * F.log("p"))).alias("source_entropy")))

# ---------------- Stage B3: DOCUMENT-level (distinct URL) + GKG features ----------------
docs = pairs.groupBy("country", "part_date", "MentionIdentifier").agg(
    fi("doc_tone").alias("doc_tone"), fi("doc_len").alias("doc_len"),
    F.max(F.col("gkg_doc").isNotNull().cast("int")).alias("matched"),
    fi("gkg_tone").alias("gkg_tone"), F.max("conf").alias("conf"))
doc_day = docs.groupBy("country", "part_date").agg(
    F.count("*").alias("doc_count"),
    F.mean("doc_tone").alias("doc_tone_mean"), F.stddev("doc_tone").alias("doc_tone_std"),
    F.mean("doc_len").alias("doc_len_mean"), F.mean("conf").alias("mention_conf_mean"),
    F.mean("matched").alias("gkg_match_rate"),
    F.mean("gkg_tone").alias("gkg_tone_mean"), F.stddev("gkg_tone").alias("gkg_tone_std"))

# ---------------- Join on a full country x day grid ----------------
days = [START + timedelta(n) for n in range((END - START).days + 1)]
grid = spark.createDataFrame([(c, d) for c in COUNTRIES for d in days],
                             ["country", "part_date"])
final = grid
for t in (ev_day, prop_day, src_day, doc_day):
    final = final.join(t, ["country", "part_date"], "left")
final = final.withColumn("has_data", F.col("event_count").isNotNull().cast("int"))

pdf = final.orderBy("country", "part_date").toPandas()
pdf.to_csv(OUT / "country_day_features.csv", index=False)
try:
    pdf.to_parquet(OUT / "country_day_features.parquet", index=False)
except Exception as e:
    print("parquet skipped:", e)

# ---------------- Sanity checks ----------------
print("shape:", pdf.shape, "(expect 455 rows)")
print("days without data:", int((pdf.has_data == 0).sum()))
print("sum(event_count):", int(pdf.event_count.sum()), "(expect 3,824,602)")
print("sum(pair_count):", int(pdf.pair_count.sum()))
print(pdf.groupby("country")[["event_count", "doc_count", "gkg_match_rate"]].mean().round(3))
spark.stop()
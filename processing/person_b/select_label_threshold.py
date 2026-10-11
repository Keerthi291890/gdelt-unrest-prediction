from pyspark.sql import SparkSession, functions as F
from pyspark.sql.window import Window
from pathlib import Path


# ============================================================
# CONFIG
# ============================================================

PROJECT_ROOT = Path(__file__).resolve().parents[2]

UCDP_PATH = str(
    PROJECT_ROOT
    / "data"
    / "external"
    / "ucdp_candidate"
    / "*.csv"
)

COUNTRY_MAP = {
    "United States of America": "US",
    "India": "IN",
    "Nigeria": "NI",
    "Ukraine": "UP",
    "South Africa": "SF",
}

TARGET_COUNTRIES = ["US", "IN", "NI", "UP", "SF"]

RATIOS = [1.25, 1.50]

DEATH_FLOOR = 5


# ============================================================
# SPARK
# ============================================================

spark = (
    SparkSession.builder
    .appName("UCDP_Label_Threshold_Selection")
    .master("local[2]")
    .config("spark.sql.shuffle.partitions", "8")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


# ============================================================
# 1. READ UCDP
# ============================================================

print("\n===== READING UCDP =====")

df = (
    spark.read
    .option("header", True)
    .option("multiLine", True)
    .option("quote", '"')
    .option("escape", '"')
    .csv(UCDP_PATH)
    .withColumn("src_file", F.input_file_name())
    .select(
        "id",
        "type_of_violence",
        "country",
        "date_start",
        "date_end",
        "best",
        "src_file"
    )
)

raw_n = df.count()

print("Raw rows:", raw_n)

assert raw_n == 11879, (
    f"Expected 11,879 rows, but Spark read {raw_n}. "
    "Check the UCDP CSV files / multiline parsing."
)


# ============================================================
# 2. CLEAN TYPES
# ============================================================

df = (
    df
    .withColumn("date_start", F.to_date("date_start"))
    .withColumn("date_end", F.to_date("date_end"))
    .withColumn("best", F.col("best").cast("double"))
)


# ============================================================
# 3. KEEP TARGET COUNTRIES
# ============================================================

mapping_expr = F.create_map(
    *[
        item
        for pair in COUNTRY_MAP.items()
        for item in (F.lit(pair[0]), F.lit(pair[1]))
    ]
)

df = (
    df
    .withColumn(
        "country_code",
        mapping_expr[F.col("country")]
    )
    .filter(F.col("country_code").isNotNull())
)

print("\n===== TARGET COUNTRY COUNTS BEFORE DEDUPE =====")

(
    df.groupBy("country_code")
    .count()
    .orderBy("country_code")
    .show()
)


# ============================================================
# 4. DEDUPE UCDP EVENTS
# ============================================================

print("\n===== DEDUPLICATING UCDP EVENTS =====")

# The July release is later than the Jan-Jun release.
# Keep the row from the lexicographically latest source file.

w_id = (
    Window
    .partitionBy("id")
    .orderBy(F.col("src_file").desc())
)

df = (
    df
    .withColumn("rn", F.row_number().over(w_id))
    .filter(F.col("rn") == 1)
    .drop("rn", "src_file")
)

print("Rows after event-id dedupe:", df.count())


print("\n===== TARGET COUNTRY COUNTS AFTER DEDUPE =====")

(
    df.groupBy("country_code")
    .count()
    .orderBy("country_code")
    .show()
)


# ============================================================
# 5. DAILY UCDP DEATHS
# ============================================================

print("\n===== BUILDING DAILY DEATHS =====")

# Temporary / explicit rule for threshold selection:
# assign the event's "best" fatalities to date_start.
#
# We are NOT yet freezing the final label specification.
#
# Multi-day event allocation will be documented separately
# before final labels are generated.

daily = (
    df
    .groupBy(
        "country_code",
        F.col("date_start").alias("date")
    )
    .agg(
        F.sum("best").alias("deaths")
    )
)


# ============================================================
# 6. CREATE COMPLETE COUNTRY-DAY GRID
# ============================================================

print("\n===== CREATING DAILY GRID =====")

date_df = spark.sql(
    """
    SELECT explode(
        sequence(
            to_date('2026-01-01'),
            to_date('2026-07-31'),
            interval 1 day
        )
    ) AS date
    """
)

country_df = spark.createDataFrame(
    [(country,) for country in TARGET_COUNTRIES],
    ["country_code"]
)

grid = country_df.crossJoin(date_df)

daily = (
    grid
    .join(
        daily,
        ["country_code", "date"],
        "left"
    )
    .withColumn(
        "deaths",
        F.coalesce(F.col("deaths"), F.lit(0.0))
    )
)


# ============================================================
# 7. 28-DAY TRAILING BASELINE
# ============================================================

print("\n===== CALCULATING BASELINE =====")

w_past = (
    Window
    .partitionBy("country_code")
    .orderBy("date")
    .rowsBetween(-27, 0)
)

daily = (
    daily
    .withColumn(
        "baseline_daily",
        F.avg("deaths").over(w_past)
    )
    .withColumn(
        "baseline_7d",
        F.col("baseline_daily") * F.lit(7.0)
    )
)


# ============================================================
# 8. FUTURE 7-DAY DEATHS
# ============================================================

print("\n===== CALCULATING FUTURE 7-DAY OUTCOME =====")

w_future = (
    Window
    .partitionBy("country_code")
    .orderBy("date")
    .rowsBetween(1, 7)
)

daily = daily.withColumn(
    "future_7d_deaths",
    F.sum("deaths").over(w_future)
)


# ============================================================
# 9. JAN-MAR EVALUATION PERIOD
# ============================================================

print("\n===== PREPARING THRESHOLD EVALUATION =====")

# Jan 28 gives us 28 days of history.
#
# March 24 is the final evaluation date because:
#
# March 24 + 7 days = March 31
#
# Therefore the threshold-selection experiment does not
# use April outcomes.

evaluation = (
    daily
    .filter(
        (F.col("date") >= F.lit("2026-01-28"))
        &
        (F.col("date") <= F.lit("2026-03-24"))
    )
    .filter(F.col("baseline_7d").isNotNull())
)


# ============================================================
# 10. TEST THRESHOLDS
# ============================================================

print("\n==============================================")
print("THRESHOLD SELECTION RESULTS")
print("==============================================")

for ratio in RATIOS:

    print(f"\n===== RATIO = {ratio} =====")

    result = (
        evaluation
        .withColumn(
            "threshold",
            F.greatest(
                F.col("baseline_7d") * F.lit(ratio),
                F.lit(float(DEATH_FLOOR))
            )
        )
        .withColumn(
            "label",
            (
                F.col("future_7d_deaths")
                >= F.col("threshold")
            ).cast("int")
        )
    )

    summary = (
        result
        .groupBy("country_code")
        .agg(
            F.count("*").alias("days"),

            F.sum("label")
            .alias("positive_days"),

            F.sum(
                F.when(F.col("label") == 0, 1)
                .otherwise(0)
            )
            .alias("negative_days"),

            F.round(
                F.avg("label") * 100,
                2
            )
            .alias("positive_percent"),

            F.round(
                F.avg("future_7d_deaths"),
                2
            )
            .alias("avg_future_7d_deaths"),

            F.round(
                F.avg("baseline_7d"),
                2
            )
            .alias("avg_baseline_7d")
        )
        .orderBy("country_code")
    )

    summary.show(
        20,
        truncate=False
    )


# ============================================================
# 11. SHOW SOME ROWS
# ============================================================

print("\n===== SAMPLE EVALUATION ROWS =====")

(
    evaluation
    .orderBy("country_code", "date")
    .show(
        100,
        truncate=False
    )
)


print("\n===== THRESHOLD SELECTION COMPLETE =====")

spark.stop()
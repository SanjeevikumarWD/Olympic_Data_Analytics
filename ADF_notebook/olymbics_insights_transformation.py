# Databricks notebook source
configs = {"fs.azure.account.auth.type": "OAuth",
"fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
"fs.azure.account.oauth2.client.id": "**********************",
"fs.azure.account.oauth2.client.secret": '**********************',
"fs.azure.account.oauth2.client.endpoint": "https://login.microsoftonline.com/**********************/oauth2/token"}


dbutils.fs.mount(
source = "abfss://tokyo-olympic-data@tokyoolympicdata.dfs.core.windows.net", # contrainer@storageacc
mount_point = "/mnt/olymbic",
extra_configs = configs)

# COMMAND ----------

# MAGIC %fs
# MAGIC ls "mnt/olymbic"

# COMMAND ----------

from pyspark.sql.functions import col, hash, when, lit, abs
from pyspark.sql.types import IntegerType

# Load data (assuming mount is already set up as provided)
athletes = spark.read.format("csv").option("header", "true").load("/mnt/olymbic/raw-data/athletes.csv")
coaches = spark.read.format("csv").option("header", "true").load("/mnt/olymbic/raw-data/coaches.csv")
entriesgender = spark.read.format("csv").option("header", "true").load("/mnt/olymbic/raw-data/entriesgender.csv")
medals = spark.read.format("csv").option("header", "true").load("/mnt/olymbic/raw-data/medals.csv")
teams = spark.read.format("csv").option("header", "true").load("/mnt/olymbic/raw-data/teams.csv")

# 1. Athletes Transformation
athletes = athletes.withColumnRenamed("Name", "person_name") \
    .withColumnRenamed("NOC", "country") \
    .withColumnRenamed("Discipline", "discipline") \
    .withColumn("athlete_id", hash(col("person_name"), col("country"))) \
    .dropDuplicates(["athlete_id"])
athlete_counts = athletes.groupBy("country", "discipline").count().withColumnRenamed("count", "athlete_count")

# 2. Coaches Transformation
coaches = coaches.withColumnRenamed("Name", "name") \
    .withColumnRenamed("NOC", "country") \
    .withColumnRenamed("Discipline", "discipline") \
    .withColumnRenamed("Event", "event") \
    .withColumn("event", when(col("event").isNull(), "Unknown").otherwise(col("event")))
coach_counts = coaches.groupBy("country", "discipline").count().withColumnRenamed("count", "coach_count")

# 3. EntriesGender Transformation
entriesgender = entriesgender.withColumnRenamed("Discipline", "discipline") \
    .withColumnRenamed("Female", "female") \
    .withColumnRenamed("Male", "male") \
    .withColumnRenamed("Total", "total") \
    .withColumn("female", col("female").cast(IntegerType())) \
    .withColumn("male", col("male").cast(IntegerType())) \
    .withColumn("total", col("total").cast(IntegerType())) \
    .withColumn("male_percentage", col("male") / col("total")) \
    .withColumn("female_percentage", col("female") / col("total")) \
    .withColumn("gender_gap", abs(col("male") - col("female")) / col("total")) \
    .withColumn("high_gender_gap", when(col("gender_gap") > 0.3, True).otherwise(False))

# 4. Medals Transformation
medals = medals.withColumnRenamed("Team/NOC", "team_country") \
    .withColumnRenamed("Gold", "gold") \
    .withColumnRenamed("Silver", "silver") \
    .withColumnRenamed("Bronze", "bronze") \
    .withColumnRenamed("Total", "total") \
    .withColumnRenamed("Rank", "rank") \
    .withColumnRenamed("Rank by Total", "rank_by_total") \
    .withColumn("gold", col("gold").cast(IntegerType())) \
    .withColumn("silver", col("silver").cast(IntegerType())) \
    .withColumn("bronze", col("bronze").cast(IntegerType())) \
    .withColumn("total", col("total").cast(IntegerType())) \
    .withColumn("rank", col("rank").cast(IntegerType())) \
    .withColumn("rank_by_total", col("rank_by_total").cast(IntegerType())) \
    .withColumn("weighted_medal_score", col("gold") * 3 + col("silver") * 2 + col("bronze"))

# 5. Teams Transformation
teams = teams.withColumnRenamed("Name", "team_name") \
    .withColumnRenamed("Discipline", "discipline") \
    .withColumnRenamed("NOC", "country") \
    .withColumnRenamed("Event", "event")
teams = teams.join(medals.select("team_country", "total").withColumnRenamed("total", "total_medals"), teams.country == medals.team_country, "left") \
    .withColumn("medal_contributor", when(col("total_medals").isNotNull(), True).otherwise(False))
teams = teams.join(entriesgender.select("discipline", "total").withColumnRenamed("total", "total_entries"), "discipline", "left") \
    .withColumn("medals_per_team_member", when(col("total_medals").isNotNull(), col("total_medals") / col("total_entries")).otherwise(lit(0))) \
    .drop("total_medals")

# Save transformed data to Data Lake Gen 2
athletes.write.mode("overwrite").parquet("/mnt/olymbic/transformed-data/athletes")
athlete_counts.write.mode("overwrite").parquet("/mnt/olymbic/transformed-data/athlete_counts")
coaches.write.mode("overwrite").parquet("/mnt/olymbic/transformed-data/coaches")
coach_counts.write.mode("overwrite").parquet("/mnt/olymbic/transformed-data/coach_counts")
entriesgender.write.mode("overwrite").parquet("/mnt/olymbic/transformed-data/entriesgender")
medals.write.mode("overwrite").parquet("/mnt/olymbic/transformed-data/medals")
teams.write.mode("overwrite").parquet("/mnt/olymbic/transformed-data/teams")

# COMMAND ----------


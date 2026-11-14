# Databricks notebook source
spark.sql("CREATE SCHEMA IF NOT EXISTS workspace.churn_lakehouse")
spark.sql("CREATE VOLUME IF NOT EXISTS workspace.churn_lakehouse.landing")

# COMMAND ----------

path = "/Volumes/workspace/churn_lakehouse/landing/Customer-Churn-Records.csv"
df = spark.read.option("header", True).option("inferSchema", True).csv(path)
display(df)
df.printSchema()
print(df.count())

# COMMAND ----------

df.select("_c18").distinct().show()

# COMMAND ----------

df = df.drop("_c18")
print(len(df.columns))

# COMMAND ----------

extra = [c for c in df.columns if c.startswith("_c")]
print(extra)
print(df.select(extra).dropna(how="all").count())

# COMMAND ----------

df.dropna(subset=extra, how="all").select("RowNumber", "CustomerId", "Surname", *extra).show(truncate=False)

# COMMAND ----------

display(df.dropna(subset=extra, how="all").select(*extra))

# COMMAND ----------

df = df.drop(*extra)
print(len(df.columns))

# COMMAND ----------

import re

for old_name in df.columns:
    new_name = re.sub(r"[^0-9a-zA-Z]+", "_", old_name).strip("_").lower()
    df = df.withColumnRenamed(old_name, new_name)

print(df.columns)

# COMMAND ----------

from pyspark.sql import functions as F

df = (df
      .withColumn("_ingested_at", F.current_timestamp())
      .withColumn("_source_file", F.lit(path)))

df.select("customerid", "_ingested_at", "_source_file").show(3, truncate=False)

# COMMAND ----------

df.write.format("delta").mode("overwrite").saveAsTable("workspace.churn_lakehouse.bronze_churn")

print(spark.table("workspace.churn_lakehouse.bronze_churn").count())

# COMMAND ----------

from pyspark.sql import functions as F

bronze = spark.table("workspace.churn_lakehouse.bronze_churn")

print("rows:", bronze.count())
print("distinct customers:", bronze.select("customerid").distinct().count())

bronze.select(
    F.min("creditscore"), F.max("creditscore"),
    F.min("age"), F.max("age"),
    F.min("balance"), F.max("balance")
).show()

bronze.groupBy("geography").count().show()

# COMMAND ----------

rules = {
    "customer_id_not_null": "customerid IS NOT NULL",
    "credit_score_range": "creditscore BETWEEN 300 AND 900",
    "age_range": "age BETWEEN 18 AND 100",
    "geography_valid": "geography IN ('France', 'Germany', 'Spain')",
    "balance_non_negative": "balance >= 0",
    "exited_flag": "exited IN (0, 1)",
}

for name, condition in rules.items():
    failed = bronze.filter(f"NOT coalesce({condition}, false)").count()
    print(name, "->", failed, "rows fail")

# COMMAND ----------

from functools import reduce

flagged = bronze
for name, condition in rules.items():
    flagged = flagged.withColumn(f"_ok_{name}", F.expr(f"coalesce({condition}, false)"))

ok_cols = [f"_ok_{name}" for name in rules]
all_ok = reduce(lambda a, b: a & b, [F.col(c) for c in ok_cols])

valid = flagged.filter(all_ok).drop(*ok_cols)
rejects = flagged.filter(~all_ok)

print("valid:", valid.count(), "| rejects:", rejects.count())

# COMMAND ----------

def split_valid_rejects(df, rules):
    flagged = df
    for name, condition in rules.items():
        flagged = flagged.withColumn(f"_ok_{name}", F.expr(f"coalesce({condition}, false)"))

    ok_cols = [f"_ok_{name}" for name in rules]
    all_ok = reduce(lambda a, b: a & b, [F.col(c) for c in ok_cols])
    failed = F.concat_ws(",", *[F.when(~F.col(f"_ok_{n}"), F.lit(n)) for n in rules])

    valid = flagged.filter(all_ok).drop(*ok_cols)
    rejects = flagged.filter(~all_ok).withColumn("_failed_rules", failed).drop(*ok_cols)
    return valid, rejects

# COMMAND ----------

good = bronze.limit(1)
bad_age = bronze.limit(1).withColumn("age", F.lit(7))
bad_balance = bronze.limit(1).withColumn("balance", F.lit(-100.0))
bad_country = bronze.limit(1).withColumn("geography", F.lit("Italy"))

test = good.unionByName(bad_age).unionByName(bad_balance).unionByName(bad_country)

v, r = split_valid_rejects(test, rules)
print("valid:", v.count(), "| rejects:", r.count())
r.select("customerid", "age", "balance", "geography", "_failed_rules").show(truncate=False)

# COMMAND ----------

valid, rejects = split_valid_rejects(bronze, rules)

valid.write.format("delta").mode("overwrite").saveAsTable("workspace.churn_lakehouse.silver_churn")
rejects.write.format("delta").mode("overwrite").saveAsTable("workspace.churn_lakehouse.silver_churn_rejects")

print("silver:", spark.table("workspace.churn_lakehouse.silver_churn").count())
print("rejects:", spark.table("workspace.churn_lakehouse.silver_churn_rejects").count())

# COMMAND ----------

# 500 existing customers change country
changed = (bronze.limit(500)
           .withColumn("geography", F.when(F.col("geography") == "France", "Germany").otherwise("France")))

# 100 brand-new customers (new IDs)
new_customers = bronze.limit(100).withColumn("customerid", F.col("customerid") + 50000000)

# 2 bad rows (age 5)
bad = (bronze.limit(2)
       .withColumn("customerid", F.col("customerid") + 90000000)
       .withColumn("age", F.lit(5)))

day2 = changed.unionByName(new_customers).unionByName(bad)

valid2, rejects2 = split_valid_rejects(day2, rules)
print("valid:", valid2.count(), "| rejects:", rejects2.count())

# COMMAND ----------

from delta.tables import DeltaTable

silver = DeltaTable.forName(spark, "workspace.churn_lakehouse.silver_churn")

(silver.alias("t")
    .merge(valid2.alias("s"), "t.customerid = s.customerid")
    .whenMatchedUpdateAll()
    .whenNotMatchedInsertAll()
    .execute())

rejects2.write.format("delta").mode("append").saveAsTable("workspace.churn_lakehouse.silver_churn_rejects")

print("silver:", spark.table("workspace.churn_lakehouse.silver_churn").count())
print("rejects:", spark.table("workspace.churn_lakehouse.silver_churn_rejects").count())

# COMMAND ----------

s = spark.table("workspace.churn_lakehouse.silver_churn").select("customerid", F.col("geography").alias("silver_geo"))
b = bronze.select("customerid", F.col("geography").alias("bronze_geo"))
print(s.join(b, "customerid").filter("silver_geo <> bronze_geo").count())

# COMMAND ----------

print("silver:", spark.table("workspace.churn_lakehouse.silver_churn").count())
print("rejects:", spark.table("workspace.churn_lakehouse.silver_churn_rejects").count())

# COMMAND ----------

from delta.tables import DeltaTable

silver = DeltaTable.forName(spark, "workspace.churn_lakehouse.silver_churn")

(silver.alias("t")
    .merge(valid2.alias("s"), "t.customerid = s.customerid")
    .whenMatchedUpdateAll()
    .whenNotMatchedInsertAll()
    .execute())

print("silver:", spark.table("workspace.churn_lakehouse.silver_churn").count())

# COMMAND ----------

dim = (bronze
       .select("customerid", "geography", "numofproducts", "isactivemember", "hascrcard")
       .withColumn("_hash", F.sha2(F.concat_ws("||",
            F.col("geography"), F.col("numofproducts").cast("string"),
            F.col("isactivemember").cast("string"), F.col("hascrcard").cast("string")), 256))
       .withColumn("effective_from", F.lit("2026-11-01").cast("date"))
       .withColumn("effective_to", F.lit(None).cast("date"))
       .withColumn("is_current", F.lit(True)))

dim.write.format("delta").mode("overwrite").saveAsTable("workspace.churn_lakehouse.dim_customer_scd2")

print(spark.table("workspace.churn_lakehouse.dim_customer_scd2").count())

# COMMAND ----------

silver_now = (spark.table("workspace.churn_lakehouse.silver_churn")
    .select("customerid", "geography", "numofproducts", "isactivemember", "hascrcard")
    .withColumn("_hash", F.sha2(F.concat_ws("||",
         F.col("geography"), F.col("numofproducts").cast("string"),
         F.col("isactivemember").cast("string"), F.col("hascrcard").cast("string")), 256)))

current = (spark.table("workspace.churn_lakehouse.dim_customer_scd2")
    .filter("is_current")
    .select("customerid", F.col("_hash").alias("_old_hash")))

changed = (silver_now.join(current, "customerid", "left")
    .filter("_old_hash IS NULL OR _old_hash <> _hash"))

print("changed or new:", changed.count())
print("brand new:", changed.filter("_old_hash IS NULL").count())

# COMMAND ----------

as_of = "2026-11-02"

to_close = changed.filter("_old_hash IS NOT NULL").withColumn("_merge_key", F.col("customerid"))
to_insert = changed.withColumn("_merge_key", F.lit(None).cast("int"))
updates = to_close.unionByName(to_insert).drop("_old_hash")

print(updates.count())

# COMMAND ----------

from delta.tables import DeltaTable

dim_table = DeltaTable.forName(spark, "workspace.churn_lakehouse.dim_customer_scd2")

(dim_table.alias("t")
    .merge(updates.alias("u"), "t.customerid = u._merge_key AND t.is_current = true")
    .whenMatchedUpdate(set={
        "is_current": "false",
        "effective_to": f"CAST('{as_of}' AS DATE)"})
    .whenNotMatchedInsert(values={
        "customerid": "u.customerid",
        "geography": "u.geography",
        "numofproducts": "u.numofproducts",
        "isactivemember": "u.isactivemember",
        "hascrcard": "u.hascrcard",
        "_hash": "u._hash",
        "effective_from": f"CAST('{as_of}' AS DATE)",
        "effective_to": "CAST(NULL AS DATE)",
        "is_current": "true"})
    .execute())

display(spark.sql("SELECT is_current, COUNT(*) AS versions FROM workspace.churn_lakehouse.dim_customer_scd2 GROUP BY is_current"))

# COMMAND ----------

display(spark.sql("""
    SELECT customerid, geography, effective_from, effective_to, is_current
    FROM workspace.churn_lakehouse.dim_customer_scd2
    WHERE customerid IN (SELECT customerid FROM workspace.churn_lakehouse.dim_customer_scd2
                         GROUP BY customerid HAVING COUNT(*) > 1)
    ORDER BY customerid, effective_from
    LIMIT 10
"""))

# COMMAND ----------

silver = spark.table("workspace.churn_lakehouse.silver_churn")

geo = (silver.groupBy("geography")
       .agg(F.count("*").alias("customers"),
            F.round(F.avg("exited") * 100, 2).alias("churn_rate_pct"),
            F.round(F.sum(F.when(F.col("exited") == 1, F.col("balance"))), 2).alias("balance_lost"))
       .orderBy(F.desc("churn_rate_pct")))

geo.write.format("delta").mode("overwrite").saveAsTable("workspace.churn_lakehouse.gold_churn_by_geography")
display(geo)

# COMMAND ----------

silver = spark.table("workspace.churn_lakehouse.silver_churn")

age = (silver
       .withColumn("age_band",
            F.when(F.col("age") < 30, "<30")
             .when(F.col("age") < 40, "30-39")
             .when(F.col("age") < 50, "40-49")
             .when(F.col("age") < 60, "50-59")
             .otherwise("60+"))
       .groupBy("age_band")
       .agg(F.count("*").alias("customers"),
            F.round(F.avg("exited") * 100, 2).alias("churn_rate_pct"))
       .orderBy("age_band"))

age.write.format("delta").mode("overwrite").saveAsTable("workspace.churn_lakehouse.gold_churn_by_age_band")
display(age)

# COMMAND ----------

risk = (silver.filter("exited = 0 AND isactivemember = 0 AND numofproducts = 1")
        .agg(F.count("*").alias("customers_at_risk"),
             F.round(F.sum("balance"), 2).alias("balance_at_risk")))

risk.write.format("delta").mode("overwrite").saveAsTable("workspace.churn_lakehouse.gold_at_risk_balance")
display(risk)

# COMMAND ----------

display(spark.sql("SHOW TABLES IN workspace.churn_lakehouse"))
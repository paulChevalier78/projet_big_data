import os
import re
import time
import unicodedata
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

t0 = time.time()

# 1. Initialisation Spark + S3A (Onyxia / SeaweedFS)
builder = (
    SparkSession.builder
    .appName("TGV_Regularite_ETL_Medallion")
    .config("spark.hadoop.fs.s3a.endpoint", f"https://{os.environ['AWS_S3_ENDPOINT']}")
    .config("spark.hadoop.fs.s3a.access.key", os.environ["AWS_ACCESS_KEY_ID"])
    .config("spark.hadoop.fs.s3a.secret.key", os.environ["AWS_SECRET_ACCESS_KEY"])
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
)
if os.environ.get("AWS_SESSION_TOKEN"):
    builder = (
        builder
        .config("spark.hadoop.fs.s3a.session.token", os.environ["AWS_SESSION_TOKEN"])
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider")
    )

spark = builder.getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

BUCKET = "s3a://user-p-chevalier-ece/bigdata_project"
RAW_PATH = f"{BUCKET}/raw/regularite-mensuelle-tgv-aqst.csv"
SILVER_PATH = f"{BUCKET}/processed/silver_tgv"
GOLD_ROUTES_PATH = f"{BUCKET}/processed/gold_tgv_routes"
GOLD_SEASON_PATH = f"{BUCKET}/processed/gold_tgv_seasonal"

# --- BRONZE ---
df_raw = (
    spark.read
    .option("header", "true")
    .option("sep", ";")
    .option("multiLine", "true")
    .option("escape", '"')
    .option("inferSchema", "true")
    .csv(RAW_PATH)
)
raw_count = df_raw.count()

# Normalisation des noms de colonnes
def clean_col(name):
    s = unicodedata.normalize("NFKD", name).encode("ascii", "ignore").decode("ascii").lower()
    return re.sub(r"[^a-z0-9]+", "_", s).strip("_")

df = df_raw.toDF(*[clean_col(c) for c in df_raw.columns])

# --- SILVER (Nettoyage & Enrichissement) ---
df_dedup = df.dropDuplicates()
dedup_removed = raw_count - df_dedup.count()

# Filtrage strict : format date YYYY-MM + circulations prevues > 0
df_valid = df_dedup.filter(
    F.col("date").rlike(r"^\d{4}-\d{2}$") &
    (F.col("nombre_de_circulations_prevues") > 0)
)
invalid_removed = df_dedup.count() - df_valid.count()

df_silver = (
    df_valid
    .withColumn("gare_de_depart", F.trim(F.col("gare_de_depart")))
    .withColumn("gare_d_arrivee", F.trim(F.col("gare_d_arrivee")))
    .withColumn("liaison", F.concat_ws(" -> ", F.col("gare_de_depart"), F.col("gare_d_arrivee")))
    .withColumn("annee", F.substring(F.col("date"), 1, 4).cast("int"))
    .withColumn("mois", F.substring(F.col("date"), 6, 2).cast("int"))
    .withColumn(
        "saison",
        F.when(F.col("mois").isin(12, 1, 2), "Hiver")
         .when(F.col("mois").isin(3, 4, 5), "Printemps")
         .when(F.col("mois").isin(6, 7, 8), "Ete")
         .otherwise("Automne")
    )
    .withColumn(
        "taux_annulation_pct",
        F.round((F.col("nombre_de_trains_annules") / F.col("nombre_de_circulations_prevues")) * 100, 2)
    )
    .withColumn(
        "taux_retard_arrivee_pct",
        F.round((F.col("nombre_de_trains_en_retard_a_l_arrivee") / F.col("nombre_de_circulations_prevues")) * 100, 2)
    )
)

silver_count = df_silver.count()
df_silver.write.mode("overwrite").parquet(SILVER_PATH)

# --- GOLD 1 : KPIs par liaison TGV ---
df_gold_routes = (
    df_silver.groupBy("liaison", "service")
    .agg(
        F.sum("nombre_de_circulations_prevues").alias("total_prevus"),
        F.sum("nombre_de_trains_annules").alias("total_annules"),
        F.sum("nombre_de_trains_en_retard_a_l_arrivee").alias("total_retards_arrivee"),
        F.round(F.avg("taux_annulation_pct"), 2).alias("taux_annulation_moy_pct"),
        F.round(F.avg("taux_retard_arrivee_pct"), 2).alias("taux_retard_moy_pct"),
        F.round(F.avg("prct_retard_pour_causes_externes"), 1).alias("cause_externe_pct"),
        F.round(F.avg("prct_retard_pour_cause_infrastructure"), 1).alias("cause_infra_pct"),
        F.round(F.avg("prct_retard_pour_cause_materiel_roulant"), 1).alias("cause_materiel_pct"),
    )
    .filter(F.col("total_prevus") >= 500)
    .orderBy(F.desc("taux_retard_moy_pct"))
)
df_gold_routes.write.mode("overwrite").parquet(GOLD_ROUTES_PATH)

# --- GOLD 2 : Evolution par saison ---
df_gold_season = (
    df_silver.groupBy("saison")
    .agg(
        F.sum("nombre_de_circulations_prevues").alias("total_prevus"),
        F.round(F.avg("taux_retard_arrivee_pct"), 2).alias("taux_retard_moy_pct"),
        F.round(F.avg("taux_annulation_pct"), 2).alias("taux_annulation_moy_pct"),
    )
    .orderBy(F.desc("taux_retard_moy_pct"))
)
df_gold_season.write.mode("overwrite").parquet(GOLD_SEASON_PATH)

print("\n=== RESUME PIPELINE MEDALLION ===")
print(f"Lignes Bronze (raw)        : {raw_count}")
print(f"Doublons supprimes         : {dedup_removed}")
print(f"Lignes invalides ecartees  : {invalid_removed}")
print(f"Lignes Silver (nettoyee)   : {silver_count}")
print(f"Liaisons Gold (>=500 tr)   : {df_gold_routes.count()}")

print("\n=== TOP 10 LIAISONS LES PLUS EN RETARD ===")
df_gold_routes.select("liaison", "total_prevus", "taux_retard_moy_pct", "taux_annulation_moy_pct", "cause_externe_pct", "cause_infra_pct").show(10, truncate=38)

print("\n=== RETARDS PAR SAISON ===")
df_gold_season.show()

print(f"Temps total d'execution ETL : {time.time() - t0:.2f} s")
spark.stop()

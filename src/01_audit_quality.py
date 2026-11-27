import os
import time
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

t0 = time.time()

# Configuration Spark avec accès S3 (Onyxia / SeaweedFS)
builder = (
    SparkSession.builder
    .appName("TGV_Regularite_Audit_Qualite")
    .config("spark.hadoop.fs.s3a.endpoint", f"https://{os.environ['AWS_S3_ENDPOINT']}")
    .config("spark.hadoop.fs.s3a.access.key", os.environ["AWS_ACCESS_KEY_ID"])
    .config("spark.hadoop.fs.s3a.secret.key", os.environ["AWS_SECRET_ACCESS_KEY"])
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
)
if os.environ.get("AWS_SESSION_TOKEN"):
    builder = builder.config("spark.hadoop.fs.s3a.session.token", os.environ["AWS_SESSION_TOKEN"]) \
                     .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider")

spark = builder.getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

RAW_PATH = "s3a://user-p-chevalier-ece/bigdata_project/raw/regularite-mensuelle-tgv-aqst.csv"

df = spark.read.option("header", "true").option("sep", ";").option("inferSchema", "true").csv(RAW_PATH)

total_rows = df.count()
print(f"\n=== 1. APERCU GENERAL ===")
print(f"Nombre total de lignes   : {total_rows}")
print(f"Nombre de colonnes       : {len(df.columns)}")
print(f"Colonnes                 : {df.columns}")

print(f"\n=== 2. DOUBLONS EXACTS ===")
dup_count = total_rows - df.dropDuplicates().count()
print(f"Lignes dupliquees exactes : {dup_count}")

print(f"\n=== 3. VALEURS NULLES PAR COLONNE (uniquement > 0) ===")
null_counts = df.select([F.sum(F.col(f"`{c}`").isNull().cast("int")).alias(c) for c in df.columns]).collect()[0].asDict()
for col_name, cnt in sorted(null_counts.items(), key=lambda x: -x[1]):
    if cnt > 0:
        print(f"  - {col_name}: {cnt} ({cnt/total_rows*100:.2f}%)")

print(f"\n=== 4. EXEMPLES DE LIGNES AVEC ANOMALIES / NULLS ===")
df.show(3, truncate=40)

print(f"\nTemps d'execution audit : {time.time() - t0:.2f} s")
spark.stop()

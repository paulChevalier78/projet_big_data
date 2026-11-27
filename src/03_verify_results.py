import os
import subprocess
import pandas as pd

endpoint = os.environ["S3_ENDPOINT"]
local_dir = "/tmp/verify_tgv"
os.makedirs(local_dir, exist_ok=True)

# 1. Telechargement des Parquet depuis S3
subprocess.run(
    ["aws", "--endpoint-url", endpoint, "s3", "cp",
     "s3://user-p-chevalier-ece/bigdata_project/processed/", local_dir,
     "--recursive", "--quiet"],
    check=True
)

# 2. Lecture independante avec Pandas (hors Spark)
df_silver = pd.read_parquet(f"{local_dir}/silver_tgv")
df_routes = pd.read_parquet(f"{local_dir}/gold_tgv_routes")
df_season = pd.read_parquet(f"{local_dir}/gold_tgv_seasonal")

print("=== VERIFICATION INDEPENDANTE (PANDAS / PARQUET) ===")
print(f"Lignes silver_tgv        : {len(df_silver)}")
print(f"Lignes gold_tgv_routes   : {len(df_routes)}")
print(f"Lignes gold_tgv_seasonal : {len(df_season)}")

if len(df_routes) > 0:
    print("\n=== TOP 5 LIAISONS (GOLD) ===")
    print(df_routes[["liaison", "total_prevus", "taux_retard_moy_pct", "taux_annulation_moy_pct"]].head(5).to_string(index=False))

if len(df_season) > 0:
    print("\n=== SAISONS (GOLD) ===")
    print(df_season.to_string(index=False))

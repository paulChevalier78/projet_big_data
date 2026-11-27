# Analyse de la régularité mensuelle des TGV (SNCF) — Pipeline Big Data sur Onyxia

**Auteur :** Paul Chevalier  
**Namespace Onyxia :** `user-p-chevalier-ece`  
**Cours :** Big Data — ECE Paris (ING5)  
**Plateforme :** Onyxia (`onyxia.adm.adaltas.cloud`) · Stockage S3 SeaweedFS (`s3.seaweedfs.adm.adaltas.cloud`) · PySpark

---

## 1. Problématique métier

Quelles liaisons TGV enregistrent les taux de retard à l'arrivée et d'annulation les plus élevés, quelles sont les causes principales de ces retards (causes externes, infrastructure, matériel roulant), et comment la régularité évolue-t-elle selon les saisons ?

---

## 2. Jeu de données

* **Source :** [data.gouv.fr — Régularité mensuelle TGV par liaisons (SNCF Voyageurs)](https://www.data.gouv.fr/datasets/regularite-mensuelle-tgv-par-liaisons)
* **Fichier brut :** `regularite-mensuelle-tgv-aqst.csv`
* **Taille brute :** 2,91 Mo (`2 909 439` octets) — conforme à la plage imposée (1 à 5 Mo)
* **Volumétrie brute :** `12 907` lignes, `26` colonnes (séparateur `;`)
* **Chemin S3 (Bronze) :** `s3://user-p-chevalier-ece/bigdata_project/raw/regularite-mensuelle-tgv-aqst.csv`

---

## 3. Architecture Medallion & Stack technique

Le pipeline suit une architecture **Medallion (Bronze → Silver → Gold)** exécutée en **PySpark** sur Onyxia, avec stockage objet **S3 (Parquet compressé Snappy)** :

```text
S3 Bronze (CSV brut, 2.91 Mo)
       │
       ▼  [src/01_audit_quality.py]  -> Audit schéma, doublons, valeurs nulles
       ▼  [src/02_etl_pipeline.py]   -> Nettoyage, typage, enrichissement saisonnier
S3 Silver (Parquet, 12 834 lignes)
       │
       ├──► S3 Gold 1 : gold_tgv_routes   (133 liaisons >= 500 trains prévus)
       └──► S3 Gold 2 : gold_tgv_seasonal (4 saisons)
       │
       ▼  [src/03_verify_results.py] -> Vérification indépendante hors Spark (Pandas)
```

---

## 4. Audit de qualité des données & corrections apportées

L'exploration et l'exécution du pipeline sous Spark 4 (mode ANSI strict) ont mis en évidence trois problèmes concrets :

1. **Commentaires multi-lignes dans le CSV :** certains champs texte (`Commentaire retards à l'arrivée`, etc.) contiennent des retours à la ligne, provoquant un décalage de colonnes lors d'une lecture CSV standard (ex. valeur `'du'` retrouvée dans la colonne `Date`, levant une erreur `CAST_INVALID_INPUT`).  
   *Correction :* activation de `.option("multiLine", "true").option("escape", '"')` à la lecture.
2. **Inférence de type sur `Date` :** avec `multiLine=true`, Spark infère `Date` en `DateType` (`YYYY-MM-DD`).  
   *Correction :* cast explicite en `string` et validation par regex `^\d{4}-\d{2}`.
3. **Lignes invalides / incomplètes :** `73` lignes présentaient une date invalide ou un nombre de circulations prévues nul/manquant (`0` doublon exact détecté).  
   *Correction :* filtrage strict avant écriture en couche Silver (`12 907` → `12 834` lignes valides).

---

## 5. Résultats analytiques (Couche Gold)

* **Temps d'exécution total du pipeline ETL (Bronze → Silver → Gold) :** `60.82 s`

### Top 5 des liaisons TGV les plus en retard (liaisons >= 500 trains prévus)

| Liaison | Trains prévus | Taux retard moyen (%) | Taux annulation moyen (%) | Causes externes (%) | Cause infrastructure (%) |
| :--- | :---: | :---: | :---: | :---: | :---: |
| STRASBOURG -> PARIS EST | 727 | 31.54 | 1.22 | 8.7 | 13.9 |
| ITALIE -> PARIS LYON | 7 208 | 30.87 | 4.03 | 25.1 | 13.7 |
| CHAMBERY CHALLES LES EAUX -> PARIS LYON | 22 248 | 26.11 | 3.38 | 26.9 | 18.7 |
| MACON LOCHE -> PARIS LYON | 22 868 | 24.62 | 3.43 | 25.0 | 19.2 |
| LYON PART DIEU -> LILLE | 30 165 | 24.49 | 5.06 | 20.3 | 18.6 |

### Régularité par saison

| Saison | Trains prévus | Taux retard moyen (%) | Taux annulation moyen (%) |
| :--- | :---: | :---: | :---: |
| Été | 952 260 | 16.55 | 2.24 |
| Automne | 833 105 | 14.97 | 1.77 |
| Hiver | 858 209 | 13.73 | 3.06 |
| Printemps | 885 763 | 11.98 | 7.62 |

*Observation :* l'été concentre le taux de retard le plus élevé (`16.55 %`, fort trafic + aléas climatiques), tandis que le printemps enregistre le taux d'annulation le plus marqué (`7.62 %`, impact des mouvements sociaux et travaux saisonniers).

---

## 6. Vérification indépendante

Le script `src/03_verify_results.py` télécharge les fichiers Parquet générés sur S3 et les relit **indépendamment de Spark** avec **Pandas**. Les comptages (`12 834` lignes Silver, `133` liaisons Gold, `4` saisons) et les indicateurs clés concordent à l'identique.

---

## 7. Structure du dépôt & Preuves d'exécution

```text
projet_big_data/
├── README.md
├── src/
│   ├── 01_audit_quality.py      # Audit initial des données brutes sur S3
│   ├── 02_etl_pipeline.py       # Pipeline PySpark Medallion (Bronze -> Silver -> Gold)
│   └── 03_verify_results.py     # Vérification indépendante des Parquet avec Pandas
└── evidence/
    ├── 01_s3_raw.png            # Upload du CSV brut dans S3 (raw/)
    ├── 02_spark_audit.png       # Exécution de l'audit qualité PySpark
    ├── 03_spark_pipeline.png    # Exécution du pipeline ETL Medallion
    ├── 04_s3_processed.png      # Contenu S3 processed/ (Parquet Silver & Gold)
    └── 05_verification.png      # Vérification indépendante Pandas
```

---

## 8. Reproduction du projet sur Onyxia

Depuis un service **Jupyter-pyspark** sur Onyxia (les variables d'environnement S3 sont injectées automatiquement) :

```bash
git clone [https://github.com/paulChevalier78/projet_big_data.git](https://github.com/paulChevalier78/projet_big_data.git)
cd projet_big_data

# 1. Audit qualité
python3 src/01_audit_quality.py

# 2. Pipeline ETL Medallion (écrit les Parquet sur S3)
python3 src/02_etl_pipeline.py

# 3. Vérification indépendante (Pandas)
python3 src/03_verify_results.py
```

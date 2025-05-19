from loguru import logger
from datetime import datetime
import asyncio
from adapters.download_file import download_index, download_files
from core.manipulate_df import simplify_dataframe, get_generation_date_from_csv
from core.update_rcp import update_rcp_pdfs
import os
import shutil
import glob

# Configurer le logger
today: str = datetime.now().strftime("%d-%m-%Y_%H-%M-%S")
logger.add(f"log_{today}.log", rotation="500 KB", level="INFO")  # Log

url_index_file: str = (
    "https://www.ema.europa.eu/en/documents/report/medicines-output-medicines-report_en.xlsx"  # noqa:E501
)
index_file_path: str = "index_file.xlsx"
langage: str = "en"

# Sauvegarder l'ancien CSV avant d'écraser
if os.path.exists("fichier_simplifie.csv"):
    gen_date = get_generation_date_from_csv("fichier_simplifie.csv")
    if gen_date:
        logger.info(f"Ancien fichier trouvé, date de génération : {gen_date}")
        save_name = f"fichier_simplifie_{gen_date}.csv"
    else:
        logger.warning("Aucune date de génération trouvée dans le fichier CSV.")
        save_name = f"fichier_simplifie_{today}.csv"
    shutil.copy("fichier_simplifie.csv", save_name)
    old_csv = save_name
else:
    old_csv = None

# Télécharger le fichier d'index
df_human = asyncio.run(download_index(url_index_file, index_file_path))

# Simplifier le dataframe
df_light = simplify_dataframe(df_human)

# Si on a un ancien CSV, on compare et on update
if old_csv:
    meds_to_update = update_rcp_pdfs(old_csv, "fichier_simplifie.csv")
    # On ne télécharge que ceux qui ont changé
    df_to_download = df_light[df_light["Name"].isin(meds_to_update)]
else:
    df_to_download = df_light

#Télécharger les fichiers
asyncio.run(download_files(langage=langage, df_light=df_light, nb_workers=2))

# Trouver tous les fichiers datés
files = glob.glob("fichier_simplifie_*.csv")
if files:
    files.sort(key=os.path.getmtime, reverse=True)
    # Garde le plus récent, supprime les autres
    for f in files[1:]:
        try:
            os.remove(f)
            logger.info("Suppression de {f}")
        except Exception as e:
            logger.error("Erreur lors de la suppression de {f}: {e}")


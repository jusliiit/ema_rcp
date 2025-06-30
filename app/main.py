from loguru import logger
from datetime import datetime
import asyncio
from adapters.download_file import download_index, download_files
from core.manipulate_df import simplify_dataframe
from core.update_rcp import rename_update_rcp, update_rcp

# Configurer le logger
today_log: str = datetime.now().strftime("%d-%m-%Y_%H-%M-%S")
today: str = datetime.now().strftime("%d-%m-%Y")
logger.add(f"log/log_{today_log}.log", rotation="500 KB", level="INFO")  # Log

url_index_file: str = (
    "https://www.ema.europa.eu/en/documents/report/medicines-output-medicines-report_en.xlsx"  # noqa:E501
)
index_file_path: str = "index_file.xlsx"
language: str = "en"

# Télécharger le fichier d'index des médicaments (Medicine Data Table)
df_authorised, df_withdrawn = asyncio.run(download_index(url_index_file, index_file_path))

# Simplifier les dataframes
df_authorised_light = simplify_dataframe(
    df_authorised,
    path_csv="archives_authorised/simplified_file.csv",
    path_json="list_of_authorised_med.json",
    authorised_names_clean=None)
logger.info("Authorised medicines DataFrame successfully simplified.")

authorised_names_clean = set(df_authorised_light["Name"])

df_withdrawn_light = simplify_dataframe(
    df_withdrawn,
    path_csv="archives_withdrawn/simplified_file.csv",
    path_json="list_of_withdrawn_med.json",
    authorised_names_clean=authorised_names_clean)
logger.info("Withdrawn medicines DataFrame successfully simplified.")

# Renommer les fichiers RCP mis à jour
rename_update_rcp(
    df_authorised_today_path="archives_authorised/simplified_file.csv",
    df_authorised_yesterday_path=f"archives_authorised/simplified_file.csv_{today}.csv"
)
# Mettre à jour les RCP
asyncio.run(update_rcp(
    df_authorised_light,
    language,
    nb_workers=5,
    failed_urls_file="failed_urls_authorised.csv",
    dl_path="ema_authorised_rcp",
    status="Authorised"))

# Télécharger les fichiers PDF authorised
logger.info("Downloading authorised RCP files...")
asyncio.run(download_files(
    language,
    df_authorised_light,
    dl_path="ema_authorised_rcp",
    nb_workers=5,
    failed_urls_file="failed_urls_authorised.csv",
    status="Authorised"))

# Télécharger les fichiers PDF withdrawn
logger.info("Downloading withdrawn RCP files...")
asyncio.run(download_files(
    language,
    df_withdrawn_light,
    dl_path="ema_withdrawn_rcp",
    nb_workers=5,
    failed_urls_file="failed_urls_withdrawn.csv",
    status="Withdrawn"))

logger.info("All tasks completed successfully.")

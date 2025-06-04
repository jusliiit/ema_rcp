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
langage: str = "en"

# Télécharger le fichier d'index des médicaments
df_authorised, df_withdrawn = asyncio.run(download_index(url_index_file, index_file_path))

# Simplifier le dataframe
df_authorised_light = simplify_dataframe(df_authorised, path_csv = "archives_authorised/fichier_simplifie.csv", path_json = "list_of_authorised_med.json")
df_withdrawn_light = simplify_dataframe(df_withdrawn, path_csv = "archives_withdrawn/fichier_simplifie.csv", path_json = "list_of_withdrawn_med.json")

# Renommer les fichiers RCP mis à jour
rename_update_rcp(
    df_authorised_today_path = "archives_authorised/fichier_simplifie.csv",
    df_authorised_yesterday_path = f"archives_authorised/fichier_simplifie_{today}.csv"
)
# Mettre à jour les RCP
asyncio.run(update_rcp(df_authorised_light, langage, nb_workers=5, failed_urls_file="failed_urls_authorised.csv"))

# Télécharger les fichiers PDF
asyncio.run(download_files(langage, df_authorised_light,dl_path = "ema_authorised_rcp", nb_workers=5, failed_urls_file="failed_urls_authorised.csv"))

#Télécharger les fichiers PDF withdrawn
asyncio.run(download_files(langage, df_withdrawn_light, dl_path = "ema_withdrawn_rcp", nb_workers=5, failed_urls_file="failed_urls_withdrawn.csv"))

from loguru import logger
from datetime import datetime
import asyncio
from adapters.download_file import download_index, download_files
from core.manipulate_df import simplify_dataframe
from core.update_rcp import rename_update_rcp, update_rcp

# Configurer le logger
today: str = datetime.now().strftime("%d-%m-%Y_%H-%M-%S")
logger.add(f"log/log_{today}.log", rotation="500 KB", level="INFO")  # Log

url_index_file: str = (
    "https://www.ema.europa.eu/en/documents/report/medicines-output-medicines-report_en.xlsx"  # noqa:E501
)
index_file_path: str = "index_file.xlsx"
langage: str = "en"

# Télécharger le fichier d'index
df_edited = asyncio.run(download_index(url_index_file, index_file_path))

# Simplifier le dataframe
df_light = simplify_dataframe(df_edited)

# Renommer les fichiers RCP mis à jour
rename_update_rcp(
    df_today_path="archives/fichier_simplifie.csv",
    df_yesterday_path=f"archives/fichier_simplifie_{today}.csv"
)
# Télécharger les fichiers
asyncio.run(download_files(langage, df_light, nb_workers=5))

# Mettre à jour les RCP
asyncio.run(update_rcp(df_light, langage, nb_workers=5, failed_urls_file="failed_urls.csv"))


from loguru import logger
from datetime import datetime
import asyncio
from adapters.download_file import download_index, download_files
from core.manipulate_df import simplify_dataframe

# Config vars
today: str = datetime.now().strftime("%d-%m-%Y_%H-%M-%S")
logger.add(f"log_{today}.log", rotation="500 KB", level="INFO")  # Log

url_index_file: str = (
    "https://www.ema.europa.eu/en/documents/report/medicines-output-medicines-report_en.xlsx"  # noqa:E501
)
index_file_path: str = "index_file.xlsx"
langage: str = "en"

# Télécharger le fichier d'index
df_human = asyncio.run(download_index(url_index_file, index_file_path))

# Simplifier le dataframe
df_light = simplify_dataframe(df_human)

#Télécharger les fichiers
asyncio.run(download_files(langage=langage, df_light=df_light, nb_workers=2))


import pandas as pd
import os
import shutil
import asyncio
import aiohttp
from adapters.download_file import download_pdf, retry_failed_downloads
from loguru import logger
from datetime import datetime

today = datetime.now().strftime('%d-%m-%Y')


# Fonction pour renommer les fichiers RCP mis à jour
def rename_update_rcp(
        df_authorised_today_path: str = "archives_authorised/fichier_simplifie.csv",
        df_authorised_yesterday_path: str = f"archives_authorised/fichier_simplifie.csv_{today}.csv"
):
    if not df_authorised_yesterday_path or not os.path.exists(df_authorised_yesterday_path):
        logger.error("No file from the previous day found, nothing to compare.")
        return None

    df_today = pd.read_csv(df_authorised_today_path).set_index("Name")
    df_yesterday = pd.read_csv(df_authorised_yesterday_path).set_index("Name")

    for medoc_name in df_today.index:
        if medoc_name in df_yesterday.index:
            rev_today = df_today.loc[medoc_name, "Revision_nb"]
            rev_yesterday = df_yesterday.loc[medoc_name, "Revision_nb"]
            if rev_today != rev_yesterday:
                file_path = f"ema_authorised_rcp/{medoc_name}.pdf"
                file_old_path = f"ema_authorised_rcp/{medoc_name}_old.pdf"
                if os.path.exists(file_path):
                    shutil.move(file_path, file_old_path)
                    logger.info(f"The file {medoc_name}.pdf has been renamed to {medoc_name}_old.pdf due to an update.")


async def update_rcp(
        df_today: pd.DataFrame,
        langage: str = "en",
        nb_workers: int = 5,
        failed_urls_file: str = "failed_urls_authorised.csv",
        dl_path: str = "ema_authorised_rcp",
        status: str = "authorised"
) -> int:

    sem = asyncio.Semaphore(nb_workers)
    nb_updates = 0

    async with aiohttp.ClientSession() as session:
        tasks = []
        for medoc_name in df_today["Name"]:
            file_old_path = f"ema_authorised_rcp/{medoc_name}_old.pdf"
            file_path = f"ema_authorised_rcp/{medoc_name}.pdf"
            if os.path.exists(file_old_path):
                row = df_today[df_today["Name"] == medoc_name].iloc[0]
                tasks.append(
                    download_pdf(
                        langage,
                        row,
                        medoc_name,
                        len(df_today),
                        dl_path,
                        file_path,
                        session,
                        sem,
                        failed_urls_file,
                        status
                    )
                )
                nb_updates += 1
                logger.info(f"Update #{nb_updates} : RCP for {medoc_name} added to the download list.")
        await asyncio.gather(*tasks)

    while await retry_failed_downloads(failed_urls_file, langage, nb_workers):
        logger.info("Retrying download of failed files.")

    for medoc_name in df_today["Name"]:
        file_path = f"ema_authorised_rcp/{medoc_name}.pdf"
        file_old_path = f"ema_authorised_rcp/{medoc_name}_old.pdf"

        if os.path.exists(file_path) and os.path.exists(file_old_path):
            os.remove(file_old_path)
            logger.info(f"Old RCP file deleted for {medoc_name} (new version downloaded successfully).")

    if nb_updates == 0:
        logger.info("No RCP update was performed.")

    return nb_updates

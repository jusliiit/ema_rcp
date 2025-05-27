import pandas as pd
import os 
import shutil 
import asyncio 
import aiohttp
from adapters.download_file import download_pdf
from loguru import logger

def rename_update_rcp(
        df_today_path: str = "archives/fichier_simplifie.csv",
        df_yesterday_path: str = "archives/fichier_simplifie_{today}.csv"
):
    if not df_yesterday_path or not os.path.exists(df_yesterday_path):
        logger.error(f"Aucun fichier de la veille trouvé, il n'y a rien à comparer.")
        return
    
    df_today = pd.read_csv(df_today_path).set_index("Name")
    df_yesterday = pd.read_csv(df_yesterday_path).set_index("Name")

    for medoc_name in df_today.index:
        if medoc_name in df_yesterday.index: 
            rev_today = df_today.loc[medoc_name, "Revision_nb"]
            rev_yesterday = df_yesterday.loc[medoc_name, "Revision_nb"]
            if rev_today != rev_yesterday:
                pdf_path = f"ema_rcp/{medoc_name}.pdf"
                pdf_old_path = f"ema_rcp/{medoc_name}_old.pdf"
                if os.path.exists(pdf_path):
                    shutil.move(pdf_path, pdf_old_path)
                    logger.info(f"Le fichier {medoc_name}.pdf a été renommé en {medoc_name}_old.pdf en raison d'une mise à jour.")

async def update_rcp(
        df_today : pd.DataFrame, 
        langage: str = "en",
        nb_workers: int = 5,
        failed_urls_file: str = "failed_urls.csv",
):

    sem = asyncio.Semaphore(nb_workers)
    nb_updates = 0 

    async with aiohttp.ClientSession() as session:
        tasks = []
        for medoc_name in df_today["Name"]:
            pdf_old_path = f"ema_rcp/{medoc_name}_old.pdf"
            pdf_path = f"ema_rcp/{medoc_name}.pdf"
            if os.path.exists(pdf_old_path):
                row = df_today[df_today["Name"] == medoc_name].iloc[0]
                tasks.append(
                    download_pdf(
                        langage,
                        row,
                        medoc_name,
                        len(df_today),
                        pdf_path,
                        session,
                        sem,
                        failed_urls_file
                    )
                )
        await asyncio.gather(*tasks)

    for medoc_name in df_today["Name"]:
        pdf_path = f"ema_rcp/{medoc_name}.pdf"
        pdf_old_path = f"ema_rcp/{medoc_name}_old.pdf"

        if os.path.exists(pdf_path) and os.path.exists(pdf_old_path):
            os.remove(pdf_old_path)
            logger.info(f"Le RCP du {medoc_name} est mis à jour, il est supprimé.")

    if nb_updates == 0:
        logger.info("Aucune mise à jour de RCP n'a été effectuée.")


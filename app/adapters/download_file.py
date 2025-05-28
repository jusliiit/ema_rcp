import requests
import pandas as pd
from loguru import logger
import time
import asyncio
import aiohttp
import os
import random
import csv

SPECIAL_CASES = {
    "Arikayce-liposomal": "arikayce-liposomal-product-information",
    # Ajouter d'autres cas spéciaux ici si nécessaire
}

async def download_index(
    url_index_file: str,
    index_file_path: str,
) -> pd.DataFrame:
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}
    try:
        response: requests.Response = requests.get(url_index_file, headers=headers)
        while response.status_code == 429:
            logger.info(
                "Erreur 429: Too much requests... nouvelles tentatives dans 10 secondes"
            )
            time.sleep(10)
            response = requests.get(url_index_file, headers=headers)

        if response.status_code == 200:
            with open(index_file_path, "wb") as f:
                f.write(response.content)
                logger.success(f"Téléchargement réussi depuis {url_index_file}")

                df: pd.DataFrame = pd.read_excel(index_file_path, skiprows=8)
                df_human: pd.DataFrame = df[df["Category"] == "Human"]
                df_edited : pd.DataFrame = df_human[df_human["Medicine status"] == "Authorised"]

                return df_edited
        else:
            logger.error(
                f"Échec du téléchargement - statut {response.status_code} pour {url_index_file}"
            )
            raise RuntimeError
    except Exception as exc:
        logger.exception(f"Erreur: {exc}")
        raise RuntimeError


async def download_pdf(
    langage: str,
    row: pd.Series,
    index: int,
    total_count: int,
    file_path: str,
    session: aiohttp.ClientSession,
    sem: asyncio.Semaphore,
    failed_urls_file: str = "failed_urls.csv",
) -> None:
    nb_retries = 5
    medoc_name = row.Name

    # Construire url_path selon cas spéciaux ou normal
    if medoc_name in SPECIAL_CASES:
        url_path = SPECIAL_CASES[medoc_name]
    else:
        url_path = f"{medoc_name.replace(' ', '-').lower()}-epar-product-information"

    # Construire URL complète
    url = f"https://www.ema.europa.eu/{langage}/documents/product-information/{url_path}_{langage}.pdf"

    # Construire le nom du fichier local (toujours medoc_name, espaces remplacés par des tirets)
    file_name = f"{medoc_name.replace(' ', '-')}.pdf"
    file_path = f"ema_rcp/{file_name}"

    if os.path.exists(file_path):
        logger.info(f"Le fichier {file_path} existe déjà. Téléchargement ignoré.")
        return

    async with sem:
        try:
            logger.info(f"Téléchargement de {index}/{total_count} : {medoc_name}")
            retries = 0
            while retries < nb_retries:
                async with session.get(url) as resp:
                    if resp.status == 200:
                        content = await resp.read()
                        with open(file_path, "wb") as f:
                            f.write(content)
                        logger.success(f"Succès: {medoc_name}")
                        return
                    elif resp.status == 429:
                        logger.warning(
                            f"Erreur 429 (Too Many Requests) pour {medoc_name}. Nouvelle tentative... ({retries + 1}/{nb_retries})"
                        )
                        sleeptime = random.randint(1, 10)
                        await asyncio.sleep(sleeptime)
                        retries += 1
                    else:
                        logger.error(f"Échec {resp.status} pour {medoc_name}")
                        break
            # Si on sort de la boucle sans succès
            logger.error(
                f"Erreur: Nombre maximum de tentatives ({nb_retries}) atteint pour {medoc_name}"
            )
            # Ajouter l'URL échouée au fichier texte
            write_failed_url(failed_urls_file, medoc_name, url)
        except Exception as e:
            logger.exception(f"Exception lors du téléchargement de {medoc_name} : {e}")
            write_failed_url(failed_urls_file, medoc_name, url)

def write_failed_url(failed_urls_file: str, medoc_name: str, url: str):
    file_exists = os.path.exists(failed_urls_file)
    with open(failed_urls_file, "a", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        if not file_exists:
            writer.writerow(["Name", "URL"])
        writer.writerow([medoc_name, url])

async def retry_failed_downloads(
    failed_urls_file: str = "failed_urls.csv",
    langage: str = "en",
    nb_workers: int = 5,
) -> bool:
    if not os.path.exists(failed_urls_file):
        logger.info("Aucun fichier failed_urls.csv trouvé. Aucun téléchargement à réessayer.")
        return False
    
    df_failed = pd.read_csv(failed_urls_file)
    if df_failed.empty: 
        logger.info("Le fichier failed_urls.csv est vide. Aucun téléchargement échoué à réessayer.")
        return False
     
    total_count = len(df_failed)
    os.makedirs("ema_rcp", exist_ok=True)

#Téléchargement des fichiers échoués
    sem = asyncio.Semaphore(nb_workers)
    async with aiohttp.ClientSession() as session:
        tasks = []
        for idx, row in enumerate(df_failed.itertuples(), 1):
            medoc_name = row.Name
            file_path = f"ema_rcp/{medoc_name}.pdf"
            tasks.append(
                download_pdf(
                    langage,
                    row,
                    idx,
                    total_count,
                    file_path,
                    session,
                    sem,
                    failed_urls_file,
                )
            )
        await asyncio.gather(*tasks)

    clean_failed_urls(failed_urls_file)
    if not os.path.exists(failed_urls_file):
        logger.info("Tous les fichiers ont été téléchargés avec succès. Suppression du fichier failed_urls.csv.")
        return False
    df_failed = pd.read_csv(failed_urls_file)
    return not df_failed.empty

def clean_failed_urls(failed_urls_file: str):
    if not os.path.exists(failed_urls_file):
        logger.info("Aucun fichier failed_urls.csv trouvé. Aucun nettoyage à effectuer.")
        return False
    
    df_failed = pd.read_csv(failed_urls_file)
    df_failed = df_failed[~df_failed["Name"].apply(lambda medoc_name: os.path.exists(f"ema_rcp/{medoc_name}.pdf"))]

#S'il est vide, il n'y a plus rien à réessayer, on le supprime 
    if df_failed.empty:
        os.remove(failed_urls_file)
        logger.info("Tous les fichiers ont été téléchargés, suppression du fichier failed_urls.csv.")
    else:
        df_failed.to_csv(failed_urls_file, index=False)
        logger.info(f"{len(df_failed)} fichiers restent à télécharger.")

# Fonction principale pour télécharger les fichiers PDF
async def download_files(
    langage: str,
    df_light: pd.DataFrame,
    nb_workers: int,
):
    total_count = len(df_light)
    os.makedirs("ema_rcp", exist_ok=True)
    failed_urls_file : str = "failed_urls.csv"

    sem = asyncio.Semaphore(nb_workers)
    async with aiohttp.ClientSession() as session:
        tasks = []
        for idx, row in enumerate(df_light.itertuples(), 1):
            medoc_name = row.Name
            file_path = f"ema_rcp/{medoc_name}.pdf"
            tasks.append(
                download_pdf(
                    langage,
                    row,
                    idx,
                    total_count,
                    file_path,
                    session,
                    sem,
                    failed_urls_file,
                )
            )
        await asyncio.gather(*tasks)

    while await retry_failed_downloads("failed_urls.csv", langage, nb_workers):
        logger.info("Nouvelle tentative pour les fichiers échoués...")

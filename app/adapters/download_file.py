import requests
import pandas as pd
from loguru import logger
import time
import asyncio
import aiohttp
import os
import random

SPECIAL_CASES_AUTHORISED = {
    "Arikayce-liposomal": "arikayce-liposomal-product-information",   
}

SPECIAL_CASES_WITHDRAWN = {
    "Budesonide-formoterol-teva-pharma-bv": "budesonideformoterol-teva-epar-product-information",
    "Pandemic-influenza-vaccine-h5n1-baxter-ag": "pandemic-influenza-vaccine-h5n1-baxter-epar-product-information",
    "Lamivudine-zidovudine-teva": "lamivudinezidovudine-teva-epar-product-information",
}


async def download_index(
    url_index_file: str,
    index_file_path: str,
) -> pd.DataFrame:
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"}  # noqa: E501
    try:
        response: requests.Response = requests.get(url_index_file, headers=headers)
        while response.status_code == 429:
            logger.info("Error 429 : Too many requests. Retrying in 10 seconds...")
            time.sleep(10)
            response = requests.get(url_index_file, headers=headers)

        if response.status_code == 200:
            with open(index_file_path, "wb") as f:
                f.write(response.content)
                logger.success(f"Download succesful from {url_index_file}")
                df: pd.DataFrame = pd.read_excel(index_file_path, skiprows=8)
                df_human: pd.DataFrame = df[df["Category"] == "Human"]
                df_authorised: pd.DataFrame = df_human[df_human["Medicine status"] == "Authorised"]
                df_withdrawn: pd.DataFrame = df_human[df_human["Medicine status"].isin(["Withdrawn", "Withdrawn from rolling review"])]
                return df_authorised, df_withdrawn
        else:
            logger.error(f"Download failed - status {response.status_code} for {url_index_file}")
            raise RuntimeError
    except Exception as exc:
        logger.exception(f"Error: {exc}")
        raise RuntimeError


async def download_pdf(
        langage: str,
        row: pd.Series,
        index: int,
        total_count: int,
        dl_path: str,
        file_path: str,
        session: aiohttp.ClientSession,
        sem: asyncio.Semaphore,
        failed_urls_file: str,
        status: str # "authorised" or "withdrawn"
) -> None:
    nb_retries = 5
    medoc_name = row.Name
    echec = False

    if status == "Authorised":
        SPECIAL_CASES = SPECIAL_CASES_AUTHORISED
    else:
        SPECIAL_CASES = SPECIAL_CASES_WITHDRAWN

    if medoc_name in SPECIAL_CASES:
        url_path = SPECIAL_CASES[medoc_name]
    else:
        url_path = f"{medoc_name.replace(' ', '-').lower()}-epar-product-information"

    url = f"https://www.ema.europa.eu/{langage}/documents/product-information/{url_path}_{langage}.pdf"
    file_name = f"{medoc_name.replace(' ', '-')}.pdf"
    file_path = f"{dl_path}/{file_name}"
    if os.path.exists(file_path):
        logger.info(f"The file {file_path} already exists. Download skipped.")
        return

    not_found_file = "not_found_urls.csv"
    if os.path.exists(not_found_file):
        df_not_found = pd.read_csv(not_found_file)
        if (df_not_found["Name"] == medoc_name).any():
            logger.info(f"{medoc_name} already marked as not found. Download skipped.")
            return

    async with sem:
        try:
            logger.info(f"Downloading {index}/{total_count} : {medoc_name}")
            retries = 0
            while retries < nb_retries:
                async with session.get(url) as resp:
                    if resp.status == 200:
                        content = await resp.read()
                        with open(file_path, "wb") as f:
                            f.write(content)
                        logger.success(f"Success: {medoc_name}")
                        return
                    elif resp.status == 404:
                        logger.error(f"Error 404 for {medoc_name}")
                        not_found_file = "not_found_urls.csv"
                        df_404 = pd.DataFrame([[medoc_name, status, url]], columns=["Name", "Status","Url"])
                        echec = False
                        if os.path.exists(not_found_file):
                            df_404_existing = pd.read_csv(not_found_file)
                        else:
                            df_404_existing = pd.DataFrame(columns=["Name", "Status", "Url"])
                        if not (df_404_existing["Name"] == medoc_name).any():
                            df_404_final = pd.concat([df_404_existing, df_404], ignore_index=True)
                            df_404_final.to_csv(not_found_file, index=False)
                            logger.info(f"{medoc_name} recorded in {not_found_file}")
                        return
                    elif resp.status == 429:
                        logger.warning(f"Error 429 : too many requests for {medoc_name}. Retrying...({retries + 1}/{nb_retries})")
                        sleeptime = random.randint(1, 15)
                        await asyncio.sleep(sleeptime)
                        retries += 1
            if not echec:
                logger.error(f"Error: Maximum number of retries ({nb_retries}) reached for {medoc_name}")
                echec = True
        except Exception as e:
            logger.exception(f"Exception during download of {medoc_name} : {e}")
            echec = True

        if echec:
            df_echec = pd.DataFrame([[medoc_name, status, url]], columns=["Name", "Status", "Url"])
            if os.path.exists(failed_urls_file):
                df_echec_existing = pd.read_csv(failed_urls_file)
            else:
                df_echec_existing = pd.DataFrame(columns=["Name", "Status", "Url"])
            if not (df_echec_existing["Name"] == medoc_name).any():
                df_echec_final = pd.concat([df_echec_existing, df_echec], ignore_index=True)
                df_echec_final.to_csv(failed_urls_file, index=False)
                logger.info(f"{medoc_name} recorded in {failed_urls_file}")


async def retry_failed_downloads(
        failed_urls_file: str,
        dl_path: str,
        status: str,
        langage: str = "en",
        nb_workers: int = 3,
        not_found_file: str = "not_found_urls.csv",
        ) -> bool:

    if not os.path.exists(failed_urls_file):
        logger.info("No failed_urls.csv file found. No downloads to retry.")
        return False

    df_failed = pd.read_csv(failed_urls_file)
    if df_failed.empty:
        logger.info("The failed_urls.csv file is empty. No downloads to retry.")
        return False

    total_count = len(df_failed)
    os.makedirs(dl_path, exist_ok=True)

    # Telechargement des fichiers échoués
    sem = asyncio.Semaphore(nb_workers)
    async with aiohttp.ClientSession() as session:
        tasks = []
        for idx, row in enumerate(df_failed.itertuples(), 1):
            medoc_name = row.Name
            file_path = f"{dl_path}/{medoc_name}.pdf"
            tasks.append(download_pdf(
                    langage,
                    row,
                    idx,
                    total_count,
                    dl_path,
                    file_path,
                    session,
                    sem,
                    failed_urls_file,
                    status))
        await asyncio.gather(*tasks)

    if os.path.exists(failed_urls_file):
        df_failed = pd.read_csv(failed_urls_file)

    # Supprimer ceux qui ont été téléchargés
        df_failed = df_failed[~df_failed["Name"].apply(lambda medoc_name: os.path.exists(f"{dl_path}/{medoc_name}.pdf"))]

    # Supprimer ceux en 404
    if os.path.exists(not_found_file):
        df_404 = pd.read_csv(not_found_file)
        df_failed = df_failed[~df_failed["Name"].isin(df_404["Name"])]

    # S'il ne reste rien, on supprime le fichier
    if df_failed.shape[0] == 0:
        os.remove(failed_urls_file)
        logger.info("All files have been downloads, deleting the failed_urls.csv file.")
        return False
    else:
        # Sinon, on met à jour la liste
        df_failed.to_csv(failed_urls_file, index=False)
        logger.info(f"{len(df_failed)} files remain to be downloaded.")
        return True


# Fonction principale pour télécharger les fichiers PDF
async def download_files(
    langage: str,
    df_light: pd.DataFrame,
    dl_path: str,
    nb_workers: int,
    failed_urls_file: str,
    status: str 
):
    total_count = len(df_light)
    os.makedirs(dl_path, exist_ok=True)

    sem = asyncio.Semaphore(nb_workers)
    async with aiohttp.ClientSession() as session:
        tasks = []
        for idx, row in enumerate(df_light.itertuples(), 1):
            medoc_name = row.Name
            file_path = f"{dl_path}/{medoc_name}.pdf"
            tasks.append(
                download_pdf(
                    langage,
                    row,
                    idx,
                    total_count,
                    dl_path,
                    file_path,
                    session,
                    sem,
                    failed_urls_file,
                    status
                )
            )
        await asyncio.gather(*tasks)

    while await retry_failed_downloads(failed_urls_file, dl_path, status, langage, nb_workers, not_found_file="not_found_urls.csv"):
        logger.info("Retrying failed files...")

import requests
import pandas as pd
from loguru import logger
import time
import asyncio
import aiohttp
import os
import random

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
                df_human = df_human[df_human["Medicine status"] == "Authorised"]

                return df_human
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
    failed_urls_file: str = "failed_urls.txt",
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
    file_path = f"docs/{file_name}"

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
            with open(failed_urls_file, "a") as f:
                f.write(f"{medoc_name} {url}\n")
        except Exception as e:
            logger.exception(f"Exception lors du téléchargement de {medoc_name} : {e}")
            with open(failed_urls_file, "a") as f:
                f.write(f"{medoc_name} {url}\n")


async def retry_failed_downloads(
    failed_urls_file: str,
    nb_workers: int,
) -> bool:
    if not os.path.exists(failed_urls_file):
        logger.info(
            "Aucun fichier failed_urls.txt trouvé. Aucun téléchargement à réessayer."
        )
        return False

    with open(failed_urls_file, "r") as f:
        lines = [line.strip() for line in f.readlines() if line.strip()]

    if not lines:
        logger.info(
            "Le fichier failed_urls.txt est vide. Aucun téléchargement à réessayer."
        )
        return False

    sem = asyncio.Semaphore(nb_workers)
    async with aiohttp.ClientSession() as session:
        tasks = []
        for line in lines:
            try:
                medoc_name, url = line.split(maxsplit=1)
            except ValueError:
                logger.error(f"Ligne mal formée dans {failed_urls_file} : {line}")
                continue
            file_path = f"docs/{medoc_name}.pdf"
            # Construire un faux row pour passer à download_pdf (on peut juste créer un pd.Series avec Name)
            fake_row = pd.Series({"Name": medoc_name})
            tasks.append(
                download_pdf(
                    "en", fake_row, 0, 0, file_path, session, sem, failed_urls_file
                )
            )
        await asyncio.gather(*tasks)

    # Après réessai, nettoyer les URLs qui ont été téléchargées
    # Lecture du fichier failed_urls.txt et suppression des lignes pour lesquelles le fichier existe désormais
    remaining = []
    with open(failed_urls_file, "r") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            medoc_name = line.split(maxsplit=1)[0]
            file_path = f"docs/{medoc_name}.pdf"
            if not os.path.exists(file_path):
                remaining.append(line)

    if remaining:
        with open(failed_urls_file, "w") as f:
            f.write("\n".join(remaining) + "\n")
        return True
    else:
        os.remove(failed_urls_file)
        logger.info("Tous les téléchargements échoués ont été réessayés avec succès.")
        logger.info("Téléchargements terminés.")
        return False


async def download_files(
    langage: str,
    df_light: pd.DataFrame,
    nb_workers: int = 5,
):
    total_count = len(df_light)
    os.makedirs("docs", exist_ok=True)

    sem = asyncio.Semaphore(nb_workers)
    async with aiohttp.ClientSession() as session:
        tasks = []
        for idx, row in enumerate(df_light.itertuples(), 1):
            medoc_name = row.Name
            file_path = f"docs/{medoc_name}.pdf"
            tasks.append(
                download_pdf(
                    langage,
                    row,
                    idx,
                    total_count,
                    file_path,
                    session,
                    sem,
                    "failed_urls.txt",
                )
            )
        await asyncio.gather(*tasks)

    # Réessayer les téléchargements échoués tant que le fichier failed_urls.txt n'est pas vide
    while await retry_failed_downloads("failed_urls.txt", nb_workers):
        logger.info("Nouvelle tentative pour les fichiers échoués...")

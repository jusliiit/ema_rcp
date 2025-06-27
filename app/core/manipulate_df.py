import re
import pandas as pd
from loguru import logger
from datetime import datetime
import shutil
import os


def clean_name_authorised(name: str) -> str:

    # Remplacer les apostrophes typographiques par une apostrophe simple
    name_edit_authorised = name.replace("’", "'").replace("'", "")

    # Supprimer toute la partie entre parenthèses commençant par "in"
    name_edit_authorised = re.sub(r'\(in [^)]+\)', '', name_edit_authorised)

    # Supprimer toute la partie (previously ...)
    name_edit_authorised = re.sub(r'\(previously.*\)', '', name_edit_authorised)

    # Supprimer toutes les parenthèses restantes mais garder leur contenu
    name_edit_authorised = re.sub(r'[()]', '', name_edit_authorised)

    # Remplacer "/" par espace
    name_edit_authorised = name_edit_authorised.replace("/", " ")

    # Supprimer les points
    name_edit_authorised = name_edit_authorised.replace(".", "")

    # Remplacer les virgules, deux-points, points-virgules par des espaces
    name_edit_authorised = re.sub(r"[,;:]", " ", name_edit_authorised)

    # Remplacer plusieurs espaces par un seul espace
    name_edit_authorised = re.sub(r'\s+', ' ', name_edit_authorised).strip()

    # Mettre en majuscule la première lettre de chaque mot
    name_edit_authorised = name_edit_authorised.capitalize()

    # Découper en mots et filtrer petits mots inutiles (optionnel)
    words_to_remove = {'a', 'the', 'of', 'and', 'in', 'on', 'for'}
    words = [w for w in name_edit_authorised.split() if w not in words_to_remove]

    # Remettre en forme avec des tirets
    name_edit_authorised = '-'.join(words)
    return name_edit_authorised


def clean_name_withdrawn(name: str,
                         name_edit_authorised: str) -> str:

    # Remplacer les apostrophes typographiques par une apostrophe simple
    name_edit_withdrawn = name.replace("’", "'").replace("'", "")

    # Supprimer toute la partie entre parenthèses commençant par "in"
    name_edit_withdrawn = re.sub(r'\(in [^)]+\)', '', name_edit_withdrawn)

    # Supprimer toute la partie (previously ...)
    name_edit_withdrawn = re.sub(r'\(previously.*\)', '', name_edit_withdrawn)
    
    # Supprimer toutes les parenthèses restantes mais garder leur contenu
    name_edit_withdrawn = re.sub(r'[()]', '', name_edit_withdrawn)
   
    # Remplacer "/" par espace
    name_edit_withdrawn = name_edit_withdrawn.replace("/", " ")

    # Supprimer les points
    name_edit_withdrawn = name_edit_withdrawn.replace(".", "")

    # Remplacer les virgules, deux-points, points-virgules par des espaces
    name_edit_withdrawn = re.sub(r"[,;:]", " ", name_edit_withdrawn)

    # Remplacer plusieurs espaces par un seul espace
    name_edit_withdrawn = re.sub(r'\s+', ' ', name_edit_withdrawn).strip()

    # Mettre en majuscule la première lettre de chaque mot
    name_edit_withdrawn = name_edit_withdrawn.capitalize()

    # Découper en mots et filtrer petits mots inutiles (optionnel)
    words_to_remove = {'a', 'the', 'of', 'and', 'in', 'on', 'for'}
    words = [w for w in name_edit_withdrawn.split() if w not in words_to_remove]

    # Remettre en forme avec des tirets
    name_edit_withdrawn = '-'.join(words)

    # Si le nom est identique à l'authorised
    if name_edit_withdrawn == name_edit_authorised:
        logger.warning(f"The name {name_edit_withdrawn} is identical for both authorised and withdrawn medicines.")
        # Chercher la partie (previously ...)
        match = re.search(r'\(previously ([^)]+)\)', name, re.IGNORECASE)
        if match:
            previously = match.group(1)
            # Nettoyer et formater la partie previously
            previously_clean = '-'.join([w.strip() for w in previously.split()])
            name_edit_withdrawn = f"{name_edit_withdrawn}-{previously_clean.lower()}"
        else:
            name_edit_withdrawn += "-0"
    return name_edit_withdrawn
    
# Fonction pour simplifier le DataFrame
def simplify_dataframe(
    df: pd.DataFrame,
    path_csv: str,
    path_json: str,
    authorised_names_clean: set = None
) -> pd.DataFrame:

    try:
        columns_to_keep: list[str] = [
            "Name of medicine",
            "Revision number",
            "Medicine status",
        ]
        df_light: pd.DataFrame = df.loc[:, columns_to_keep]
        df_light = df_light.rename(
            columns={
                "Name of medicine": "Name",
                "Revision number": "Revision_nb",
                "Medicine status": "Status",
            }
        )
        df_light["Revision_nb"] = df_light["Revision_nb"].fillna(0)
        df_light["Revision_nb"] = df_light["Revision_nb"].astype(
            "int"
        )

        # Correction du nom
        if df_light["Status"].iloc[0] == "Authorised":
            df_light["Name"] = df_light["Name"].apply(clean_name_authorised)
            logger.success("Successfully simplified the Excel file for authorised medicines.")
            df_light.to_json(path_json, orient="records")
        else:
            # On suppose que authorised_names_clean est passé en argument
            def clean_withdrawn_row(name):
                name_clean = clean_name_authorised(name)
                name_edit_authorised = name_clean if authorised_names_clean and name_clean in authorised_names_clean else ""
                return clean_name_withdrawn(name, name_edit_authorised)
            df_light["Name"] = df_light["Name"].apply(clean_withdrawn_row)
            df_light.to_json(path_json, orient="records")
            logger.success("Successfully simplified the Excel file for withdrawn medicines.")

# Avant d'écraser, archive l'ancien fichier s'il existe
        today: str = datetime.now().strftime("%d-%m-%Y")
        os.makedirs(os.path.dirname(path_csv), exist_ok=True)
        if os.path.exists(path_csv):
            shutil.copy(path_csv, f"{path_csv}_{today}.csv")

        df_light.to_csv(path_csv, index=False)
        logger.success("Successfully archived and saved the new simplified file.")
        return df_light

    except Exception as exc:
        logger.exception(f"Error: {exc}")
    raise RuntimeError

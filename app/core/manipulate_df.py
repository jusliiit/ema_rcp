import re
import pandas as pd
from loguru import logger
from datetime import datetime
import shutil
import os

def clean_name(name: str) -> str:

    # Remplacer les apostrophes typographiques par une apostrophe simple
    name_edit = name.replace("’", "'").replace("'", "")

    # Supprimer toute la partie entre parenthèses commençant par "in"
    name_edit = re.sub(r'\(in [^)]+\)', '', name_edit)

    # Supprimer toute la partie (previously ...)
    name_edit = re.sub(r'\(previously.*\)', '', name_edit)

    # Supprimer toutes les parenthèses restantes mais garder leur contenu
    name_edit = re.sub(r'[()]', '', name_edit)

    # Remplacer "/" par espace
    name_edit = name_edit.replace("/", " ")

    # Supprimer les points
    name_edit = name_edit.replace(".", "")

     # Remplacer les virgules, deux-points, points-virgules par des espaces (pour éviter les tirets collés)
    name_edit = re.sub(r"[,;:]", " ", name_edit)

    # Remplacer plusieurs espaces par un seul espace
    name_edit = re.sub(r'\s+', ' ', name_edit).strip()

    # Mettre en majuscule la première lettre de chaque mot
    name_edit = name_edit.capitalize()

    # Découper en mots et filtrer petits mots inutiles (optionnel)
    words_to_remove = {'a', 'the', 'of', 'and', 'in', 'on', 'for'}
    words = [w for w in name_edit.split() if w not in words_to_remove]

    # Remettre en forme avec des tirets 
    name_edit = '-'.join(words)

    return name_edit

# Fonction pour simplifier le DataFrame
def simplify_dataframe(
    df: pd.DataFrame,
    path_csv: str,
    path_json: str,
) -> pd.DataFrame:
    try:
        columns_to_keep: list[str] = [
            "Name of medicine",
            "Revision number",
        ]
        df_light: pd.DataFrame = df.loc[:, columns_to_keep]
        df_light = df_light.rename(
            columns={
                "Name of medicine": "Name",
                "Revision number": "Revision_nb"
            }
        )
        df_light["Revision_nb"] = df_light["Revision_nb"].fillna(0)
        df_light["Revision_nb"] = df_light["Revision_nb"].astype(
            "int"
        )

        # Correction du nom
        df_light["Name"] = df_light["Name"].apply(clean_name)
        logger.success("Réussite de la simplification du fichier excel")
        df_light.to_json(path_json, orient="records")
    
# Avant d'écraser, archive l'ancien fichier s'il existe 
        today: str = datetime.now().strftime("%d-%m-%Y")
        os.makedirs(os.path.dirname(path_csv), exist_ok=True)
        if os.path.exists(path_csv):
            shutil.copy(path_csv, f"{path_csv}_{today}.csv")
            
        df_light.to_csv(path_csv, index=False)
        logger.success("Réussite de l'archivage et de la sauvegarde du nouveau fichier simplifié")
        return df_light
    
    except Exception as exc:
        logger.exception(f"Erreur: {exc}")
        raise RuntimeError
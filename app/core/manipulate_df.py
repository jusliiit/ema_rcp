import re
import pandas as pd
from loguru import logger
from datetime import datetime


def clean_name(name: str) -> str:
    # Supprimer toute la partie entre parenthèses commençant par "in"
    name_edit = re.sub(r'\(in [^)]+\)', '', name)

    # Supprimer toute la partie (previously ...)
    name_edit = re.sub(r'\(previously.*\)', '', name_edit)

    # Supprimer toutes les parenthèses restantes mais garder leur contenu
    name_edit = re.sub(r'[()]', '', name_edit)

    # Remplacer "/" par espace
    name_edit = name_edit.replace("/", " ")

    # Supprimer les points
    name_edit = name_edit.replace(".", "")

    # Remplacer plusieurs espaces par un seul espace
    name_edit = re.sub(r'\s+', ' ', name_edit).strip()

    # Passer en minuscules
    name_edit = name_edit.capitalize()

    # Découper en mots et filtrer petits mots inutiles (optionnel)
    words_to_remove = {'a', 'the', 'of', 'and', 'in', 'on'}
    words = [w for w in name_edit.split() if w not in words_to_remove]

    # Remettre en forme avec des tirets
    name_edit = '-'.join(words)

    return name_edit



def simplify_dataframe(
    df: pd.DataFrame,
) -> pd.DataFrame:
    try:
        columns_to_keep: list[str] = [
            "Name of medicine",
            "Revision number",
            "Last updated date"
        ]
        df_light: pd.DataFrame = df.loc[:, columns_to_keep]
        df_light = df_light.rename(
            columns={
                "Name of medicine": "Name",
                "Revision number": "Revision_nb",
                "Last updated date": "Last_updated_date",
            }
        )
        df_light["Revision_nb"] = df_light["Revision_nb"].fillna(0)
        df_light["Revision_nb"] = df_light["Revision_nb"].astype(
            "int"
        )
        df_light["Last_updated_date"] = pd.to_datetime(
            df_light["Last_updated_date"], errors="coerce"
        )
        df_light["Last_updated_date"] = df_light[
            "Last_updated_date"
        ].dt.strftime(
            "%d-%m-%Y"
        )

        # Correction du nom
        df_light["Name"] = df_light["Name"].apply(clean_name)
        logger.success("Réussite de la simplification du fichier excel")
        df_light.to_json("list_of_medic.json", orient="records")

        #Ajoute la colonne de date de génération
        generation_date = datetime.now().strftime("%d-%m-%Y-%H-%M-%S")
        df_light["Generated_on"] = generation_date

        # Sauvegarde le fichier simplifié
        df_light.to_csv("fichier_simplifie.csv", index=False)
        logger.success("Réussite de la sauvegarde du fichier simplifié")
        return df_light
    
    except Exception as exc:
        logger.exception(f"Erreur: {exc}")
        raise RuntimeError


def get_generation_date_from_csv(csv_path):
    try:
        df = pd.read_csv(csv_path)
        if "Generated_on" in df.columns:
            return df["Generated_on"].iloc[0]
        else:
            logger.error("La colonne 'Generated on' n'existe pas dans le fichier CSV.")
            return None
    except Exception as e:
        logger.exception(f"Erreur lors de la lecture du fichier CSV : {e}")
        return None

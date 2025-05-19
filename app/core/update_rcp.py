import os
import pandas as pd 
from loguru import logger

#Detecter les médicaments mis à jour entre deux fichiers CSV
def detect_updated_rcp(
        old_csv_path: str, 
        new_csv_path: str) -> list[dict]:
    logger.info("Début détection des médicaments mis à jour entre les fichiers CSV")
    try:
        old_df = pd.read_csv(old_csv_path)
        new_df = pd.read_csv(new_csv_path)
        logger.success(f"Fichiers CSV chargés : '{old_csv_path}', '{new_csv_path}'")
    except Exception as e:
        logger.exception(f"Erreur lors de la lecture des fichiers CSV : {e}")
        raise

    #Garder uniquement les colonnes nécessaires
    merged_df = pd.merge(
        old_df[["Name", "Revision_nb"]],
        new_df[["Name", "Revision_nb"]],
        on="Name",
        how="inner",
        suffixes=("_old", "_new"),
    ) ## useless
    ## ranger par ordre alphabétique (sort) puis verifier que les noms sont pareil. Si pas pareil > telecharger le nouveau, Si pareil, tu compare les versions. Si version !=, tu ajoute quelques part
    # Comparer les numéros de révision et creer la liste des médicaments mis à jour
    changed = merged_df[merged_df["Revision_nb_old"] != merged_df["Revision_nb_new"]]
    updated_list = []
    for _, row in changed.iterrows():
        updated_list.append(
            {
                "Name": row["Name"],
                "Revision_nb": row["Revision_nb_new"],
            })
    logger.info(f"{len(updated_list)} médicaments détectés comme mis à jour")
    return updated_list
        
#Supprimer l'ancien PDF des médicaments à mettre à jour
def delete_old_pdf(med_name: str, docs_dir: str = "docs"):
    file_name = f"{med_name.replace(' ', '-')}.pdf"
    file_path = os.path.join(docs_dir, file_name)
    logger.info(f"Tentative de suppression du PDF : {file_path}")
    try:
        if os.path.exists(file_path):
            os.remove(file_path)
            logger.info(f"Ancien PDF supprimé : {file_path}")
        else:
            logger.info(f"Aucun PDF à supprimer pour : {file_name}")
    except Exception as e:
        logger.exception(f"Erreur lors de la suppression du fichier {file_path} : {e}")

def update_rcp_pdfs(
    old_csv_path: str,
    new_csv_path: str,
    docs_dir: str = "docs"
) -> list[str]:

    logger.info("Début de la mise à jour des RCP PDFs")
    updated_meds = detect_updated_rcp(old_csv_path, new_csv_path)
    meds_to_update = []
    for med in updated_meds:
        delete_old_pdf(med["Name"], docs_dir)
        meds_to_update.append(med["Name"])
    logger.success(f"{len(meds_to_update)} médicaments à mettre à jour : {meds_to_update}")
    return meds_to_update
    ## L'ancien tu le renomme _old, pour éviter de tout perdre
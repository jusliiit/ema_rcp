# Technical Documentation - EMA SmPC Scraping

## 📰 **Context and Objective**

This Python project automatically downloads the PDF files of SmPCs (Summary of Product Characteristics) for human medicines authorized and withdrawn from the European market from the EMA website. 
The goal is to build a reliable and usable database to speed up access to this information.

---

## 📚 **Project Organization**

The code is organized into several folders:

- **`adapters`**: contains "adapters" to the outside world (e.g., website connection, downloads)
    - `download_file.py`
- **`core`**: contains the pure business logic (independent from the outside world)
    - `update_rcp.py`
    - `manipulate_df.py`
- **`main.py`**: entry point of the program that orchestrates everything

---

## 🛠️ **How `main.py` Works**

### 1. **Importing Tools**
See the requirements.txt file to install all the necessary dependencies for proper use of the Python code.

---

### 2. Initial launch 


--- 

### 3. Scheduled daily execution 



---


## 🔧 **Key Functions to Remember**

- `download_index`: Downloads and filters the medicines index
- `simplify_dataframe`: Cleans and simplifies the data
- `rename_update_rcp`: Renames old PDFs if updated
- `update_rcp`: Downloads updated SmPCs
- `download_files`: Downloads new SmPCs (authorized and withdrawn)
- `download_pdf`: Handles individual downloads with error management
- `retry_failed_downloads`: Retries failed downloads

---

## **Authors**

Rouanet Julie  
5th year pharmacy student, industrial track, University of Bordeaux  
julierouanet5@gmail.com
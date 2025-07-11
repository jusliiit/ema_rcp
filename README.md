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
- The first run takes about **5 hours** and downloads **all RCPs**.
- Open a command prompt and run:

/c "cd /d {path_to_project} && python app\main.py"

**Example:**
/c "cd /d C:\Users\rouajul\Desktop\ema_rcp-main && python app\main.py"

- This command executes `main.py`, the central orchestrator of the project.
- ✅ Check the terminal for any **errors**.
- Logs are also saved in the `log` folder.

---

### 3. Scheduled daily execution 
To update and download new RCPs **daily**:

1. **Open Windows Task Scheduler.**
2. **Create a new task** and give it a meaningful name.
3. Under the **General tab**:
 - Ideally check:
   - `Run whether user is logged on or not`
   - `Run with highest privileges`
4. Under the **Triggers tab**:
 - Click “New” and schedule the task (e.g., every morning at **9:00 AM**).
5. Under the **Actions tab**:
 - Click “New”
 - Action: `Start a program`
 - Program/script: `cmd`
 - Add arguments:  
   ```
   /c "cd /d {path_to_project} && python app\main.py"
   ```
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
5th year industrial pharmacy student, University of Bordeaux  
julierouanet5@gmail.com
# Fuzzy Matching and Data Consolidation

This repository is the result of a technical assessment, its a script to consolidate data and analysis it.

---

## Features

1. **Fuzzy Matching CSV Loader**:
   - Reads CSV files and performs fuzzy matching on specified columns.
   - Corrects typographical errors with public data.
   - Loads the cleaned and matched data into a Snowflake table.

2. **Data Analysis with Plotly**:
   - Jupyter Notebook for detailed data analysis.
   - Interactive visualizations and insights from the Snowflake-stored data.

---

## Repository Structure

```plaintext
.
├── README.md               # Project documentation (this file)
├── req.txt                 # Python dependencies
├── fuzzy_load.py           # Main script for loading CSV data with fuzzy matching
├── app.ipynb               # Jupyter Notebook for data analysis and visualization
├── data/                   # Data folder with transactions and customers csv files
├── sql/                    # Folder with the sql queries
```

---

## Setup

### Prerequisites
- Python 3.12.6+
- Snowflake account and credentials
- Jupyter Notebook installed

### Installation

1. Clone this repository:
   ```bash
   git clone https://github.com/rsacerdote/fuzzymatching.git
   cd fuzzymatching
   ```

2. Install dependencies (if you are running on linux you may need to delete the pywin32 requirement from the file):
   ```bash
   pip install -r req.txt
   ```

3. Set up environment variables:
   Create a `.env` file in the project directory with the following:
   ```plaintext
   SNOWFLAKE_USER=<your_snowflake_username>
   SNOWFLAKE_PASSWORD=<your_snowflake_password>
   SNOWFLAKE_ACCOUNT=<your_snowflake_account_identifier>
   SNOWFLAKE_DATABASE=<your_snowflake_database>
   SNOWFLAKE_SCHEMA=<your_snowflake_schema>
   SNOWFLAKE_WAREHOUSE=<your_snowflake_warehouse>
   ```

---

## Usage

### Load CSV Data with Fuzzy Matching

Run the `fuzzy_load.py` script to load and fuzzy match your data:
```bash
python fuzzy_load.py
```

### OR

### Analyze Data in Jupyter Notebook

Open the analysis notebook:
```bash
jupyter notebook app.ipynb
```

The notebook already calls the fuzzy_load.py script, and also contains analysis and graphs.

---

## Dependencies

Key libraries used:
- `pandas`: Data manipulation and analysis.
- `thefuzz`: Fuzzy matching logic.
- `snowflake-connector-python`: Snowflake database integration.
- `plotly`: Interactive visualizations in the notebook.
- Check the full list in the requirements.txt

Install all dependencies using the `requirements.txt` file.

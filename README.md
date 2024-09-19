# ADGM Web Scraping Project

[![Open in GitHub Codespaces](https://github.com/codespaces/badge.svg)](https://codespaces.new/cr2007/ADGM-Web-Scraping)

This project is designed to scrape data from the [FSRA Public Register](https://www.adgm.com/public-registers/fsra) in [Abu Dhabi Global Market](https://www.adgm.com/).

The scraped data is parsed and then stored in a DataFrame, and finally exported to a CSV file.

# Installation

Make sure that you have the latest version of [Python](https://python.org) installed.

## Clone the repository

```bash
git clone https://github.com/cr2007/adgm-web-scraping
cd adgm-web-scraping
```

Optionally, you can also download the repository as a ZIP file.

## Create and activate a virtual environment

To keep dependencies isolated, it's recommended to create a virtual environment:

### On macOS/Linux

```bash
python3 -m venv .venv
source .venv/bin/activate
```
### On Windows

```powershell
python -m venv .venv
.venv\Scripts\activate
```

## Install Dependencies

Once the virtual environment is activated, install the required Python packages by running:

```bash
pip install -r requirements.txt
```

## Environment Variables

Create a .env file in the root directory and add the necessary environment variables.

Check out [sample.env](./sample.env) for the variables required.

# Running the Code

To run the scraping script, execute the following command:

```bash
# For macOS/Linux
python3 main.py

# For Windows
python main.py
```

# Deactivating the Virtual Environment

After you are done, deactivate the virtual environment by running:

```bash
decativate
```

# Customizing the Code

- Modify the `COMPANY_NAME_SPECIAL_CASES` dictionary  to handle any specific company names that need special formatting.

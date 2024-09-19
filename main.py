from concurrent.futures import ThreadPoolExecutor, as_completed
import os
import re
import sys
import time
from bs4 import BeautifulSoup
from dotenv import load_dotenv
import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

all_parsed_data = []
load_dotenv()

ntfy_url = os.getenv("NTFY_URL")

COMPANY_NAME_SPECIAL_CASES = {
    "Abrdn Investments Middle East Limited": "aberdeen-asset-middle-east-limited",
    "Xanara ME LTD": "xanara-management-limited",
    "SS&C Financial Services Middle East Limited": "ssandc-financial-services-middle-east-limited",
    "Perella Weinberg Partners UK LLP - branch": "perella-weinberg-partners-uk-llp",
    "Mubadala (Re)insurance Limited": "mubadala-re-insurance-limited",
    "Bitmena Limited": "venomex-limited",
    "Bank Lombard Odier & Co. Limited": "bank-lombard-odier--co-limited",
    "AT Capital Markets Limited (Withdrawn)": "at-capital-markets-limited",
    "Worldwide Cash Express Limited": "worldwide-cash-express",
    "BNP Paribas S.A.": "bnp-paribas-sa",
    "Shorooq Partners Ltd": "shorooq-vc-partners-ltd",
    "UniCredit S.p.A.": "unicredit-spa",
}


def format_company_name(company_name: str) -> str:
    # Handle special cases using the dictionary
    if company_name in COMPANY_NAME_SPECIAL_CASES:
        return COMPANY_NAME_SPECIAL_CASES[company_name]
    
    # General case formatting

    # Convert to lowercase
    company_name = company_name.lower()

    company_name = company_name.replace("&", " and ")  # Replace '&' with 'and'
    company_name = company_name.replace(".", "-")  # Replace periods with hyphens

    # Replace non-alphanumeric characters (except spaces) with empty string
    company_name = re.sub(r"[^\w\s-]", "", company_name)

    # Replace spaces with hyphens
    formatted_name = company_name.replace(" ", "-")

    # Replace multiple spaces or hyphens with a single hyphen
    company_name = re.sub(r"[\s-]+", "-", company_name)
    
    # Remove trailing hyphens
    company_name = company_name.rstrip("-")  

    return company_name


def create_session() -> requests.Session:
    session = requests.Session()

    session.headers = {
        "User-Agent": "Mozilla/5.0 (X11; Linux x86_64; rv:130.0) Gecko/20100101 Firefox/130.0",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/jxl,image/webp,image/png,image/svg+xml,*/*;q=0.8",
    }

    retries = Retry(total=5, backoff_factor=0.1, status_forcelist=[500, 502, 503, 504])
    adapter = HTTPAdapter(max_retries=retries)
    session.mount("https://", adapter)
    session.mount("https://", HTTPAdapter(max_retries=retries))

    return session


def is_date(string) -> bool:
    """Check if a string looks like a date in a common format."""
    date_pattern = r"\d{1,2} \w+ \d{4}"
    return re.match(date_pattern, string) is not None


def get_regulated_activities(soup: BeautifulSoup) -> list[dict[str, str]]:
    regulated_activities: BeautifulSoup = soup.find(id="raTableContainer_fsfdetail")

    ra_list = []

    elements = regulated_activities.find_all("div", class_="opn-accord")
    for element in elements:
        text = (
            element.get_text().strip().split("\n")
        )  # Strip and split based on new lines

        # Filter out any empty or whitespace strings from the list
        text = [item.strip() for item in text if item.strip()]
        ra_list.extend(text)

    # Remove every second empty string
    result = []
    i = 0
    while i < len(ra_list):
        activity = ra_list[i]
        effective_date = None
        withdrawn_date = None

        # Check if the next item is a date (effective date)
        if i + 1 < len(ra_list) and is_date(ra_list[i + 1]):
            effective_date = ra_list[i + 1]
            i += 1  # Move to the next item (withdrawn date)

        # Check if the next item is a withdrawn date
        if i + 1 < len(ra_list) and is_date(ra_list[i + 1]):
            withdrawn_date = ra_list[i + 1]
            i += 1  # Move to the next item

        # Create the dictionary and append it to the result
        result.append(
            {
                "Regulated Activity": activity,
                "Effective Date": effective_date,
                "Withdrawn Date": withdrawn_date,
            }
        )

        i += 1  # Move to the next activity

    return result


def get_conditions(soup: BeautifulSoup) -> str:
    conditions = soup.find(class_="fsp-first-table specialinfo-table")

    conditions_list: list[str] = []

    elements: BeautifulSoup = conditions.find_all("div", class_="container")
    for element in elements:
        text = element.get_text().split("\n")
        conditions_list.extend(text)

    # Strip blank values
    conditions_list = [item.strip() for item in conditions_list if item.strip()]

    return conditions_list[1]


def fetch_company_data(session: requests.Session, company: str) -> dict[str, str]:
    loop_start_time = time.time()

    url = (
        f"https://www.adgm.com/public-registers/fsra/fsf/{format_company_name(company)}"
    )

    try:
        response = session.get(url, timeout=10)
        if response.status_code == 404:
            print(
                f"There is a problem with the URL for {company}."
                f"\n{format_company_name(company)} does not seem to be the correct URL for this company."
            )

            if ntfy_url:
                requests.post(
                    ntfy_url,
                    data=f"Got {response.status_code} for {company}",
                    headers={
                        "Title": f"Incorrect link for {company}.\n\nCheck if the link ending is correct by any chance.",
                        "Priority": "urgent",
                        "Tags": "warning,adgm, fsra-register,incorrect-link,404-Error",
                        "Actions": "view, Go to FSRA Public Register, https://www.adgm.com/public-registers/fsra",
                    },
                )
            else:
                print(
                    "NTFY_URL not configured in environment variables. Include a URL to get notifications.\nMore Info: https://ntfy.sh"
                )

            return {"Company": company}

        response.raise_for_status()  # Raises an HTTPError for bad responses
    except requests.exceptions.RequestException as e:
        print(f"Error fetching data for {company}: {e}")

        if ntfy_url:
            requests.post(
                ntfy_url,
                data=f"Error fetching data for {company}: {e}",
                headers={
                    "Title": f"Error fetching data for {company}",
                    "Priority": "urgent",
                    "Tags": "warning,adgm,fsra-register,error",
                },
            )
        else:
            print(
                "NTFY_URL not configured in environment variables. Include a URL to get notifications.\nMore Info: https://ntfy.sh"
            )

        return {"Company": company}

    soup = BeautifulSoup(response.content, "html.parser")

    # Extract Regulated Activities
    regulated_activities = get_regulated_activities(soup)
    conditions = get_conditions(soup)

    company_data = {"Company": company, "Conditions": conditions}

    # Append data to Dataframe
    if regulated_activities:
        for i, activity in enumerate(regulated_activities, start=1):
            company_data[f"Regulated Activity {i}"] = activity["Regulated Activity"]
            company_data[f"Effective Date {i}"] = activity["Effective Date"]
            company_data[f"Withdrawn Date {i}"] = activity["Withdrawn Date"]

    print(
        f"Data extracted for {company} - Took {time.time() - loop_start_time:.2f} seconds"
    )
    return company_data


def main(company_names: list[str], output_file: str) -> None:
    try:
        session = create_session()
        df = pd.DataFrame()

        print("Starting data extraction...")
        start_time = time.time()

        with ThreadPoolExecutor(max_workers=10) as executor:
            future_to_company = {
                executor.submit(fetch_company_data, session, company): company
                for company in company_names
            }
            for future in as_completed(future_to_company):
                company_data = future.result()
                if company_data:
                    df = pd.concat(
                        [df, pd.DataFrame([company_data])], ignore_index=True
                    )

        df.to_csv(output_file, index=False)

        total_time = time.time() - start_time
        minutes, seconds = divmod(total_time, 60)

        print(f"Data extraction completed in {int(minutes)} min {seconds:.2f} sec")

        if ntfy_url:
            requests.post(
                ntfy_url,
                data=f"Job completed in {int(minutes)} minutes {seconds:.2f} seconds.",
                headers={
                    "Title": "ADGM Register data extraction successful",
                    "Priority": "4",
                    "Tags": "white_check_mark,muscle,adgm-register",
                },
            )
        else:
            print(
                "NTFY_URL not configured in environment variables. Include a URL to get notifications.\nMore Info: https://ntfy.sh"
            )
    except Exception as e:
        if ntfy_url:
            requests.post(
                ntfy_url,
                data=f"App crashed\n\nAn error occurred during data extraction:\n {e}",
                headers={
                    "Title": "ADGM Register data extraction failed",
                    "Priority": "5",
                    "Tags": "warning,adgm-register,error",
                },
            )
        else:
            print(
                "NTFY_URL not configured in environment variables. Include a URL to get notifications.\nMore Info: https://ntfy.sh"
            )

        raise e


if __name__ == "__main__":
    try:
        if file_path := os.getenv("COMPANY_NAMES_FILE_PATH"):
            with open(file_path, "r", encoding="utf-8") as file:
                # List comprehension to read and strip each line
                company_names = [line.strip() for line in file]
        else:
            print(
                "File path not specified. Please specify it in the '.env' file with the variable 'COMPANY_NAMES_FILE_PATH'"
            )
            sys.exit()
    except FileNotFoundError:
        print(f"The file at {file_path} was not found.")
    except IOError:
        print(f"An error occurred while reading the file at {file_path}.")

    main(company_names, "adgm_public_register_data.csv")

"""
main.py - This module handles web scraping for company data and notifies users of incorrect links.

This module contains functions for web scraping company data from the ADGM public register,
formatting company names, creating requests sessions, parsing HTML, and handling the main
scraping process. It also includes functionality for sending notifications about errors
or completion status.
"""

from concurrent.futures import ThreadPoolExecutor, as_completed
import os
import re
import signal
import sys
import threading
import time
from typing import Optional
from bs4 import BeautifulSoup, ParserRejectedMarkup
from dotenv import load_dotenv
import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from requests.exceptions import RequestException
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


def send_ntfy_notification(message: str, headers: Optional[dict[str, str]]) -> None:
    """
    Send a notification using the ntfy service.

    Args:
        message (str): The message to be sent in the notification.
        headers (Optional[Dict[str, str]]): Additional headers for the notification.

    Returns:
        None
    """

    if ntfy_url:
        requests.post(
            ntfy_url,
            data=message,
            headers=headers,
            timeout=15
        )
    else:
        print(
            "NTFY_URL not configured in environment variables. Include a URL to get notifications."
            "More Info: https://ntfy.sh"
        )


def format_company_name(company_name: str) -> str:
    """
    Format a company name for use in URL construction.

    Args:
        company_name (str): The original company name.

    Returns:
        str: The formatted company name.
    """

    # Handle special cases using the dictionary
    if company_name in COMPANY_NAME_SPECIAL_CASES:
        return COMPANY_NAME_SPECIAL_CASES[company_name]

    # General case formatting

    # Convert to lowercase
    company_name = company_name.lower()

    company_name = company_name.replace("&", " and ")  # Replace '&' with 'and'
    company_name = company_name.replace(".", "-")      # Replace periods with hyphens

    # Replace non-alphanumeric characters (except spaces) with empty string
    company_name = re.sub(r"[^\w\s-]", "", company_name)

    # Replace multiple spaces or hyphens with a single hyphen
    company_name = re.sub(r"[\s-]+", "-", company_name)

    # Remove trailing hyphens
    company_name = company_name.rstrip("-")

    return company_name


def create_session() -> requests.Session:
    """
    Create a requests Session with retry configuration.

    Returns:
        requests.Session: Configured session object.
    """

    session = requests.Session()

    retries = Retry(total=5, backoff_factor=0.1, status_forcelist=[500, 502, 503, 504])
    adapter = HTTPAdapter(max_retries=retries)
    session.mount("https://", adapter)
    session.mount("https://", HTTPAdapter(max_retries=retries))

    return session


def is_date(string: str) -> bool:
    """
    Check if a string looks like a date in a common format.

    Args:
        string (str): The string to check.

    Returns:
        bool: True if the string matches a common date format, False otherwise.
    """

    date_pattern = r"\d{1,2} \w+ \d{4}"
    return re.match(date_pattern, string) is not None


def get_regulated_activities(soup: BeautifulSoup) -> list[dict[str, str]]:
    """
    Extract regulated activities from the BeautifulSoup object.

    Args:
        soup (BeautifulSoup): Parsed HTML content.

    Returns:
        List[Dict[str, str]]: List of dictionaries containing regulated activity information.
    """

    regulated_activities = soup.find(id="raTableContainer_fsfdetail")

    ra_list = []

    elements: BeautifulSoup = regulated_activities.find_all("div", class_="opn-accord")
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
        result.append({
            "Regulated Activity": activity,
            "Effective Date": effective_date,
            "Withdrawn Date": withdrawn_date,
        })

        i += 1  # Move to the next activity

    return result


def get_conditions(soup: BeautifulSoup) -> str:
    """
    Extract conditions from the BeautifulSoup object.

    Args:
        soup (BeautifulSoup): Parsed HTML content.

    Returns:
        str: Extracted conditions.
    """

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
    """
    Fetch and parse company data from the ADGM website.

    Args:
        session (requests.Session): Session object for making HTTP requests.
        company (str): Name of the company to fetch data for.

    Returns:
        Dict[str, str]: Dictionary containing parsed company data.
    """

    loop_start_time = time.time()

    headers = {
        "User-Agent": "Mozilla/5.0 (X11; Linux x86_64; rv:130.0) Gecko/20100101 Firefox/130.0",
        "Accept": ("text/html,application/xhtml+xml,application/xml;"
                   "q=0.9,image/avif,image/jxl,image/webp,image/png,image/svg+xml,*/*;q=0.8"),
    }

    url = (
        f"https://www.adgm.com/public-registers/fsra/fsf/{format_company_name(company)}"
    )

    try:
        response = session.get(url, headers=headers, timeout=10)
        if response.status_code == 404:
            print(
                f"There is a problem with the URL for {company}."
                f"\n{format_company_name(company)} does not seem to"
                " be the correct URL for this company."
            )

            send_ntfy_notification(
                message=f"Got {response.status_code} for {company}",
                headers={
                    "Title": (f"Incorrect link for {company}.\n\n"
                              "Check if the link ending is correct by any chance."),
                    "Priority": "urgent",
                    "Tags": "warning,adgm, fsra-register,incorrect-link,404-Error",
                    "Actions": ("view, Go to FSRA Public Register, "
                                "https://www.adgm.com/public-registers/fsra"),
                },
            )

            return {"Company": company}

        response.raise_for_status()  # Raises an HTTPError for bad responses
    except requests.exceptions.RequestException as e:
        print(f"Error fetching data for {company}: {e}")

        send_ntfy_notification(
            f"Error fetching data for {company}: {e}",
            headers={
                "Title": f"Error fetching data for {company}",
                "Priority": "urgent",
                "Tags": "warning,adgm,fsra-register,error",
            },
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


def main(companies: list[str], output_file: str) -> None:
    """
    Main function to orchestrate the web scraping process.

    Args:
        companies (List[str]): List of company names to scrape data for.
        output_file (str): Name of the output CSV file.

    Returns:
        None
    """

    session = create_session()
    df = pd.DataFrame()
    executor = ThreadPoolExecutor(max_workers=10)
    shutdown_event = threading.Event()

    def signal_handler(*_):
        print("\nCtrl+C pressed. Shutting down gracefully...")
        shutdown_event.set()
        executor.shutdown(wait=False, cancel_futures=True)

    signal.signal(signal.SIGINT, signal_handler)

    try:
        print("Starting data extraction...")
        start_time = time.time()

        process_company_data(companies, session, executor, shutdown_event, df)

        if shutdown_event.is_set():
            save_results(df, output_file, start_time)
        else:
            save_partial_results(df, output_file)
    except RequestException as e:
        handle_extraction_error(df, output_file, f"Network error: {e}")
    except ParserRejectedMarkup as e:
        handle_extraction_error(df, output_file, f"HTML parsing error: {e}")
    except pd.errors.EmptyDataError as e:
        handle_extraction_error(df, output_file, f"DataFrame error: {e}")
    except IOError as e:
        handle_extraction_error(df, output_file, f"I/O error: {e}")
    except Exception as e:
        handle_extraction_error(df, output_file, f"Unexpected error: {e}")
        raise
    finally:
        executor.shutdown(wait=True)
        print("All tasks have been completed or cancelled.")


def process_company_data(companies: list[str], session: requests.Session,
                         executor: ThreadPoolExecutor, shutdown_event: threading.Event,
                         df: pd.DataFrame) -> None:
    """
    Process company data using multi-threading.

    Args:
        companies (List[str]): List of company names to process.
        session (requests.Session): Session object for making HTTP requests.
        executor (ThreadPoolExecutor): Executor for multi-threading.
        shutdown_event (threading.Event): Event to signal shutdown.
        df (pd.DataFrame): DataFrame to store results.

    Returns:
        None
    """

    future_to_company = {executor.submit(fetch_company_data, session, company): company
                         for company in companies if not shutdown_event.is_set()}

    for future in as_completed(future_to_company):
        if shutdown_event.is_set():
            break

        try:
            company_data = future.result()
            if company_data:
                df = pd.concat([df, pd.DataFrame([company_data])], ignore_index=True)
        except requests.RequestException as exc:
            print(f"{future_to_company[future]} generated a request exception: {exc}")
        except ValueError as exc:
            print(f"{future_to_company[future]} generated a value error: {exc}")
        except KeyError as exc:
            print(f"{future_to_company[future]} generated a key error: {exc}")


def save_results(df: pd.DataFrame, output_file: str, start_time: float) -> None:
    """
    Save the results to a CSV file and send a notification.

    Args:
        df (pd.DataFrame): DataFrame containing the results.
        output_file (str): Name of the output CSV file.
        start_time (float): Start time of the data extraction process.

    Returns:
        None
    """

    df.to_csv(output_file, index=False)

    total_time = time.time() - start_time
    minutes, seconds = divmod(total_time, 60)

    print(f"Data extraction completed in {int(minutes)} min {seconds:.2f} sec")

    send_ntfy_notification(
        message=f"Job completed in {int(minutes)} minutes {seconds:.2f} seconds.",
        headers={
            "Title": "ADGM Register data extraction successful",
            "Priority": "4",
            "Tags": "white_check_mark,muscle,adgm-register",
        },
    )


def save_partial_results(df: pd.DataFrame, output_file: str) -> None:
    """
    Save partial results to a CSV file and send a notification.

    Args:
        df (pd.DataFrame): DataFrame containing the partial results.
        output_file (str): Name of the output CSV file.

    Returns:
        None
    """

    print("Data extraction was interrupted. Saving partial results...")

    partial_output_file = f"partial_{output_file}"
    df.to_csv(partial_output_file, index=False)

    print(f"Partial results saved to {partial_output_file}")

    send_ntfy_notification(
        message=f"Job was interrupted. Partial results saved to {partial_output_file}",
        headers={
            "Title": "ADGM Register data extraction interrupted",
            "Priority": "3",
            "Tags": "negative_squared_cross_mark,adgm-register,ctrl-c,interrupted",
        },
    )


def handle_extraction_error(df: pd.DataFrame, output_file: str, error: Exception) -> None:
    """
    Handle errors during the extraction process, save partial results, and send a notification.

    Args:
        df (pd.DataFrame): DataFrame containing the partial results.
        output_file (str): Name of the output CSV file.
        error (Exception): The exception that occurred during extraction.

    Returns:
        None
    """

    partial_output_file = f"partial_{output_file}"
    df.to_csv(partial_output_file, index=False)

    send_ntfy_notification(
        message=(f"App crashed\nPartial results saved to {partial_output_file}\n\n"
                 f"An error occurred during data extraction:\n {error}"),
        headers={
            "Title": "ADGM Register data extraction failed",
            "Priority": "5",
            "Tags": "warning,adgm.fsra-register,error",
        },
    )


if __name__ == "__main__":
    file_path = os.getenv("COMPANY_NAMES_FILE_PATH")
    if not file_path:
        print(
            "File path not specified. "
            "Please specify it in the '.env' file with the variable 'COMPANY_NAMES_FILE_PATH'"
        )
        sys.exit()

    try:
        with open(file_path, "r", encoding="utf-8") as file:
            # List comprehension to read and strip each line
            company_names = [line.strip() for line in file]
    except FileNotFoundError:
        print(f"The file at {file_path} was not found.")
        sys.exit()
    except IOError:
        print(f"An error occurred while reading the file at {file_path}.")
        sys.exit()

    main(company_names, "adgm_public_register_data.csv")

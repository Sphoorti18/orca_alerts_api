# ORCA Alert Retrieval

This Python script allows you to retrieve ORCA alerts from Hyperscaler Database (HSDB). The script can save the retrieved data in JSON or CSV format based on your choice.

## Prerequisites

Before running the script, you need to set the following environment variables:

- For HSDB:
  - `HSDB_BASE_URL`: The base URL of HSDB.
  - `HSDB_USERNAME`: Your HSDB username.
  - `HSDB_PASSWORD`: Your HSDB password.

## Usage

1. Clone or download this repository to your local PC.

2. Set the required environment variables with your HSDB.

3. Open a terminal and navigate to the directory containing the script.

4. Run the script using the following command:

    ```bash
    python3 genet_all_hsdb_orca_alerts.py
    ```

5. Depending on your choice, the script will retrieve ORCA alerts from the selected source and save them in a file with a timestamp in the filename.

6. You can find the saved file in the same directory as the script.

## Notes

- If you choose the JSON format, the file will have a `.json` extension (e.g., `orca_alerts_20231027_003849.json`).

- If you choose the CSV format, the file will have a `.csv` extension (e.g., `orca_alerts_20231027_003849.csv`).

- The script retrieves all available data from the selected source.

## Changelog

- **v1.0.0 (2024-04-18)**:
  - Initial release.

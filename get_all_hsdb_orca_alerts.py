import os
import json
import argparse
from datetime import datetime
import pandas as pd
from urllib3 import PoolManager
import certifi

# Define the required environment variables
HSDB_BASE_URL = os.environ['HSDB_BASE_URL']
HSDB_USERNAME = os.environ['HSDB_USERNAME']
HSDB_PASSWORD = os.environ['HSDB_PASSWORD']


class HSDBClient:
    def __init__(self,
                 url,
                 username,
                 password):

        self.http = PoolManager(
            cert_reqs='CERT_REQUIRED',
            ca_certs=certifi.where()
        )
        self.url = url
        self.username = username
        self.password = password
        self.token = self.get_hsdb_bearer()

    def _reauth_request(self, method, url, headers, body, attempts=2):

        if attempts <= 0:
            return

        res = self.http.request(
            method=method,
            url=url,
            headers=headers,
            body=body
        )

        if res.status == 403:
            self.token = self.get_hsdb_bearer()
            return self._reauth_request(
                method=method,
                url=url,
                headers=headers,
                body=body,
                attempts=attempts-1
            )

        return res

    def get_hsdb_bearer(self):
        body = json.dumps({
            'username': self.username,
            'password': self.password
        })

        headers = {
            'Content-Type': 'application/json'
        }
        url = f'{self.url}/jwt-token/'
        res = self._reauth_request(
            method='POST', url=url, headers=headers, body=body)
        data = json.loads(res.data.decode('utf-8'))
        return data['access']

    def get_orca_alerts(self, status=None, page=1, alert_id=None, type_string=None, cloud_id=None, orca_score=None, risk_level=None, page_size=1000):
        """
        Retrieves ORCA alerts from the HSDB with optional filtering parameters.

        :param status: Status of the alerts to retrieve (e.g., 'open', 'closed', 'in-progress', 'dismissed').
        :param page: Page number of results to retrieve.
        :param alert_id: ID of the alert to retrieve.
        :param type_string: Name of the alerts to retrieve.
        :param cloud_id: Account id associated with the alert.
        :param orca_score: ORCA score of the alert.
        :param risk_level: Risk level associated with the alert.

        :return: List of ORCA alerts matching the specified criteria.
        """
        headers = {
            'Authorization': f'Bearer {self.token}'
        }

        print("Downloading ORCA alerts started...")

        all_alerts = []
        url = f'{self.url}/compliance/orca/?page_size={page_size}&page={page}'
        
        # Construct additional filters
        filters = []
        if status:
            filters.append(f'status={status}')
        if alert_id:
            filters.append(f'alert_id={alert_id}')
        if type_string:
            filters.append(f'type_string={type_string}')
        if cloud_id:
            filters.append(f'cloud_id={cloud_id}')
        if orca_score:
            filters.append(f'orca_score={orca_score}')
        if risk_level:
            filters.append(f'risk_level={risk_level}')

        if filters:
            url += '&' + '&'.join(filters)

        page_count = 1
        while url:
            response = self._reauth_request(
                url=url,
                method='GET',
                headers=headers,
                body=None
            )

            if response.status == 200:
                alert_data = response.json()
                results = alert_data.get('results', [])
                all_alerts.extend(results)
                url = alert_data.get('next')

                if page_count % 5 == 0:
                    print(f"Downloaded page {page_count}")

                page_count += 1
            else:
                print("Error: retrieving ORCA Alerts.", response.status, response.data)
                break

        print("Downloading ORCA alerts finished.")

        return all_alerts


def write_to_json(data, filename):
    with open(filename, "w") as f:
        json.dump(data, f, ensure_ascii=False, indent=4)


def write_to_csv(data, filename):
    df = pd.json_normalize(data)
    df.to_csv(filename, index=False)


def main(output_format):
    hsdb_client = HSDBClient(
        url=HSDB_BASE_URL,
        username=HSDB_USERNAME,
        password=HSDB_PASSWORD,
    )

    orca_alerts = hsdb_client.get_orca_alerts()
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    if orca_alerts:
        if output_format == "json":
            output_filename = f"orca_alerts_{timestamp}.json"
            write_to_json(orca_alerts, output_filename)
        elif output_format == "csv":
            output_filename = f"orca_alerts_{timestamp}.csv"
            write_to_csv(orca_alerts, output_filename)
        else:
            print("Invalid format choice. Please choose 'json' or 'csv'.")
    else:
        print("No data to write to the file.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--output_format', choices=['json', 'csv'], default='json',
                        help='Output file format (json/csv). Default is json.')
    args = parser.parse_args()
    main(args.output_format)
# Import CSV File
This repository contains python scripts that take a plaintext input.csv file and uses Skyflow's APIs to create the records in Skyflow's vault and generates an output.csv file with the Skyflow tokens replacing the sensitive column.

Note: these examples are not an officially-supported product or recommended for production deployment without further review, testing, and hardening. Use with caution, this is sample code only.

## Prerequisites

- Create or log into your Skyflow account
- Create a vault and relevant schema to hold your data
- Create a service account and download the credentials.json file
- Copy your Vault URL, Vault ID and Account ID

## Script Overview
There are 2 python scripts in this repo:

**import_csv.py**
 uses Skyflow "insert" API to create records in the vault, one record at a time

**import_csv_batch.py**
 uses Skyflow "batch" API to create records in the vault, batches of 25 records at a time multi-threaded to one thread for every batch.

### Python "insert" script
To create records in the vault 1 at a time use the below script with your own account, complete the TODOs in the sample below and use your own credentials.json file for authentication:

```python
from skyflow.errors import SkyflowError
from skyflow.service_account import generate_bearer_token, is_expired
from skyflow.vault import Client, InsertOptions, Configuration
import csv

vault_id = '-skyflow_vault_id-'
account_id = '-skyflow_account_id-'
vault_url = '-skyflow_vault_url-'
table_name = 'employees'

# cache token for reuse
bearerToken = ''

def token_provider():
    global bearerToken
    if is_expired(bearerToken):
        bearerToken, _ = generate_bearer_token('credentials.json')
    return bearerToken


try:
    config = Configuration(
        vault_id, vault_url, token_provider
    )
    client = Client(config)

    options = InsertOptions(True)

    # Open the input CSV file
    with open('input.csv', mode='r') as infile:
        csv_reader = csv.reader(infile)

        # Read and store the header row
        header = next(csv_reader, None)

        # Open the output CSV file in write mode once
        with open('output.csv', mode='w', newline='') as outfile:
            csv_writer = csv.writer(outfile)

            # Write the header to the output file
            if header:
                csv_writer.writerow(header)

            # Loop through the records in the input file
            for row in csv_reader:
                employee_id = row[0]  # Assuming 'id' is in the first column
                name = row[1]         # Assuming 'name' is in the second column

                data = {
                    "records": [
                        {
                            "table": "employees",
                            "fields": {
                                "name": name,
                                "employee_id": employee_id
                            }
                        }
                    ],
                    "tokenization": 'true'
                }
                # Insert data using the Skyflow client
                response = client.insert(data, options=options)
                print('Response:', response)

                # Update the 'name' field in the row with the response
                row[1] = response['records'][0]['fields']['name']
                print('Tokenized Name:', row[1])

                # Write the updated row to the output file
                csv_writer.writerow(row)

except SkyflowError as e:
    print('Error Occurred:', e)
```

#### Use the script
Using a Terminal session, go to the directory where the script is stored, ensure the input.csv file is in the same directory then run the command below: 

```bash
python3 tokenize_csv.py
```

### Python "batch" script
To create records in the vault using batches of 25 records each use the below script with your own account, complete the TODOs in the sample below and use your own credentials.json file for authentication:

```python
import requests
import json
import csv
import time
from skyflow.errors import SkyflowError
from skyflow.service_account import generate_bearer_token, is_expired
from concurrent.futures import ThreadPoolExecutor, as_completed
from functools import partial

vault_id = '-skyflow_vault_id-'
account_id = '-skyflow_account_id-'
vault_url = '-skyflow_vault_url'
table_name = 'employees'

# Cache token for reuse
bearerToken = ''


def token_provider():
    global bearerToken
    if is_expired(bearerToken):
        bearerToken, _ = generate_bearer_token('credentials.json')
    return bearerToken


def process_batch(batch_records, batch_rows, headers, url):
    """Process a batch of records."""
    start_time = time.time()  # Start time for this batch
    batch_payload = {"records": batch_records}

    response = requests.post(url, headers=headers, data=json.dumps(batch_payload))
    elapsed_time = time.time() - start_time  # Time taken for this batch

    if response.status_code == 200:
        response_data = response.json()
        for i, response_record in enumerate(response_data["responses"]):
            batch_rows[i][1] = response_record["records"][0]["tokens"]["name"]
        print(f"Batch processed in {elapsed_time:.2f} seconds.")
        return batch_rows
    else:
        print(f"Error: {response.status_code} - {response.text}")
        return []


try:
    url = f"{vault_url}/v1/vaults/{vault_id}"

    headers = {
        "Authorization": f"Bearer {token_provider()}",
        "Content-Type": "application/json",
    }

    # Start total processing time tracking
    total_start_time = time.time()

    # Open the input CSV file
    with open('input.csv', mode='r') as infile:
        csv_reader = csv.reader(infile)

        # Read and store the header row
        header = next(csv_reader, None)

        # Open the output CSV file in write mode once
        with open('output.csv', mode='w', newline='') as outfile:
            csv_writer = csv.writer(outfile)

            # Write the header to the output file
            if header:
                csv_writer.writerow(header)

            batch_size = 25
            batch_records = []
            batch_rows = []
            tasks = []
            results = []

            # Create a thread pool
            with ThreadPoolExecutor() as executor:
                for idx, row in enumerate(csv_reader):
                    employee_id = row[0]  # Assuming 'id' is in the first column
                    name = row[1]         # Assuming 'name' is in the second column

                    record = {
                        "batchID": "1",  # Add a batch ID for tracking (can be any value)
                        "fields": {
                            "name": name,
                            "employee_id": employee_id,
                        },
                        "method": "POST",
                        "tableName": table_name,
                        "tokenization": True,
                    }

                    batch_records.append(record)
                    batch_rows.append(row)

                    # If batch size is reached, send the batch request
                    if len(batch_records) == batch_size:
                        task = executor.submit(
                            process_batch, list(batch_records), list(batch_rows), headers, url
                        )
                        tasks.append((idx, task))

                        # Reset for the next batch
                        batch_records = []
                        batch_rows = []

                # Handle any remaining records
                if batch_records:
                    task = executor.submit(
                        process_batch, list(batch_records), list(batch_rows), headers, url
                    )
                    tasks.append((idx, task))

                # Collect results and ensure the sequence
                for idx, task in sorted(tasks, key=lambda x: x[0]):
                    result = task.result()
                    results.extend(result)

            # Write all results to the output file
            csv_writer.writerows(results)

    # Total elapsed time
    total_elapsed_time = time.time() - total_start_time
    print(f"Total processing time: {total_elapsed_time:.2f} seconds.")

except SkyflowError as e:
    print('Error Occurred:', e)
```
#### Use the script
Using a Terminal session, go to the directory where the script is stored, ensure the input.csv file is in the same directory then run the command below: 

```bash
python3 tokenize_csv_batch.py
```

# Learn more
To learn more about Skyflow Detokenization APIs visit docs.skyflow.com.

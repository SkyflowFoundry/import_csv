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

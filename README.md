# custom-simple-salesforce

`custom-simple-salesforce` is a Python library for handling the Salesforce Bulk API.

Based on `simple-salesforce`, it provides features to simplify the authentication process and make Bulk API operations easier to handle.

## Installation

This library can be installed with `uv` or `pip`.

```bash
uv pip install git+https://github.com/seiji-kawaharajo/custom-simple-salesforce.git
# または
pip install git+https://github.com/seiji-kawaharajo/custom-simple-salesforce.git
```

## Usage

### 1. Connecting

You can connect to Salesforce from YAML or a Python dictionary using the library's `Sf` class. It supports both password authentication and client credentials authentication.

Alternatively, since it inherits `Salesforce` from `simple-salesforce`, you can use it by passing parameters in the same way.

[`connect.py`](sample/connect.py)
```py
from custom_simple_salesforce import Sf

# Example using a YAML string
yaml_string = """
auth_method: password
username: your_username@example.com
password: your_password
security_token: your_security_token
domain: login
"""

sf_connection = Sf.connection(yaml_string)
print("Connection successful using YAML string!")
```

### 2. Bulk Query Example

Use the `bulk` module to efficiently query large amounts of data. The results are returned as a list of dictionaries.

`bulk`モジュールを使って、大規模なデータを効率的にクエリできます。結果は辞書のリストとして返されます。

```py
from custom_simple_salesforce import Sf, SfBulk

# Get an authenticated Sf object (see above)
sf_connection = ...

# Create an SfBulk client
bulk_client = SfBulk(sf_connection)

# Create and execute a bulk query job
query_job = bulk_client.create_job_query(
    "SELECT Id, Name FROM Account"
)

# Wait for the job to complete
query_job.poll_status()

# Get the results as a list of dictionaries
results = query_job.get_results()

# Print the retrieved data
for record in results[:3]: # Print the first 3 records
    print(record)
```

### Bulk Insert Example

An example of creating large amounts of data in a single operation (Insert).

`records.csv`

```
Name,Industry
Test Account 1,Technology
Test Account 2,Finance
```

```py
from custom_simple_salesforce import Sf, SfBulk

# Get an authenticated Sf object (see above)
sf_connection = ...

# Create an SfBulk client
bulk_client = SfBulk(sf_connection)

# Read the CSV file and get the data as a string
with open("records.csv", "r") as f:
    csv_data = f.read()

# Create an Insert job
insert_job = bulk_client.create_job_insert("Account")

# Upload the data
insert_job.upload_data(csv_data)

# Wait for the job to complete
insert_job.poll_status()

# Get the results of the successful records
successful_results = insert_job.get_successful_results()
print("Successful records:", successful_results)
```

# develop setup

pre-comit active

```bash
pre-commit install
```

venv active

```bash
source /workspaces/custom-simple-salesforce/.venv/bin/activate
```
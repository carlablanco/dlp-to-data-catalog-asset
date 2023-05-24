# Inspect your tables using Data Loss Prevention for PII data and automatically tag it on Data Catalog using Python #

Our objective for this asset is to provide a solution for data cataloging that is more accessible for Python users. By leveraging [Data Loss Prevention](https://cloud.google.com/dlp) in Python, we aim to simplify the process of inspecting [BigQuery](https://cloud.google.com/bigquery) and [CloudSQL](https://cloud.google.com/sql) data and creating tags, making it easier for organizations to understand and manage their data assets.

## How it works ##

To use our program, users must provide as a parameter the name of the BigQuery dataset they wish to analyze or the name of the specific table containing the data to be inspected. Our program then uses the Google Cloud DLP API to inspect the data. Once the inspection process is complete, our program creates a Data Catalog template linked to the inspected BigQuery table. This data catalog contains detailed information about the sensitive data found, including the type of data, its location, and any other relevant metadata.

To create the Data Catalog template, our program uses the Cloud Data Catalog API, which allows you to effectively tag and organize the data. By using the Data Catalog API, our program can automatically tag the sensitive data found and add additional relevant information to the data catalog.

In short, our program is a comprehensive and effective tool for protecting the privacy of an organization's data. By using the Google Cloud DLP and Data Catalog API, our program can automatically detect any sensitive data and effectively tag it in a data catalog linked to the BigQuery table. This provides greater transparency and control over sensitive data in an organization, which can help improve overall data privacy and security.

## Technical Requirements ##
These are the Google Cloud Platform services leveraged for the solution:

1. <a href= "https://cloud.google.com/dlp?hl=es-419"> Data Loss Prevention</a>
2. <a href= "https://cloud.google.com/products?hl=es-419"> Data Catalog</a>

## Setup ##
### Google Cloud Platform Credentials
Authenticate your Google Account and setup Application Default Credentials.

```
gcloud auth login
gcloud auth application-default login --impersonate-service-account=SERVICE_ACCOUNT
```
A service account is a special type of Google Account that represents a non-human user. It is used to authenticate and authorize applications and services to access Google Cloud Platform resources securely. For more information on service accounts, refer to the <a href="https://cloud.google.com/iam/docs/service-account-overview">official documentation.</a>

### Creating and Configuring Service Account
To create a service account and assign the required roles, execute the following commands using gcloud:

```
gcloud iam service-accounts create my-service-account --display-name "My Service Account"
gcloud projects add-iam-policy-binding PROJECT_ID --member=serviceAccount:my-service-account@PROJECT_ID.iam.gserviceaccount.com --role=roles/bigquery.dataViewer
gcloud projects add-iam-policy-binding PROJECT_ID --member=serviceAccount:my-service-account@PROJECT_ID.iam.gserviceaccount.com --role=roles/cloudsql.instanceUser
gcloud projects add-iam-policy-binding PROJECT_ID --member=serviceAccount:my-service-account@PROJECT_ID.iam.gserviceaccount.com --role=roles/cloudsql.client
gcloud projects add-iam-policy-binding PROJECT_ID --member=serviceAccount:my-service-account@PROJECT_ID.iam.gserviceaccount.com --role=roles/dlp.user
```
Replace my-service-account with your desired service account name, and PROJECT_ID with your Google Cloud Platform project ID.

Additionally, grant the user account the roles/iam.serviceAccountTokenCreator role to obtain access tokens for service accounts:
```
gcloud projects add-iam-policy-binding PROJECT_ID --member=user:YOUR_USER_ACCOUNT --role=roles/iam.serviceAccountTokenCreator
```
Replace YOUR_USER_ACCOUNT with your user account email.

### Environment Setup and Package Installation
To set up the environment and install the required packages, follow these steps:

1. Create a virtual enviroment.

```
python3 -m venv myenv
```
2. Activate the virtual environment.

```
source myenv/bin/activate
```
3. Install the necessary packages.

```
pip install -r requirements.txt
```
4. Setting Up Python Project and PYTHONPATH.
To ensure proper execution and import handling, we recommend setting up your Python project and configuring the PYTHONPATH environment variable. This allows the Python interpreter to locate and import the required modules and packages correctly.
Consult the official Python documentation on <a href= "https://docs.python.org/3/tutorial/modules.html"> Modules</a> and <a href="https://docs.python.org/3/tutorial/modules.html#packages"> Packages</a> for an in-depth understanding of how Python imports work.

## Run the program locally.
To use the program locally, you need to provide the following parameters:

project: The name of the Google Cloud Platform project.
language_code: The language code specifying the localization of the inspection results.

These parameters are common to both the BigQuery and CloudSQL execution methods.

Source:
You can choose one of the following options for the source:
- BigQuery
- CloudSQL

### BigQuery:
```
python3 -m dlp.run \
--project PROJECT \
--language_code LANGUAGE_CODE \
bigquery \
--dataset DATASET \
--table TABLE
```
BigQuery Parameters:

- dataset: The name of the BigQuery dataset to analyze.
- table: The BigQuery table to be scanned. If None, the entire dataset will be scanned. Optional.

### CloudSQL:

CloudSQL Parameters:
The following additional parameters are required for running the project with CloudSQL as the data source:

- instance: The name of the CloudSQL instance.
- zone: The zone where the CloudSQL instance is located.
- service_account: Secure identity for GCP access.
- db_name: The name of the database within the CloudSQL instance.
- db_type: The type of the database (only accepts `mysql` or `postgres`).
- table: The name of the table to inspect within the CloudSQL database.

#### For CLoudSQL (MySQL):

```
python3 -m dlp.run \
--project PROJECT \
--language_code LANGUAGE_CODE \
cloudsql \
--instance INSTANCE \
--zone ZONE \
--service_account SERVICE_ACCOUNT \
--db_name DB_NAME \
--db_type mysql \
--table TABLE
```

#### For CloudSQL (Postgres):

```
python3 -m dlp.run \
--project PROJECT \
--language_code LANGUAGE_CODE \
cloudsql \
--instance INSTANCE \ 
--zone ZONE \
--service_account SERVICE_ACCOUNT \
--db_name DB_NAME \
--db_type postgres \
--table TABLE
```

Make sure to replace the placeholder values (PROJECT, LANGUAGE_CODE, DATASET, INSTANCE, ZONE, DB_NAME, and TABLE) with the appropriate values for your specific setup.

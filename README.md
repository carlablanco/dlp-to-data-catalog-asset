# Inspect your tables using Data Loss Prevention for PII data and automatically tag it on Data Catalog using Python #

Our objective for this asset is to provide a solution for data cataloging that is more accessible for Python users. By leveraging [Data Loss Prevention](https://cloud.google.com/dlp) in Python, we aim to simplify the process of inspecting [BigQuery](https://cloud.google.com/bigquery) and [CloudSQL](https://cloud.google.com/sql) data and creating tags, making it easier for organizations to understand and manage their data assets.

## How it works ##

To use our program, users must provide as a parameter the name of the BigQuery dataset they wish to analyze or the name of the specific table containing the data to be inspected. Our program then uses the Google Cloud DLP API to inspect the data. Once the inspection process is complete, our program creates a Data Catalog template linked to the inspected BigQuery table. This data catalog contains detailed information about the sensitive data found, including the type of data, its location, and any other relevant metadata.

To create the Data Catalog template, our program uses the Cloud Data Catalog API, which allows you to effectively tag and organize the data. By using the Data Catalog API, our program can automatically tag the sensitive data found and add additional relevant information to the data catalog.

In short, our program is a comprehensive and effective tool for protecting the privacy of an organization's data. By using the Google Cloud DLP and Data Catalog API, our program can automatically detect any sensitive data and effectively tag it in a data catalog linked to the BigQuery table. This provides greater transparency and control over sensitive data in an organization, which can help improve overall data privacy and security.

## Authentication instructions for running the process locally ##

Follow the steps below to authenticate your Google account and set up Application Default Credentials for your local machine.

Step 1: Install the Required Dependencies

    `sudo apt-get install apt-transport-https ca-certificates gnupg`

Step 2: Install Google Cloud SDK

    2.1: Add the gcloud CLI distribution URI as a package source:
    `echo "deb [signed-by=/usr/share/keyrings/cloud.google.gpg] https://packages.cloud.google.com/apt cloud-sdk main" | sudo tee -a /etc/apt/sources.list.d/google-cloud-sdk.list`

    2.2: Import the Google Cloud public key:
    `curl https://packages.cloud.google.com/apt/doc/apt-key.gpg | sudo apt-key --keyring /usr/share/keyrings/cloud.google.gpg add -`

    2.3: Update and install the gcloud CLI:
    `sudo apt-get update && sudo apt-get install google-cloud-sdk`

Step 3: Authenticate your Google Account

    `gcloud auth login`

Step 4: Set up Application Default Credentials

    `gcloud auth application-default login`

## Technical Requirements ##
These are the Google Cloud Platform services leveraged for the solution:

1. <a href= "https://cloud.google.com/dlp?hl=es-419"> Data Loss Prevention</a>
2. <a href= "https://cloud.google.com/products?hl=es-419"> Data Catalog</a>

{
 "cells": [
  {
   "attachments": {},
   "cell_type": "markdown",
   "metadata": {},
   "source": [
    "# Service - DLP Data Catalog Tags with Python #\n",
    "\n",
    "<p align=\"center\">\n",
    "    <a href=\"#about\">About</a> &#xa0; | &#xa0;\n",
    "    <a href=\"#how-it-works\">How it works</a> &#xa0; | &#xa0;\n",
    "    <a href=\"#technical-requirement\">Technical Requirement</a> &#xa0; | &#xa0;\n",
    "    <a href=\"#high-level-flow\">High Level Flow</a> &#xa0; | &#xa0; \n",
    "</p>\n",
    "\n",
    "## About ##\n",
    "\n",
    "Our objective for this asset is to provide a solution for data cataloging that is more accessible for Python users. By leveraging Cloud Data Loss Prevention in Python, we aim to simplify the process of inspecting BigQuery and CloudSQL data and creating tags, making it easier for organizations to understand and manage their data assets.\n",
    "\n",
    "## How it works ##\n",
    "After the DLP inspection process is complete, the functionality creates a data catalog that is linked to the BigQuery table that was inspected. The data catalog contains information about the sensitive data that was found, including the type of data, its location, and any other relevant metadata.\n",
    "\n",
    "\n",
    "## Technical Requirement ##\n",
    "These GCP services will be used:\n",
    "\n",
    "1. <a href= \"https://cloud.google.com/bigquery?hl=es-419\"> BigQuery</a>\n",
    "2. <a href= \"https://cloud.google.com/sql?hl=es-419\"> Clod SQL</a>\n",
    "3. <a href= \"https://cloud.google.com/dlp?hl=es-419\"> Data Loss Prevention</a>\n",
    "4. <a href= \"https://cloud.google.com/products?hl=es-419\"> Data Catalog</a>"
   ]
  }
 ],
 "metadata": {
  "language_info": {
   "name": "python"
  },
  "orig_nbformat": 4
 },
 "nbformat": 4,
 "nbformat_minor": 2
}
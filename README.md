# Data Warehousing with AWS Redshift and S3

This repository contains code for a data warehousing solution that involves extracting data from AWS S3, processing it, and loading it into an AWS Redshift cluster. The code is designed to be executed as an AWS Lambda function.

## Table of Contents
1. [Introduction](#introduction)
2. [Architecture](#architecture)
3. [Setup and Configuration](#setup-and-configuration)
4. [Usage](#usage)
5. [Code Overview](#code-overview)
6. [Key Functions](#key-functions)
7. [Error Handling](#error-handling)
8. [Logging](#logging)
9. [License](#license)

## Introduction

This project is designed to automate the process of data extraction, transformation, and loading (ETL) using AWS services. It retrieves data from CSV files stored in an S3 bucket, processes the data, and loads it into an AWS Redshift cluster for warehousing.

## Architecture

The architecture of this project involves the following components:
- **AWS S3**: Stores the CSV files containing the data.
- **AWS Lambda**: Executes the ETL process.
- **AWS Redshift**: Data warehousing solution where the processed data is stored.
- **Python**: Programming language used for the Lambda function.

## Setup and Configuration

### Prerequisites
- AWS account with access to S3, Lambda, and Redshift.
- Python 3.x installed.
- AWS CLI configured with appropriate permissions.

### Configuration
1. **AWS Redshift Configuration**: Update the Redshift connection details in the code with your own credentials.
    ```python
    dbname = '{-dbname-}'
    host = '{-host-}.ap-south-1.redshift.amazonaws.com'
    user = '{-username-}'
    password = '{-password-}'
    port = '{-port-}'
    bucket = '{bucket-name}'
    ```

2. **S3 Bucket Configuration**: Set the S3 bucket and prefix where the CSV files are stored.
    ```python
    S3_PREFIX = 'Bucket_Dir1/Bucket_Dir2'
    ```

3. **Entity Configuration**: Map the numeric fields for each entity.
    ```python
    entityIntegerField = {
        'entity_A' : ['NUMERIC_COLUMN_A','NUMERIC_COLUMN_B'],
        'entity_B' : ['NUMERIC_COLUMN_A'],
        ...
    }
    ```

## Usage

### Lambda Deployment
1. **Create Lambda Function**: Create a new AWS Lambda function.
2. **Upload Code**: Upload the Python code to the Lambda function.
3. **Set Environment Variables**: Configure the necessary environment variables for the Lambda function.
4. **Set Trigger**: Configure an S3 trigger to invoke the Lambda function when a new CSV file is uploaded.

### Running the Code
The Lambda function is automatically triggered by an S3 event. It processes the CSV files and loads the data into Redshift.

## Code Overview

### Main Function
The main function `lambda_handler` is responsible for:
- Retrieving CSV files from S3.
- Processing the data.
- Inserting or updating data in Redshift.

### Helper Functions
- `isNumberField(field, entity)`: Checks if a field is numeric.
- `replace_last(string, old, new)`: Replaces the last occurrence of a substring.
- `createSelectInQuery(tableName, field, fieldValues)`: Creates a SQL query to select existing entries.
- `prepareBatchInsertQueries(tableName, fieldList, entityJsonList, entity)`: Creates a SQL query for batch inserting new data.
- `prepareBulkUpdateQueries(tableName, tempTableName, entityFieldsList, joinField)`: Creates a SQL query for bulk updating existing data.
- `get_csv_data_from_s3(bucket, key)`: Retrieves CSV data from S3.

## Key Functions

### lambda_handler
This is the main entry point of the Lambda function. It:
1. Retrieves the CSV files from S3.
2. Processes each file and converts it into a list of JSON objects.
3. Inserts or updates the data in the Redshift tables.

### createSelectInQuery
Generates a SQL `SELECT` query to retrieve existing records based on a list of IDs.

### prepareBatchInsertQueries
Generates a SQL `INSERT` query for inserting new records into Redshift.

### prepareBulkUpdateQueries
Generates a SQL `UPDATE` query to update existing records in Redshift.

## Error Handling

The code includes try-except blocks to handle potential errors during database transactions. If an error occurs, the transaction is rolled back, and the error is logged.

## Logging

The script includes print statements for logging various stages of the ETL process, including:
- Steps of the Lambda function.
- CSV file paths.
- Number of incoming, existing, new, and updated records.



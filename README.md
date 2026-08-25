# nyc-taxi-data-engineering-project

**Phase 1 : Data Ingestion**

\- Use Azure Data Factory (ADF) to ingest the data dynamically. 

\- Data will be consumed via APIs from the source website ([https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page)) by going to each month and downloading the relevant file via an automated solution

\- Then, data can be uploaded/dumped in the Bronze layer/raw zone (Data Lake).

**\# How to create data ingestion ADF Pipeline?**

**\- What kind of data is coming in?**  
     
We are going to extract only files titled “Green Taxi Trip Records” for any   particular year (say, 2025). We are gonna ingest files for all months of that year.

**\- What’s the format of incoming files?**

Parquet files.

Each file will have address like : [https://d37ci6vzurychx.cloudfront.net/trip-data/green\_tripdata\_2025-01.parquet](https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2025-01.parquet)

Where,

Base URL : [https://d37ci6vzurychx.cloudfront.net/](https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2025-01.parquet)

Relative URL : [trip-data/green\_tripdata\_2025-01.parquet](https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2025-01.parquet)  
So we need to parameterize the month component in the file address like ‘2025-0{month\_variable}’

But for months 10, 11 and 12, we’ll need parameterized component like : ‘2025-{month\_variable}’

Thus, we will build a dynamic parameterized ADF pipeline for data ingestion.

**\- Pipeline creation steps :** 

1\. Create linked service for source and sink of data :   
    Source : HTTP connector as we are fetching data via API  
    Sink : Azure Data Lake Storage

2\. Create source and sink datasets.

3\. First we create a ForEach activity since we’ll be iterating through each      month and dynamically creating file addresses by passing the month parameter. In ForEach activity we pass range(1, 12\) for 12 months of the year

4\. Inside ForEach activity, create an If activity. If activity will run for each input value of For activity (i.e., range(1,12)). As we will be passing different parameterized components based on month value (either months 1-9, or months 10-12), If activity will enable us to modify the file address dynamically based on month we are ingesting file for.

5\. For months 1-9,  source dataset will have parameterized URL like :   
   [https://d37ci6vzurychx.cloudfront.net/trip-data/green\_tripdata\_2025-0@{month\_value}.parquet](https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2025-0{month_value}.parquet)

For months 10-12,  source dataset will have parameterized URL like : 

[https://d37ci6vzurychx.cloudfront.net/trip-data/green\_tripdata\_2025-@{month\_value}.parquet](https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2025-0{month_value}.parquet)

***\[Notice 0 removed before month\_value parameter\]***

![][image1]

**Phase 2 : Data Manipulation using Databricks**

## **1\. Service Principal and Its Purpose**

### **What is a Service Principal?**

A Service Principal (SP) is an application identity in Microsoft Entra ID that allows an application, pipeline, or automated process to authenticate to Azure resources without using a personal user's Azure account.

It can be thought of as a **non-human identity for an application**.

For the NYC DE Taxi project, the Service Principal can be used to provide controlled access to the Azure Data Lake Storage Gen2 (ADLS Gen2) environment.

### **Important Service Principal Details**

When an application is registered in Microsoft Entra ID, we typically work with:

| Property | Purpose |
| ----- | ----- |
| Tenant ID / Directory ID | Identifies the Microsoft Entra tenant |
| Client ID / Application ID | Identifies the registered application |
| Client Secret | Credential used by the application to authenticate |
| Service Principal | Identity representing the application in the Azure environment |

The Client Secret should be treated like a password and should not be hardcoded in notebooks, source code, or Git repositories.

The commonly used Azure RBAC role is:

**Storage Blob Data Contributor**

This role provides read, write, and delete access to blob data. Azure RBAC can be assigned at different scopes, including the storage account or a specific container. Assigning it at the narrowest appropriate scope follows the principle of least privilege.

For example:

Service Principal

      │

      │ Storage Blob Data Contributor

      ▼

bronze container

The Service Principal does not automatically receive access to other containers simply because it has access to Bronze if the RBAC assignment is scoped specifically to the Bronze container.

---

# 

# **2\. Accessing ADLS Gen2 from Databricks Using a Service Principal**

## **Objective**

The objective is to allow Databricks to read and/or write files stored in the ADLS Gen2 Bronze layer.

For example:

ADLS Gen2

└── bronze/

      └── green\_taxi/

            └── year=2025/

                  ├── month=01/

                  │     └── green\_tripdata\_2025-01.parquet

                  ├── month=02/

                  └── ...

                  └── month=12/

Databricks can access these files using Azure Data Lake Storage's `abfss` protocol.

Example:

abfss://bronze@\<storage-account\>.dfs.core.windows.net/green\_taxi/

---

# **3\. Authentication Flow**

The overall authentication and authorization flow is:

Databricks

    │

    │ Client ID \+ Client Secret

    ▼

Microsoft Entra ID

    │

    │ OAuth 2.0 access token

    ▼

Azure Storage

    │

    │ Azure RBAC checks permissions

    ▼

ADLS Gen2 Bronze

    │

    ▼

Read / Write Data

The Service Principal must first be granted appropriate permissions on the ADLS storage resource.

---

# **4\. Steps Involved in creating Service Principle accessing Data Lake**

# **Step 1 – Create the Azure Service Principal**

Create an application registration in:

Azure Portal

    ↓

Microsoft Entra ID

    ↓

App registrations

    ↓

New registration

Create the application and record:

Application / Client ID

Directory / Tenant ID

Then create a Client Secret under:

Certificates & secrets

    ↓

Client secrets

    ↓

New client secret

The secret value should be stored securely.

---

### **Step 2 – Give the Service Principal access to ADLS**

Go to:

Azure Portal

    ↓

Storage Account

    ↓

Access Control (IAM)

    ↓

Add role assignment

Assign:

Storage Blob Data Contributor to the Service Principal.

If the requirement is only access to the Bronze container, assign the role at the Bronze container scope instead of the entire storage account where practical.

---

### **Step 3 –  Access Data Lake using Legacy / Direct ABFSS Approach**

This is an older approach where the Service Principal credentials are configured directly in the Databricks cluster or notebook using Spark configuration.

The authentication configuration uses:

OAuth 2.0

    \+

Service Principal

    \+

ABFSS

Example configuration:

spark.conf.set(

    "fs.azure.account.auth.type.\<storage-account\>.dfs.core.windows.net",

    "OAuth"

)

spark.conf.set(

    "fs.azure.account.oauth.provider.type.\<storage-account\>.dfs.core.windows.net",

    "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")

spark.conf.set(

    "fs.azure.account.oauth2.client.id.\<storage-account\>.dfs.core.windows.net",

    "\<client-id\>"

)

spark.conf.set(

    "fs.azure.account.oauth2.client.secret.\<storage-account\>.dfs.core.windows.net",

    "\<client-secret\>"

)

spark.conf.set(

    "fs.azure.account.oauth2.client.endpoint.\<storage-account\>.dfs.core.windows.net",

    "https://login.microsoftonline.com/\<tenant-id\>/oauth2/token"

)

The data can then be accessed using an `abfss` path:

abfss://bronze@\<storage-account\>.dfs.core.windows.net/green\_taxi/

---

* **Silver Layer Transformations**   
  Present in silver notebook, performed on data present in bronze layer using databricks and wrote transformed data into the silver layer container in data lake.

   
* **Gold Layer Transformations**  
  \# Present in the gold notebook, data was read from the silver layer container and written into the gold layer container in Delta format.   
  \# To create Delta format/ Delta table on top parquet files in gold layer we use .saveAsTable(\<db\_name\>.\<table\_name\>) instead of .save() command of pyspark.  
  \# Delta tables enable us to use SQL commands to query data. It allows us to perform versioning and time travel.


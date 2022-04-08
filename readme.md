# About
ETL pipeline to serve data to a fleet analysis power BI dashboard. Source data is coming from a third party API from WebFleet, transformations are beign done using pandas and the final tables (csv files) are getting uploaded to sharepoint. ETL is deployed on an Azure VM and runs on a schedule using windows schedular.

# Implementation
## Config
A config file under _Utility Files_ is used to set time ranges for each table.

## Extract
15 different tables with variable time ranges and filtering options are requested from the webfleet API and stored into a temporary folder on the disk.

## Transform
Cleansing steps like renaming columns, changing data types and cleaning illegal characters are done using pandas before storing the files in temp folder.

## Load
Temp folder gets uploaded to sharepoint using sharepoint client to serve the data to power BI.

## Deploy
Azure VM is orchestrated to host the ETL pipeline on an every day schedule with an Auto Shutdown feature implemented using Logic Apps.

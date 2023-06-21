# Retrieve sora interest rate from MAS API
 
This script queries the MAS api to generate a table with the latest 1 month compounded sora interest rate. 

## Types of data
Users can choose sets of data to retrieve based on the json payload (see this link and MAS api for more: https://eservices.mas.gov.sg/statistics/dir/DomesticInterestRates.aspx)

## Loading workflow
This script saves directly into BigQuery, so you need your to authenticate and setup your IDE accordingly. 

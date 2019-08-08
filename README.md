# bdad-project

## Application
Our application consists of a Tableau dashboard, which we have saved as a packaged workbook with the data sources embedded. The user should be able to open our dashboard for viewing simply by opening the "dashboard" file in app_data/.  

## Directories and files
app_data/: this directory consists of our final application, "dashboard", which is a Tableau packaged workbook. 

app_data/analysis/: this directory consists of the code we used to analyze, clean, and reduce our data into a size ingestible by Tableau.  The modules can be run by loading them into the spark REPL with the following syntax:
```
:load [name of scala file]
```
data_ingest/:  this directory cosists of code to ingest data from our local computers to hdfs, and data from hdfs to our dumbo profiles in order to get the data back into our local computers for Tableau ingestion.  

etl_code/: this directory consists of code to pull and ingest our data. There is a separate directory for Instagram ingestion and Twitter ingestion. Instagram code was written in python, while Twitter code was written in R.  

profiling_code/:  this directory contains code to profile our data sources (ie., getting schema, finding max and mins of columns, getting range of values, looking for missing data)

sample_data/: this directory consists of small samples of our data for local testing   

screenshots/: this directory consists of screenshots of our dashboard

## Input data
Instagram data: hdfs: kp1276/instagram_comments.csv. 
Twitter data: hdfs:  

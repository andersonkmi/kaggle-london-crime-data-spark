# London crime data exploration in Spark  [![Build Status](https://travis-ci.org/andersonkmi/kaggle-london-crime-data-spark.svg?branch=master)](https://travis-ci.org/andersonkmi/kaggle-london-crime-data-spark)
Data exploration using some Kaggle data sets developed in Spark and Scala.

## 1. Extracted information from the initial data set

Initial program to filter distinct values from the London crime data set. This program currently
extracts and saves the following information:

* Distinct boroughs found in the data set
* Distinct boroughs and LSOA codes
* Distinct major categories crimes
* Distinct minor categories crimes
* Total number of crimes by borough in descending order
* Total number of crimes by major category in descending order
* Total number of crimes by borough and year
* Total number of crimes by major category and year
* Total number of crimes by minor category and year
* Total number of crimes by year
* Total number of crimes by year and month
* Crime category percentage by year (2016-2008)
* Total crimes by year and LSOA codes

In order to run it, the CSV file download from the Kaggle website must be located in the
root folder of the project and executed as follows:

```
$ sbt
...
$ run --aws-s3 bucket/path/london_crime_by_lsoa.csv --destination D:\temp --master local[*]
```

At the end, the following folders are created inside the project:

* _borough_csv_
* _lsoa_csv_
* _categories_csv_
* _major_category_csv_
* _minor_category_csv_
* _total_crimes_by_borough_csv_
* _total_crimes_by_major_category_csv_
* _total_crimes_by_minor_category_csv_
* _total_crimes_by_borough_year_csv_
* _total_crimes_by_major_category_year_csv_
* _total_crimes_by_minor_category_year_csv_
* _total_crimes_by_year_csv_
* _total_crimes_by_year_month_csv_
* _crime_percentage_2016_csv_
* _crime_percentage_2015_csv_
* _crime_percentage_2014_csv_
* _crime_percentage_2013_csv_
* _crime_percentage_2012_csv_
* _crime_percentage_2011_csv_
* _crime_percentage_2010_csv_
* _crime_percentage_2009_csv_
* _crime_percentage_2008_csv_
* _total_crimes_by_year_lsoa_code_csv_

The following folders are also created containing the parquet files:
* _borough_parquet_
* _lsoa_parquet_
* _categories_parquet_
* _major_category_parquet_
* _minor_category_parquet_
* _total_crimes_by_borough_parquet_
* _total_crimes_by_major_category_parquet_
* _total_crimes_by_minor_category_parquet_
* _total_crimes_by_borough_year_parquet_
* _total_crimes_by_major_category_year_parquet_
* _total_crimes_by_minor_category_year_parquet_
* _total_crimes_by_year_parquet_
* _total_crimes_by_year_month_parquet_
* _crime_percentage_2016_parquet_
* _crime_percentage_2015_parquet_
* _crime_percentage_2014_parquet_
* _crime_percentage_2013_parquet_
* _crime_percentage_2012_parquet_
* _crime_percentage_2011_parquet_
* _crime_percentage_2010_parquet_
* _crime_percentage_2009_parquet_
* _crime_percentage_2008_parquet_
* _total_crimes_by_year_lsoa_code_parquet_


each containing the results of each filter applied to the original data set.

## 2. Unit tests

In order to run unit tests inside the project, the following command can be executed:

```
$ sbt
...
$ test
```

Wait for the unit tests finish the execution to see the results.

## 3. Executing inside sbt - local mode
In order to run the program as standalone mode and using the CSV from S3, the following command can be used (assuming Windows machine):
```
$ sbt 
$ sbt:kaggle-london-crime-data-saprk> run --aws-s3 org.sharpsw.spark/kaggle-london-crime-data/input/london_crime_by_lsoa.csv --destination D:\temp --s3-bucket bucket --s3-prefix s3-prefix --master local[*]
```

In order to run the program as standalone mode and using the CSV locally, the following command can be used (assuming Windows machine):
```
$ sbt 
$ sbt:kaggle-london-crime-data-saprk> run --local london_crime_by_lsoa.csv --destination D:\temp --s3-bucket bucket --s3-prefix s3-prefix --master local[*]
```

## 4. Executing on Spark - local mode
In order to run the program inside Spark, the following command can be used (assuming Windows machine):
```
$ spark-submit --master local[*] --jars aws-java-sdk-kms-1.11.354.jar,aws-java-sdk-s3-1.11.354.jar,aws-java-sdk-core-1.11.354.jar --class org.sharpsw.spark.ExtractLondonCrimeData target\scala-2.11\kaggle-london-crime-data-spark_2.11-<appVersion>.jar --aws-s3 org.sharpsw.spark/kaggle-london-crime-data/input/london_crime_by_lsoa.csv --destination D:\temp --s3-bucket bucket --s3-prefix s3-prefix
```

In order to run the program inside Spark using a local CSV, the following command can be used (assuming Windows machine):
```
$ spark-submit --master local[*] --jars aws-java-sdk-kms-1.11.354.jar,aws-java-sdk-s3-1.11.354.jar,aws-java-sdk-core-1.11.354.jar --class org.sharpsw.spark.ExtractLondonCrimeData target\scala-2.11\kaggle-london-crime-data-spark_2.11-<appVersion>.jar --local london_crime_by_lsoa.csv --destination D:\temp --s3-bucket bucket --s3-prefix s3-prefix
```

## 5. Changelog
All changes are listed in [CHANGELOG.md](CHANGELOG.md)

## 6. References

[London crime data, 2008-2016 (Kaggle) - last used in May 2018](https://www.kaggle.com/jboysen/london-crime/data)

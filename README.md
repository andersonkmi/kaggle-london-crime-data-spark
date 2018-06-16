# London crime data exploration in Spark  [![Build Status](https://travis-ci.org/andersonkmi/kaggle-london-crime-data-spark.svg?branch=master)](https://travis-ci.org/andersonkmi/kaggle-london-crime-data-spark)
Data exploration using some Kaggle datasets developed in Spark and Scala.

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
$ run london_crime_by_lsoa.csv D:\temp local[*]
```

At the end, the following folders are created inside the project:

* _borough.csv_
* _lsoa.csv_
* _categories.csv_
* _major_category.csv_
* _minor_category.csv_
* _total_crimes_by_borough.csv_
* _total_crimes_by_major_category.csv_
* _total_crimes_by_minor_category.csv_
* _total_crimes_by_borough_year.csv_
* _total_crimes_by_major_category_year.csv_
* _total_crimes_by_minor_category_year.csv_
* _total_crimes_by_year.csv_
* _total_crimes_by_year_month.csv_
* _crime_percentage_2016.csv_
* _crime_percentage_2015.csv_
* _crime_percentage_2014.csv_
* _crime_percentage_2013.csv_
* _crime_percentage_2012.csv_
* _crime_percentage_2011.csv_
* _crime_percentage_2010.csv_
* _crime_percentage_2009.csv_
* _crime_percentage_2008.csv_
* _total_crimes_by_year_lsoa_code.csv_

The following folders are also created containing the parquet files:
* _borough.parquet_
* _lsoa.parquet_
* _categories.parquet_
* _major_category.parquet_
* _minor_category.parquet_
* _total_crimes_by_borough.parquet_
* _total_crimes_by_major_category.parquet_
* _total_crimes_by_minor_category.parquet_
* _total_crimes_by_borough_year.parquet_
* _total_crimes_by_major_category_year.parquet_
* _total_crimes_by_minor_category_year.parquet_
* _total_crimes_by_year.parquet_
* _total_crimes_by_year_month.parquet_
* _crime_percentage_2016.parquet_
* _crime_percentage_2015.parquet_
* _crime_percentage_2014.parquet_
* _crime_percentage_2013.parquet_
* _crime_percentage_2012.parquet_
* _crime_percentage_2011.parquet_
* _crime_percentage_2010.parquet_
* _crime_percentage_2009.parquet_
* _crime_percentage_2008.parquet_
* _total_crimes_by_year_lsoa_code.parquet_


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
In order to run the program inside Spark, the following command can be used (assuming Windows machine):
```
$ sbt 
$ sbt:kaggle-london-crime-data-saprk> run london_crime_by_lsoa.csv D:\temp local[*]
```

## 4. Executing on Spark - local mode
In order to run the program inside Spark, the following command can be used (assuming Windows machine):
```
$ spark-submit --master local[*] --class org.sharpsw.spark.ExtractLondonCrimeData target\scala-2.11\kaggle-london-crime-data-spark_2.11-<appVersion>.jar london_crime_by_lsoa.csv D:\temp
```

## 5. References

[London crime data, 2008-2016 (Kaggle) - last used in May 2018](https://www.kaggle.com/jboysen/london-crime/data)

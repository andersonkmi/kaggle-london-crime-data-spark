# Data handling in Spark
Exercises using some Kaggle datasets developed in Spark and Scala.

## 1. Programs

Here are some of the programs developed so far inside this project.

### 1.1. Extract London Crime data
Initial program to filter distinct values from the London crime data set. This program currently
extracts and saves the following information:

* Distinct boroughs found in the data set
* Distinct major categories crimes
* Distinct minor categories crimes
* Total number of crimes by borough in descending order
* Total number of crimes by major category in descending order

In order to run it, the CSV file download from the Kaggle website must be located in the
root folder of the project and executed as follows:

```
$ sbt
...
$ run london_crime_by_lsoa.csv
```

At the end, the following folders are created inside the project:

* _borough.csv_
* _categories.csv_
* _major_category.csv_
* _minor_category.csv_
* _total_crimes_by_borough.csv_
* _total_crimes_by_major_category.csv_
* _total_crimes_by_minor_category.csv_

each containing the results of each filter applied to the original data set.

## 2. References

[London crime data, 2008-2016 (Kaggle) - last used in May 2018](https://www.kaggle.com/jboysen/london-crime/data)

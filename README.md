# CA4022-MovieLens-cleaning-and-analysis

This is the repo for my CA4022 - Data at Speed and Scale - Assignment 1

This assignment makes use of Pig and Hive in the Hadoop ecosystem to clean and analyse the MovieLens dataset.
The final completed PDF can be found [here](CA4022-Adam-Tegart-MovieLens_dataset_analysis.pdf)

The steps taken to carry out this assignment are listed briefly below:

+ The data was read into Pig and cleaned.
+ The data was then joined, only on movies and ratings as tags had several tags per rating and links had little to add without webscraping.
+ The cleaned data was queried using Pig.
+ The exported data was loaded into Hive and subsequent queries were made.
+ Visualisations were made using the cleaned data and output from Hive.

The contents of this repo are as follows:

+ [Final PDF](CA4022-Adam-Tegart-MovieLens_dataset_analysis.pdf) - Documentation for the assignment.
+ [Data](data) - The folder that contains the [MovieLens](https://grouplens.org/datasets/movielens/) dataset.
+ [Outputs](outputs) - This folder contains all data output from Pig and Hive.
+ [Screenshots](screenshots) - This folder contains the evidence of output from Pig and Hive.
+ [Visualisations](visualisations) - This folder contains the visualisations made from the clean data and the Hive output.
+ [Jupyter notebook](CA4022-Apache-Pig-and-Hive-Visualisations.ipynb) - Jupyter notebook used to create visualisations for the final PDF.
+ [Cleaning.pig](cleaning.pig) - This Pig file cleans the movies, ratings and tags files.
+ [Processing.pig](processing.pig) - This Pig file joins the data and carries out the queries, saving all data generated.
+ [Hive_queries.hive](hive_queries.hive) - This Hive file contains the code to read in the data into a table, query the data and save some output for breakdown of the genres.

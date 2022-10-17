-- This file carries out the initial cleaning of the data using Pig. The outputs folder is deleted at the start of this script so fresh output can be created.

fs -rm -f -r -R outputs;

/*
Method

Movie.csv
1. Load movies.csv, I realized that PigStorage(',') didn't handle the quoted commas in the titles.
2. The CSVLoader() can be used to deal with the quoted commas.
3. Remove the header from the csv using a filter on each field.
4. Split the genres up into a tuple.
5. Extract the year from the title and create a separate column.
6. Remove the year from the title.

Ratings.csv
1. Load the CSV in with the CSVLoader().
2. Remove the header from the csv using a filter on each field.
3. Change the timestamp from Unix time to a yyyy/MM/dd HH:mm:ss format.

Tags.csv
1. Load the CSV in with the CSVLoader().
2. Remove the header from the csv using a filter on each field.
3. Change the timestamp from Unix time to a yyyy/MM/dd HH:mm:ss format.

I will then save these cleaned csv files for use later on.
*/

-- Movie.csv
-- 1. Load in the movies spreadsheet first and check the structure.
movies = LOAD 'data/movies.csv' USING PigStorage(',') AS (movieId:chararray, title:chararray, genres:chararray);
B = foreach movies generate *;
-- dump B
-- We can see that the title column is not parsed correctly due to the quoted commas being split.

-- 2. We can use the CSVLoader() to deal with these quoted commas and parse the CSV as we want.
movies = LOAD 'data/movies.csv' USING org.apache.pig.piggybank.storage.CSVLoader() AS (movieId:chararray, title:chararray, genres:chararray);
A = foreach movies GENERATE title;
-- DUMP A;
-- Now that we have the dataset read in as we would like we can continue with the cleaning.

-- 3. We can remove the header from the file using a filter
filter_out_header = FILTER movies BY (movieId != 'movieId') AND (title != 'title') AND (genres != 'genres');
-- DUMP filter_out_header;

-- 4. Use STRSPLIT to split the genre values up into individual genres. We will carry this out after joining as this is the ideal format for processing in Hive
-- split_genres = foreach filter_out_header Generate movieId, title, STRSPLIT(genres,'\\|') AS genres;
-- DUMP split_genres;

-- 5. Separate the year from the title.
add_year = foreach filter_out_header Generate movieId, title, genres, (int)SUBSTRING(title, (int)SIZE(title) - 5, (int)SIZE(title) - 1) AS year;
-- DUMP add_year;

-- 6. Remove the year from the title.
fix_title = foreach add_year Generate movieId, SUBSTRING(title, 0, (int)SIZE(title) - 7) AS title, genres, year;
-- DUMP fix_title;
STORE fix_title INTO 'outputs/clean_movies' USING org.apache.pig.piggybank.storage.CSVExcelStorage();

-- Ratings.csv
-- 1. Load in the ratings using the CSVLoader()
ratings = LOAD 'data/ratings.csv' USING org.apache.pig.piggybank.storage.CSVLoader() AS (userId:chararray, movieId:chararray, rating:double, timestamp:int);
-- DUMP ratings;

-- 2. We can remove the header from the file using a filter
filter_out_header = FILTER ratings BY (userId != 'userId') AND (movieId != 'movieId') AND ((chararray)rating != 'rating') AND ((chararray)timestamp != 'timestamp');
-- DUMP filter_out_header;

-- 3. Fix the timestamp using the ToDate method.
fix_ratings_timestamp = foreach filter_out_header Generate userId, movieId, rating, ToString(ToDate(timestamp * 1000L), 'yyyy-MM-dd HH:mm:ss') AS timestamp;
-- DUMP fix_ratings_timestamp;
STORE fix_ratings_timestamp INTO 'outputs/clean_ratings' USING org.apache.pig.piggybank.storage.CSVExcelStorage();

-- Tags.csv
-- 1. Load the tags using the CSVLoader()
tags = LOAD 'data/tags.csv' USING org.apache.pig.piggybank.storage.CSVLoader() AS (userId:chararray, movieId:chararray, tag:chararray, timestamp:int);
-- DUMP tags;

-- 2. We can remove the header from the file using a filter
filter_out_header = FILTER tags BY (userId != 'userId') AND (movieId != 'movieId') AND (tag != 'tag') AND ((chararray)timestamp != 'timestamp');
-- DUMP filter_out_header;

-- 3. Fix the timestamp using the ToDate method.
fix_tags_timestamp = foreach filter_out_header Generate userId, movieId, tag, ToString(ToDate(timestamp * 1000L), 'yyyy-MM-dd HH:mm:ss') AS timestamp;
-- DUMP fix_tags_timestamp;
STORE fix_tags_timestamp INTO 'outputs/clean_tags' USING org.apache.pig.piggybank.storage.CSVExcelStorage();

-- These have all been saved to outputs and can now be joined and used for querying.

-- This file joins the movies, ratings and tags files into one bag/relation and then queries the data.
-- This is all in one file as after joining there seems to be an issue loading the new relation back in as a CSV, possibly due to the size (> 8MB)

-- Delete all the saved data so the script can be run again
fs -rm -f -r -R outputs/joined;
fs -rm -f -r -R outputs/querying;
fs -rm -f -r -R outputs/most_rated_movie;
fs -rm -f -r -R outputs/highest_average_rated_movie;
fs -rm -f -r -R outputs/most_5star_rated_movie;
fs -rm -f -r -R outputs/user_with_highest_average_rating;

/*
I decided to exclude links as there is no insight we can gain without carrying out some scraping on the related pages.
There is plenty of data in the rest of the files to derive some insight, so we will look at these for now.

I carried out Left joins on ratings so that we kept all instances of ratings even if there may not be a tag to match against it. This is implicit in a full join,
but would include tags with no matching rating (which there shouldn't be, but this will handle that). We will perform a full join on movies however so we can see
if there are any movies with no ratings.
*/

-- Load in movies
movies = LOAD 'outputs/clean_movies/part-m-00000' USING org.apache.pig.piggybank.storage.CSVLoader() AS (movieId:chararray, title:chararray, genres:chararray, year:int);

-- Load in ratings
ratings = LOAD 'outputs/clean_ratings/part-m-00000' USING org.apache.pig.piggybank.storage.CSVLoader() AS (userId:chararray, movieId:chararray, rating:double, timestamp:chararray);

-- Load in tags
tags = LOAD 'outputs/clean_tags/part-m-00000' USING org.apache.pig.piggybank.storage.CSVLoader() AS (userId:chararray, movieId:chararray, tag:chararray, timestamp:chararray);

-- Left join tags onto ratings
ratings_tags = JOIN ratings BY (userId, movieId) LEFT OUTER, tags BY (userId, movieId);

-- Drop the duplicates of the values we joined on, so userId and movieId.
no_duplicates = foreach ratings_tags GENERATE ratings::userId AS userId, ratings::movieId AS movieId,  ratings::rating AS rating, ratings::timestamp AS ratings_timestamp, tags::tag AS tag, tags::timestamp AS tags_timestamp;
-- DUMP no_duplicates

-- Join movies on to this relation.
ratings_tags_movies = JOIN no_duplicates BY movieId, movies BY movieId;

-- Clean up the duplicate of movieId and re-order the columns as needed
final_relation = foreach ratings_tags_movies GENERATE no_duplicates::userId AS userId, movies::movieId AS movieId, movies::title AS title, no_duplicates::rating AS rating,
                no_duplicates::ratings_timestamp AS ratings_timestamp, movies::year AS year, movies::genres AS genres, no_duplicates::tag AS tag, no_duplicates::tags_timestamp AS tags_timestamp;
-- DUMP final_relation;
-- describe final_relation;

-- Store the output into our local output folder.
STORE final_relation INTO 'outputs/joined' USING PigStorage();
-- We will need to rename outputs/joined/part-m-00000 to outputs/joined/data.tsv as Hive struggles to read in files with no extension.

-- Now that we have the data saved for Hive querying, we can split the genres as mentioned before.
for_querying = foreach final_relation Generate userId, movieId, title, rating, ratings_timestamp, year, STRSPLIT(genres,'\\|') AS genres, tag, tags_timestamp;
-- DUMP split_genres;

-- Now we can process the timestamp and convert it to a DateTime object
-- for_querying = foreach split_genres GENERATE userId, movieId, title, rating, ToDate(ratings_timestamp, 'yyyy-MM-dd HH:mm:ss') AS ratings_timestamp, year, genres, tag, ToDate(tags_timestamp, 'yyyy-MM-dd HH:mm:ss') AS tags_timestamp;

STORE for_querying INTO 'outputs/querying' USING org.apache.pig.piggybank.storage.CSVExcelStorage();
/*
Now I will use this cleaned dataset to answer the below queries.
------------------------------------------------------------------------------------------------------------------------
*/

------------------------------------------------------------------------------------------------------------------------
-- 1. What is the title of the movie with the highest number of ratings (top-rated movie)?
-- My understanding is to return the movies that have the most ratings from users, so I will count how many ratings each movie has and order by the count descending.
grouped_movies = GROUP for_querying by title;
-- DUMP grouped_movies;
describe grouped_movies;
movie_counts = foreach grouped_movies GENERATE group, SIZE(for_querying) AS RatingsCount;
Sorted = ORDER movie_counts BY RatingsCount DESC;

STORE Sorted INTO 'outputs/most_rated_movie' USING org.apache.pig.piggybank.storage.CSVExcelStorage();
-- Top5 = LIMIT Sorted 5;
-- DUMP Top5;
------------------------------------------------------------------------------------------------------------------------

-- 2. What is the title of the most liked movie (e.g. only 5 stars ratings OR only 4 and 5 star ratings OR majority of 5 star ratings)
-- Here I have decided to look at the average movie rating, but only when the movie has over 20 ratings, so the average will have a reasonable representation of users.
grouped_movies = GROUP for_querying by title;
-- DUMP grouped_movies;
-- describe grouped_movies;
movie_counts = foreach grouped_movies GENERATE group, AVG(for_querying.rating) AS AverageRating, COUNT(for_querying.rating) AS RatingsCount;
Sorted = ORDER movie_counts BY AverageRating DESC;
-- describe Sorted;
filtered = FILTER Sorted BY RatingsCount >= 20;

STORE filtered INTO 'outputs/highest_average_rated_movie' USING org.apache.pig.piggybank.storage.CSVExcelStorage();

-- Top5 = LIMIT filtered 5;
-- DUMP Top5;

------------------------------------------------------------------------------------------------------------------------
-- 2a. Now let us just consider 5 star reviews and find the movies with the most 5 star ratings to look at this from a different angle.
filtered = FILTER for_querying BY rating==5;
grouped_movies = GROUP filtered by title;
-- DUMP grouped_movies;
-- describe grouped_movies;
movie_counts = foreach grouped_movies GENERATE group, AVG(filtered.rating) AS AverageRating, COUNT(filtered.rating) AS RatingsCount;
Sorted = ORDER movie_counts BY RatingsCount DESC;

STORE Sorted INTO 'outputs/most_5star_rated_movie' USING org.apache.pig.piggybank.storage.CSVExcelStorage();
-- describe Sorted;
Top5 = LIMIT Sorted 5;
DUMP Top5;

------------------------------------------------------------------------------------------------------------------------
-- 3. Who is the User with the highest average rating?
-- This is the user who has the highest average review rating, so I will look at the average and also consider the number of reviews given.
grouped_users = GROUP for_querying by userId;
-- DUMP grouped_users;
-- describe grouped_users;
user_averages = foreach grouped_users GENERATE group, AVG(for_querying.rating) AS AverageRating, COUNT(for_querying.rating) AS RatingsCount;
Sorted = ORDER user_averages BY AverageRating DESC;

STORE Sorted INTO 'outputs/user_with_highest_average_rating' USING org.apache.pig.piggybank.storage.CSVExcelStorage();
Top5 = LIMIT Sorted 5;
DUMP Top5;
------------------------------------------------------------------------------------------------------------------------

-- Now we will move to Hive and repeat these queries and try some more advanced ones.
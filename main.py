""" Importing Spark """
import DatasetAnalysisEngine
from pyspark.sql import SparkSession
from pyspark import *

import sys
reload(sys)
sys.setdefaultencoding('utf-8')

__author__ = 'Nnamdi Ekwe-Ekwe'

""" THESE ARE CONSOLE BASED METHODS """

SPARK_APP_NAME = "CS5052_Spark_Coursework_1"

conf = SparkConf().setAll([("spark.executor.memory", "8g"),
                                   ("spark.executor.cores", "8"),
                                   ("spark.cores.max", "8"),
                                   ("spark.driver.memory", "8g")])
spark = SparkSession.builder \
            .master("local") \
            .config(conf=conf) \
            .appName(SPARK_APP_NAME) \
            .getOrCreate()

engine_instance = DatasetAnalysisEngine.DatasetAnalysis(spark=spark)


# Given a userId, show the movie(s) watched by that specific user - Non SparkSQL method
engine_instance.show_all_movies_watched_by_specific_user_non_sql(1)

# Given a list of users, search all movie(s) watched by each user
# Please note I have left in the userId in the output table to ensure the correct user is searched and found
list_of_users = [20, 1, 3, 10, 4, 5, 6]
for user in list_of_users:
        engine_instance.show_all_movies_watched_by_specific_user_non_sql(user)

# Given a genre, show all movies in that genre - Non SparkSQL
engine_instance.show_movies_in_genre_non_sql("Comedy")

# Given a list of genres, show all movie(s) in that genre
list_of_genres = ["Action", "Comedy", "Drama", "Romance", "Horror", "Thriller"]
for genres in list_of_genres:
  engine_instance.show_movies_in_genre_non_sql(genres)

# Given a year, show all movies released in that year
engine_instance.show_movies_by_year(2000)

# Given a number n, show the top n movies with the highest rating (Non SparkSQL)
engine_instance.list_top_n_movies_by_rating_non_sql(10)

# Given a number n, show the top n movies with the highest number of watches
engine_instance.list_top_n_movies_by_highest_number_of_watches(10)

# Given a userId, show the number of movie(s) watched by that specific user (Non SparkSQL)
engine_instance.show_number_of_movies_watched_by_specific_user_non_sql(120)

# Given either a title of a film or movie id, show average rating of that movie and number of users that watched
# the movie
# One argument must be passed into the method, else it will prompt you to pass an argument in
engine_instance.search_movies_show_average_rating(title_of_film="Sabrina (1995)")

# Given a user_id and genre, show the number movies watched by the user in the specific genre
engine_instance.show_all_movies_watched_by_user_in_specific_genre_non_sql(20, 'Comedy')

# Find the favourite genre of a given user or group of users
# show_statistics_on_genre_preferences_by_user(6, "Most")
list_of_users = [20, 1, 3, 10, 4, 5, 6]
for user in list_of_users:
 engine_instance.show_statistics_on_genre_preferences_by_user(user, "Most")

# Find the least favourite genre of a given user or group of users
# show_statistics_on_genre_preferences_by_user(400, "Least")
list_of_users = [20, 1, 3, 10, 4, 5, 6]
for user in list_of_users:
 engine_instance.show_statistics_on_genre_preferences_by_user(user, "Least")

# Given a genre, find all the userIDs that have watched films pertaining to that genre
engine_instance.cluster_users_by_movie_taste("Action")

# Given two users, compare their movie tastes
engine_instance.compare_movie_tastes_of_two_users(1, 10)


"""
YOU CAN RUN THIS IN A MAIN METHOD IF DESIRED
"""
# if __name__ == "__main__":
#     engine_instance = DatasetAnalysisEngine.DatasetAnalysis(spark=spark)
#     engine_instance.show_all_movies_watched_by_specific_user_non_sql(user_id=200)
#

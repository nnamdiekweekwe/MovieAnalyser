from pyspark.sql.functions import *
from pyspark import *

import sys

reload(sys)
sys.setdefaultencoding('utf-8')
"""
THIS IS THE MAIN DATASET ANALYSIS ENGINE
"""


class DatasetAnalysis:
    def __init__(self, spark):
        """ INITIAL CONFIGURATION OF SPARK AND DATASETS"""

        spark_instance = spark

        # print spark.sparkContext.getConf().getAll()

        self.ratings = spark_instance.read.csv("MovieAnalyser/datasets/ratings.csv", header=True)
        self.movies = spark_instance.read.csv("MovieAnalyser/datasets/movies.csv", header=True)

        self.users_and_movies_data_frame = self.ratings
        self.movies_and_titles_data_frame = self.movies

    # This method lists the top n movies by average rating, highest first - Non SparkSQL method
    def list_top_n_movies_by_rating_non_sql(self, number):
        """
        :param number: The number of movies requested
        """

        average_ratings_of_films_by_movieId = self.users_and_movies_data_frame.groupBy('movieId').agg(
            {"rating": "avg"}).withColumnRenamed("avg(rating)", "Average_Rating")
        average_ratings_of_films_by_movieId = average_ratings_of_films_by_movieId.alias("a_r")

        movies_and_ratings = self.users_and_movies_data_frame.join(average_ratings_of_films_by_movieId,
                                                                   self.users_and_movies_data_frame.movieId ==
                                                                   col("a_r.movieId")). \
            drop(col("a_r.movieId")).drop('userId', 'rating', 'timestamp')

        movie_name_and_ratings = movies_and_ratings.join(self.movies_and_titles_data_frame,
                                                         movies_and_ratings.movieId ==
                                                         self.movies_and_titles_data_frame.movieId).drop(
            self.movies_and_titles_data_frame.movieId)

        result = movie_name_and_ratings.drop('movieId', 'genres') \
            .sort(desc("Average_Rating")) \
            .dropDuplicates().limit(number).toJSON().collect()
        return result

    # This method shows all movies by a specific year
    def show_movies_by_year(self, desired_year):
        """
        :param desired_year: The year requested 
        :return result: The movies released in the year
        """
        result = self.movies_and_titles_data_frame.filter \
            (self.movies['title'].__contains__('(' + str(desired_year))) \
            .select(self.movies['title'])
        return result.toJSON().collect()

    # This method shows the movies that pertain to the specific genre
    def show_movies_in_genre_non_sql(self, genre):
        """
        :param genre: The genre requested
        :return: the movies pertaining to that genre
        """
        movies_in_genre = self.movies_and_titles_data_frame. \
            filter(self.movies_and_titles_data_frame['genres'].__contains__(genre)) \
            .drop('movieId', 'genres')
        if movies_in_genre.count() != 0:
            return movies_in_genre.toJSON().collect()
        else:
            "Nothing was found for your query, please try again!"

    # This method searches movie by id/title,
    # shows the average rating and the number of users that have watched the movie
    def search_movies_show_average_rating(self, movie_identification=None, title_of_film=None):
        """
        :param movie_identification: movieId (optional argument)
        :param title_of_film: title (of the film - again, optional argument)
        If no argument supplied, method will prompt you to pass in an argument
        Only one argument can be supplied at a time
        """
        if title_of_film is None and movie_identification is None:
            print "Please pass an argument!"
        elif title_of_film and movie_identification:
            print "Please pass in one argument!"
        else:
            average_ratings_of_films_by_movieId = self.users_and_movies_data_frame.groupBy('movieId') \
                .agg({"rating": "avg"}).withColumnRenamed("avg(rating)", "Average_Rating")
            average_ratings_of_films_by_movieId = average_ratings_of_films_by_movieId.alias("a_r")

            titles_of_movies = self.movies_and_titles_data_frame.drop('genres')

            movie_id_average_rating_number_of_users_watched = self.users_and_movies_data_frame.join(
                average_ratings_of_films_by_movieId,
                self.users_and_movies_data_frame.movieId == col("a_r.movieId")).drop(col("a_r.movieId")) \
                .drop('timestamp', 'rating').groupBy(['movieId', 'Average_Rating']).count() \
                .withColumnRenamed("count", "Number_Of_Users_Who_Have_Watched_This_Movie")

            final_df_containing_all_data = movie_id_average_rating_number_of_users_watched.join(
                titles_of_movies,
                movie_id_average_rating_number_of_users_watched.movieId == titles_of_movies.movieId,
                'inner'
            ).drop(titles_of_movies.movieId).drop(movie_id_average_rating_number_of_users_watched.movieId)

            final_df_containing_all_data_filtered = final_df_containing_all_data.where(
                (col('movieId') == movie_identification) |
                (col('title') == title_of_film))

            if final_df_containing_all_data_filtered.count() != 0:
                print "Here are your results!"
                final_df_containing_all_data_filtered.show(truncate=False)
            else:
                print "Nothing was found for your query, please try again!"

    # This method searches the ratings dataset to show the movies the user has watched
    def show_all_movies_watched_by_specific_user_non_sql(self, user_id):
        """
        :param user_id: The user id to search for. 
        """
        names_of_movies_watched_by_user = self.users_and_movies_data_frame.join(self.movies_and_titles_data_frame,
                                                                                self.users_and_movies_data_frame.movieId ==
                                                                                self.movies_and_titles_data_frame.movieId,
                                                                                'inner').drop(
            self.movies_and_titles_data_frame.movieId) \
            .drop('rating', 'timestamp', 'genres', 'movieId').filter(
            self.users_and_movies_data_frame.userId == str(user_id))
        if names_of_movies_watched_by_user != 0:
            print "Hello there! User: " + str(user_id) + " has watched these movies: "
            names_of_movies_watched_by_user.show(truncate=False)
        else:
            print "Nothing was found for your query, please try again!"

    # This method lists the top n movies by number of watches, highest first
    def list_top_n_movies_by_highest_number_of_watches(self, number):
        """
        :param number: The number of movies requested to be shown
        """
        movies_and_watches = self.users_and_movies_data_frame.join(self.movies_and_titles_data_frame,
                                                                   self.users_and_movies_data_frame.movieId ==
                                                                   self.movies_and_titles_data_frame.movieId) \
            .drop(self.movies_and_titles_data_frame.movieId) \
            .drop('userId', 'rating', 'genres', 'timestamp').groupBy('movieId').agg({"movieId": "count"}) \
            .withColumnRenamed("count(movieId)", "Number_Of_Watches")

        movies_and_watches_sorted = movies_and_watches.sort(movies_and_watches.Number_Of_Watches.desc())
        movies_and_watches_sorted.show(n=number, truncate=False)

    # This method shows the genre preferences of a particular user
    def show_statistics_on_genre_preferences_by_user(self, user_id, favourite):
        """
        Most Favourite is defined by the genre with the highest number of films watched by the user.
        Least Favourite is defined by the genre with the lowest number of films watched by the user.
        :param user_id: user_id to search for
        :param favourite: what search to make (whether Least Favourite or Most Favourite)
        :return final_result: the final result containing the genre preferences of the users
        """
        user_check = self.users_and_movies_data_frame.filter \
            (self.ratings['userId'].__contains__(str(user_id)))
        if user_check.count() == 0:
            print "User not found in dataset! Please try again."

        else:
            movies_watched_by_user = self.users_and_movies_data_frame \
                .join(self.movies_and_titles_data_frame,
                      self.users_and_movies_data_frame.movieId ==
                      self.movies_and_titles_data_frame.movieId,
                      'inner').drop(self.movies_and_titles_data_frame.movieId) \
                .filter(self.users_and_movies_data_frame.userId == str(user_id))

            movies_watched_by_user = movies_watched_by_user.drop('userId', 'rating', 'timestamp', 'movieId', 'title')

            rdd_genre_analysis = movies_watched_by_user.rdd.map(list)

            rdd_genre_analysis = rdd_genre_analysis.flatMap(lambda x: x[0].split("|")) \
                .map(lambda x: (x, 1)) \
                .reduceByKey(lambda x, y: x + y)

            counted_df = rdd_genre_analysis.toDF(['Genre', 'Amount_of_Films'])
            if favourite == "Most":
                final_genre_df = counted_df.sort(desc('Amount_of_Films'))
                favourite_genre = final_genre_df.select("Genre").rdd.flatMap(list).first()
                amount_of_films_for_favourite_genre = final_genre_df.select("Amount_of_Films").rdd.flatMap(list).first()
                print "User ID " + str(user_id) + "'s most favourite genre was " + str(favourite_genre) + \
                      "." + " The user watched " + str(
                    amount_of_films_for_favourite_genre) + " films in this genre."
                return favourite_genre, amount_of_films_for_favourite_genre
            elif favourite == "Least":
                final_genre_df = counted_df.sort(asc('Amount_of_Films'))
                amount_of_films_for_least_favourite_genre = final_genre_df \
                    .groupBy("Amount_of_Films").agg({"Amount_of_Films": "min"}).rdd.flatMap(list).first()
                final_result = final_genre_df.filter(
                    final_genre_df.Amount_of_Films == amount_of_films_for_least_favourite_genre
                ).drop('Amount_of_Films')
                print "User ID " + str(user_id) + "'s least favourite genre(s) were: "
                final_result.show(truncate=False)
                return final_result
            else:
                print "Nothing found for your query! Please try again!"

    def compare_movie_tastes_of_two_users(self, user_id_1=None, user_id_2=None):
        if user_id_1 is None and user_id_2 is None:
            print "Please in pass an argument!"
        elif not user_id_1 or not user_id_2:
            print "Please pass in two arguments!"
        else:
            a = self.show_statistics_on_genre_preferences_by_user(user_id_1, "Least")
            b, c = self.show_statistics_on_genre_preferences_by_user(user_id_1, "Most")

            d = self.show_statistics_on_genre_preferences_by_user(user_id_2, "Least")
            e, f = self.show_statistics_on_genre_preferences_by_user(user_id_2, "Most")

            g = self.show_number_of_movies_watched_by_specific_user_non_sql(user_id=user_id_1)
            h = self.show_number_of_movies_watched_by_specific_user_non_sql(user_id=user_id_2)

            i, j = self.show_highest_rated_movies_for_user(user_id=user_id_1)
            k, l = self.show_highest_rated_movies_for_user(user_id=user_id_2)

            movies_watched_by_user_id_1 = g.select("Number_Of_Films_Watched_By_User").head()[0]
            movies_watched_by_user_id_2 = h.select("Number_Of_Films_Watched_By_User").head()[0]

            if movies_watched_by_user_id_1 > movies_watched_by_user_id_2:
                movie_message = "User " + str(
                    user_id_1) + " seems to be a bit of a film buff watching a total of " + str(
                    movies_watched_by_user_id_1) + " movies. User " + str(user_id_2) + \
                                " on the other hand watched " + str(movies_watched_by_user_id_2) + "movies."
            elif movies_watched_by_user_id_2 > movies_watched_by_user_id_1:
                movie_message = "User " + str(
                    user_id_2) + " seems to be a bit of a film buff watching a total of " + str(
                    movies_watched_by_user_id_2) + " movies. User " + str(user_id_1) + \
                                " on the other hand watched " + str(movies_watched_by_user_id_1) + " movies."
            else:
                movie_message = "User " + str(user_id_1) + " and " + "User " + str(
                    user_id_2) + "watched the same number of films! They watched: " + str(
                    movies_watched_by_user_id_1) + " movies."

            rdd_genre = a.select("Genre").rdd
            rdd_genre_2 = d.select("Genre").rdd

            def processRecord(record):
                print(record)

            message = """
                        User %(user_id_1)s's least favourite genre(s) were: %(a)s. Their most favourite genre was:
                        %(b)s. They watched %(c)s movies in this genre. User %(user_id_1)s's highest rating given to a movie(s) was
                        %(h_rating_1)s.

                        In comparison, User %(user_id_2)s's least favourite genre(s) were: %(d)s. Their most favourite genre was:
                        %(e)s. They watched %(f)s movies in this genre. User %(user_id_2)s's highest rating given to a movie(s) was
                        %(h_rating_2)s.

                        %(movie_message)s


                        """ % {'user_id_1': user_id_1,
                               'user_id_2': user_id_2, 'a': a.select("Genre").collect(), 'b': b, 'c': c,
                               'd': d.select("Genre").collect(),
                               'e': e,
                               'f': f,
                               'movie_message': movie_message, 'h_rating_1': j, 'h_rating_2': l}
            print message
            return message

    # This method searches the ratings dataset and shows the number of movies the user has watched
    def show_number_of_movies_watched_by_specific_user_non_sql(self, user_id):
        movies_and_watches = self.users_and_movies_data_frame.join(self.movies_and_titles_data_frame,
                                                                   self.users_and_movies_data_frame.movieId ==
                                                                   self.movies_and_titles_data_frame.movieId,
                                                                   'inner').drop(
            self.movies_and_titles_data_frame.movieId) \
            .drop('title', 'rating', 'genres', 'timestamp').filter(
            self.users_and_movies_data_frame.userId == str(user_id)).groupBy('userId').agg({"userId": "count"}) \
            .withColumnRenamed("count(userId)", "Number_Of_Films_Watched_By_User")
        if movies_and_watches != 0:
            print "Here are your results for: " + str(user_id)
            movies_and_watches.show(truncate=False)
            return movies_and_watches

        else:
            print "Nothing was found for your query, please try again!"

    # This method searches and displays the number movies watched by the user in a specific genre
    def show_all_movies_watched_by_user_in_specific_genre_non_sql(self, user_id, genre):
        """
        :param user_id: user_id to search for
        :param genre: genre to look for
        """
        number_of_movies_watched_by_user_in_specific_genre = self.users_and_movies_data_frame \
            .join(self.movies_and_titles_data_frame,
                  self.users_and_movies_data_frame.movieId ==
                  self.movies_and_titles_data_frame.movieId,
                  'inner').drop(self.movies_and_titles_data_frame.movieId) \
            .drop('rating', 'timestamp', 'movieId', 'title') \
            .filter(self.users_and_movies_data_frame.userId == str(user_id)) \
            .filter(self.movies_and_titles_data_frame['genres'].__contains__(genre)) \
            .groupBy('userId').agg({"genres": "count"}) \
            .withColumnRenamed("count(genres)", "Number_Of_Films_Watched_By_User_In_" + genre + "_Genre")

        if number_of_movies_watched_by_user_in_specific_genre.count() != 0:
            print "Hello there! Here are your results for your requested film: "
            number_of_movies_watched_by_user_in_specific_genre.show(truncate=False)
        else:
            print "Nothing was found for your query, please try again!"

    # This method clusters users by movie taste.
    # Given a genre, it displays all the users that have watched films in that genre.
    def cluster_users_by_movie_taste(self, genre=None):
        """
        :param genre: The genre desired
        :return: The genre and its respective users
        """
        if genre is None:
            print "Please pass in an argument"
        else:
            movies_in_genre = self.movies_and_titles_data_frame. \
                filter(self.movies_and_titles_data_frame['genres'].__contains__(genre))

            genre_and_users = movies_in_genre.join(self.users_and_movies_data_frame,
                                                   movies_in_genre.movieId ==
                                                   self.users_and_movies_data_frame.movieId).drop(
                movies_in_genre.movieId)

            print "These are the userIDs that have watched Genre: " + str(genre) + "."

            # We drop duplicate userId entries as our table gives a userId entry for every film
            # that they have watched that fits our genre

            # We only need to know that they watched a film in that genre
            genre_and_users = genre_and_users.drop('rating', 'timestamp', 'movieId', 'genres', 'title').dropDuplicates(
                ['userId'])
            genre_and_users.show(
                truncate=False)
            return genre_and_users

    # This is a helper method that shows the highest rated movies for a specific user
    def show_highest_rated_movies_for_user(self, user_id):
        """
        
        :param user_id: User Id
        :return: The movies and ratings for the user as well as the highest rating the user made
        """
        specific_user_results = self.users_and_movies_data_frame. \
            filter(self.users_and_movies_data_frame.userId == str(user_id)).drop('timestamp')
        highest_rating_from_user = specific_user_results.drop('userId') \
            .agg({"rating": "max"}).rdd.flatMap(list).first()

        final_result = specific_user_results.filter(
            specific_user_results.rating == highest_rating_from_user
        )
        movies_and_ratings = final_result.join(self.movies_and_titles_data_frame,
                                               final_result.movieId == self.movies_and_titles_data_frame.movieId) \
            .drop(final_result.movieId).drop('genres', 'movieId', 'rating', 'userId')

        movies_and_ratings.show(truncate=False)
        return movies_and_ratings, highest_rating_from_user

    # This is a helper method that shows the lowest rated movies for a specific user
    def show_lowest_rated_movies_for_user(self, user_id):
        """
            :param user_id: User Id
            :return: The movies and ratings for the user as well as the lowest rating the user made
        """
        specific_user_results = self.users_and_movies_data_frame. \
            filter(self.users_and_movies_data_frame.userId == str(user_id)).drop('timestamp')
        lowest_rating_from_user = specific_user_results.drop('userId') \
            .agg({"rating": "min"} ).rdd.flatMap(list).first()

        final_result = specific_user_results.filter(
            specific_user_results.rating == lowest_rating_from_user
        )
        movies_and_ratings = final_result.join(self.movies_and_titles_data_frame,
                                               final_result.movieId == self.movies_and_titles_data_frame.movieId) \
            .drop(final_result.movieId).drop('genres', 'movieId', 'rating', 'userId')

        movies_and_ratings.show(truncate=False)
        return movies_and_ratings, lowest_rating_from_user

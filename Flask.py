from flask import Flask, render_template, jsonify, json, request
import DatasetAnalysisEngine
from pyspark import *
from pyspark.sql import SparkSession

app = Flask(__name__)

"""
THIS IS THE MAIN FLASK API FILE THAT MAPS ALL MY ROUTES
"""


def init_spark_context():
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

    return spark


# GET ROUTE
@app.route('/')
def home_route():
    return render_template('index.html')


# POST ROUTE
@app.route('/movies_released_by_year', methods=['POST'])
def show_movies_by_year():
    spark = init_spark_context()
    engine_instance = DatasetAnalysisEngine.DatasetAnalysis(spark=spark)
    results = engine_instance.show_movies_by_year(desired_year=request.form['year'])
    return render_template('movies_by_year.html',
                           results=results, desired_year=request.form['year'])


# POST ROUTE
@app.route('/movies_belonging_to_a_genre', methods=['POST'])
def show_movies_belonging_to_a_genre():
    spark = init_spark_context()
    engine_instance = DatasetAnalysisEngine.DatasetAnalysis(spark=spark)
    if request.method == 'POST':
        results = engine_instance.show_movies_in_genre_non_sql(genre=request.form['genre'])
        return render_template('movies_in_genre.html',
                               results=results, genre=request.form['genre'])


# THIS IS A GET ROUTE
@app.route('/show_average_rating_movies/<string:title_of_film>')
@app.route('/show_average_rating_movies/<int:movieId>')
def search_movies_show_average_rating(title_of_film=None, movieId=None):
    spark = init_spark_context()
    engine_instance = DatasetAnalysisEngine.DatasetAnalysis(spark=spark)
    if title_of_film:
        engine_instance.search_movies_show_average_rating \
            (title_of_film=title_of_film)
    elif movieId:
        engine_instance.search_movies_show_average_rating(
            movie_identification=movieId
        )
    return render_template('genericpage.html')


# THIS IS A GET ROUTE
@app.route('/show_all_movies_watched_by_user/<int:user_id>')
def show_all_movies_watched_by_user(user_id):
    spark = init_spark_context()
    engine_instance = DatasetAnalysisEngine.DatasetAnalysis(spark=spark)
    engine_instance.show_all_movies_watched_by_specific_user_non_sql \
        (user_id=user_id)
    return render_template('genericpage.html')


# THIS IS A GET ROUTE
@app.route('/topnmoviesbyhighestwatchnumbers/<int:number>/')
def show_top_n_movies_by_highest_watches(number):
    spark = init_spark_context()
    engine_instance = DatasetAnalysisEngine.DatasetAnalysis(spark=spark)
    engine_instance.list_top_n_movies_by_highest_number_of_watches \
        (number=number)
    return render_template('genericpage.html')


# THIS IS A GET ROUTE
@app.route('/show_number_of_movies_watched_by_user/<int:user_id>/')
def show_number_of_movies_watched_by_user(user_id):
    spark = init_spark_context()
    engine_instance = DatasetAnalysisEngine.DatasetAnalysis(spark=spark)
    results = engine_instance.show_number_of_movies_watched_by_specific_user_non_sql \
        (user_id=user_id)
    return render_template('genericpage.html')


# THIS IS A GET ROUTE
@app.route('/movieswatchedbyuserspecificgenre/<int:user_id>/<string:genre>/')
def show_all_movies_watched_by_user_on_specific_genre(user_id, genre):
    spark = init_spark_context()
    engine_instance = DatasetAnalysisEngine.DatasetAnalysis(spark=spark)
    results = engine_instance.show_all_movies_watched_by_user_in_specific_genre_non_sql \
        (user_id=user_id, genre=genre)
    return render_template('genericpage.html')


# THIS IS A GET ROUTE
@app.route('/clusterusersbymovietaste/<string:genre>/')
def cluster_users_by_movie_taste(genre):
    spark = init_spark_context()
    engine_instance = DatasetAnalysisEngine.DatasetAnalysis(spark=spark)
    results = engine_instance.cluster_users_by_movie_taste(genre=genre)
    return render_template('genericpage.html')


# THIS IS A GET ROUTE
@app.route('/statsongenrepreferences/<int:userId>/<string:option>/')
def stats_on_genre_preferences(userId, option):
    spark = init_spark_context()
    engine_instance = DatasetAnalysisEngine.DatasetAnalysis(spark=spark)

    print "THIS IS OPTION: " + option
    if (option == "Most"):
        favourite_genre, amount_of_films_for_favourite_genre = \
            engine_instance.show_statistics_on_genre_preferences_by_user(user_id=userId, favourite=option)
    elif (option == "Least"):
        results = engine_instance. \
            show_statistics_on_genre_preferences_by_user(
            user_id=userId, favourite=option)

    return render_template('genericpage.html')


# THIS IS A GET ROUTE
@app.route('/highestratedmoviesforuser/<int:userId>/')
def highest_rated_movies_for_user(userId):
    spark = init_spark_context()
    engine_instance = DatasetAnalysisEngine.DatasetAnalysis(spark=spark)
    results = engine_instance.show_highest_rated_movies_for_user(user_id=userId)
    return render_template('genericpage.html')


# THIS IS A GET ROUTE
@app.route('/comparemovietastesoftwousers/<int:userId_1>/<int:userId_2>/')
def compare_movie_tastes_of_two_users(userId_1, userId_2):
    spark = init_spark_context()
    engine_instance = DatasetAnalysisEngine.DatasetAnalysis(spark=spark)
    results = engine_instance.compare_movie_tastes_of_two_users(user_id_1=userId_1, user_id_2=userId_2)
    return render_template('movietastes.html', results=results, userId_1=userId_1, userId_2=userId_2)


# MAIN PROGRAM ENTRY
if __name__ == '__main__':
    app.run(debug=True)

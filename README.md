# Movie Analyser

MovieAnalyser is an Apache Spark Application that I built to analyse and glean insights from Grouplens's MovieLens [dataset](https://grouplens.org/datasets/movielens/latest/).

This application is built in Python and Apache Spark. I use Flask as a web microframework on top of my analysis engine to serve results to the user.

# API Routes

Below are a list of API routes that display the information gleaned from my analysis. Some of the results from these API calls are viewable in the browser, others in the console.

Currently, there are only two browser visualised results. The user can see all the movies by a particular genre and all movies released in a particular year in their browser. Please note that these are `POST` routes, all other routes are `GET` routes.

These are the full list of routes:

`/movies_released_by_year` - *Can be visualised in the browser*

`/movies_belonging_to_a_genre` - *Can be visualised in the browser*

`/show_average_rating_movies/<string:title_of_film>`

`/show_average_rating_movies/<int:movieId>`

`/show_all_movies_watched_by_user/<int:user_id>`

`/topnmoviesbyhighestwatchnumbers/<int:number>/`

`/show_number_of_movies_watched_by_user/<int:user_id>/`

`/movieswatchedbyuserspecificgenre/<int:user_id>/<string:genre>/`

`/clusterusersbymovietaste/<string:genre>/`

`/statsongenrepreferences/<int:userId>/<string:option>/`

`/highestratedmoviesforuser/<int:userId>/`

`/comparemovietastesoftwousers/<int:userId_1>/<int:userId_2>/`

**Example:**

`http://127.0.0.1:5000/show_all_movies_watched_by_user/10`

would show you all the movies watched by the user with id: 10

# Instructions

You will need to add the `ratings.csv` and `movies.csv` files to the `datasets` folder. You'll find these csv files in Grouplens's MovieLens [dataset](https://grouplens.org/datasets/movielens/latest/) (`ml-latest.zip`) folder.

You can run this file by submitting it to your local `spark-submit` script
in your local spark installation. 

If on a mac, this should be in:
the `/usr/local/spark` folder (by default).

The main file to run is `Flask.py`

You can run it like this:

`bin/spark-submit --master local[*] --total-executor-cores 14 --executor-memory 6g MovieAnalyser/Flask.py`

You can either leave this command as is or change the `total-executor-cores` and `--executor-memory` arguments for your specific machine specifications.

# Author

Nnamdi Ekwe-Ekwe

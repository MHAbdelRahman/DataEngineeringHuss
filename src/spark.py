import json
import os
import sys

import requests
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, get_json_object, concat_ws, udf
from pyspark.sql.types import StructType, StructField, StringType, ArrayType

from Utils import constants


def parse_array_from_string(x):
    if x is None:
        return ''
    res = json.loads(x)
    if isinstance(res, str):
        ult_res = list()
        ult_res.append(res)
        return ult_res
    return res


def get_s3_clubs_tweet(file_path):
    s3_clubs_tweets = spark.read.parquet(file_path)
    return s3_clubs_tweets


def get_clubs_info():
    resp_json = requests.get("http://localhost:8888/info").json()
    try:
        resp_list = resp_json[constants.JSON_RESULTS_NAME]
    except KeyError as e:
        raise Exception(f'API server exception, please check API logs. exception Type: {KeyError}')
    df = sc.parallelize(resp_list).map(lambda x: json.dumps(x))
    return spark.read.json(df)


def join_tweets_file_with_clubs_info():
    with open("../db_info/s3_url.txt") as f:
        path = f.readline()
    s3_clubs_tweets = get_s3_clubs_tweet(path)
    clubs_info_df = get_clubs_info()
    return s3_clubs_tweets.join(clubs_info_df,
                                (s3_clubs_tweets.user_id_str == clubs_info_df.external_id_str)
                                & (s3_clubs_tweets.user_name == clubs_info_df.name),
                                'inner').drop('name', 'external_id_str')


def get_df_processed_with_tweets_column():
    joined_df = join_tweets_file_with_clubs_info()
    tweet_entities_schema = StructType([
        StructField("hashtags", StringType(), True),
        StructField("symbols", StringType(), True),
        StructField("user_mentions", StringType(), True),
        StructField("urls", StringType(), True),
        StructField("media", StringType(), True)
    ])

    get_array = udf(parse_array_from_string, ArrayType(StringType()))
    return joined_df.withColumn("tweet_entities", from_json(col("tweet_entities"), tweet_entities_schema)) \
        .withColumn('hashtags', get_json_object('tweet_entities.hashtags', '$[*].text')) \
        .withColumn("hashtags", get_array(col('hashtags'))) \
        .withColumn('hashtags', concat_ws(",", col('hashtags')))


def save_as_hive_table():
    processed_df.write.mode('overwrite').partitionBy('club', 'user_id_str').format('parquet').saveAsTable('processed_info')


def get_query1():
    return spark.sql("SELECT a.club FROM processed_info a INNER JOIN processed_info b ON a.club=b.club "
                     "WHERE a.club='Barcelona' OR b.club='Real Madrid' GROUP BY a.club ORDER BY COUNT(a.tweet_entities) DESC LIMIT 1")


def get_query2():
    return spark.sql("SELECT club, total_wins_uefa, COUNT(if(tweet_favorite_count is not NULL and tweet_retweet_count is not NULL, 1, NULL)) "
                     "as reactions FROM processed_info GROUP BY club, total_wins_uefa ORDER BY total_wins_uefa DESC, reactions LIMIT 1")


def get_query3():
    pass


if __name__ == '__main__':
    os.environ['PYSPARK_PYTHON'] = sys.executable
    os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

    '''
    100 GB data
    16 GB Ram
    10~ usable
    
    yields: we need 10 partitions -> .master('local[10]') 
    '''
    spark = SparkSession.builder.master('local[10]').appName("Football data engineering exercise").getOrCreate()
    spark.conf.set("hive.exec.dynamic.partition", "true")
    spark.conf.set("hive.exec.dynamic.partition.mode", "nonstrict")
    sc = spark.sparkContext

    processed_df = get_df_processed_with_tweets_column()
    save_as_hive_table()
    get_query1().show()
    get_query2().show()

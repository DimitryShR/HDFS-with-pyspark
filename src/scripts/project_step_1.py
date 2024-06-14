import sys

import pyspark.sql.functions as F
from pyspark.sql.window import Window
from pyspark.sql import SparkSession
from common_func import city_define

def read_events_geo(event_path_geo:str, spark):
     df_events_geo = spark.read.parquet(f"{event_path_geo}")

     return df_events_geo

def read_city_geo(city_path_geo:str, spark):
     df_city_geo = spark.read.parquet(f"{city_path_geo}")

     return df_city_geo

# Определяем для всех сообщений с геопозицией ближайше расположенный город
def message_geo(df_events_geo, df_city_geo):
    df_message = df_events_geo \
        .filter(F.col("event_type")=="message") \
        .select(F.col("event.message_from").alias("user_id"), \
                F.col("event.message_id"), \
                F.coalesce(F.col("event.message_ts"), F.col("event.datetime")).cast("timestamp").alias("datetime"),
                F.col("lat").alias("lat_message"), \
                F.col("lon").alias("lon_message"))

    df_city_define = city_define(df_message, df_city_geo, "lat_message", "lat", "lon_message", "lon")

    df_message_geo = df_city_define \
        .drop(F.col("lat_message")) \
        .drop(F.col("lon_message")) \
        .drop(F.col("lat")) \
        .drop(F.col("lon")) \
        .cache()

    return df_message_geo

# Определяем последнее событие в разрезе пользователя и его город
def last_geo(df_message_geo):

    window_filter_last_city = Window().partitionBy("user_id")

    last_city = df_message_geo \
        .select("user_id", "datetime", "city") \
        .withColumn("dt_max", F.max("datetime").over(window_filter_last_city)) \
        .filter(F.col("datetime") == F.col("dt_max")) \
        .drop("dt_max") \
        .withColumn("local_time", F.from_utc_timestamp(F.col("datetime"), F.lit("Australia/Sydney"))) \
        .drop("datetime") \
        .groupBy("user_id", "local_time").agg(F.first("city").alias("act_city"))
        #.withColumn("local_time", F.from_utc_timestamp(F.col("datetime"), F.concat(F.lit("Australia/"), F.col('city'))))
    
    return last_city

# Определяем вероятный домашний адрес по непрерывной последовательности событий
# Допущение: берется не последние 27 календарных дней, а последние 27 дней с хотя бы 1 событием в разрезе пользователя
# Берется последовательность и рангуется в разрезе даты (т.е. у городов с одинаковой датой один ранг)
# Используется dense_rank
# Берем окно в разрезе пользователь, город с сортировкой по дате и сравниваем текущее и предыдущее значение, оставляем либо null (старт последовательности), либо с разницей в 1
# Считаем количество непрерывных событий в разрезе пользователь, город и оставляем только те, что больше 27
# Берем событие с максимальной датой (самая последняя последовательность)

def home_geo(df_message_geo):

    window_dense_rank = Window().partitionBy('user_id').orderBy(F.col('date').desc())
    window_city = Window().partitionBy('user_id', 'city').orderBy(F.col('date').desc())
    window_user_id = Window().partitionBy('user_id')

    home_city = df_message_geo \
        .select("user_id", F.col("datetime").cast("date").alias("date"), "city") \
        .distinct() \
        .withColumn("dense_rank", F.dense_rank().over(window_dense_rank)) \
        .withColumn("lag_dense_rank", F.lag("dense_rank", 1, 0).over(window_city)) \
        .filter((F.col("dense_rank") == (F.col("lag_dense_rank") + F.lit(1)))) \
        .withColumn("row_number", F.row_number().over(window_city)) \
        .withColumn("diff", F.col("dense_rank") - F.col("row_number")) \
        .groupBy("user_id", "city", "diff").agg(F.max(F.col("date")).alias("date"), F.count('*').alias("count")) \
        .filter(F.col("count") >= F.lit(27)) \
        .withColumn("max_dt", F.max("date").over(window_user_id)) \
        .filter(F.col("date") == F.col("max_dt")) \
        .groupBy(F.col("user_id")).agg(F.first("city").alias("home_city")) \
        .cache()
    
    return home_city

# При определении списка городов в качестве путешествия определяем непрерывную последовательность городов с сортировкой по дате события
# Сравниваем значение текущего и предыдущего городов, оставляем отличающиеся
# Вычитаем домашние адреса
# Считаем количество и формируем список
def travel(df_message_geo, df_home_city):
    
    window = Window().partitionBy("user_id").orderBy("datetime", "city")
    
    df_travel = df_message_geo \
                .select("user_id", F.col("datetime"), "city") \
                .distinct() \
                .withColumn("lag_city", F.lag("city").over(window)) \
                .filter((F.col("city") != F.col("lag_city")) | (F.col("lag_city")).isNull()) \
                .join(df_home_city \
                      .withColumnRenamed("home_city", "city") \
                , ["user_id", "city"], "left_anti") \
                .groupBy("user_id").agg(F.count("city").alias("travel_count"), F.collect_list("city").alias("travel_array"))
    
    return df_travel

# Формируем итоговый датасет
def user_mart(df_last_city, df_home_city, df_travel):

    user_mart = df_last_city.join(df_home_city, ["user_id"], "left").join(df_travel, ["user_id"], "left")

    return user_mart

def main():
    event_path_geo = sys.argv[1]
    city_path_geo = sys.argv[2]
    output_base_path = sys.argv[3]

    spark = SparkSession.builder \
                    .master("yarn") \
                    .config("spark.executor.memory", "3g")\
                    .config("spark.executor.cores", "2")\
                    .appName("Project s7 step_1") \
                    .getOrCreate()

    df_events_geo = read_events_geo(event_path_geo, spark)

    df_city_geo = read_city_geo(city_path_geo, spark)

    df_message_geo = message_geo(df_events_geo, df_city_geo)

    df_last_city = last_geo(df_message_geo)

    df_home_city = home_geo(df_message_geo)

    df_travel = travel(df_message_geo, df_home_city)

    df_user_mart = user_mart(df_last_city, df_home_city, df_travel)
    
    df_user_mart.write.mode("overwrite").parquet(f"{output_base_path}")

if __name__ == "__main__":
        main()
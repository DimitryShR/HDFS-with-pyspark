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

def last_message(df_events_geo):

    window_filter_last_event = Window().partitionBy("user_id").orderBy(F.col("datetime").desc())

    # определяем геопозицию последнего сообщения в разрезе отправителя
    df_last_message = df_events_geo \
                    .filter(F.col("event_type") == "message") \
                    .select(F.col("event.message_from").alias("user_id"), \
                                    F.coalesce("event.datetime", "event.message_ts").alias("datetime"), \
                                    F.col("lat").alias("lat_last_msg"), \
                                    F.col("lon").alias("lon_last_msg")) \
                    .withColumn("row_number", F.row_number().over(window_filter_last_event)) \
                    .filter(F.col("row_number")==F.lit(1)) \
                    .drop("datetime") \
                    .drop("row_number")
    return df_last_message

    # Подтягиваем ко всем событиям, где нет геопозиции, геопозицию последнего сообщения
    # Считаем количество событый в разрезе пользователь, тип события, неделя, месяц, геопозиция (предрасчет)
def events_with_geo(df_events_geo, df_last_message):

    df_events_with_geo = df_events_geo \
                    .select(F.coalesce("event.message_from", "event.reaction_from", "event.user").alias("user_id"), \
                        F.coalesce("event.datetime", "event.message_ts").alias("datetime"), \
                        "event_type", \
                        "lat", \
                        "lon") \
                    .join(df_last_message, "user_id", "inner") \
                    .select(
                            "user_id", \
                            F.month(F.col("datetime")).alias("month"), \
                            F.weekofyear(F.col("datetime")).alias("week"), \
                            "event_type", \
                            F.coalesce("lat", "lat_last_msg").alias("lat_def"), \
                            F.coalesce("lon", "lon_last_msg").alias("lon_def") \
                            ) \
                    .groupBy("week","month","event_type","lat_def","lon_def").count() \
                    .cache()

    return df_events_with_geo

    # Определяем зоны id по геопозиции
def events_with_zone(df_city_geo, df_events_with_geo):

    df_city_geo_alias = df_city_geo \
                        .select(F.col("id").alias("city_id"), \
                                "lat", \
                                "lon")
    df_events_with_zone_define = city_define(df_events_with_geo, df_city_geo_alias, "lat_def", "lat", "lon_def", "lon")

    df_events_with_zone = df_events_with_zone_define \
                    .select(F.col("week"), \
                            F.col("month"), \
                            F.col("event_type"),\
                            F.col("count").alias("events_count"), \
                            F.col("city_id"))

    return df_events_with_zone

    # Рассчитываем количество событий по типам в разрезе недели и месяца отдельно
    # джойним к уникальному отношению зона - месяц - неделя первоначальной таблицы
def events_count_period(df_events_with_zone):

    df_week = df_events_with_zone.groupBy("week", "city_id", "event_type").agg(F.sum(F.col("events_count")).alias("week_count")) \
                    .groupBy("week", "city_id").pivot("event_type", ["message", "reaction", "subscription", "registration"]).agg(F.sum("week_count")) \
                    .withColumnRenamed("message", "week_message") \
                    .withColumnRenamed("reaction", "week_reaction") \
                    .withColumnRenamed("subscription", "week_subscription") \
                    .withColumnRenamed("registration", "week_user")

    df_month = df_events_with_zone.groupBy("month", "city_id", "event_type").agg(F.sum(F.col("events_count")).alias("month_count")) \
                        .groupBy("month", "city_id").pivot("event_type", ["message", "reaction", "subscription", "registration"]).agg(F.sum("month_count")) \
                        .withColumnRenamed("message", "month_message") \
                        .withColumnRenamed("reaction", "month_reaction") \
                        .withColumnRenamed("subscription", "month_subscription") \
                        .withColumnRenamed("registration", "month_user")
    
    # Берем из первоначальной таблицы связки месяц - неделя - регион, т.к. в зависимости от года недели по разному относятся к месяцам
    df_events_count_period = df_events_with_zone.select("month", "week", "city_id").distinct() \
            .join(df_week, ["week", "city_id"], "left") \
            .join(df_month, ["month", "city_id"], "left")
    
    return df_events_count_period


def main():
    event_path_geo = sys.argv[1]
    city_path_geo = sys.argv[2]
    output_base_path = sys.argv[3]

    spark = SparkSession.builder \
                    .master("yarn") \
                    .config("spark.executor.memory", "3g")\
                    .config("spark.executor.cores", "2")\
                    .appName("Project s7 step_2") \
                    .getOrCreate()

    df_events_geo = read_events_geo(event_path_geo, spark)

    df_last_message = last_message(df_events_geo)
    
    df_city_geo = read_city_geo(city_path_geo, spark)

    df_events_with_geo = events_with_geo(df_events_geo, df_last_message)

    df_events_with_zone = events_with_zone(df_city_geo, df_events_with_geo)

    df_events_count_period = events_count_period(df_events_with_zone)

    df_events_count_period.write.mode("overwrite").parquet(f"{output_base_path}")

if __name__ == "__main__":
        main()
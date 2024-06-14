import sys

import pyspark.sql.functions as F
from pyspark.sql.window import Window
from pyspark.sql import SparkSession
from common_func import city_define, distance

def read_events_geo(event_path_geo:str, spark):
        df_events_geo = spark.read.parquet(f"{event_path_geo}")

        return df_events_geo

def read_events_geo_date(event_path_geo:str, date:str, spark):
        df_events_geo_date = spark.read.parquet(f"{event_path_geo}/date={date}")
        
        return df_events_geo_date

def read_city_geo(city_path_geo:str, spark):
        df_city_geo = spark.read.parquet(f"{city_path_geo}")

        return df_city_geo

# Пользователи, подписанные на какие-либо каналы за все время
def subscribers(df_events):
        
        df_subscription_user_all = \
                        df_events \
                        .filter((F.col("event_type")==F.lit("subscription")) & F.col("event.subscription_channel").isNotNull()) \
                        .select(F.col("event.user").alias("user_id"), \
                                F.col("event.subscription_channel")) \
                        .distinct() \
                        .cache()
        
        return df_subscription_user_all

# Общающиеся подписчики
def communicating_subscribers(df_events, df_subscription_user_all):
        
        # Формируем фильтр по пользователям, которые хотя бы где-то подписаны
        df_subscription_user_filter = df_subscription_user_all.select(F.col("user_id")).distinct()

        # Грузим связки отправитель-получатель, ограничивая выборку по подписчикам на что-либо
        df_message_user = \
                        df_events \
                        .filter(F.col("event_type")==F.lit("message")) \
                        .select(F.col("event.message_from"), \
                                F.col("event.message_to")) \
                        .join(df_subscription_user_filter.select(F.col("user_id").alias("message_from")), "message_from", "inner") \
                        .join(df_subscription_user_filter.select(F.col("user_id").alias("message_to")), "message_to", "inner") \
                        .distinct() \
                        .cache()

        # Формируем фрейм с отправитель = подписчик
        df_user_channel_1 = df_subscription_user_all \
                .join(df_message_user.select(F.col("message_from").alias("user_id"), F.col("message_to")), "user_id", "left")

        # Формируем фрейм с получатель = подписчик
        df_user_channel_2 = df_subscription_user_all \
                .join(df_message_user.select(F.col("message_to").alias("user_id"), F.col("message_from")), "user_id", "left")

        # Определяем общающихся подписчиков в рамках одного канала
        # Удаляем каналы и берем уникальные связки тех, кто уже общается в рамках каких-то каналов
        df_communicate = df_user_channel_1 \
        .join(df_user_channel_2\
                .select("subscription_channel", \
                        F.col("user_id").alias("message_to"), \
                        F.col("message_from").alias("user_id")), \
                ["subscription_channel", "user_id", "message_to"], \
                "inner") \
        .drop("subscription_channel") \
        .distinct()

        # Для дальнейшего анализа всех возможных связок
        # Соединяем инвертированные связки отправитель - получатель
        # Для сокращения объема данных оставляем только связки, где отправитель id > получатель id
        df_communicating_subscribers = df_communicate \
        .unionByName(df_communicate \
                        .select(F.col("user_id").alias("message_to"), \
                                F.col("message_to").alias("user_id"))) \
        .filter(F.col("user_id") > F.col("message_to"))

        ### ↑↑↑ Определили полный список тех, кто подписан хотя бы на 1 общий канал и когда-либо общался ↑↑↑

        #     # Определяем всевозможные связки отправитель - получатель среди подписчиков в разрезе каналов (декартово множество в разрезе каналов)
        #     # Удаляем канал и оставляем связки, где отправитель id > получатель id (чтобы избежать избыточности данных)
        #     df_possible_all = df_subscription_user.join(df_subscription_user \
        #                           .select(F.col("subscription_channel"), \
        #                                   F.col("user_id").alias("message_to")), \
        #                           "subscription_channel", "left") \
        #         .filter(F.col("user_id") > F.col("message_to")) \
        #         .drop("subscription_channel") \
        #         .distinct()

        #     df_possible_user_for_communicate = df_possible_all.join(df_communicate_full, ["user_id", "message_to"], "leftanti")

        return df_communicating_subscribers

# Предполагаемые к общению подписчики за дату, не общавшиеся ранее
def possible_subscribers_to_communicate(df_subscription_user_all, df_communicating_subscribers, df_events_geo_date, date, max_distance:float):

        window_dt_max = Window().partitionBy("user_id")

        # Формируем список пользователей за дату (геопозиция последнего события)
        all_users = df_events_geo_date \
                        .filter((F.col("lat").isNotNull()) & (F.col("lon").isNotNull())) \
                        .select(F.coalesce("event.message_from", "event.reaction_from", "event.user").alias("user_id"), \
                                F.coalesce("event.datetime", "event.message_ts").alias("datetime"), \
                                "lat", \
                                "lon") \
                        .withColumn("dt_max", F.max(F.col("datetime")).over(window_dt_max)) \
                        .filter(F.col("datetime") == F.col("dt_max") ) \
                        .drop("dt_max") \
                        .drop("datetime") \
                        .groupBy("user_id").agg(F.first("lat").alias("last_lat"), F.first("lon").alias("last_lon"))

        # Фильтруем через подписчиков
        df_subscription_user = all_users \
                        .join(df_subscription_user_all, "user_id", "inner")

        # Формируем связки для предложения общения пользователей и вычитаем тех, кто уже общался
        possible_subscribers_to_communicate = df_subscription_user.join(df_subscription_user \
                                .select(F.col("subscription_channel"), \
                                        F.col("user_id").alias("message_to"), \
                                        F.col("last_lat").alias("last_lat_to"), \
                                        F.col("last_lon").alias("last_lon_to")), \
                                "subscription_channel", "left") \
        .filter(F.col("user_id") > F.col("message_to")) \
        .drop("subscription_channel") \
        .distinct() \
        .join(df_communicating_subscribers, ["user_id", "message_to"], "leftanti") \
        .withColumn("distance", distance("last_lat", "last_lat_to", "last_lon", "last_lon_to") ) \
        .filter(F.col("distance") < max_distance) \
        .select( \
                F.col("user_id").alias("user_left"), \
                F.col("user_id").alias("user_right"), \
                ((F.col("last_lat") + F.col("last_lat_to")) / F.lit(2)).alias("lat_avg"), \
                ((F.col("last_lon") + F.col("last_lon_to")) / F.lit(2)).alias("lon_avg"), \
                F.lit(date).alias("processed_dttm") \
        )

        return possible_subscribers_to_communicate

# Подтягиваем зоны (города)
def define_possible_subscribers_to_communicate_with_zone(df_city_geo, df_possible_subscribers_to_communicate):

        df_city_geo_select = df_city_geo.select(F.col("id").alias("city_id"), \
                                                F.col("lat").alias("lat_city"), \
                                                F.col("lon").alias("lon_city"))

        # Определяем Зоны (города) по устредненным координатам между пользователями 
        # (на случай, если пользователи будут на расстоянии меньше задаваемого, но при этом относиться к разным городам)

        possible_subscribers_to_communicate_with_zone_define = city_define(df_possible_subscribers_to_communicate, df_city_geo_select, "lat_avg", "lat_city", "lon_avg", "lon_city")

        possible_subscribers_to_communicate_with_zone = possible_subscribers_to_communicate_with_zone_define \
                                                        .select( \
                                                                F.col("user_left"), \
                                                                F.col("user_right"), \
                                                                F.col("city_id").alias("zone_id"), \
                                                                F.col("processed_dttm"), \
                                                                F.from_utc_timestamp(F.current_timestamp(), F.lit('Australia/Sydney')).alias("local_time")
                                                                )

        return possible_subscribers_to_communicate_with_zone


def main():
        date = sys.argv[1]
        max_distance = float(sys.argv[2])
        event_path_geo = sys.argv[3]
        city_path_geo = sys.argv[4]
        output_base_path = sys.argv[5]

        spark = SparkSession.builder \
                        .master("yarn") \
                        .config("spark.executor.memory", "3g")\
                        .config("spark.executor.cores", "2")\
                        .appName("Project s7 step_3") \
                        .getOrCreate()

        df_events = read_events_geo(event_path_geo, spark)

        df_subscription_user_all = subscribers(df_events)

        df_communicating_subscribers = communicating_subscribers(df_events, df_subscription_user_all)

        df_events_geo_date = read_events_geo_date(event_path_geo, date, spark)

        df_possible_subscribers_to_communicate = possible_subscribers_to_communicate(df_subscription_user_all, df_communicating_subscribers, df_events_geo_date, date, max_distance)

        df_city_geo = read_city_geo(city_path_geo, spark)
        
        df_possible_subscribers_to_communicate_with_zone = define_possible_subscribers_to_communicate_with_zone(df_city_geo, df_possible_subscribers_to_communicate)

        df_possible_subscribers_to_communicate_with_zone.write.mode("overwrite").parquet(f"{output_base_path}")

if __name__ == "__main__":
        main()
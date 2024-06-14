import pyspark.sql.functions as F
from pyspark.sql.window import Window

def distance(lat_1, lat_2, lon_1, lon_2):

    K = 3.1415926535 / 180

    distance = 2 * 6371 * F.asin( F.sqrt( \
                    F.pow(F.sin((F.col(f"{lat_1}") - F.col(f"{lat_2}")) * K / F.lit(2)), F.lit(2)) \
                    + F.cos(F.col(f"{lat_1}") * K) * F.cos(F.col(f"{lat_2}") * K) \
                    * F.pow(F.sin((F.col(f"{lon_1}") - F.col(f"{lon_2}")) * K / F.lit(2)), F.lit(2)) \
                    ))
    return distance

def city_define(df_source, df_geo, source_lat, city_lat, source_lon, city_lon):
        
    window_distance_filter = Window().partitionBy(source_lat, source_lon)

    df_with_distance_and_city = df_source.crossJoin(df_geo) \
                .withColumn("distance", distance(source_lat, city_lat, source_lon, city_lon) )\
                .withColumn("distance_min", F.min("distance").over(window_distance_filter)) \
                .filter(F.col("distance") == F.col("distance_min")) \
                .drop("distance_min")
        
    return df_with_distance_and_city
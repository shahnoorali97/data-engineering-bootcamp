""" Reads bucketed Hive tables created from CSVs, performs joins, explicit broadcast, and aggregations. """


import argparse
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import broadcast, col, sum as _sum, count
from pyspark.sql.types import StructType, StructField, StringType, LongType




def run_experiments(df, out_dir, keys_to_sort):
    os.makedirs(out_dir, exist_ok=True)
    sizes = []
    for key in keys_to_sort:
        path = os.path.join(out_dir, f"sorted_by_{key}")
        prepared = df.repartition(16, col(key)).sortWithinPartitions(col(key))
        prepared.write.mode("overwrite").parquet(path)


        total = 0
        for root, _, files in os.walk(path):
            for f in files:
                total += os.path.getsize(os.path.join(root, f))
        sizes.append((key, total))
    return sizes




# Initialize Spark session globally
spark = SparkSession.builder.appName("01_aggs_joins_csv").enableHiveSupport().getOrCreate()

def main(args):
    # Disable automatic broadcast
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")


    db = args.db
    md = spark.table(f"{db}.match_details_bucketed")
    matches = spark.table(f"{db}.matches_bucketed")
    mmp = spark.table(f"{db}.medal_matches_players_bucketed")
    medals = spark.table(f"{db}.medals_small")


    try:
        maps = spark.table(f"{db}.maps_small")
    except Exception:
        maps = None

    # Explicitly broadcast JOINs medals and maps
    medals_b = broadcast(medals)
    maps_b = broadcast(maps) if maps is not None else None


    # Join match_details <-> matches
    joined = md.join(matches.hint("BUCKET", "match_id"), on="match_id", how="left")
    # Join medal_matches_players
    joined = joined.join(mmp.hint("BUCKET", "match_id"), on=["match_id", "player_gamertag"], how="left")
    # Join medals
    joined = joined.join(medals_b, on="medal_id", how="left")
    # maps join
    if maps_b is not None:
        joined = joined.join(maps_b, on="mapid", how="left")


    # Aggregations
    kills_by_player = joined.groupBy("player_gamertag").agg(
    _sum("player_total_kills").alias("total_kills"),
    count("match_id").alias("games_played"),
    (_sum("player_total_kills") / count("match_id")).alias("avg_kills_per_game")
    ).orderBy(col("avg_kills_per_game").desc())
    kills_by_player.write.mode("overwrite").parquet(os.path.join(args.out, "avg_kills_per_player"))


    if 'playlist_id' in matches.columns:
        playlist_counts = matches.groupBy("playlist_id").agg(count("match_id").alias("times_played")).orderBy(col("times_played").desc())
        playlist_counts.write.mode("overwrite").parquet(os.path.join(args.out, "playlist_counts"))


    if 'mapid' in matches.columns:
        map_counts = matches.groupBy("mapid").agg(count("match_id").alias("times_played")).orderBy(col("times_played").desc())
        map_counts.write.mode("overwrite").parquet(os.path.join(args.out, "map_counts"))


    if 'mapid' in matches.columns and 'name' in medals.columns:
        spree_df = joined.filter(col("name") == "KillingSpree")
        spree_by_map = spree_df.groupBy("mapid").agg(_sum("count").alias("num_spree_medals")).orderBy(col("num_spree_medals").desc())
        # spree_by_map.write.mode("overwrite").parquet(os.path.join(args.out, "map_killing_spree_counts"))
        spree_by_map.coalesce(1).write.mode("overwrite").parquet(os.path.join(args.out, "map_killing_spree_counts"))
        # Note: coalesce(1) used above to produce a single output file for easier verification of results.

    # sortWithinPartitions experiments
    candidates = []
    if 'playlist_id' in matches.columns:
        candidates.append('playlist_id')
    if 'mapid' in matches.columns:
        candidates.append('mapid')
    candidates.extend(['match_id', 'player_gamertag'])


    sizes = run_experiments(kills_by_player, os.path.join(args.out, 'sort_experiments'), candidates)
    print("SortWithinPartitions experiment sizes (bytes):")
    for k, s in sizes:
        print(k, s)


spark.stop()



if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--db', required=True, default='homework_db')
    parser.add_argument('--out', required=True, default='/tmp/homework_out')
    args = parser.parse_args()
    main(args)
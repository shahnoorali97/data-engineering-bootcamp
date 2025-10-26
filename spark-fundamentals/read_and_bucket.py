"""
Prepare and write bucketed tables for the homework.
- Disables automatic broadcast joins.
- Reads: match_details, matches, medal_matches_players, medals (and optional maps table if provided).
- Writes Hive managed tables (Parquet) for: match_details_bucketed, matches_bucketed, medal_matches_players_bucketed (all bucketed by match_id with 16 buckets).
- Writes medals_small (not bucketed) and optional maps_small (broadcastable).


Notes:
- This script uses saveAsTable to create bucketed tables. A Hive metastore is required. If you don't have one, modify to write/read from paths.
"""

import argparse
from pyspark.sql import SparkSession



def main(args):
    spark = SparkSession.builder.appName("00_bucket_prep_csv").enableHiveSupport().getOrCreate()


    # Disable automatic broadcast
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
    print("spark.sql.autoBroadcastJoinThreshold set to -1")


    # Read CSVs with header and schema inference
    md = spark.read.csv(args.match_details, header=True, inferSchema=True)
    matches = spark.read.csv(args.matches, header=True, inferSchema=True)
    mmp = spark.read.csv(args.medal_matches_players, header=True, inferSchema=True)
    medals = spark.read.csv(args.medals, header=True, inferSchema=True)
    maps = spark.read.csv(args.maps, header=True, inferSchema=True) if args.maps else None


    out_db = args.output_db
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {out_db}")


    # Print schemas
    md.printSchema()
    matches.printSchema()
    mmp.printSchema()
    medals.printSchema()
    if maps is not None:
        maps.printSchema()


    # Write bucketed tables
    md.write.format("parquet").mode("overwrite") \
    .bucketBy(16, "match_id").sortBy("match_id").saveAsTable(f"{out_db}.match_details_bucketed")


    matches.write.format("parquet").mode("overwrite") \
    .bucketBy(16, "match_id").sortBy("match_id").saveAsTable(f"{out_db}.matches_bucketed")


    mmp.write.format("parquet").mode("overwrite") \
    .bucketBy(16, "match_id").sortBy("match_id").saveAsTable(f"{out_db}.medal_matches_players_bucketed")


    medals.write.format("parquet").mode("overwrite").saveAsTable(f"{out_db}.medals_small")
    if maps is not None:
        maps.write.format("parquet").mode("overwrite").saveAsTable(f"{out_db}.maps_small")


    print("All writes complete.")
    spark.stop()




if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--match_details', required=True, default='/data/match_details.csv')
    parser.add_argument('--matches', required=True, default='/data/matches.csv')
    parser.add_argument('--medal_matches_players', required=True, default='/data/medals_matches_players.csv')
    parser.add_argument('--medals', required=True, default='/data/medals.csv')
    parser.add_argument('--maps', required=False, default='/data/maps.csv')
    parser.add_argument('--output_db', required=True, default='homework_db')
    args = parser.parse_args()
    main(args)
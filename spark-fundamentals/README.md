# Make sure about the path of csv files

# Run this command:
python3 read_and_bucket.py \
  --match_details "/data/match_details.csv path required" \
  --matches "/data/matches.csv path required" \
  --medal_matches_players "/data/medals_matches_players.csv \
  --medals "/data/medals.csv path required" \
  --maps "/data/maps.csv path required" \
  --output_db homework_db

# Run this command
brew install apache-spark

# Run the aggregation script
spark-submit aggregations_and_joins.py --db homework_db --out /tmp/homework_out

# Inspect the output
ls /tmp/homework_out

# Open the shell
pyspark

# inside the PySpark shell

Q. Which player averages the most kills per game?
df = spark.read.parquet("/tmp/homework_out/avg_kills_per_player")
df.orderBy(df.avg_kills_per_game.desc()).show(10, truncate=False)

Q. Which playlist gets played the most?
df = spark.read.parquet("/tmp/homework_out/playlist_counts")
df.orderBy("times_played", ascending=False).show(10, truncate=False)

Q. Which map gets played the most?
maps = spark.read.parquet("/tmp/homework_out/map_counts")
maps.orderBy("times_played", ascending=False).show(10, truncate=False)

Q. Which map do players get the most Killing Spree medals on?
df = spark.read.parquet("/tmp/homework_out/map_killing_spree_counts")
df.orderBy("num_spree_medals", ascending=False).show(10, truncate=False)


# Filter to Killing Spree medals
killing_spree_ids = medals.filter(medals.name == "Killing Spree").select("medal_id")

# Join and count
joined = mmp.join(killing_spree_ids, on="medal_id", how="inner") \
             .join(matches.select("match_id", "mapid"), on="match_id", how="left")

result = joined.groupBy("mapid").sum("count").withColumnRenamed("sum(count)", "total_killing_sprees")

result.orderBy("total_killing_sprees", ascending=False).show(10, truncate=False)





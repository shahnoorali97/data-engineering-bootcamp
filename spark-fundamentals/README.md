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



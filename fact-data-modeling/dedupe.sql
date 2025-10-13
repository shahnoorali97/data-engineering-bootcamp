WITH deduped AS (
  SELECT 
    gd.ctid,
    ROW_NUMBER() OVER (
      PARTITION BY gd.game_id, gd.player_id
      ORDER BY gd.ctid
    ) AS rn
  FROM game_details gd
  JOIN games g 
    ON gd.game_id = g.game_id
  WHERE g.game_date_est = DATE('2023-01-03')  -- Day 1
)
DELETE FROM game_details
WHERE ctid IN (
  SELECT ctid FROM deduped WHERE rn > 1
)
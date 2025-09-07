import duckdb

duckdb_connection = duckdb.connect("data/player_data.duckdb")

table_names = duckdb_connection.execute("SHOW TABLES").fetchall()

df = duckdb_connection.execute(f"SELECT * FROM player_data_season_2025_2026").fetchdf()

import duckdb
from datetime import datetime

duckdb_connection = duckdb.connect("data/player_data.duckdb")

tables = duckdb_connection.execute("SHOW TABLES").fetchall()
table_names = [t[0] for t in tables]

seasons = [f"{i-1}-{i}" for i in range(2018, datetime.now().year + 2)]

string_cols = ["row_id", "player_id", "player", "nation", "squad", "comp", "age", "born", "position_gk", "position_df", "position_mf", "position_fw"]

dfs = {
    season: duckdb_connection.execute(f"SELECT * FROM {table}").df().convert_dtypes()
    for season, table in zip(seasons, table_names)
}

dfs = {
    df_name: df.dropna(subset=["player", "nation", "squad", "comp", "age", "born"])
    for df_name, df in dfs.items()
}

dfs = {df_name: df.fillna(0) for df_name, df in dfs.items()}

for df_name in dfs.keys():
    dfs[df_name] = dfs[df_name][
        string_cols + [col for col in dfs[df_name].columns if col not in string_cols]
    ]

    dfs[df_name] = dfs[df_name].rename(columns={
        "playingtime_team_success_on_off_1": "playingtime_team_success_xg_on_off",
        "defense_tackles_tkl_1": "defense_challenges_tkl",
        "gca_sca_types_passlive_1": "gca_gca_types_passlive",
        "gca_sca_types_passdead_1": "gca_gca_types_passdead",
        "gca_sca_types_to_1": "gca_gca_types_to",
        "gca_sca_types_sh_1": "gca_gca_types_sh",
        "gca_sca_types_fld_1": "gca_gca_types_fld",
        "gca_sca_types_def_1": "gca_gca_types_def",
        "keepers_performance_save%_1": "keepers_penalty_kicks_save%",
        "keepersadv_launched_att_1": "keepersadv_goal_kicks_att",
        "keepersadv_passes_launch%_1": "keepersadv_goal_kicks_launch%",
        "keepersadv_passes_avglen_1": "keepersadv_goal_kicks_avglen",
        "passing_total_cmp_1": "passing_short_cmp",
        "passing_total_att_1": "passing_short_att",
        "passing_total_cmp%_1": "passing_short_cmp%",
        "passing_total_cmp_2": "passing_medium_cmp",
        "passing_total_att_2": "passing_medium_att",
        "passing_total_cmp%_2": "passing_medium_cmp%",
        "passing_total_cmp_3": "passing_long_cmp",
        "passing_total_att_3": "passing_long_att",
        "passing_total_cmp%_3": "passing_long_cmp%",
        "att": "passing_types_att",
        "stats_performance_gls_1": "stats_per_90_minutes_gls",
        "stats_performance_ast_1": "stats_per_90_minutes_ast",
        "stats_performance_g+a_1": "stats_per_90_minutes_g+a",
        "stats_performance_g_pk_1": "stats_per_90_minutes_g_pk",
        "stats_expected_xg_1": "stats_per_90_minutes_xg",
        "stats_expected_xag_1": "stats_per_90_minutes_xag",
        "stats_expected_npxg_1": "stats_per_90_minutes_npxg",
        "stats_expected_npxg+xag_1": "stats_per_90_minutes_npxg+xag"
    })

for season, df in dfs.items():
    table_name = "player_data_season_" + season.replace("-", "_")
    duckdb_connection.register("temp_df", df)
    duckdb_connection.execute(f'DROP TABLE IF EXISTS "{table_name}"')
    duckdb_connection.execute(f'CREATE TABLE "{table_name}" AS SELECT * FROM temp_df')

duckdb_connection.close()
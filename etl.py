from __future__ import annotations

import argparse
from pathlib import Path

import polars as pl


DROP_COLS = [
    "spin_rate_deprecated",
    "break_angle_deprecated",
    "break_length_deprecated",
    "tfs_deprecated",
    "tfs_zulu_deprecated",
]

IS_BATTED_BALL = pl.col("bb_type").is_not_null()


def mean_on_bb(col_name: str, round_digits: int = 3) -> pl.Expr:
    return pl.col(col_name).filter(IS_BATTED_BALL).mean().round(round_digits)


def rate_on_bb(col_name: str, round_digits: int = 3) -> pl.Expr:
    count = pl.col(col_name).filter(IS_BATTED_BALL).sum()
    total = pl.col(col_name).filter(IS_BATTED_BALL).count()
    return (count / total).round(round_digits)


def add_ranking_cols(df: pl.DataFrame, *col_names: str) -> pl.DataFrame:
    for col_name in col_names:
        orig_col_index = df.get_column_index(col_name)
        rank_col_name = f"{col_name}_rank"
        new_col = pl.col(col_name).rank("dense", descending=True).cast(pl.Int32).alias(rank_col_name)
        df = df.insert_column(orig_col_index + 1, new_col)
    return df


def ensure_parent_dir(file_path: Path) -> None:
    file_path.parent.mkdir(parents=True, exist_ok=True)


def build_player_lookup(inputs_dir: Path, lookup_output: Path) -> None:
    source = inputs_dir / "player_id_map.parquet"
    ensure_parent_dir(lookup_output)

    (
        pl.scan_parquet(source)
        .with_columns(
            pl.col("BIRTHDATE").str.strptime(pl.Date, "%m/%d/%Y"),
            pl.col("ACTIVE").eq("Y"),
        )
        .drop_nulls("MLBID")
        .select("MLBID", "PLAYERNAME", "BIRTHDATE", "FIRSTNAME", "LASTNAME", "TEAM", "LG", "POS", "BATS", "THROWS", "ACTIVE")
        .rename(
            {
                "MLBID": "player_pk",
                "PLAYERNAME": "player_name",
                "BIRTHDATE": "birth_date",
                "FIRSTNAME": "first_name",
                "LASTNAME": "last_name",
                "TEAM": "team",
                "LG": "league",
                "POS": "position",
                "BATS": "bats",
                "THROWS": "throws",
                "ACTIVE": "active",
            }
        )
        .sink_parquet(lookup_output, engine="streaming")
    )


def build_hr_reports(inputs_dir: Path, lookup_output: Path, output_dir: Path) -> None:
    source = inputs_dir / "statcast_era_batted_balls.parquet"
    player_lookup = pl.scan_parquet(lookup_output).select("player_pk", "player_name").unique()

    bb_df = (
        pl.scan_parquet(source)
        .rename({"batter": "player_pk"})
        .filter(pl.col("events").eq("home_run"))
        .join(player_lookup, on="player_pk", how="left")
        .select(
            "player_pk",
            "player_name",
            "game_pk",
            "game_date",
            "game_year",
            "events",
            "bb_type",
            "pitch_type",
            "release_speed",
            "zone",
            "stand",
            "home_team",
            "away_team",
            "hit_location",
            "balls",
            "strikes",
            "outs_when_up",
            "on_1b",
            "on_2b",
            "on_3b",
            "hit_distance_sc",
            "launch_speed",
            "launch_angle",
            "effective_speed",
        )
    )

    hr_by_season_output = output_dir / "hr_report_by_season.parquet"
    hr_output = output_dir / "hr_report.parquet"
    ensure_parent_dir(hr_by_season_output)
    ensure_parent_dir(hr_output)

    (
        bb_df
        .group_by("player_pk", "player_name", "game_year")
        .agg(
            pl.col("events").count().alias("hr_count"),
            pl.col("hit_distance_sc").max().alias("max_distance_ft"),
            pl.col("launch_speed").max().alias("max_launch_speed_mph"),
        )
        .sort("hr_count", descending=True)
        .sink_parquet(hr_by_season_output, engine="streaming")
    )

    (
        bb_df
        .group_by("player_pk", "player_name")
        .agg(
            pl.col("events").count().alias("hr_count"),
            pl.col("hit_distance_sc").max().alias("max_distance_ft"),
            pl.col("launch_speed").max().alias("max_launch_speed_mph"),
        )
        .sort("hr_count", descending=True)
        .sink_parquet(hr_output, engine="streaming")
    )


def build_batter_report_2025(inputs_dir: Path, output_dir: Path) -> None:
    statcast_2025 = pl.scan_parquet(inputs_dir / "statcast_2025.parquet")
    player_map = (
        pl.scan_parquet(inputs_dir / "player_id_map.parquet")
        .select("PLAYERNAME", "MLBID")
        .rename({"MLBID": "player_pk", "PLAYERNAME": "player_name"})
        .unique()
    )

    bat_speed_qtile = (
        statcast_2025
        .filter(pl.col("game_type").eq("R"))
        .group_by("batter")
        .agg(pl.quantile("bat_speed", 0.1).alias("90th_pct_bat_speed"))
    )

    bat_speed = pl.col("bat_speed")
    pitch_speed = pl.col("release_speed")
    max_theoretical_velocity = (pl.lit(1.23) * bat_speed) + (pl.lit(0.2116) * pitch_speed)
    is_competitive_swing = bat_speed.gt(pl.col("90th_pct_bat_speed")) | (bat_speed.ge(60) & pl.col("launch_speed").ge(90))

    report_output = output_dir / "2025_batter_report.parquet"
    ensure_parent_dir(report_output)

    (
        statcast_2025
        .join(bat_speed_qtile, on="batter", how="left")
        .filter(pl.col("game_type").eq("R"))
        .with_columns(
            pl.col("launch_speed_angle").eq(6).alias("barrel"),
            pl.col("launch_speed").ge(95).cast(pl.Int32).alias("hard_hit"),
            pl.col("launch_angle").is_between(8, 32, closed="right").alias("sweet_spot"),
            is_competitive_swing.alias("competitive_swing"),
            (pl.col("launch_speed") / max_theoretical_velocity).alias("squared_up_pct"),
        )
        .group_by("batter")
        .agg(
            pl.col("estimated_woba_using_speedangle").mean().round(3).alias("xwOBA"),
            mean_on_bb("launch_speed", 1).alias("avg_exit_velo"),
            rate_on_bb("barrel").alias("barrel_pct"),
            rate_on_bb("hard_hit").alias("hard_hit_pct"),
            rate_on_bb("sweet_spot").alias("sweet_spot_pct"),
            pl.col("bat_speed").filter(is_competitive_swing).mean().round(1).alias("avg_bat_speed"),
            pl.sum("delta_run_exp").round(1).alias("batting_run_value"),
        )
        .rename({"batter": "player_pk"})
        .join(player_map, on="player_pk", how="left")
        .collect(engine="streaming")
        .pipe(
            add_ranking_cols,
            "xwOBA",
            "avg_exit_velo",
            "barrel_pct",
            "hard_hit_pct",
            "sweet_spot_pct",
            "avg_bat_speed",
            "batting_run_value",
        )
        .select("player_pk", "player_name", pl.exclude("player_pk", "player_name"))
        .drop_nulls()
        .write_parquet(report_output)
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run ETL jobs for phanavision-report parquet outputs.")
    parser.add_argument(
        "command",
        choices=["player-lookup", "hr-report", "batter-report", "all"],
        help="ETL command to run.",
    )
    parser.add_argument(
        "--inputs-dir",
        type=Path,
        default=Path("inputs"),
        help="Directory containing notebook input parquet files.",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("output"),
        help="Directory for output parquet files.",
    )
    parser.add_argument(
        "--lookup-output",
        type=Path,
        default=Path("output/player_lookup.parquet"),
        help="Output path for player lookup parquet.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    if args.command in {"player-lookup", "all"}:
        build_player_lookup(args.inputs_dir, args.lookup_output)

    if args.command in {"hr-report", "all"}:
        if not args.lookup_output.exists():
            build_player_lookup(args.inputs_dir, args.lookup_output)
        build_hr_reports(args.inputs_dir, args.lookup_output, args.output_dir)

    if args.command in {"batter-report", "all"}:
        build_batter_report_2025(args.inputs_dir, args.output_dir)


if __name__ == "__main__":
    main()

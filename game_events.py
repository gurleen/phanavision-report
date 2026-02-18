from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
import json
from pathlib import Path
import time
from urllib.error import HTTPError, URLError
from urllib.request import urlopen

import polars as pl
from loguru import logger
from tqdm import tqdm


URL_TEMPLATE = "https://statsapi.mlb.com/api/v1.1/game/{game_id}/feed/live"
REQUEST_STAGGER_SECONDS = 0.03


def _tqdm_log_sink(message: str) -> None:
    tqdm.write(str(message).rstrip("\n"))


logger.remove()
logger.add(_tqdm_log_sink, format="{message}")


def fetch_game_events(game_id: int) -> pl.DataFrame:
    url = URL_TEMPLATE.format(game_id=game_id)

    try:
        with urlopen(url) as response:
            data = json.load(response)
    except HTTPError as exc:
        raise ValueError(f"Failed to fetch game_id={game_id}: HTTP {exc.code}") from exc
    except URLError as exc:
        raise ConnectionError(f"Failed to fetch game_id={game_id}: {exc.reason}") from exc

    all_plays = data.get("liveData", {}).get("plays", {}).get("allPlays", [])
    events: list[dict[str, object]] = []

    for play in all_plays:
        for event in play.get("playEvents", []):
            events.append(
                {
                    "game_pk": data.get("gamePk"),
                    "at_bat_number": play.get("atBatIndex"),
                    "pitch_number": event.get("pitchNumber"),
                    "start_time": event.get("startTime"),
                    "end_time": event.get("endTime"),
                    "play_id": event.get("playId"),
                }
            )

    events_df = pl.DataFrame(events, infer_schema_length=None)
    if events_df.is_empty():
        return events_df

    return events_df.with_columns(
        pl.col("start_time").str.to_datetime("%Y-%m-%dT%H:%M:%S.%3fZ"),
        pl.col("end_time").str.to_datetime("%Y-%m-%dT%H:%M:%S.%3fZ"),
    ).drop_nulls(["pitch_number", "play_id"])


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Fetch MLB game event rows with bounded concurrency.")
    parser.add_argument(
        "--concurrent-games",
        type=int,
        default=8,
        help="Number of games to fetch concurrently.",
    )
    parser.add_argument(
        "--input-parquet",
        default="inputs/statcast_2025.parquet",
        help="Parquet file containing a game_pk column.",
    )
    parser.add_argument(
        "--max-games",
        type=int,
        default=None,
        help="Optional limit for number of games to request.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    if args.concurrent_games <= 0:
        raise ValueError("--concurrent-games must be greater than 0")

    game_ids = (
        pl.scan_parquet(args.input_parquet)
        .select("game_pk")
        .unique()
        .sort("game_pk")
        .collect(engine="streaming")
        .get_column("game_pk")
        .to_list()
    )

    if args.max_games is not None:
        game_ids = game_ids[: args.max_games]

    all_dfs = []

    logger.info(
        "Fetching game events for {} game_ids with concurrency={}",
        len(game_ids),
        args.concurrent_games,
    )

    with ThreadPoolExecutor(max_workers=args.concurrent_games) as executor:
        future_to_game_id = {}
        for game_id in game_ids:
            future_to_game_id[executor.submit(fetch_game_events, game_id)] = game_id
            time.sleep(REQUEST_STAGGER_SECONDS)

        for future in tqdm(
            as_completed(future_to_game_id),
            total=len(game_ids),
            desc="Fetching games",
            unit="game",
        ):
            game_id = future_to_game_id[future]
            try:
                events_df = future.result()
                all_dfs.append(events_df)
            except Exception as exc:
                logger.warning("Failed game_id {}: {}", game_id, exc)

    if all_dfs:
        all_events_df = pl.concat(all_dfs, how="vertical_relaxed")
    else:
        all_events_df = pl.DataFrame()

    logger.success(
        "Done. game requests={} dataframes={} rows={}",
        len(game_ids),
        len(all_dfs),
        all_events_df.height,
    )

    output_path = Path("output/game_events.parquet")
    output_path.parent.mkdir(parents=True, exist_ok=True)
    all_events_df.write_parquet(output_path)
    logger.success("Saved game events to {}", output_path)


if __name__ == "__main__":
    main()
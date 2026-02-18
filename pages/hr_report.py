from typing import Literal
from dash import dcc, html
import dash_ag_grid as dag
import polars as pl

CAREER_HR_MAX = pl.scan_parquet("output/hr_report.parquet").drop("player_name")
SEASON_HR_MAX = pl.scan_parquet("output/hr_report_by_season.parquet").drop("player_name")
STATCAST_2025 = pl.scan_parquet("inputs/statcast_2025.parquet")
GAME_EVENTS = pl.scan_parquet("output/game_events.parquet")
PITCHER_LOOKUP = (
	pl.scan_parquet("output/player_lookup.parquet")
	.select("player_pk", "first_name", "last_name")
	.with_columns(
		pl.when(pl.col("first_name").is_not_null() & pl.col("last_name").is_not_null())
		.then(pl.concat_str([pl.col("first_name").str.slice(0, 1), pl.lit(". "), pl.col("last_name")]))
		.otherwise(None)
		.alias("pitcher_name")
	)
	.select("player_pk", "pitcher_name")
)


def _fetch_play_video_url(play_guid: str, feed: Literal["home", "away"]) -> str:
	return f"https://baseballsavant.mlb.com/sporty-videos?playId={play_guid}&feed={feed}&videoType={feed.upper()}"


def _interpolate_color(ratio: float) -> str:
	low_rgb = (248, 215, 218)
	high_rgb = (212, 237, 218)
	red = int(low_rgb[0] + (high_rgb[0] - low_rgb[0]) * ratio)
	green = int(low_rgb[1] + (high_rgb[1] - low_rgb[1]) * ratio)
	blue = int(low_rgb[2] + (high_rgb[2] - low_rgb[2]) * ratio)
	return f"rgb({red}, {green}, {blue})"


def _cell_heat_color(value: float | int | None, min_value: float, max_value: float) -> str:
	if value is None:
		return "transparent"
	if min_value == max_value:
		return _interpolate_color(1.0)
	ratio = (float(value) - min_value) / (max_value - min_value)
	ratio = max(0.0, min(1.0, ratio))
	return _interpolate_color(ratio)


def _ag_heatmap_cell_style(rows: list[dict], col_name: str) -> dict | None:
	values = [float(row[col_name]) for row in rows if row.get(col_name) is not None]
	if not values:
		return None

	min_value = min(values)
	max_value = max(values)
	if min_value == max_value:
		return {
			"styleConditions": [
				{
					"condition": "params.value != null",
					"style": {"backgroundColor": _interpolate_color(1.0)},
				},
			],
		}

	levels = [0.0, 0.25, 0.5, 0.75, 1.0]
	style_conditions = []
	for level in reversed(levels):
		threshold = min_value + ((max_value - min_value) * level)
		style_conditions.append(
			{
				"condition": f"params.value != null && params.value >= {threshold}",
				"style": {"backgroundColor": _interpolate_color(level)},
			}
		)

	return {
		"styleConditions": style_conditions,
	}


def _render_all_hrs_grid(player_pk: int):
	all_hrs_rows = (
		STATCAST_2025
		.with_columns(
			pl.col("game_date").str.strptime(pl.Date, "%Y-%m-%d"),
			pl.when(pl.col("inning_topbot").eq("Top")).then(pl.col("away_team")).otherwise(pl.col("home_team")).alias("bat_team"),
			pl.when(pl.col("inning_topbot").eq("Top")).then(pl.col("home_team")).otherwise(pl.col("away_team")).alias("field_team"),
		)
		.filter(
			pl.col("events").eq("home_run"),
			pl.col("batter").eq(player_pk),
		)
		.with_columns((pl.col("at_bat_number") - 1).alias("event_at_bat_index"))
		.join(
			GAME_EVENTS,
			left_on=["game_pk", "event_at_bat_index", "pitch_number"],
			right_on=["game_pk", "at_bat_number", "pitch_number"],
			how="left",
		)
		.join(PITCHER_LOOKUP, left_on="pitcher", right_on="player_pk", how="left")
		.with_columns(
			pl.coalesce([pl.col("pitcher_name"), pl.col("pitcher").cast(pl.Utf8)]).alias("pitcher_name"),
			pl.col("estimated_woba_using_speedangle").round(3)
		)
		.select(
			"play_id",
			"game_date",
			"game_pk",
			"bat_team",
			"field_team",
			"batter",
			"pitcher_name",
			"pitch_type",
			"release_speed",
			"hit_distance_sc",
			"launch_speed",
			"launch_angle",
			"bat_speed",
			"swing_length",
			"estimated_woba_using_speedangle",
			"delta_run_exp",
		)
		.collect(engine="streaming")
		.sort("game_date")
		.to_dicts()
	)

	for row in all_hrs_rows:
		play_id = row.get("play_id")
		if play_id is None:
			row["open_play"] = "—"
			continue
		play_url = _fetch_play_video_url(str(play_id), "home")
		row["open_play"] = f"<a href=\"{play_url}\" target=\"_blank\" rel=\"noopener noreferrer\">🎬</a>"

	if not all_hrs_rows:
		return html.Div("No home run events available for this player.")

	column_order = [
		"game_date",
		"field_team",
		"pitcher_name",
		"pitch_type",
		"release_speed",
		"hit_distance_sc",
		"launch_speed",
		"launch_angle",
		"bat_speed",
		"swing_length",
		"estimated_woba_using_speedangle",
		"delta_run_exp",
	]
	header_names = {
		"game_date": "Date",
		"field_team": "Fld",
		"pitcher_name": "Pitcher",
		"pitch_type": "Pitch",
		"release_speed": "RelVelo",
		"hit_distance_sc": "Dist",
		"launch_speed": "EV",
		"launch_angle": "LA",
		"bat_speed": "BatVelo",
		"swing_length": "SwLen",
		"estimated_woba_using_speedangle": "xwOBA",
		"delta_run_exp": "RunVal",
	}
	heatmap_cols = {
		"hit_distance_sc",
		"launch_speed",
		"bat_speed",
		"estimated_woba_using_speedangle",
	}
	column_defs = [
		{
			"field": "open_play",
			"headerName": "",
			"width": 42,
			"sortable": False,
			"filter": False,
			"resizable": False,
			"pinned": "left",
			"cellStyle": {"textAlign": "center", "cursor": "pointer"},
			"cellRenderer": "markdown",
		},
		*[
			{
				"field": col,
				"headerName": header_names[col],
				"width": 88,
				**(
					{"cellStyle": _ag_heatmap_cell_style(all_hrs_rows, col)}
					if col in heatmap_cols and _ag_heatmap_cell_style(all_hrs_rows, col) is not None
					else {}
				),
			}
			for col in column_order
		],
	]

	return dag.AgGrid(
		id="all-hrs-grid",
		columnDefs=column_defs,
		rowData=all_hrs_rows,
		dangerously_allow_code=True,
		defaultColDef={"sortable": True, "filter": True, "resizable": True, "minWidth": 72, "maxWidth": 120},
		getRowStyle={
			"styleConditions": [
				{"condition": "params.node.rowIndex % 2 === 0", "style": {"backgroundColor": "#f7f7f7"}},
			],
		},
		dashGridOptions={
			"animateRows": False,
			"pagination": True,
			"paginationPageSize": 25,
			"rowHeight": 22,
			"headerHeight": 24,
		},
		style={
			"height": "520px",
			"width": "100%",
			"--ag-font-size": "10px",
			"--ag-grid-size": "2px",
			"--ag-font-family": "Berkeley Mono, monospace",
		},
	)


def render(player_pk: int | None):
	if player_pk is None:
		return html.Div("Select a player to view HR Report.")

	career_hr_max = (
		CAREER_HR_MAX
		.filter(pl.col("player_pk").eq(player_pk))
		.with_columns(pl.lit("Career").alias("span"))
		.drop("player_pk")
		.select("span", "hr_count", "max_distance_ft", "max_launch_speed_mph")
	)

	season_hr_rows = (
		SEASON_HR_MAX
		.filter(pl.col("player_pk").eq(player_pk))
		.sort("game_year")
		.with_columns(pl.col("game_year").cast(pl.Utf8).alias("span"))
		.drop("player_pk", "game_year")
		.select("span", "hr_count", "max_distance_ft", "max_launch_speed_mph")
		.collect(engine="streaming")
		.to_dicts()
	)

	career_rows = (
		career_hr_max
		.collect(engine="streaming")
		.to_dicts()
	)

	report_rows = season_hr_rows + career_rows

	if not report_rows:
		return html.Div("No HR report data available for this player.")

	heatmap_cols = ["hr_count", "max_distance_ft", "max_launch_speed_mph"]
	season_only_rows = [row for row in report_rows if row["span"] != "Career"]
	column_ranges: dict[str, tuple[float, float]] = {}
	for col_name in heatmap_cols:
		column_values = [float(row[col_name]) for row in season_only_rows if row[col_name] is not None]
		if not column_values:
			column_ranges[col_name] = (0.0, 0.0)
			continue
		column_ranges[col_name] = (min(column_values), max(column_values))

	season_records_table = html.Table(
		[
			html.Thead(
				html.Tr(
					[
						html.Th("Span", style={"textAlign": "left", "border": "1px solid currentColor", "paddingRight": "1rem"}),
						html.Th("HR Count", style={"textAlign": "left", "border": "1px solid currentColor", "paddingRight": "1rem"}),
						html.Th("Max Distance (ft)", style={"textAlign": "left", "border": "1px solid currentColor", "paddingRight": "1rem"}),
						html.Th("Max Launch Speed (mph)", style={"textAlign": "left", "border": "1px solid currentColor", "paddingRight": "1rem"}),
					],
					style={"backgroundColor": "#f7f7f7"},
				)
			),
			html.Tbody(
				[
					html.Tr(
						[
							html.Td(
								str(row["span"]),
								style={
									"border": "1px solid currentColor",
									"backgroundColor": "#eaeaea" if row["span"] == "Career" else "transparent",
								},
							),
							html.Td(
								str(row["hr_count"]),
								style={
									"border": "1px solid currentColor",
									"backgroundColor": "#eaeaea" if row["span"] == "Career" else _cell_heat_color(
										row["hr_count"],
										column_ranges["hr_count"][0],
										column_ranges["hr_count"][1],
									),
								},
							),
							html.Td(
								str(row["max_distance_ft"]),
								style={
									"border": "1px solid currentColor",
									"backgroundColor": "#eaeaea" if row["span"] == "Career" else _cell_heat_color(
										row["max_distance_ft"],
										column_ranges["max_distance_ft"][0],
										column_ranges["max_distance_ft"][1],
									),
								},
							),
							html.Td(
								str(row["max_launch_speed_mph"]),
								style={
									"border": "1px solid currentColor",
									"backgroundColor": "#eaeaea" if row["span"] == "Career" else _cell_heat_color(
										row["max_launch_speed_mph"],
										column_ranges["max_launch_speed_mph"][0],
										column_ranges["max_launch_speed_mph"][1],
									),
								},
							),
						],
						style={
							"fontWeight": "700" if row["span"] == "Career" else "400",
						},
					)
					for row in report_rows
				]
			),
		],
		style={"borderCollapse": "collapse", "border": "1px solid currentColor", "width": "100%"},
	)

	return html.Div(
		[
			dcc.Tabs(
				value="season-records",
				children=[
					dcc.Tab(label="Season Records", value="season-records", children=[season_records_table]),
					dcc.Tab(label="All HRs", value="all-hrs", children=[_render_all_hrs_grid(player_pk)]),
				],
			),
		]
	)

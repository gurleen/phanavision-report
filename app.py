from dash import Dash, html, dcc, Input, Output, State
import polars as pl
from pages import render_hr_report, render_statcast_ranks

# player_pk, player_name, birth_date, first_name, last_name, team, league, position, bats, throws, active
players = pl.read_parquet("output/player_lookup.parquet")

def _player_headshot_url(player_pk: int) -> str:
    return f"https://img.mlbstatic.com/mlb-photos/image/upload/w_213,d_people:generic:headshot:silo:current.png,q_auto:best,f_auto/v1/people/{player_pk}/headshot/67/current"

def _value_or_dash(value: object) -> str:
    if value is None:
        return "—"
    return str(value)


player_rows = (
    players.sort(["active", "player_name"], descending=[True, False])
    .select([
        "player_pk",
        "player_name",
        "team",
        "position",
        "bats",
        "throws",
        "birth_date",
        "active",
    ])
    .to_dicts()
)

def _player_option(row: dict) -> dict:
    return {
        "label": f"{row['player_name']} ({_value_or_dash(row['team'])})",
        "value": row["player_pk"],
    }


player_options_by_id = {row["player_pk"]: _player_option(row) for row in player_rows}

player_search_index = [
    {
        "value": row["player_pk"],
        "option": player_options_by_id[row["player_pk"]],
        "search_text": " ".join(
            [
                _value_or_dash(row["player_name"]),
                _value_or_dash(row["team"]),
                _value_or_dash(row["position"]),
                _value_or_dash(row["player_pk"]),
            ]
        ).lower(),
    }
    for row in player_rows
]

players_by_id = {row["player_pk"]: row for row in player_rows}

TAB_CONTENT_BY_VALUE = {
    "hr-report": render_hr_report,
    "statcast-ranks": render_statcast_ranks,
}

app = Dash(__name__, title="phanavision-report", suppress_callback_exceptions=True)
server = app.server

app.layout = html.Div(
    [
        html.H1("phanavision-report"),
        dcc.Dropdown(
            id="player-lookup",
            options=[],
            placeholder="Search for an MLB player...",
            clearable=True,
            persistence=True,
            persistence_type="local",
        ),
        html.Div(
            id="player-details",
            style={"marginTop": "1rem"},
            children="Select a player to view profile details.",
        ),
        dcc.Tabs(
            id="report-tabs",
            value="hr-report",
            style={"marginTop": "1rem"},
            children=[
                dcc.Tab(label="HR Report", value="hr-report"),
                dcc.Tab(label="Statcast Ranks", value="statcast-ranks"),
            ],
        ),
        html.Div(id="report-tab-content", style={"marginTop": "1rem"}),
    ],
    style={
        "maxWidth": "100%",
        "margin": "2rem 0.25rem",
        "padding": "0 0.25rem",
        "fontFamily": "Berkeley Mono, monospace",
    },
)


@app.callback(
    Output("player-lookup", "options"),
    Input("player-lookup", "search_value"),
    State("player-lookup", "value"),
)
def update_player_options(search_value: str | None, selected_player_pk: int | None):
    options = []
    query = (search_value or "").strip().lower()

    if query:
        for item in player_search_index:
            if query in item["search_text"]:
                options.append(item["option"])
            if len(options) >= 25:
                break

    if selected_player_pk in player_options_by_id and all(
        option["value"] != selected_player_pk for option in options
    ):
        options.insert(0, player_options_by_id[selected_player_pk])

    return options


@app.callback(Output("player-details", "children"), Input("player-lookup", "value"))
def render_player_details(selected_player_pk: int | None):
    if selected_player_pk is None:
        return "Select a player to view profile details."

    player = players_by_id.get(selected_player_pk)
    if player is None:
        return "Player not found."

    return html.Div(
        [
            html.Img(
                src=_player_headshot_url(player["player_pk"]),
                alt="Player Headshot",
                style={"width": "140px", "height": "auto", "flexShrink": 0},
            ),
            html.Div(
                [
                    html.H3(_value_or_dash(player["player_name"]), style={"marginTop": 0}),
                    html.Table(
                        html.Tbody(
                            [
                                html.Tr(
                                    [
                                        html.Th("Team", style={"textAlign": "left", "paddingRight": "1rem", "border": "1px solid currentColor"}),
                                        html.Td(_value_or_dash(player["team"]), style={"border": "1px solid currentColor"}),
                                    ],
                                    style={"backgroundColor": "#f7f7f7"},
                                ),
                                html.Tr(
                                    [
                                        html.Th("Position", style={"textAlign": "left", "paddingRight": "1rem", "border": "1px solid currentColor"}),
                                        html.Td(_value_or_dash(player["position"]), style={"border": "1px solid currentColor"}),
                                    ]
                                ),
                                html.Tr(
                                    [
                                        html.Th("Bats / Throws", style={"textAlign": "left", "paddingRight": "1rem", "border": "1px solid currentColor"}),
                                        html.Td(f"{_value_or_dash(player['bats'])} / {_value_or_dash(player['throws'])}", style={"border": "1px solid currentColor"}),
                                    ],
                                    style={"backgroundColor": "#f7f7f7"},
                                ),
                                html.Tr(
                                    [
                                        html.Th("Birth Date", style={"textAlign": "left", "paddingRight": "1rem", "border": "1px solid currentColor"}),
                                        html.Td(_value_or_dash(player["birth_date"]), style={"border": "1px solid currentColor"}),
                                    ]
                                ),
                                html.Tr(
                                    [
                                        html.Th("Active", style={"textAlign": "left", "paddingRight": "1rem", "border": "1px solid currentColor"}),
                                        html.Td("Yes" if player["active"] else "No", style={"border": "1px solid currentColor"}),
                                    ],
                                    style={"backgroundColor": "#f7f7f7 "},
                                ),
                                html.Tr(
                                    [
                                        html.Th("MLBAM Player ID", style={"textAlign": "left", "paddingRight": "1rem", "border": "1px solid currentColor"}),
                                        html.Td(_value_or_dash(player["player_pk"]), style={"border": "1px solid currentColor"}),
                                    ]
                                ),
                            ]
                        ),
                        style={"borderCollapse": "collapse", "border": "1px solid currentColor"},
                    ),
                ],
                style={"minWidth": 0},
            ),
        ]
        ,
        style={
            "display": "flex",
            "alignItems": "flex-start",
            "gap": "1rem",
            "border": "1px solid currentColor",
            "borderRadius": "8px",
            "padding": "1rem",
        },
    )


@app.callback(
    Output("report-tab-content", "children"),
    Input("report-tabs", "value"),
    Input("player-lookup", "value"),
)
def render_tab_content(selected_tab: str, selected_player_pk: int | None):
    tab_renderer = TAB_CONTENT_BY_VALUE.get(selected_tab)
    if tab_renderer is None:
        return html.Div()
    return tab_renderer(selected_player_pk)

if __name__ == "__main__":
    app.run(debug=True)
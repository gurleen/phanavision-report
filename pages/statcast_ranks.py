from dash import html


def render(player_pk: int | None):
	_ = player_pk
	return html.Div("Statcast Ranks coming soon.", style={"marginTop": "0.5rem"})

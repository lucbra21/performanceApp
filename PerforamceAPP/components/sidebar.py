from dash import html
import dash_bootstrap_components as dbc
from dash import callback_context

# Sidebar de navegación reutilizable

# Layout de la sidebar con logo, título y enlaces a las páginas
def make_sidebar():
    sidebar_style = {
        "position": "fixed",
        "top": 0,
        "left": 0,
        "bottom": 0,
        "width": "15%",
        "padding": "2rem 1rem",
        "height": "100vh",
        "display": "flex",
        "flexDirection": "column",
        "alignItems": "center",
        "backgroundColor": "#121212",
    }
    logo_style = {
        "width": "40%",
        "marginBottom": "1rem"
    }
    title_style = {
        "fontWeight": "bold",
        "fontSize": "1.2rem",
        "marginBottom": "3rem",
        "color": "#FFFFFF",
    }
    link_style = {
        "margin": "0.4rem 0",
        "width": "100%",
        "color": "#FFFFFF",
    }
    
    return html.Div([
        html.Img(src="/assets/images/logo.png", style=logo_style),
        html.Div("Performance APP", style=title_style),
        dbc.Nav([
            dbc.NavLink("Summary", href="/summary", active="exact", style=link_style),
            dbc.NavLink("Session Report", href="/sessionReport", active="exact", style=link_style),
            dbc.NavLink("Cargar Dados", href="/cargar_datos", active="exact", style=link_style),
            dbc.NavLink("Settings", href="/settings", active="exact", style=link_style),
        ], vertical=True, pills=True, style={"width": "100%"})
    ], style=sidebar_style)
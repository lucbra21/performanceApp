import dash
from dash import dcc, html, Output, Input
import dash_bootstrap_components as dbc
from components.sidebar import make_sidebar

# Inicializa la aplicación Dash con Bootstrap
app = dash.Dash(__name__, external_stylesheets=[dbc.themes.BOOTSTRAP], use_pages=True, suppress_callback_exceptions=True)
server = app.server

# Importar páginas después de inicializar la app
from pages import cargar_datos, sessionReport, settings, summary

# Layout principal con sidebar y área de contenido
app.layout = html.Div([
    dcc.Location(id='url', refresh=False),
    html.Div([
        make_sidebar(),
    ], style={"width": "15%", "float": "left", "height": "100vh"}),
    html.Div([
        html.Div(id='page-content')
    ], style={"width": "85%", "float": "right", "padding": "3rem"})
])

# Callback para renderizar la página correcta
@app.callback(Output('page-content', 'children'), [Input('url', 'pathname')])
def display_page(pathname):
    # Esta función selecciona el layout de la página según el URL
    if pathname == '/cargar_datos':
        return cargar_datos.layout
    elif pathname == '/sessionReport':
        return sessionReport.layout
    elif pathname == '/settings':
        return settings.layout
    elif pathname == '/summary':
        return summary.layout
    else:
        return html.H1('Bienvenido a Performance APP')

# Registrar callbacks das páginas
cargar_datos.register_callbacks(app)
sessionReport.register_callbacks(app)

if __name__ == '__main__':
    app.run_server(host='0.0.0.0', port=8050, debug=False)

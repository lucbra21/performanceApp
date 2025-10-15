import dash
from dash import dcc, html, Output, Input
import dash_bootstrap_components as dbc
from components.sidebar import make_sidebar
from login import layout as login_layout, register_callbacks as register_login_callbacks  # 👈 importar login


# Inicializa la aplicación Dash con Bootstrap
app = dash.Dash(__name__, 
                external_stylesheets=[
                    dbc.themes.BOOTSTRAP,
                    '/assets/style.css',
                    '/assets/style_sessionReport.css',
                    '/assets/style_references.css'
                ], 
                use_pages=True, 
                suppress_callback_exceptions=True)
server = app.server

# Importar páginas después de inicializar la app
from pages import BenchmarkMD, IndividualValues, MicrocycleLoad, MicrocyclesContents, cargar_datos, sessionReport, settings, summary, preTraining, postTraining, Drills, TrainingContents, References

# Layout principal (incluye memoria de sesión)
app.layout = html.Div([
    dcc.Location(id="url", refresh=False),
    dcc.Store(id="session-store", storage_type="session"),  # almacena si el usuario está autenticado
    html.Div(id="page-content")
])

# Callback para renderizar la página correcta
@app.callback(
    Output("page-content", "children"),
    Input("url", "pathname"),
    Input("session-store", "data")
)
def display_page(pathname, session_data):
    # Si no hay sesión activa → mostrar login
    if not session_data or not session_data.get("authenticated"):
        return login_layout

    # Si el usuario está logueado → mostrar contenido con sidebar
    if pathname == "/" or pathname == "/summary":
        content = summary.layout
    elif pathname == "/settings":
        content = settings.layout
    elif pathname == "/cargar_datos":
        content = cargar_datos.layout
    elif pathname == "/training/sessionReport":
        content = sessionReport.layout
    elif pathname == "/training/preTraining":
        content = preTraining.layout
    elif pathname == "/training/postTraining":
        content = postTraining.layout
    elif pathname == "/training/References":
        content = References.layout
    elif pathname == "/training/MicrocycleLoad":
        content = MicrocycleLoad.layout
    elif pathname == "/training/MicrocycleContents":
        content = MicrocyclesContents.layout
    elif pathname == "/training/IndividualValues":
        content = IndividualValues.layout
    elif pathname == "/training/Drills":
        content = Drills.layout
    elif pathname == "/training/TrainingContents":
        content = TrainingContents.layout
    elif pathname == "/training/BenchmarkMD":
        content = BenchmarkMD.layout
    elif pathname == "/references":
        content = References.layout
    else:
        content = html.H1("Página no encontrada", style={"textAlign": "center"})

    return html.Div([
        html.Div([make_sidebar()], style={"width": "15%", "float": "left", "height": "100vh"}),
        html.Div([content], style={"width": "85%", "float": "right", "padding": "3rem"})
    ])

# Registrar los callbacks del login y las demás páginas
register_login_callbacks(app)
cargar_datos.register_callbacks(app)
sessionReport.register_callbacks(app)
References.register_callbacks(app)

# Para ejecutar localmente
if __name__ == '__main__':
    app.run(debug=True)

#Para ejecutar en el servidor
# if __name__ == '__main__':
#    app.run(host='0.0.0.0', port=8050, debug=False)

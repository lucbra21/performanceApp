import dash
from dash import dcc, html, Output, Input
import dash_bootstrap_components as dbc
from components.sidebar import make_sidebar

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
from pages import BenchmarkMD, IndividualValues, MicrocycleLoad, MicrocyclesContents, cargar_datos, sessionReport, settings, summary, preTraining, postTraining, Drills, TrainingContents, References, Benchmarking

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
    # Páginas independientes
    if pathname == '/cargar_datos':
        return cargar_datos.layout
    elif pathname == '/settings':
        return settings.layout
    elif pathname == '/summary':
        return summary.layout
    
    # Segmento Training
    elif pathname == '/training/sessionReport':
        return sessionReport.layout
    # Si agregas más páginas dentro de Training, las podés poner acá:
    elif pathname == '/training/preTraining':
        return preTraining.layout
    elif pathname == '/training/postTraining':
        return postTraining.layout
    elif pathname == '/training/References':
        return References.layout
    elif pathname == '/training/MicrocycleLoad':
        return MicrocycleLoad.layout
    elif pathname == '/training/MicrocycleContents':
        return MicrocyclesContents.layout
    elif pathname == '/training/IndividualValues':
        return IndividualValues.layout
    elif pathname == '/training/Drills':
        return Drills.layout
    elif pathname == '/training/TrainingContents':
        return TrainingContents.layout
    elif pathname == '/training/BenchmarkMD':
        return BenchmarkMD.layout
    elif pathname == '/references':
        return References.layout
    elif pathname == '/training/benchmarking':
        return Benchmarking.layout

    else:
        return html.H1('Bienvenido a Performance APP')

# Registrar callbacks das páginas
cargar_datos.register_callbacks(app)
sessionReport.register_callbacks(app)
References.register_callbacks(app)
Benchmarking.register_callbacks(app)

# Para ejecutar localmente
# if __name__ == '__main__':
#    app.run(debug=True)

#Para ejecutar en el servidor
 if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8050, debug=False)

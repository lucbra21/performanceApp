from dash import html

# Layout de la página
layout = html.Div([
    html.H1("Brenchmark MD"),
    html.P("Aquí va el contenido de la página Brenchmark MD.")
])

# Función para registrar callbacks si los necesitás
def register_callbacks(app):
    pass  # No hace nada si no tenés callbacks
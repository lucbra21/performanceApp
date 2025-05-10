# components/header.py
from dash import html

def Header():
    """Crea el componente de encabezado para la aplicación."""
    return html.Div([
        html.H1('Análisis de Rendimiento Deportivo')
    ], className='header')
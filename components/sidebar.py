# components/sidebar.py
from dash import html, dcc

def Sidebar(username=None):
    """Crea el componente de la barra lateral con logo y navegación."""
    return html.Div([
        html.Div([
            html.Img(src="/assets/logo.png", style={"width": "100px", "margin": "0 auto", "display": "block", "marginBottom": "20px"}),
        ], style={"textAlign": "center"}),
        html.H2('Menú', style={'textAlign': 'center', 'color': '#FFD700', 'fontWeight': 'bold'}),
        html.Hr(style={"borderColor": "#FFD700"}),
        dcc.Link('Página de Inicio', href='/home', className='sidebar-link'),
        dcc.Link('Sesion Report', href='/resumen-sesion', className='sidebar-link'),
        dcc.Link('Análisis de Partido', href='/analise-jogo', className='sidebar-link'),
        dcc.Link('Análisis de Temporada', href='/analise-epoca', className='sidebar-link'),
        dcc.Link('Configuración', href='/configuracoes', className='sidebar-link'),
        html.Div(id='username-display', style={'position': 'absolute', 'bottom': '20px', 'padding': '10px', 'textAlign': 'center', 'width': '100%'})
    ], className='sidebar', style={"position": "fixed", "left": 0, "top": 0, "bottom": 0, "width": "220px", "backgroundColor": "#1E1E1E", "color": "#FFD700", "height": "100vh", "zIndex": 100})
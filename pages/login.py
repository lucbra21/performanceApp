# pages/login.py
from dash import html, dcc
from dash.dependencies import Input, Output, State
# Placeholder for auth functions - to be defined in utils/auth.py
# from utils.auth import authenticate_user 

def layout():
    """Crea el diseño para la página de inicio de sesión."""
    return html.Div([
        html.H2('Iniciar Sesión'),
        dcc.Input(id='username-input', type='text', placeholder='Usuario'),
        dcc.Input(id='password-input', type='password', placeholder='Contraseña'),
        html.Button('Entrar', id='login-button', n_clicks=0),
        html.Div(id='login-output') # To display login status or errors
    ], className='login-container')

# TODO: Add callback for login button to authenticate user
# This will require importing the app object from app.py to use @app.callback
# and the authenticate_user function from utils/auth.py
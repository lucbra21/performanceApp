# app.py
import dash
from dash import html, dcc
from dash.dependencies import Input, Output, State

app = dash.Dash(__name__, suppress_callback_exceptions=True)
server = app.server

app.layout = html.Div([
    dcc.Location(id='url', refresh=False),
    html.Div(id='page-content')
])

# Import pages
from pages import login, home
# Import components (if they are used directly in app.py, otherwise they are in pages)
# from components.header import Header # Example, if needed directly
# from components.sidebar import Sidebar # Example, if needed directly

# Import authentication utility
from utils.auth import authenticate_user

# Callback to manage page content based on URL
@app.callback(Output('page-content', 'children'),
              [Input('url', 'pathname')])
def display_page(pathname):
    if pathname == '/login' or pathname == '/': # Default to login
        return login.layout()
    elif pathname == '/home':
        # Here you might want to check if the user is authenticated
        # For now, let's assume direct access after "login"
        return home.layout()
    # TODO: Add other pages here
    # elif pathname == '/analise-treino':
    #     return analise_treino.layout()
    else:
        return '404 - Página no encontrada'

# Callback for login
@app.callback(
    [Output('url', 'pathname', allow_duplicate=True),
     Output('login-output', 'children')], 
    [Input('login-button', 'n_clicks')],
    [State('username-input', 'value'),
     State('password-input', 'value')],
    prevent_initial_call=True
)
def login_attempt(n_clicks, username, password):
    if n_clicks > 0:
        if authenticate_user(username, password):
            # Store user session or token here if needed for more robust auth
            # For now, just redirect
            return '/home', f'Bienvenido, {username}!' # Removed username display update
        else:
            return dash.no_update, 'Usuario o contraseña inválidos.'
    return dash.no_update, ''

# Placeholder for username display in sidebar (if not logged in)
@app.callback(
    Output('username-display', 'children'),
    Input('url', 'pathname') # Trigger on page change
)
def update_username_on_sidebar_load(pathname):
    # This is a simple way, ideally, session management would handle this
    if pathname != '/login' and pathname != '/':
        # Attempt to get username if stored (e.g. from a dcc.Store or global var)
        # For this basic setup, it's tricky without proper session state. 
        # The login callback directly updates it for now.
        return dash.no_update # Or a default like "Usuario"
    return "" # Clear if on login page

if __name__ == '__main__':
    app.run(debug=True)
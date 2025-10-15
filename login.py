from dash import html, dcc, Output, Input, State
import dash_bootstrap_components as dbc
import dash

# Layout del login
layout = html.Div(
    [
        html.Div(
            [
                # Logo
                html.Img(
                    src="/assets/images/logo.png",
                    style={
                        "width": "120px",
                        "marginBottom": "25px",
                        "borderRadius": "10px"
                    },
                ),
                html.H2("Iniciar sesión", className="text-center mb-4 fw-bold", style={"color": "white"}),

                # Inputs
                dbc.Input(id="login-username", placeholder="Usuario", type="text", className="mb-3"),
                dbc.Input(id="login-password", placeholder="Contraseña", type="password", className="mb-3"),

                # Botón de login
                dbc.Button(
                    "Entrar",
                    id="login-btn",
                    color="primary",
                    className="w-100 fw-bold",
                    style={
                        "background": "linear-gradient(to right, #EBE4C0, #857100)",
                        "border": "none"
                    },
                ),
                html.Div(id="login-error", className="text-danger text-center mt-3"),
            ],
            style={
                "width": "360px",
                "textAlign": "center",
                "padding": "40px",
                "borderRadius": "20px",
                "backgroundColor": "rgba(20, 20, 20, 0.9)",
                "boxShadow": "0 0 25px rgba(0,0,0,0.6)",
            },
        )
    ],
    style={
        "height": "100vh",
        "background": "linear-gradient(135deg, #000000 0%, #111111 50%, #1c1c1c 100%)",
        "display": "flex",
        "justifyContent": "center",
        "alignItems": "center",
        "flexDirection": "column",
    },
)


def register_callbacks(app):
    @app.callback(
        Output("session-store", "data"),
        Output("login-error", "children"),
        Input("login-btn", "n_clicks"),
        State("login-username", "value"),
        State("login-password", "value"),
        prevent_initial_call=True
    )
    def validate_login(n, username, password):
        VALID_USERS = {"admin": "1234", "nahuel": "futbol"}  # credenciales temporales

        if username in VALID_USERS and VALID_USERS[username] == password:
            return {"authenticated": True}, ""
        else:
            return dash.no_update, "Usuario o contraseña incorrectos"

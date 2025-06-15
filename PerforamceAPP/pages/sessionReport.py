# ============================================================================
# IMPORTACIONES
# ============================================================================

# Importaciones de Dash
from dash import html, dcc, Output, Input, State, callback_context
import dash

# Importaciones del sistema y utilidades
import os
import datetime

# ============================================================================
# LAYOUT DE LA PÁGINA
# ============================================================================

layout = html.Div([
    # Título de la página
    html.H2('Session Report', className="page-title"),
    html.Hr(),
    
    # Contenedor principal
    html.Div([              
              
    ])
])

# ============================================================================
# CALLBACKS
# ============================================================================


def register_callbacks(app):
    """Registra todos los callbacks de la página"""

    pass
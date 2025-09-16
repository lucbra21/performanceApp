# ============================================================================
# IMPORTACIONES
# ============================================================================

# Importaciones de Dash
from dash import html, dcc, Output, Input, State, callback_context, dash_table
import dash

# Importaciones del sistema y utilidades
import os
from datetime import datetime
import polars as pl
import pandas as pd
import numpy as np
from utils.utils import *

# Importaciones del Plotly para gráficos
import plotly.graph_objects as go
import plotly.express as px

# ============================================================================
# CONSTANTES Y CONFIGURACIÓN
# ============================================================================

# Ruta al archivo de estadísticas de match day
ESTADISTICAS_MATCHDAY_PATH = os.path.join('data', 'processed', 'references', 'estadisticas_matchday.parquet')

# ============================================================================
# FUNCIONES AUXILIARES
# ============================================================================

def load_estadisticas_matchday():
    """
    Carga y procesa los datos de estadísticas de match day.
    
    Aplica los filtros solicitados:
    - Reemplaza valores nulos en Position por 'Team'
    - Reemplaza valores '0' en Position por 'Team'
    - Mantiene todos los Match Days (incluyendo 'MD')
    
    Returns:
        pl.DataFrame: DataFrame procesado con las estadísticas de match day
    """
    try:
        if not os.path.exists(ESTADISTICAS_MATCHDAY_PATH):
            print(f"No se encontró el archivo: {ESTADISTICAS_MATCHDAY_PATH}")
            return None
        
        # Cargar datos
        df = pl.read_parquet(ESTADISTICAS_MATCHDAY_PATH)
        
        # Reemplazar valores nulos y '0' en Position por 'Team'
        df_processed = df.with_columns([
            pl.when(pl.col('Position').is_null() | (pl.col('Position') == '0'))
            .then(pl.lit('Team'))
            .otherwise(pl.col('Position'))
            .alias('Position')
        ])
        
        return df_processed
        
    except Exception as e:
        print(f"Error al cargar estadísticas de match day: {e}")
        return None

def get_unique_values_from_estadisticas():
    """
    Obtiene todos los valores únicos de las columnas principales para los dropdowns.
    
    Returns:
        dict: Diccionario con listas de valores únicos para cada columna
    """
    df = load_estadisticas_matchday()
    if df is None:
        return {
            'match_days': [],
            'estadisticas': [],
            'players': [],
            'positions': []
        }
    
    try:
        # Obtener valores únicos filtrando nulos
        match_days = df.filter(pl.col('Match Day').is_not_null())['Match Day'].unique().sort().to_list()
        estadisticas = df.filter(pl.col('Estadistica').is_not_null())['Estadistica'].unique().sort().to_list()
        players = df.filter(pl.col('Player').is_not_null())['Player'].unique().sort().to_list()
        positions = df.filter(pl.col('Position').is_not_null())['Position'].unique().sort().to_list()
        
        return {
            'match_days': match_days,
            'estadisticas': estadisticas,
            'players': players,
            'positions': positions
        }
    except Exception as e:
        print(f"Error al obtener valores únicos: {e}")
        return {
            'match_days': [],
            'estadisticas': [],
            'players': [],
            'positions': []
        }

def get_available_dates_from_estadisticas():
    """
    Obtiene las fechas disponibles en los datos de estadísticas.
    
    Returns:
        list: Lista de fechas ordenadas cronológicamente
    """
    try:
        # Usar la función existente de utils para obtener fechas del GPS
        # ya que las fechas deben coincidir entre ambos datasets
        return get_sorted_dates()
    except Exception as e:
        print(f"Error al obtener fechas disponibles: {e}")
        return []

# ============================================================================
# LAYOUT DE LA PÁGINA
# ============================================================================

layout = html.Div([
    
    # Título de la página
    html.H2('Referencias - Estadísticas Match Day', className="page-title"),
    html.Hr(),
    
    # Contenedor principal
    html.Div([
        # Container para selección de parámetros
        html.Div([
            html.H4('Seleccionar Parámetros', className="section-title"),
            html.Div([
                # Input para rango de fechas (primer campo)
                html.Div([
                    html.Label('Rango de Fechas:', className="input-label"),
                    dcc.DatePickerRange(
                        id='ref-date-range-selector',
                        start_date=get_first_date_for_picker(),
                        end_date=get_latest_date_for_picker(),
                        display_format='DD/MM/YYYY',
                        start_date_placeholder_text='Fecha inicio',
                        end_date_placeholder_text='Fecha fin',
                        className="date-picker-range"
                    )
                ], className="input-item"),
                
                # Input para estadística
                html.Div([
                    html.Label('Estadística:', className="input-label"),
                    dcc.Dropdown(
                        id='ref-estadistica-selector',
                        placeholder='Selecciona una o más estadísticas...',
                        className="statistic-dropdown",
                        multi=True
                    )
                ], className="input-item"),
                
                # Input para Match Day
                html.Div([
                    html.Label('Match Day:', className="input-label"),
                    dcc.Dropdown(
                        id='ref-matchday-selector',
                        placeholder='Selecciona uno o más Match Days...',
                        className="statistic-dropdown",
                        multi=True
                    )
                ], className="input-item"),
                
                # Input para Player
                html.Div([
                    html.Label('Jugador:', className="input-label"),
                    dcc.Dropdown(
                        id='ref-player-selector',
                        placeholder='Selecciona uno o más jugadores...',
                        className="statistic-dropdown",
                        multi=True
                    )
                ], className="input-item"),
                
                # Input para Position
                html.Div([
                    html.Label('Posición:', className="input-label"),
                    dcc.Dropdown(
                        id='ref-position-selector',
                        placeholder='Selecciona una o más posiciones...',
                        className="statistic-dropdown",
                        multi=True
                    )
                ], className="input-item")
                
            ], className="inputs-row", style={'display': 'flex', 'flex-wrap': 'wrap', 'gap': '20px'})
        ], className="date-selection-container"),
        
        # Container para mostrar los datos filtrados
        html.Div([
            html.H4('Datos de Referencia', className="section-title", style={'margin-top': '30px'}),
            html.Div(id='ref-data-output')
        ], className="references-data-container")
        
    ])
])

# ============================================================================
# CALLBACKS
# ============================================================================

def register_callbacks(app):
    """
    Registra todos los callbacks de la página References.
    
    Esta función configura toda la interactividad de la página mediante callbacks
    de Dash que manejan la navegación de fechas, actualización de dropdowns y
    filtrado de datos de referencia.
    
    Args:
        app (dash.Dash): Instancia de la aplicación Dash donde se registrarán los callbacks
    """
    
    # ============================================================================
    # CALLBACKS - Configuración de rango de fechas
    # ============================================================================
    
    @app.callback(
        [Output('ref-date-range-selector', 'min_date_allowed'),
         Output('ref-date-range-selector', 'max_date_allowed')],
        [Input('ref-date-range-selector', 'id')]
    )
    def configure_date_range_limits(selector_id):
        """
        Configura los límites mínimo y máximo para el selector de rango de fechas.
        
        Args:
            selector_id: ID del selector (para inicialización)
        
        Returns:
            tuple: (fecha_mínima, fecha_máxima)
        """
        try:
            fechas_ordenadas = get_available_dates_from_estadisticas()
            
            if not fechas_ordenadas:
                return None, None
            
            # Configurar límites del calendario
            min_date = datetime.strptime(fechas_ordenadas[0], '%d/%m/%Y').date()
            max_date = datetime.strptime(fechas_ordenadas[-1], '%d/%m/%Y').date()
            
            return min_date, max_date
            
        except Exception as e:
            print(f"Error al configurar límites de fechas: {e}")
            return None, None
    
    # ============================================================================
    # CALLBACKS - Actualización de opciones de dropdowns
    # ============================================================================
    
    @app.callback(
        [Output('ref-estadistica-selector', 'options'),
         Output('ref-matchday-selector', 'options'),
         Output('ref-player-selector', 'options'),
         Output('ref-position-selector', 'options')],
        [Input('ref-date-range-selector', 'start_date'),
         Input('ref-date-range-selector', 'end_date')]
    )
    def update_dropdown_options(start_date, end_date):
        """
        Actualiza las opciones de todos los dropdowns basado en los datos disponibles.
        
        Args:
            start_date (str): Fecha de inicio del rango (no se usa para filtrar por ahora)
            end_date (str): Fecha de fin del rango (no se usa para filtrar por ahora)
        
        Returns:
            tuple: Tupla con las opciones para cada dropdown
        """
        try:
            unique_values = get_unique_values_from_estadisticas()
            
            # Crear opciones para estadísticas
            estadistica_options = [
                {'label': 'Media', 'value': 'mean'},
                {'label': 'Mediana', 'value': 'median'},
                {'label': 'Máximo', 'value': 'max'},
                {'label': 'Mínimo', 'value': 'min'},
                {'label': 'Percentil 75', 'value': 'p75'},
                {'label': 'Percentil 90', 'value': 'p90'},
                {'label': 'Percentil 95', 'value': 'p95'}
            ]
            
            # Crear opciones para Match Day
            matchday_options = [{'label': md, 'value': md} for md in unique_values['match_days']]
            
            # Crear opciones para Players
            player_options = [{'label': player, 'value': player} for player in unique_values['players']]
            
            # Crear opciones para Positions
            position_options = [{'label': pos, 'value': pos} for pos in unique_values['positions']]
            
            return estadistica_options, matchday_options, player_options, position_options
            
        except Exception as e:
            print(f"Error al actualizar opciones de dropdowns: {e}")
            return [], [], [], []
    
    # ============================================================================
    # CALLBACKS - Mostrar datos filtrados
    # ============================================================================
    
    @app.callback(
        Output('ref-data-output', 'children'),
        [Input('ref-date-range-selector', 'start_date'),
         Input('ref-date-range-selector', 'end_date'),
         Input('ref-estadistica-selector', 'value'),
         Input('ref-matchday-selector', 'value'),
         Input('ref-player-selector', 'value'),
         Input('ref-position-selector', 'value')]
    )
    def update_reference_data(start_date, end_date, selected_estadistica, selected_matchday, 
                            selected_player, selected_position):
        """
        Actualiza la visualización de los datos de referencia basado en los filtros seleccionados.
        
        Args:
            start_date (str): Fecha de inicio del rango
            end_date (str): Fecha de fin del rango
            selected_estadistica (list): Lista de estadísticas seleccionadas
            selected_matchday (list): Lista de Match Days seleccionados
            selected_player (list): Lista de jugadores seleccionados
            selected_position (list): Lista de posiciones seleccionadas
        
        Returns:
            html.Div: Componente con los datos filtrados
        """
        try:
            df = load_estadisticas_matchday()
            if df is None:
                return html.Div("No se pudieron cargar los datos de referencia.", 
                              className="error-message")
            
            # Aplicar filtros según las selecciones (ahora soporta múltiples valores)
            df_filtered = df
            
            # Filtrar por rango de fechas si están seleccionadas
            if start_date and end_date:
                try:
                    # Convertir fechas del formato YYYY-MM-DD a dd/mm/yyyy para comparar
                    start_dt = datetime.strptime(start_date, '%Y-%m-%d')
                    end_dt = datetime.strptime(end_date, '%Y-%m-%d')
                    
                    # Filtrar fechas que estén dentro del rango (usando columna 'Date' en lugar de 'Fecha')
                    df_filtered = df_filtered.filter(
                        pl.col('Date').str.strptime(pl.Date, format='%d/%m/%Y')
                        .is_between(start_dt.date(), end_dt.date())
                    )
                except Exception as e:
                    print(f"Error al filtrar por rango de fechas: {e}")
            
            if selected_estadistica and len(selected_estadistica) > 0:
                df_filtered = df_filtered.filter(pl.col('Estadistica').is_in(selected_estadistica))
            
            if selected_matchday and len(selected_matchday) > 0:
                df_filtered = df_filtered.filter(pl.col('Match Day').is_in(selected_matchday))
            
            if selected_player and len(selected_player) > 0:
                df_filtered = df_filtered.filter(pl.col('Player').is_in(selected_player))
            
            if selected_position and len(selected_position) > 0:
                df_filtered = df_filtered.filter(pl.col('Position').is_in(selected_position))
            
            if df_filtered.height == 0:
                return html.Div("No se encontraron datos con los filtros seleccionados.", 
                              className="warning-message")
            
            # Convertir a pandas para mostrar en tabla
            df_pandas = df_filtered.to_pandas()
            
            # Crear tabla con los datos filtrados
            return html.Div([
                html.P(f"Mostrando {len(df_pandas)} registros", className="info-text"),
                dash_table.DataTable(
                    data=df_pandas.to_dict('records'),
                    columns=[{"name": col, "id": col} for col in df_pandas.columns],
                    style_table={'overflowX': 'auto'},
                    style_cell={
                        'textAlign': 'left',
                        'padding': '10px',
                        'fontFamily': 'Arial'
                    },
                    style_header={
                        'backgroundColor': 'rgb(230, 230, 230)',
                        'fontWeight': 'bold'
                    },
                    page_size=20,
                    sort_action="native",
                    filter_action="native"
                )
            ])
            
        except Exception as e:
            print(f"Error al actualizar datos de referencia: {e}")
            return html.Div(f"Error al procesar los datos: {str(e)}", 
                          className="error-message")
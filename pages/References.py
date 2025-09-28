# ============================================================================
# IMPORTACIONES
# ============================================================================

import dash
from dash import dcc, html, Input, Output, callback, dash_table
import pandas as pd
import polars as pl
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, date
import os

# Registrar a página
dash.register_page(__name__, path='/training/References', name='Referencias')

# Importaciones específicas de los módulos utils
from utils.data_access import load_gps_data

# ============================================================================
# CONSTANTES Y CONFIGURACIÓN
# ============================================================================

# Ruta al archivo de estadísticas de match day
ESTADISTICAS_MATCHDAY_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 
                                    'data', 'processed', 'references', 'estadisticas_matchday.parquet')

# ============================================================================
# FUNCIONES AUXILIARES
# ============================================================================

def get_first_date_for_picker():
    """
    Obtiene la primera fecha disponible para el date picker.
    
    Returns:
        date: Primera fecha disponible o fecha actual si no hay datos
    """
    try:
        df = load_gps_data()
        if df is not None and df.height > 0:
            # Obtener la fecha más antigua
            fechas = df.select('Date').unique().sort('Date')
            primera_fecha_str = fechas.row(0)[0]
            # Convertir de formato dd/mm/yyyy a date object
            return datetime.strptime(primera_fecha_str, '%d/%m/%Y').date()
        else:
            return date.today()
    except Exception as e:
        print(f"Error al obtener primera fecha: {e}")
        return date.today()

def get_latest_date_for_picker():
    """
    Obtiene la última fecha disponible para el date picker.
    
    Returns:
        date: Última fecha disponible o fecha actual si no hay datos
    """
    try:
        df = load_gps_data()
        if df is not None and df.height > 0:
            # Obtener la fecha más reciente
            fechas = df.select('Date').unique().sort('Date', descending=True)
            ultima_fecha_str = fechas.row(0)[0]
            # Convertir de formato dd/mm/yyyy a date object
            return datetime.strptime(ultima_fecha_str, '%d/%m/%Y').date()
        else:
            return date.today()
    except Exception as e:
        print(f"Error al obtener última fecha: {e}")
        return date.today()

def load_estadisticas_matchday():
    """
    Loads and concatenates statistics data from two sources:
    - estadisticas_matchday.parquet: for Match Day values different from "MD"
    - metrics_94min_historical.parquet: for Match Day values equal to "MD"
    
    Returns:
        pl.DataFrame: Concatenated DataFrame with statistics or None if error
    """
    try:
        # Path to metrics_94min_historical.parquet
        metrics_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 
                                   'data', 'processed', 'references', 'metrics_94min_historical.parquet')
        
        df_final = None
        
        # Load estadisticas_matchday.parquet and filter Match Day != "MD"
        if os.path.exists(ESTADISTICAS_MATCHDAY_PATH):
            df_estadisticas = pl.read_parquet(ESTADISTICAS_MATCHDAY_PATH)
            # Filter out records where Match Day = "MD"
            df_estadisticas_filtered = df_estadisticas.filter(pl.col('Match Day') != 'MD')
            # Replace null and '0' values in Position with 'Team'
            df_estadisticas_processed = df_estadisticas_filtered.with_columns([
                pl.when(pl.col('Position').is_null() | (pl.col('Position') == '0'))
                .then(pl.lit('Team'))
                .otherwise(pl.col('Position'))
                .alias('Position')
            ])
            df_final = df_estadisticas_processed
        
        # Load metrics_94min_historical.parquet and add Match Day = "MD"
        if os.path.exists(metrics_path):
            df_metrics = pl.read_parquet(metrics_path)
            
            # Create a mapping from metrics columns to estadisticas columns
            # We need to transform the metrics data to match the estadisticas structure
            if df_final is not None:
                # Get the columns from estadisticas to match the structure
                target_columns = df_final.columns
                
                # Create a base DataFrame with Match Day = "MD" and required columns
                df_metrics_transformed = df_metrics.with_columns([
                    pl.lit('MD').alias('Match Day'),
                    pl.lit('mean').alias('Estadistica'),  # Default statistic type
                    pl.lit('Match').alias('tipo'),  # Default type for match data
                    pl.lit('Team').alias('Team')  # Default team value
                ])
                
                # Map metrics columns to estadisticas columns where possible
                column_mapping = {
                    'Distance (m)_94min': 'Distance (m)',
                    'Abs HSR(m)_94min': 'Abs HSR(m)',
                    'HSR Rel (m)_94min': 'HSR Rel (m)',
                    'Sprint Abs (m)_94min': 'Sprint Abs (m)',
                    'Sprint Rel (m)_94min': 'Sprint Rel (m)',
                    'Explosive Dist (m)_94min': 'Explosive Dist (m)',
                    'MAX Speed(km/h)_94min': 'MAX Speed(km/h)',
                    'Max Acceleration_94min': 'Max Acceleration',
                    'Accelerations_94min': 'Accelerations',
                    'Decelerations_94min': 'Decelerations',
                    'Dif. ACC/DEC_94min': 'Dif. ACC/DEC',
                    'Step Balance (%)_94min': 'Step Balance (%)',
                    'Total impacts_94min': 'Total impacts',
                    'Speed Zones (m) [0.0, 6.0]km/h (m)_94min': 'Speed Zones (m) [0.0, 6.0]km/h (m)'
                }
                
                # Rename columns in metrics DataFrame
                for old_col, new_col in column_mapping.items():
                    if old_col in df_metrics_transformed.columns:
                        df_metrics_transformed = df_metrics_transformed.rename({old_col: new_col})
                
                # Add missing columns with null values
                for col in target_columns:
                    if col not in df_metrics_transformed.columns:
                        if col in ['Abs HSR(m)/Min', 'Distance (m)/min', 'Explosive Dist (m)/min']:
                            # Calculate per-minute metrics if possible
                            base_col = col.replace('/Min', '').replace('/min', '')
                            if base_col in df_metrics_transformed.columns and 'Total Minutes en Partido' in df_metrics_transformed.columns:
                                df_metrics_transformed = df_metrics_transformed.with_columns([
                                    (pl.col(base_col) / pl.col('Total Minutes en Partido')).alias(col)
                                ])
                            else:
                                df_metrics_transformed = df_metrics_transformed.with_columns([
                                    pl.lit(None).alias(col)
                                ])
                        else:
                            df_metrics_transformed = df_metrics_transformed.with_columns([
                                pl.lit(None).alias(col)
                            ])
                
                # Select only the columns that exist in the target structure
                df_metrics_final = df_metrics_transformed.select(target_columns)
                
                # Concatenate both DataFrames
                df_final = pl.concat([df_final, df_metrics_final], how="vertical")
            else:
                # If no estadisticas data, just return metrics with MD
                df_final = df_metrics.with_columns([
                    pl.lit('MD').alias('Match Day'),
                    pl.lit('mean').alias('Estadistica'),
                    pl.lit('Match').alias('tipo'),
                    pl.lit('Team').alias('Team')
                ])
        
        # If we have data, ensure Position column is properly handled
        if df_final is not None:
            df_final = df_final.with_columns([
                pl.when(pl.col('Position').is_null() | (pl.col('Position') == '0'))
                .then(pl.lit('Team'))
                .otherwise(pl.col('Position'))
                .alias('Position')
            ])
        
        return df_final
        
    except Exception as e:
        print(f"Error loading and concatenating statistics: {e}")
        # Fallback to original estadisticas only
        try:
            if os.path.exists(ESTADISTICAS_MATCHDAY_PATH):
                df = pl.read_parquet(ESTADISTICAS_MATCHDAY_PATH)
                df_processed = df.with_columns([
                    pl.when(pl.col('Position').is_null() | (pl.col('Position') == '0'))
                    .then(pl.lit('Team'))
                    .otherwise(pl.col('Position'))
                    .alias('Position')
                ])
                return df_processed
            else:
                return None
        except Exception as fallback_error:
            print(f"Fallback error: {fallback_error}")
            return None

def get_unique_values_from_estadisticas():
    """
    Obtiene valores únicos para los dropdowns desde los datos.
    
    Returns:
        dict: Diccionario con listas de valores únicos para cada campo
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
        match_days = df.filter(pl.col('Match Day').is_not_null())['Match Day'].unique().sort().to_list() if 'Match Day' in df.columns else []
        estadisticas = df.filter(pl.col('Estadistica').is_not_null())['Estadistica'].unique().sort().to_list() if 'Estadistica' in df.columns else []
        players = df.filter(pl.col('Player').is_not_null())['Player'].unique().sort().to_list() if 'Player' in df.columns else []
        positions = df.filter(pl.col('Position').is_not_null())['Position'].unique().sort().to_list() if 'Position' in df.columns else []
        
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
    Obtiene las fechas disponibles desde los datos de estadísticas.
    
    Returns:
        list: Lista de fechas ordenadas en formato dd/mm/yyyy
    """
    try:
        df = load_gps_data()
        if df is not None and df.height > 0:
            fechas = df.select('Date').unique().sort('Date')
            return fechas.get_column('Date').to_list()
        else:
            return []
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
        # Container para rango de fechas (design melhorado)
        html.Div([
            html.Div([
                # Ícone e título
                html.Div([
                    html.I(className="fas fa-calendar-alt", style={
                        'font-size': '24px',
                        'color': '#3498db',
                        'margin-right': '12px'
                    }),
                    html.Span('RANGO DE FECHAS', style={
                        'font-weight': '700',
                        'font-size': '18px',
                        'color': '#2c3e50',
                        'letter-spacing': '0.5px'
                    })
                ], style={
                    'display': 'flex',
                    'align-items': 'center',
                    'justify-content': 'center',
                    'margin-bottom': '25px'
                }),
                
                # Container do date picker com design moderno
                html.Div([
                    html.Div([
                        dcc.DatePickerRange(
                            id='ref-date-range-selector',
                            start_date=get_first_date_for_picker(),
                            end_date=get_latest_date_for_picker(),
                            display_format='DD/MM/YYYY',
                            start_date_placeholder_text='📅 Fecha inicio',
                            end_date_placeholder_text='📅 Fecha fin',
                            style={
                                'width': '100%',
                                'font-size': '15px',
                                'font-weight': '500'
                            },
                            calendar_orientation='horizontal',
                            number_of_months_shown=2,
                            with_portal=True,
                            clearable=True,
                            className='modern-date-picker'
                        )
                    ], style={
                        'background': '#ffffff',
                        'border-radius': '12px',
                        'padding': '15px',
                        'box-shadow': '0 2px 10px rgba(0, 0, 0, 0.08)',
                        'border': '2px solid #e8f4fd',
                        'transition': 'all 0.3s ease'
                    })
                ], style={
                    'max-width': '550px', 
                    'margin': '0 auto',
                    'display': 'flex',
                    'justify-content': 'center'
                })
            ], style={
                'text-align': 'center', 
                'margin-bottom': '45px', 
                'padding': '35px 30px', 
                'background': 'linear-gradient(135deg, #f8fbff 0%, #e8f4fd 50%, #dbeafe 100%)', 
                'border-radius': '20px', 
                'border': '1px solid #bfdbfe',
                'box-shadow': '0 8px 25px rgba(59, 130, 246, 0.08), 0 3px 10px rgba(0, 0, 0, 0.05)',
                'position': 'relative',
                'overflow': 'hidden'
            })
        ], className="date-range-container"),
        
        # Container para selección de otros parámetros
        html.Div([
            html.H4('Seleccionar Parámetros', className="section-title"),
            html.Div([
                # Input para estadística
                html.Div([
                    html.Label('ESTADÍSTICA:', className="input-label"),
                    dcc.Dropdown(
                        id='ref-estadistica-selector',
                        placeholder='Selecciona una o más estadísticas...',
                        className="statistic-dropdown",
                        multi=True,
                        style={
                            'font-size': '14px'
                        },
                        optionHeight=40,
                        maxHeight=300
                    )
                ], className="input-item"),
                
                # Input para Match Day
                html.Div([
                    html.Label('MATCH DAY:', className="input-label"),
                    dcc.Dropdown(
                        id='ref-matchday-selector',
                        placeholder='Selecciona uno o más Match Days...',
                        className="statistic-dropdown",
                        multi=True,
                        style={
                            'font-size': '14px'
                        },
                        optionHeight=40,
                        maxHeight=300
                    )
                ], className="input-item"),
                
                # Input para Player
                html.Div([
                    html.Label('JUGADOR:', className="input-label"),
                    dcc.Dropdown(
                        id='ref-player-selector',
                        placeholder='Selecciona uno o más jugadores...',
                        className="statistic-dropdown",
                        multi=True,
                        style={
                            'font-size': '14px'
                        },
                        optionHeight=40,
                        maxHeight=300
                    )
                ], className="input-item"),
                
                # Input para Position
                html.Div([
                    html.Label('POSICIÓN:', className="input-label"),
                    dcc.Dropdown(
                        id='ref-position-selector',
                        placeholder='Selecciona una o más posiciones...',
                        className="statistic-dropdown",
                        multi=True,
                        style={
                            'font-size': '14px'
                        },
                        optionHeight=40,
                        maxHeight=300
                    )
                ], className="input-item")
                
            ], className="inputs-row", style={'display': 'flex', 'flex-wrap': 'wrap', 'gap': '20px'})
        ], className="parameters-selection-container"),
        
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
            
            # Round numeric columns to 2 decimal places
            numeric_columns = df_pandas.select_dtypes(include=['float64', 'float32', 'int64', 'int32']).columns
            for col in numeric_columns:
                df_pandas[col] = df_pandas[col].round(2)
            
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
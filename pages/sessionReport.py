"""
============================================================================
MÓDULO: SESSION REPORT - ANÁLISIS DE SESIONES DE ENTRENAMIENTO
============================================================================

Este módulo implementa la página principal de análisis de sesiones de entrenamiento,
proporcionando una interfaz completa para visualizar y analizar datos de rendimiento
de jugadores en sesiones específicas.

FUNCIONALIDADES PRINCIPALES:
- Navegación temporal entre sesiones de entrenamiento
- Análisis comparativo de métricas de rendimiento
- Visualización de datos en múltiples formatos (absolutos, Z-scores, diferencias porcentuales)
- Sistema de tarjetas para métricas de equipo y posiciones
- Generación de gráficos de análisis de rendimiento

FUNCIONES DISPONIBLES:
========================

1. register_callbacks(app)
   - Propósito: Registra todos los callbacks de la página Session Report
   - Funcionalidad: Configura la interactividad completa de la página
   - Dependencias: Todas las funciones callback del módulo

2. manage_date_navigation_and_config(minus_clicks, plus_clicks, selector_id, current_date)
   - Propósito: Gestiona la navegación de fechas y configuración del calendario
   - Funcionalidad: Permite navegación secuencial entre fechas con datos disponibles
   - Dependencias: utils/date.py (get_sorted_dates, get_date_range, format_date)

3. update_session_info(selected_date, selected_statistic)
   - Propósito: Actualiza la información de la sesión seleccionada
   - Funcionalidad: Muestra resumen de jugadores, duración y match days
   - Dependencias: utils/sessionReport_utils.py (get_table_data), utils/date.py (format_date)

4. update_comparative_table(selected_date, selected_statistic, table_mode)
   - Propósito: Genera tabla comparativa con múltiples modos de visualización
   - Funcionalidad: Muestra datos en formato absoluto, Z-scores o diferencias porcentuales
   - Dependencias: utils/sessionReport_utils.py (generate_comparative_table, format_data_for_display_mode, generate_zscore_color_scheme)

5. update_tarjetas_columns_options(selected_date)
   - Propósito: Actualiza opciones del dropdown de columnas para tarjetas
   - Funcionalidad: Proporciona lista de métricas disponibles para selección
   - Dependencias: utils/sessionReport_utils.py (get_table_data), utils/date.py (format_date)

6. update_cards_view_options(selected_date, selected_statistic)
   - Propósito: Actualiza opciones de vista para tarjetas (equipo/posiciones)
   - Funcionalidad: Genera opciones basadas en posiciones disponibles en los datos
   - Dependencias: utils/sessionReport_utils.py (get_table_data), utils/date.py (format_date)

7. update_team_cards(selected_date, selected_statistic, selected_view, selected_columns)
   - Propósito: Genera tarjetas de métricas para equipos y posiciones
   - Funcionalidad: Muestra valores absolutos de métricas en formato de tarjetas visuales
   - Dependencias: utils/sessionReport_utils.py (get_table_data), utils/date.py (format_date)


"""

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

# Importaciones específicas de los módulos utils
from utils.date import *
from utils.data_access import *
from utils.pages.sessionReport_utils import *


# Importaciones del Plotly para gráficos
import plotly.graph_objects as go
import plotly.express as px


# ============================================================================
# LAYOUT DE LA PÁGINA
# ============================================================================

"""
ESTRUCTURA DEL LAYOUT DE SESSION REPORT:
========================================

El layout de la página está organizado en secciones jerárquicas que proporcionan
una interfaz completa para el análisis de sesiones de entrenamiento:

1. ENCABEZADO DE PÁGINA:
   - Título principal "Session Report"
   - Separador visual (html.Hr)

2. CONTENEDOR DE PARÁMETROS DE SELECCIÓN:
   - Sección "Seleccionar Parámetros" con controles principales:
     * Selector de fecha con navegación (botones -/+ y DatePickerSingle)
     * Dropdown de estadísticas (mean, median, max, min, percentiles)

3. CONTENEDOR PRINCIPAL DE DATOS:
   - Información de sesión (número de jugadores, duración, match days)
   - Controles para tarjetas de métricas:
     * Selector de vista (Equipo/Posiciones)
     * Selector múltiple de columnas a mostrar
   - Área de visualización de tarjetas de métricas
   - Controles para tabla comparativa:
     * RadioItems para modo de visualización (Absolutos/Z-Score/Diferencia %)
   - Área de tabla comparativa dinámica

4. SECCIÓN DE GRÁFICOS (EXCLUIDA DE DOCUMENTACIÓN):
   - Múltiples gráficos de análisis de rendimiento
   - Controles específicos para gráficos
   - Gráfico personalizable con selección de ejes

5. COMPONENTES DE ESTADO:
   - dcc.Store para controlar estado de Z-Score


"""

layout = html.Div([

    
    # Título de la página
    html.H2('Session Report', className="page-title"),
    html.Hr(),
    
    # Contenedor principal
    html.Div([
        # Container para selección de fecha y estadística
        html.Div([
            html.H4('Seleccionar Parámetros', className="section-title"),
            html.Div([
                # Input para fecha con botones de navegación
                html.Div([
                    html.Label('Fecha:', className="input-label"),
                    html.Div([
                        html.Button(
                            '-',
                            id='date-minus-btn',
                            className='date-nav-btn date-minus',
                            title='Día anterior'
                        ),
                        dcc.DatePickerSingle(
                            id='date-selector',
                            date=get_latest_date_for_picker(),
                            placeholder='Selecciona una fecha...',
                            display_format='DD/MM/YYYY',
                            className="date-picker"
                        ),
                        html.Button(
                            '+',
                            id='date-plus-btn',
                            className='date-nav-btn date-plus',
                            title='Día siguiente'
                        )
                    ], className="date-input-container")
                ], className="input-item"),
                
                # Input para estadística
                html.Div([
                    html.Label('Estadística:', className="input-label"),
                    dcc.Dropdown(
                        id='statistic-selector',
                        options=[
                            {'label': 'Media', 'value': 'mean'},
                            {'label': 'Mediana', 'value': 'median'},
                            {'label': 'Máximo', 'value': 'max'},
                            {'label': 'Mínimo', 'value': 'min'},
                            {'label': 'Percentil 75', 'value': 'p75'},
                            {'label': 'Percentil 90', 'value': 'p90'},
                            {'label': 'Percentil 95', 'value': 'p95'}
                        ],
                        value='median',
                        placeholder='Selecciona una estadística...',
                        className="statistic-dropdown"
                    )
                ], className="input-item")
            ], className="inputs-row")
        ], className="date-selection-container"),
        
        # Container unificado para información de sesión, tarjetas y tabla de jugadores
        html.Div([
            html.Div(id='session-info-output'),
            # Controles para tarjetas
            html.Div([
                # Dropdown para seleccionar vista de tarjetas
                html.Div([
                    html.Label('Vista de tarjetas:', className="input-label"),
                    dcc.Dropdown(
                        id='cards-view-selector',
                        placeholder='Selecciona vista...',
                        value='Equipo',
                        className="statistic-dropdown",
                        style={'width': '300px'}
                    )
                ], className="input-item", style={'display': 'inline-block', 'margin-right': '100px'}),
                
                # Selector de columnas de las tarjetas
                html.Div([
                    html.Label('Columnas a mostrar:', className="input-label"),
                    dcc.Dropdown(
                        id='tarjetas-columns-selector',
                        placeholder='Selecciona columnas...',
                        multi=True,
                        className="statistic-dropdown",
                        style={'width': '400px'}
                    )
                ], className="input-item", style={'display': 'inline-block'})
            ], style={'margin-bottom': '10px', 'margin-top': '20px'}),
            html.Div(id='team-tarjetas-output'),
            
            # Controles para la tabla comparativa dinâmica
            html.Div([
                html.H4('Tabla Comparativa', className="section-title", style={'margin-top': '30px'}),
                html.Div([
                    html.Label('Modo de visualización:', className="input-label"),
                    dcc.RadioItems(
                        id='table-mode-selector',
                        options=[
                            {'label': 'Valores Absolutos', 'value': 'absolutos'},
                            {'label': 'Absolutos + Z-Score', 'value': 'zscore'},
                            {'label': 'Absolutos + Diferencia %', 'value': 'diferencia'}
                        ],
                        value='diferencia',
                        inline=True,
                        inputStyle={'margin-right': '12px', 'margin-left': '6px'},
                        className="statistic-radioitems"
                    )
                ], className="input-item", style={'margin-bottom': '15px'})
            ], className="table-controls-container"),
            
            # Contenedor para la tabla comparativa dinâmica
            html.Div(id='comparative-table-output'),
            
            html.Div(id='players-table-output'),

            html.Hr(),

            html.Div([
                    html.Div([
                        dcc.Graph(id='grafico-distance')
                    ], className="graph-box", style={'width': '100%', 'display': 'inline-block'})
                ], style={'margin-bottom': '20px'}),
            
            # Seção de gráficos
            html.Div([
                html.H4('Gráficos de Análisis', className="section-title", style={'margin-top': '30px'}),
            html.Div([
                html.Label('Estadística:', className="input-label"),
                dcc.RadioItems(
                    id='graphs-statistic-selector',
                    options=[
                        {'label': 'Media', 'value': 'mean'},
                        {'label': 'Mediana', 'value': 'median'},
                        {'label': 'Máximo', 'value': 'max'},
                        {'label': 'Mínimo', 'value': 'min'},
                        {'label': 'Percentil 75', 'value': 'p75'},
                        {'label': 'Percentil 90', 'value': 'p90'},
                        {'label': 'Percentil 99', 'value': 'p99'}
                    ],
                    value='median',
                    inline=True,
                    inputStyle={'margin-right': '12px', 'margin-left': '6px'},
                    className="statistic-radioitems"
                )
            ], className="input-item"),
                # Primera fila de gráficos - Distance e HSR
                html.Div([
                    html.Div([
                        dcc.Graph(id='grafico-nuevo1')
                    ], className="graph-box", style={'width': '96%', 'display': 'inline-block', 'margin-right': '2%'}),
                    
                    html.Div([
                        dcc.Graph(id='grafico-nuevo2')
                    ], className="graph-box", style={'width': '96%', 'display': 'inline-block', 'margin-right': '2%'})
                ], style={'margin-bottom': '20px'}),  # ← esto cierra la fila de los dos primeros gráficos

                # Segunda fila - gráfico de ACC/DCC ocupando todo el ancho
                html.Div([
                    html.Div([
                        dcc.Graph(id='grafico-nuevo3')
                    ], className="graph-box", style={'width': '96%', 'display': 'inline-block', 'margin-right': '2%'})
                ], style={'margin-bottom': '20px'}),

                html.Div([
                    html.Div([
                        dcc.Graph(id='grafico-nuevo5')
                    ], className="graph-box", style={'width': '96%', 'display': 'inline-block', 'margin-right': '2%'})
                ], style={'margin-bottom': '20px'}),

                html.Div([
                    html.H4("Seleccioná las variables para el gráfico personalizado:", style={"margin-top": "40px"}),
                    
                    html.Div([
                        html.Div([
                            html.Label("Eje X"),
                            dcc.Dropdown(id='dropdown-x-axis', placeholder="Seleccioná columna X")
                        ], style={'width': '48%', 'display': 'inline-block'}),
                        
                        html.Div([
                            html.Label("Eje Y"),
                            dcc.Dropdown(id='dropdown-y-axis', placeholder="Seleccioná columna Y")
                        ], style={'width': '48%', 'float': 'right', 'display': 'inline-block'})
                    ]),
                    
                    dcc.Graph(id='grafico-nuevo4')
                ], className="graph-box", style={'width': '100%', 'margin-top': '30px'}),

                
                
                # # Segunda fila de gráficos - ACC, DCC e Velocidad
                # html.Div([
                #     html.Div([
                #         html.Div([
                #             dcc.Graph(id='grafico-acc')
                #         ], className="graph-box")
                #     ], style={'width': '32%', 'display': 'inline-block', 'margin-right': '2%'}),
                    
                #     html.Div([
                #         html.Div([
                #             dcc.Graph(id='grafico-dcc')
                #         ], className="graph-box")
                #     ], style={'width': '32%', 'display': 'inline-block', 'margin-right': '2%'}),
                    
                #     html.Div([
                #         html.Div([
                #             dcc.Graph(id='grafico-velocidad')
                #         ], className="graph-box")
                #     ], style={'width': '32%', 'display': 'inline-block'})
                # ], style={'margin-bottom': '20px'}),
                
                # Tercera fila de gráficos - Posiciones
                html.Div([
                    html.Div([
                        html.Div([
                            dcc.Graph(id='grafico-posiciones')
                        ], className="graph-box")
                    ], style={'width': '100%'})
                ], style={'margin-bottom': '20px'})
            ], id='graficos-section')
        ], className="session-and-players-container")
    ]),
    
    # Store para controlar o estado do Z-Score
    dcc.Store(id='zscore-state-store', data={'active': True})
])

# ============================================================================
# CALLBACKS
# ============================================================================

def register_callbacks(app):
    """
    Registra todos los callbacks de la página Session Report.
    
    Esta función central configura toda la interactividad de la página mediante callbacks
    de Dash que manejan la navegación de fechas, actualización de datos, formateo de tablas,
    generación de gráficos y alternancia entre diferentes vistas de datos.
    
    Args:
        app (dash.Dash): Instancia de la aplicación Dash donde se registrarán los callbacks
    
    Callbacks incluidos:
        - Navegación de fechas con botones y configuración del calendario
        - Actualización de información de sesión
        - Alternancia entre visualización de z-scores y valores relativos
        - Actualización de tabla de jugadores con formateo condicional
        - Generación de múltiples gráficos de análisis de rendimiento
        - Gestión de estados de visualización mediante dcc.Store
    
    Note:
        Esta función debe ser llamada durante la inicialización de la aplicación
        para activar toda la funcionalidad interactiva de la página.
    """
    
    # ============================================================================
    # CALLBACKS - Input fecha y metricas
    # ============================================================================

    # Callback unificado para navegación de fechas y configuración del calendario
    @app.callback(
        [Output('date-selector', 'date'),
         Output('date-selector', 'min_date_allowed'),
         Output('date-selector', 'max_date_allowed'),
         Output('date-selector', 'disabled_days')],
        [Input('date-minus-btn', 'n_clicks'),
         Input('date-plus-btn', 'n_clicks'),
         Input('date-selector', 'id')],
        State('date-selector', 'date')
    )
    def manage_date_navigation_and_config(minus_clicks, plus_clicks, selector_id, current_date):
        """
        Gestiona la navegación de fechas y configuración del calendario de forma unificada.
        
        Esta función callback maneja tanto la navegación mediante botones como la configuración
        inicial del calendario, asegurando que solo se muestren fechas con datos disponibles
        y permitiendo navegación secuencial entre fechas válidas.
        
        Utiliza las funciones del módulo utils/date.py para:
        - get_sorted_dates(): Obtener fechas ordenadas cronológicamente
        - get_date_range(): Obtener rango de fechas para límites del calendario
        - get_latest_date_for_picker(): Obtener fecha más reciente para inicialización
        - format_date(): Convertir entre formatos de fecha
        
        Args:
            minus_clicks (int): Número de clics en el botón de fecha anterior
            plus_clicks (int): Número de clics en el botón de fecha siguiente  
            selector_id (str): ID del selector de fecha (usado para inicialización)
            current_date (str): Fecha actualmente seleccionada en formato YYYY-MM-DD
        
        Returns:
            tuple: (nueva_fecha, fecha_mínima, fecha_máxima, días_deshabilitados)
                  - nueva_fecha: Fecha seleccionada en formato YYYY-MM-DD
                  - fecha_mínima/máxima: Límites del calendario
                  - días_deshabilitados: Lista de fechas sin datos (vacía en este caso)
        """
        ctx = dash.callback_context
        
        # Obtener fechas ordenadas cronológicamente usando función de utils/date.py
        fechas_ordenadas = get_sorted_dates()
        
        if not fechas_ordenadas:
            return dash.no_update, None, None, []
        
        # Configurar límites del calendario usando función de utils/date.py
        min_date_str, max_date_str = get_date_range()
        try:
            min_date = datetime.strptime(min_date_str, '%Y-%m-%d').date() if min_date_str else None
            max_date = datetime.strptime(max_date_str, '%Y-%m-%d').date() if max_date_str else None
        except:
            min_date = max_date = None
        
        # Si no hay trigger (inicialización de la página), usar función de utils/date.py
        if not ctx.triggered:
            latest_date_formatted = get_latest_date_for_picker()
            return latest_date_formatted, min_date, max_date, []
        
        # Si es solo inicialización del selector, solo configurar calendario
        if ctx.triggered[0]['prop_id'] == 'date-selector.id':
            return dash.no_update, min_date, max_date, []
        
        # Manejar navegación con botones
        if not current_date:
            # Si no hay fecha actual, usar función de utils/date.py para obtener la primera fecha
            first_date_str = get_first_date_for_picker()
            return first_date_str, min_date, max_date, []
        
        # Convertir current_date usando función de utils/date.py
        current_date_formatted = format_date(current_date)
        
        # Encontrar índice de la fecha actual en la lista ordenada
        try:
            current_index = fechas_ordenadas.index(current_date_formatted)
        except ValueError:
            # Si la fecha actual no está en la lista, buscar la más cercana
            try:
                current_dt = datetime.strptime(current_date_formatted, '%d/%m/%Y')
                fechas_dt = [datetime.strptime(f, '%d/%m/%Y') for f in fechas_ordenadas]
                closest_index = min(range(len(fechas_dt)), key=lambda i: abs((fechas_dt[i] - current_dt).days))
                current_index = closest_index
            except:
                current_index = 0
        
        # Determinar qué botón fue presionado y navegar
        button_id = ctx.triggered[0]['prop_id'].split('.')[0]
        
        if button_id == 'date-minus-btn' and current_index > 0:
            # Navegar a la fecha anterior
            new_date = fechas_ordenadas[current_index - 1]
        elif button_id == 'date-plus-btn' and current_index < len(fechas_ordenadas) - 1:
            # Navegar a la fecha siguiente
            new_date = fechas_ordenadas[current_index + 1]
        else:
            # No se puede navegar más o no hay cambio
            new_date = fechas_ordenadas[current_index]
        
        # Convertir la nueva fecha de dd/mm/aaaa a aaaa-mm-dd para el DatePickerSingle
        try:
            new_date_dt = datetime.strptime(new_date, '%d/%m/%Y')
            new_date_formatted = new_date_dt.strftime('%Y-%m-%d')
        except:
            new_date_formatted = new_date
        
        return new_date_formatted, min_date, max_date, []
    
    
    # ============================================================================
    # CALLBACKS - Información de Sesión
    # ============================================================================
    
    @app.callback(
        Output('session-info-output', 'children'),
        [Input('date-selector', 'date'),
         Input('statistic-selector', 'value')]
    )
    def update_session_info(selected_date, selected_statistic):
        """
        Actualiza la información de la sesión basada en la fecha y estadística seleccionadas.
        
        Esta función callback genera un resumen de la sesión de entrenamiento incluyendo
        número de jugadores, duración total, match days disponibles y la estadística
        actualmente seleccionada para análisis.
        
        Utiliza las funciones del módulo utils/sessionReport_utils.py para:
        - get_table_data(): Obtener datos de jugadores filtrados por fecha
        
        Y del módulo utils/date.py para:
        - format_date(): Convertir formato de fecha para visualización
        
        Args:
            selected_date (str): Fecha seleccionada en formato YYYY-MM-DD
            selected_statistic (str): Código de la estadística seleccionada 
                                    ('mean', 'median', 'max', 'min', 'p75', 'p90', 'p95')
        
        Returns:
            html.Div: Componente HTML con la información de la sesión formateada,
                     incluyendo título, detalles de la sesión y estadística seleccionada
        

        """
        if not selected_date:
            return html.Div("Selecciona una fecha para ver la información de la sesión.", 
                          className="info-message")
        
        try:
            # Convertir fecha de YYYY-MM-DD a dd/mm/aaaa usando función de utils/date.py
            formatted_date_internal = format_date(selected_date)
            
            # Obtener datos de jugadores usando función de utils/sessionReport_utils.py
            df_players = get_table_data(
                selected_date=formatted_date_internal,
                selected_statistic=selected_statistic or 'mean'  # Usar 'mean' como valor por defecto
            )
            
            if df_players is None or df_players.height == 0:
                return html.Div(f"No se encontraron datos para la fecha {selected_date}.", 
                              className="warning-message")
            
            # Obtener información básica de la sesión
            num_jugadores = df_players.select('Player').n_unique()
            
            # Calcular duración total de drills si la columna existe
            duration = "N/A"
            if 'Drills Duration' in df_players.columns:
                duration_value = df_players.select('Total Minutes').row(0)[0]
                if duration_value:
                    duration = str(duration_value)
            
            # Filtrar Match Days excluyendo 'Rehab' si la columna existe
            match_days_filtered = []
            if 'Match Day' in df_players.columns:
                match_days_filtered = (df_players
                                     .filter(pl.col('Match Day') != 'Rehab')
                                     .select('Match Day')
                                     .unique()
                                     .to_series()
                                     .to_list())
            
            # Formatear fecha para visualización usando función de utils/date.py
            formatted_date_display = format_date(selected_date)
            
            # Crear información base
            session_info = [
                html.H5(f"Información de la Sesión - {formatted_date_display}", className="session-title"),
                html.P(f"Número de jugadores: {num_jugadores}", className="session-detail"),
                html.P(f"Duración total: {duration}", className="session-detail"),
            ]
            
            # Agregar información de Match Days si está disponible
            if match_days_filtered:
                session_info.append(
                    html.P(f"Match Days: {', '.join(match_days_filtered)}", className="session-detail")
                )
            
            # Agregar información de estadística si está seleccionada
            if selected_statistic:
                statistic_labels = {
                    'mean': 'Media',
                    'median': 'Mediana', 
                    'max': 'Máximo',
                    'min': 'Mínimo',
                    'p75': 'Percentil 75',
                    'p90': 'Percentil 90',
                    'p95': 'Percentil 95'
                }
                statistic_name = statistic_labels.get(selected_statistic, selected_statistic)
                session_info.append(
                    html.P(f"Estadística seleccionada: {statistic_name}", 
                          className="session-detail statistic-selected")
                )
            
            return html.Div(session_info, className="session-info-card")
            
        except Exception as e:
            return html.Div(f"Error al cargar información de la sesión: {str(e)}", 
                          className="error-message")
            
    
    # ============================================================================
    # CALLBACK - Tabla Comparativa Dinâmica
    # ============================================================================
    
    @app.callback(
        Output('comparative-table-output', 'children'),
        [Input('date-selector', 'date'),
         Input('statistic-selector', 'value'),
         Input('table-mode-selector', 'value')]
    )
    def update_comparative_table(selected_date, selected_statistic, table_mode):
        """
        Actualiza la tabla comparativa dinâmica con tres modos de visualización.
        
        Esta función callback genera una tabla que muestra datos de jugadores en tres formatos:
        - Valores absolutos únicamente
        - Valores absolutos con Z-scores en formato: valor_absoluto (Z = zscore)
        - Valores absolutos con diferencias porcentuales en formato: valor_absoluto (diferencia%)
        
        El sistema de colores se basa siempre en los Z-scores correspondientes, aplicando
        un gradiente de azul oscuro (valores bajos) a rojo oscuro (valores altos) con
        blanco para valores neutros (Z-score ≈ 0).
        
        Args:
            selected_date (str): Fecha seleccionada en formato YYYY-MM-DD
            selected_statistic (str): Estadística seleccionada para el análisis
            table_mode (str): Modo de visualización ('absolutos', 'zscore', 'diferencia')
        
        Returns:
            html.Div: Componente HTML con la tabla comparativa formateada según el modo seleccionado
        """
        if not selected_date or not selected_statistic or not table_mode:
            return html.Div("Selecciona todos los parámetros para ver la tabla comparativa.", 
                          className="info-message")
        
        try:
            # Convertir formato de fecha de YYYY-MM-DD a dd/mm/aaaa
            from datetime import datetime
            date_obj = datetime.strptime(selected_date, '%Y-%m-%d')
            formatted_date = date_obj.strftime('%d/%m/%Y')
            
            # Obtener datos usando la función generate_comparative_table
            comparative_data = generate_comparative_table(
                selected_date=formatted_date,
                selected_statistic=selected_statistic
            )
            
            if comparative_data is None:
                return html.Div(f"No se pudieron obtener los datos comparativos para la fecha {formatted_date}.", 
                              className="warning-message")
            
            # Filtrar datos según el tipo de entidad (solo jugadores para la tabla comparativa)
            players_data = comparative_data.filter(
                ~pl.col('Player').str.starts_with('TEAM') & 
                ~pl.col('Player').str.starts_with('POS_')
            )
            
            if players_data.height == 0:
                return html.Div("No se encontraron datos de jugadores para la fecha seleccionada.", 
                              className="warning-message")
            
            # Separar los tres tipos de datos
            absolutos_data = players_data.filter(pl.col('Tipo') == 'Absoluto')
            zscore_data = players_data.filter(pl.col('Tipo') == 'Z-Score')
            diferencia_data = players_data.filter(pl.col('Tipo') == 'Diferencia %')
            
            # Obtener columnas de métricas (excluyendo Player y Tipo)
            metric_columns = [col for col in players_data.columns if col not in ['Player', 'Tipo']]
            
            # Formatear datos usando la función auxiliar
            display_data, zscore_data_for_colors = format_data_for_display_mode(
                players_data, table_mode
            )
            
            # Convertir a formato compatible con DataTable
            data_for_table = display_data.to_dicts()
            
            # Generar esquema de colores usando la función auxiliar
            color_styles = generate_zscore_color_scheme(zscore_data_for_colors, metric_columns, display_data)
            
            # Crear la tabla con estilos responsivos y transiciones suaves
            comparative_table = dash_table.DataTable(
                id='comparative-dynamic-table',
                data=data_for_table,
                columns=[
                    {"name": col, "id": col, "type": "text"}
                    for col in display_data.columns
                ],
                style_table={
                    'overflowX': 'auto',
                    'height': 'auto',
                    'overflowY': 'visible',
                    'minWidth': '100%'
                },
                style_cell={
                    'textAlign': 'left',
                    'padding': '10px',
                    'fontFamily': 'Arial, sans-serif',
                    'fontSize': '13px',
                    'border': '1px solid #ddd',
                    'whiteSpace': 'normal',
                    'height': 'auto',
                    'minWidth': '120px',
                    'maxWidth': '200px'
                },
                style_header={
                    'backgroundColor': '#e8f4fd',
                    'fontWeight': 'bold',
                    'textAlign': 'center',
                    'border': '1px solid #ddd',
                    'position': 'sticky',
                    'top': '0',
                    'zIndex': '1',
                    'color': '#2c3e50'
                },
                style_data={
                    'backgroundColor': 'white',
                    'border': '1px solid #ddd',
                    'color': '#2c3e50'
                },
                style_data_conditional=color_styles,
                sort_action="native",
                filter_action="native",
                page_action="none",
                export_format="xlsx",
                export_headers="display",
                fixed_rows={'headers': True},
                css=[{
                    'selector': '.dash-table-container',
                    'rule': 'border-radius: 8px; box-shadow: 0 2px 8px rgba(0,0,0,0.1);'
                }]
            )
            
            
            # Determinar título según el modo
            title_map = {
                'absolutos': 'Valores Absolutos',
                'zscore': 'Valores Absolutos con Z-Scores',
                'diferencia': 'Valores Absolutos con Diferencias Porcentuales'
            }
            table_title = title_map.get(table_mode, 'Tabla Comparativa')
            
            num_players = len(data_for_table)
            info_text = f"Jugadores: {num_players} | Estadística: {selected_statistic}"
            
            return html.Div([
                html.H5(table_title, className="section-subtitle", style={'margin-bottom': '10px'}),
                html.P(info_text, className="table-info", style={'margin-bottom': '15px', 'color': '#666'}),
                html.Div([
                    comparative_table
                ], style={
                    'border-radius': '8px',
                    'box-shadow': '0 2px 8px rgba(0,0,0,0.1)',
                    'margin-top': '10px'
                })
            ], className="comparative-table-container", style={
                'padding': '20px',
                'background-color': '#fafafa',
                'border-radius': '10px',
                'margin': '10px 0'
            })
            
        except Exception as e:
            return html.Div(f"Error al cargar tabla comparativa: {str(e)}", 
                          className="error-message")
    
    
    # ============================================================================
    # CALLBACKS - Tarjetas
    # ============================================================================
    
    # Callback para popular as opções do dropdown tarjetas-columns-selector
    @app.callback(
        Output('tarjetas-columns-selector', 'options'),
        [Input('date-selector', 'date'),
         Input('statistic-selector', 'value')]
    )
    def update_tarjetas_columns_options(selected_date, selected_statistic):
        """
        Actualiza las opciones del dropdown tarjetas-columns-selector con todas las columnas disponibles.
        
        Esta función callback obtiene todas las columnas de métricas disponibles para la fecha
        seleccionada y las presenta como opciones en el dropdown para que el usuario pueda
        seleccionar qué métricas mostrar en las tarjetas.
        
        Utiliza las funciones del módulo utils/sessionReport_utils.py para:
        - get_table_data(): Obtener datos de jugadores con todas las columnas disponibles
        
        Y del módulo utils/date.py para:
        - format_date(): Convertir formato de fecha
        
        Args:
            selected_date (str): Fecha seleccionada en formato YYYY-MM-DD
            selected_statistic (str): Estadística seleccionada para obtener los datos correctos
            
        Returns:
            list: Lista de opciones para el dropdown con todas las columnas de métricas disponibles,
                 ordenadas alfabéticamente. Cada opción tiene formato {'label': nombre_columna, 'value': nombre_columna}
        """
        if not selected_date:
            return []
        
        try:
            # Convertir fecha usando función de utils/date.py
            formatted_date_internal = format_date(selected_date)
            
            # Obtener datos de jugadores usando función de utils/sessionReport_utils.py
            df_players = get_table_data(
                selected_date=formatted_date_internal,
                selected_statistic=selected_statistic or 'mean'  # Usar 'mean' como valor por defecto
            )
            
            if df_players is None or df_players.height == 0:
                # Fallback para columnas básicas si no hay datos
                basic_columns = [
                    'Distance (m)',
                    'Speed Zones (m) [0.0, 6.0]km/h (m)',
                    'Abs HSR(m)',
                    'Rel HSR(m)',
                ]
                return [{'label': col, 'value': col} for col in sorted(basic_columns)]
            
            # Excluir columnas de identificación y metadatos
            exclude_cols = ['Player', 'Position', 'Team', 'Match Day', 'Estadistica', 'Date']
            all_columns = [col for col in df_players.columns if col not in exclude_cols]
            
            # Crear opciones para el dropdown ordenadas alfabéticamente
            options = [{'label': col, 'value': col} for col in sorted(all_columns)]
            
            return options
            
        except Exception as e:
            print(f"Error al actualizar opciones del dropdown de columnas: {e}")
            # Fallback para columnas básicas en caso de error
            basic_columns = [
                'Distance (m)',
                'Speed Zones (m) [0.0, 6.0]km/h (m)',
                'Abs HSR(m)',
                'Rel HSR(m)',
            ]
            return [{'label': col, 'value': col} for col in sorted(basic_columns)]

    # Callback para popular as opções do dropdown cards-view-selector
    @app.callback(
        Output('cards-view-selector', 'options'),
        [Input('date-selector', 'date'),
         Input('statistic-selector', 'value')]
    )
    def update_cards_view_options(selected_date, selected_statistic):
        """
        Actualiza las opciones del dropdown cards-view-selector con base en la fecha y estadística seleccionadas.
        
        Esta función callback genera opciones de visualización que incluyen vista de equipo
        y vistas por posición específica, basándose en las posiciones disponibles en los
        datos para la fecha y estadística seleccionadas.
        
        Utiliza las funciones del módulo utils/sessionReport_utils.py para:
        - get_table_data(): Obtener datos de jugadores para identificar posiciones disponibles
        
        Y del módulo utils/date.py para:
        - format_date(): Convertir formato de fecha
        
        Args:
            selected_date (str): Fecha seleccionada en formato YYYY-MM-DD
            selected_statistic (str): Estadística seleccionada para el análisis
        
        Returns:
            list: Lista de opciones para el dropdown que incluye:
                 - Opción 'Equipo' para vista general del equipo
                 - Opciones 'Posición: X' para cada posición disponible en los datos
                 Formato: [{'label': texto_mostrar, 'value': valor_interno}, ...]
        """
        if not selected_date or not selected_statistic:
            return [{'label': 'Equipo', 'value': 'Equipo'}]
        
        try:
            # Convertir fecha usando función de utils/date.py
            formatted_date_internal = format_date(selected_date)
            
            # Obtener datos de jugadores usando función de utils/sessionReport_utils.py
            df_players = get_table_data(
                selected_date=formatted_date_internal,
                selected_statistic=selected_statistic  # Usar la estadística seleccionada
            )
            
            if df_players is None or df_players.height == 0:
                return [{'label': 'Equipo', 'value': 'Equipo'}]
            
            # Crear opciones básicas
            options = [{'label': 'Equipo', 'value': 'Equipo'}]
            
            # Agregar posiciones únicas disponibles si la columna existe
            if 'Player' in df_players.columns:
                positions = df_players['Player'].unique().to_list()
                for position in sorted(positions):
                    if position and str(position).startswith('POS_'):
                        position_name = position.replace('POS_', '')
                        options.append({
                            'label': f'Posición: {position_name}',
                            'value': f'Position_{position_name}'
                        })
            
            return options
            
        except Exception as e:
            print(f"Error al actualizar opciones del dropdown: {e}")
            return [{'label': 'Equipo', 'value': 'Equipo'}]

    # Callback para actualizar las tarjetas de métricas absolutas
    @app.callback(
        Output('team-tarjetas-output', 'children'),
        [Input('date-selector', 'date'),
         Input('statistic-selector', 'value'),
         Input('cards-view-selector', 'value'),
         Input('tarjetas-columns-selector', 'value')]
    )
    def update_team_cards(selected_date, selected_statistic, selected_view, selected_columns):
        """
        Crea tarjetas mostrando los valores absolutos de métricas para equipos y posiciones.
        
        Esta función callback genera tarjetas visuales que muestran los valores absolutos
        de las métricas para el equipo completo o por posición específica, calculando
        estadísticas agregadas según la vista seleccionada.
        
        Utiliza las funciones del módulo utils/sessionReport_utils.py para:
        - get_table_data(): Obtener datos de jugadores para la fecha seleccionada
        
        Y del módulo utils/date.py para:
        - format_date(): Convertir formato de fecha
        
        Y del módulo utils/metrics.py para:
        - calcular_metricas(): Calcular estadísticas agregadas (mean, sum, etc.)
        
        Args:
            selected_date (str): Fecha seleccionada en formato YYYY-MM-DD
            selected_statistic (str): Estadística seleccionada para el análisis ('mean', 'sum', etc.)
            selected_view (str): Vista seleccionada ('Equipo' o 'Position_X')
            selected_columns (list): Lista de columnas de métricas a mostrar en las tarjetas
        
        Returns:
            html.Div: Componente HTML con tarjetas de métricas que incluyen:
                     - Título descriptivo de la vista
                     - Tarjetas individuales para cada métrica seleccionada
                     - Valores absolutos de las métricas formateados apropiadamente
        """
        
        if not selected_date or not selected_statistic:
            return html.Div()
        
        try:
            # Convertir fecha usando función de utils/date.py
            formatted_date_internal = format_date(selected_date)
            
            # Obtener datos completos (jugadores + equipo + posiciones) usando función de utils/sessionReport_utils.py
            df_complete = get_table_data(
                selected_date=formatted_date_internal,
                selected_statistic=selected_statistic,
            )
            
            if df_complete is None or df_complete.height == 0:
                return html.Div("No se encontraron datos para la fecha seleccionada.", 
                              className="info-message")
            
            # Determinar qué datos usar basado en selected_view
            if selected_view == 'Equipo':
                # Filtrar solo la fila del equipo (Player = "TEAM")
                df_to_use = df_complete.filter(pl.col('Player') == "TEAM")
                title_prefix = f'Métricas del Equipo - {selected_statistic}'
                
            elif selected_view and selected_view.startswith('Position_'):
                # Extraer la posición del valor selected_view
                position = selected_view.replace('Position_', '')
                
                # Filtrar solo la fila de la posición específica (Player = "POS_posición")
                df_to_use = df_complete.filter(pl.col('Player') == f"POS_{position}")
                
                if df_to_use.height == 0:
                    return html.Div(f"No se encontraron jugadores para la posición {position}.", 
                                  className="info-message")
                
                title_prefix = f'Métricas de la Posición {position} - {selected_statistic}'
                
            else:
                # Fallback a equipo - filtrar solo la fila del equipo
                df_to_use = df_complete.filter(pl.col('Player') == "TEAM")
                title_prefix = f'Métricas del Equipo - {selected_statistic}'
            
            if df_to_use.height == 0:
                return html.Div("No hay datos disponibles para esta selección.", 
                              className="info-message")
            
            # Convertir a pandas para facilitar el manejo
            df_pandas = df_to_use.to_pandas()
            
            # Obtener columnas de métricas (excluyendo columnas de identificación)
            exclude_cols = ['Player', 'Position', 'Team', 'Match Day', 'Estadistica', 'Date']
            all_metric_columns = [col for col in df_pandas.columns if col not in exclude_cols]
            
            # Filtrar por las columnas seleccionadas por el usuario
            if selected_columns:
                # Si hay columnas seleccionadas, usar solo esas
                metric_columns = [col for col in selected_columns if col in all_metric_columns]
            else:
                # Si no hay columnas seleccionadas, mostrar todas
                metric_columns = all_metric_columns
            
            if not metric_columns:
                return html.Div("Selecciona al menos una métrica para mostrar.", 
                              className="info-message")
            
            # Crear tarjetas para cada métrica
            cards = []
            
            # Título de la sección
            cards.append(
                html.H5(title_prefix, 
                        className="section-subtitle", 
                        style={'margin-top': '10px', 'margin-bottom': '20px'})
            )
            
            # Contenedor para las tarjetas
            cards_container = []
            
            for col in metric_columns:
                # Obtener el valor absoluto de la métrica
                if len(df_pandas) > 0:
                    metric_value = df_pandas[col].iloc[0]
                    
                    # Formatear valor de la métrica
                    if pd.isna(metric_value):
                        formatted_value = 'N/A'
                    else:
                        try:
                            metric_value_num = float(metric_value)
                            # Formatear según el tipo de métrica
                            if '%' in col:
                                formatted_value = f'{metric_value_num:.2f}%'
                            elif '(m)' in col:
                                formatted_value = f'{metric_value_num:.0f} m'
                            elif 'km/h' in col:
                                formatted_value = f'{metric_value_num:.2f} km/h'
                            elif 'cnt' in col.lower() or 'count' in col.lower():
                                formatted_value = f'{metric_value_num:.0f}'
                            else:
                                formatted_value = f'{metric_value_num:.2f}'
                        except (ValueError, TypeError):
                            formatted_value = str(metric_value)
                     
                    # Crear tarjeta usando estilos CSS definidos
                    card_content = [
                         # Título de la métrica usando clase card-title
                         html.H6(col, className="card-title"),
                         
                         # Container de valores usando clase card-values-container
                         html.Div([
                             html.Span("Valor absoluto", className="session-label"),
                             html.Span(formatted_value, className="stat-value")
                         ], className="card-values-container")
                     ]
                     
                    card = html.Div(card_content, className="metric-card")
                    
                    cards_container.append(card)
            
            # Contenedor usando estilos CSS definidos
            if cards_container:
                 cards.append(
                     html.Div(cards_container, className="metrics-cards-container")
                 )
            
            return html.Div(cards)
            
        except Exception as e:
            return html.Div(f"Error al cargar tarjetas de métricas: {str(e)}", 
                          className="error-message")
        

# ============================================================================
# GRÁFICOS: Nuevos Gráficos
# ============================================================================

        # ...existing code...
    
   # ...existing code...

    @app.callback(
    [Output('grafico-nuevo1', 'figure'),
     Output('grafico-nuevo2', 'figure'),
     Output('grafico-nuevo3', 'figure'),
     Output('grafico-nuevo5', 'figure')],
    [Input('date-selector', 'date'),
     Input('statistic-selector', 'value')]
)
    def update_nuevos_graficos(selected_date, selected_statistic):
        empty_fig = {
            'data': [],
            'layout': {
                'title': 'Sin datos disponibles',
                'plot_bgcolor': '#f8f9fa',
                'paper_bgcolor': '#f8f9fa',
                'font': {'color': '#000000'},
                'margin': dict(t=40, b=40, l=40, r=40)
            }
        }
        try:
            if not selected_date or not selected_statistic:
                return [empty_fig, empty_fig, empty_fig, empty_fig]

            df_fecha, formatted_date = format_and_filter_date_graphs(selected_date)
            if df_fecha is None or df_fecha.height == 0:
                return [empty_fig, empty_fig, empty_fig, empty_fig]

            df_pandas = df_fecha.to_pandas()
            df_players = df_pandas[df_pandas['Player'] != 'TEAM'].copy()
            if len(df_players) == 0:
                return [empty_fig, empty_fig, empty_fig, empty_fig]


            # ================= FIGURA 1: Distancia Total + Metros/minuto =================
            zonas = [
                "Speed Zones (m) [0.0, 6.0]km/h (m)",
                "Speed Zones (m) [6.0, 12.0]km/h (m)",
                "Speed Zones (m) [12.0, 18.0]km/h (m)",
                "Speed Zones (m) [18.0, 21.0]km/h (m)",
                "Speed Zones (m) [21.0, 24.0]km/h (m)",
                "Speed Zones (m) [24.0, 27.0]km/h (m)",
                "Speed Zones (m) [27.0, 30.0]km/h (m)",
                "Speed Zones (m) [30.0, 50.0]km/h (m)"
            ]
            df_players['Distance'] = df_players[zonas].sum(axis=1)

            # Convertir string de duración a timedelta y a minutos
            df_players['Drills Duration'] = pd.to_timedelta(df_players['Drills Duration'], errors='coerce')
            df_players['Drills Duration (min)'] = df_players['Drills Duration'].dt.total_seconds() / 60

            # Metros por minuto
            df_players['Metros/min'] = df_players['Distance'] / df_players['Drills Duration (min)']

            df_agrupado1 = df_players.groupby('Player').agg({
                'Distance': 'sum',
                'Drills Duration (min)': 'sum'
            }).reset_index()
            df_agrupado1['Metros/min'] = df_agrupado1['Distance'] / df_agrupado1['Drills Duration (min)']

            # --- referencia 94min: usar la estadística seleccionada ---
            df_94_stats = calculate_player_statistics_94min()
            try:
                df_94_stats = df_94_stats.to_pandas()
            except AttributeError:
                pass

            # filtrar por la estadística elegida
            if 'estadistica' in df_94_stats.columns:
                df_94_stats = df_94_stats[df_94_stats['estadistica'] == selected_statistic]

            # quedarnos con Player + Distance (m)_94min
            if 'Distance (m)_94min' in df_94_stats.columns:
                df_94_stats = df_94_stats[['Player', 'Distance (m)_94min']]
            else:
                df_94_stats = pd.DataFrame(columns=['Player', 'Distance (m)_94min'])

            # jugadores de referencia
            # --- calcular % de cumplimiento respecto a 94min (Distance) ---
            df_dist_pct = pd.merge(
                df_players.groupby("Player", as_index=False)["Distance"].sum(),
                df_94_stats,
                on="Player",
                how="left"
            )

            # asegurar tipos numéricos
            df_dist_pct["Distance"] = pd.to_numeric(df_dist_pct["Distance"], errors="coerce")
            df_dist_pct["Distance (m)_94min"] = pd.to_numeric(df_dist_pct["Distance (m)_94min"], errors="coerce")

            # calcular porcentaje
            df_dist_pct["Distance_pct"] = np.where(
                df_dist_pct["Distance (m)_94min"] > 0,
                df_dist_pct["Distance"] / df_dist_pct["Distance (m)_94min"] * 100,
                np.nan
            )

            # ordenar por porcentaje (descendente = mayor a menor)
            df_dist_pct = df_dist_pct.sort_values("Distance_pct", ascending=False).reset_index(drop=True)

            # eje X ordenado
            players_x = df_dist_pct["Player"].tolist()

            # valores alineados al nuevo orden
            distance_y = df_dist_pct["Distance_pct"].fillna(0).values
            text_line = df_dist_pct["Distance_pct"].round(0).astype(str) + "%"

            # Metros/min para esos jugadores en el mismo orden
            df_metros_min = (
                df_players.groupby('Player')['Metros/min']
                .mean()
                .reindex(players_x)
                .fillna(0)
                .values
            )

            fig1 = go.Figure()
            fig1.add_trace(go.Scatter(
                x=players_x,
                y=df_metros_min,
                mode='markers+text',
                name='Metros/min',
                marker=dict(size=30, color='#80bfff', symbol='circle'),
                text=np.round(df_metros_min),
                textposition='middle center',
                textfont=dict(color='white', size=12),
                hovertemplate='<b>%{x}</b><br>Metros/min: %{y:.1f} m/min<extra></extra>'
            ))

            # Crear texto como string entero
            text_line = df_dist_pct["Distance_pct"].fillna(0).astype(int).astype(str) + "%"

            # --- Línea roja de % Cumplimiento (eje Y principal) ---
            fig1.add_trace(go.Scatter(
                x=players_x,
                y=distance_y,
                mode='lines+markers+text',
                name=f"Cumplimiento vs 94min ({selected_statistic})",
                text=text_line,
                textposition='top center',
                textfont=dict(color='red', size=11),
                marker=dict(color='red', size=8),
                line=dict(color='red'),
                hovertemplate='<b>%{x}</b><br>Cumplimiento: %{text}<extra></extra>'  # 👈 usar %{text} para entero
            ))

            # --- Barras de Distance (van al eje Y2, detrás) ---
            fig1.add_trace(go.Bar(
                x=df_agrupado1['Player'],
                y=df_agrupado1['Distance'],
                name="Distance total",
                yaxis='y2',
                marker_color='lightgray',
                opacity=0.5,
                text=df_agrupado1['Distance'].round(0).astype(int),  # 👈 valores enteros
                textposition="outside",  # 👈 texto arriba de la barra
                textfont=dict(color="black", size=11),
                hovertemplate='<b>%{x}</b><br>Distance: %{y:.0f} m<extra></extra>'
            ))

            # --- Layout ---
            fig1.update_layout(
                title='<b>Distancia total, Cumplimiento 94min y Metros/min por Jugador</b>',
                xaxis_title='Jugador',
                yaxis=dict(
                    title='Metros/min y % Cumplimiento',
                    range=[0, max(max(df_metros_min), max(distance_y)) * 1.2],
                    layer="above traces"   # 👈 Esto pone las líneas/puntos por encima de las barras
                ),
                yaxis2=dict(
                    title='Distance total (m)',
                    overlaying='y',
                    side='right'
                ),
                plot_bgcolor='white',
                paper_bgcolor='white',
                font=dict(size=12),
                height=600,
                margin=dict(t=80, b=50, l=50, r=50),
                barmode='overlay'
            )


            # ================= FIGURA 5: HSR =================
            # --- Calcular HSR/min por jugador (día seleccionado) ---
            df_players['HSR/min'] = df_players['Abs HSR(m)'] / df_players['Drills Duration (min)']

            df_hsr = df_players.groupby('Player').agg({
                'Abs HSR(m)': 'sum',
                'Drills Duration (min)': 'sum'
            }).reset_index()
            df_hsr['HSR/min'] = df_hsr['Abs HSR(m)'] / df_hsr['Drills Duration (min)']

            # --- Valores 94min para HSR según estadística seleccionada ---
            df_hsr_stats = calculate_player_statistics_94min()
            try:
                df_hsr_stats = df_hsr_stats.to_pandas()
            except AttributeError:
                pass

            if 'estadistica' in df_hsr_stats.columns:
                df_hsr_stats = df_hsr_stats[df_hsr_stats['estadistica'] == selected_statistic]

            if 'Abs HSR(m)_94min' in df_hsr_stats.columns:
                df_hsr_stats = df_hsr_stats[['Player', 'Abs HSR(m)_94min']]
            else:
                df_hsr_stats = pd.DataFrame(columns=['Player', 'Abs HSR(m)_94min'])

            # --- Calcular % cumplimiento ---
            df_hsr_pct = pd.merge(
                df_hsr[['Player', 'Abs HSR(m)']],  # valores del día
                df_hsr_stats,                      # referencia
                on='Player',
                how='left'
            )

            # asegurar tipos numéricos
            df_hsr_pct['Abs HSR(m)'] = pd.to_numeric(df_hsr_pct['Abs HSR(m)'], errors='coerce')
            df_hsr_pct['Abs HSR(m)_94min'] = pd.to_numeric(df_hsr_pct['Abs HSR(m)_94min'], errors='coerce')

            df_hsr_pct['HSR_pct'] = np.where(
                df_hsr_pct['Abs HSR(m)_94min'] > 0,
                df_hsr_pct['Abs HSR(m)'] / df_hsr_pct['Abs HSR(m)_94min'] * 100,
                np.nan
            )

            # --- Ordenar por % (descendente) ---
            df_hsr_pct = df_hsr_pct.sort_values("HSR_pct", ascending=False).reset_index(drop=True)

            players_x = df_hsr_pct["Player"].tolist()
            y_line_hsr = df_hsr_pct["HSR_pct"].fillna(0).values
            text_line_hsr = df_hsr_pct["HSR_pct"].round(0).astype(str) + "%"

            # HSR/min en el mismo orden
            df_hsr_sorted = df_hsr.set_index("Player").reindex(players_x).reset_index()
            hsr_min_values = df_hsr_sorted["HSR/min"].fillna(0).values

            # --- Graficar ---
            fig_hsr = go.Figure()

            # --- Barras = Abs HSR (m), eje derecho (y2) ---
            fig_hsr.add_trace(go.Bar(
                x=players_x,
                y=df_hsr_sorted["Abs HSR(m)"],
                name="Abs HSR (m)",
                yaxis="y2",
                marker_color="lightgray",
                opacity=0.5,
                hovertemplate='<b>%{x}</b><br>Abs HSR: %{y:.0f} m<extra></extra>'
            ))

            # --- Puntos azules = HSR/min, eje izquierdo (y) ---
            fig_hsr.add_trace(go.Scatter(
                x=players_x,
                y=hsr_min_values,
                mode='markers+text',
                name='HSR/min',
                marker=dict(size=30, color='#80bfff', symbol='circle'),
                text=np.round(hsr_min_values, 1),
                textposition='middle center',
                textfont=dict(color='white', size=12),
                hovertemplate='<b>%{x}</b><br>HSR/min: %{y:.1f} m/min<extra></extra>'
            ))

            # Crear texto como string entero
            text_line_hsr = df_hsr_pct["HSR_pct"].fillna(0).astype(int).astype(str) + "%"

            # --- Línea roja = % cumplimiento ---
            fig_hsr.add_trace(go.Scatter(
                x=players_x,
                y=y_line_hsr,
                mode='lines+markers+text',
                name=f"HSR vs 94min ({selected_statistic})",
                text=text_line_hsr,
                textposition='top center',
                textfont=dict(color='red', size=11),
                marker=dict(color='red', size=8),
                line=dict(color='red'),
                hovertemplate='<b>%{x}</b><br>Cumplimiento: %{text}<extra></extra>'  # 👈 usar %{text} en lugar de %{y}
            ))

            # --- Layout ---
            fig_hsr.update_layout(
                title='<b>HSR Absoluto, Cumplimiento 94min y HSR/min por Jugador</b>',
                xaxis_title='Jugador',
                yaxis=dict(
                    title='HSR/min y % Cumplimiento'
                ),
                yaxis2=dict(
                    title='Abs HSR (m)',
                    overlaying='y',
                    side='right'
                ),
                plot_bgcolor='white',
                paper_bgcolor='white',
                font=dict(size=12),
                height=600,
                margin=dict(t=80, b=50, l=50, r=50),
                barmode='overlay'
            )

            # ================= FIGURA 2: Velocidad =================
            # --- Calcular Nº de Sprints por jugador ---
            zonas_sprints = [
                "Acceleration Zones  [0, 50]% Cnt",
                "Acceleration Zones  [50, 60]% Cnt",
                "Acceleration Zones  [60, 70]% Cnt",
                "Acceleration Zones  [70, 80]% Cnt",
                "Acceleration Zones  [80, 90]% Cnt",
                "Acceleration Zones  [90, 100]% Cnt"
            ]
            df_players['Nº de Sprints'] = df_players[zonas_sprints].sum(axis=1)

            df_agrupado2 = df_players.groupby('Player', as_index=False).agg({
                'Nº de Sprints': 'sum',
                'MAX Speed(km/h)': 'max'
            })
            df_agrupado2['MAX Speed(km/h)'] = pd.to_numeric(df_agrupado2['MAX Speed(km/h)'], errors='coerce')

            # --- Obtener referencia 94min según estadística seleccionada ---
            df_speed_stats = calculate_player_statistics_94min()
            try:
                df_speed_stats = df_speed_stats.to_pandas()
            except AttributeError:
                pass

            if 'estadistica' in df_speed_stats.columns:
                df_speed_stats = df_speed_stats[df_speed_stats['estadistica'] == selected_statistic]

            if 'MAX Speed(km/h)_94min' in df_speed_stats.columns:
                df_speed_stats = df_speed_stats[['Player', 'MAX Speed(km/h)_94min']].copy()
                df_speed_stats['MAX Speed(km/h)_94min'] = pd.to_numeric(df_speed_stats['MAX Speed(km/h)_94min'], errors='coerce')
            else:
                df_speed_stats = pd.DataFrame(columns=['Player', 'MAX Speed(km/h)_94min'])

            # --- Merge y calcular % ---
            df_plot = pd.merge(
                df_agrupado2[['Player', 'MAX Speed(km/h)']],
                df_speed_stats,
                on='Player',
                how='left'
            )

            df_plot['Speed_pct'] = np.where(
                df_plot['MAX Speed(km/h)_94min'].notna() & (df_plot['MAX Speed(km/h)_94min'] > 0),
                df_plot['MAX Speed(km/h)'] / df_plot['MAX Speed(km/h)_94min'] * 100,
                np.nan
            )

            # --- Ordenar por % ---
            df_plot = df_plot.sort_values('Speed_pct', ascending=False, na_position='last').reset_index(drop=True)

            players_x = df_plot['Player'].tolist()
            speed_day = df_plot['MAX Speed(km/h)'].fillna(0).values
            speed_pct = df_plot['Speed_pct'].values
            speed_pct_text = [f"{int(round(v))}%" if not pd.isna(v) else "" for v in speed_pct]

            sprints_values = df_agrupado2.set_index("Player").reindex(players_x)["Nº de Sprints"].fillna(0).values

            # --- Graficar ---
            fig2 = go.Figure()

            # --- Barras grises = Nº de Sprints (eje derecho, y2) ---
            fig2.add_trace(go.Bar(
                x=players_x,
                y=sprints_values,
                name="Nº de Sprints",
                yaxis="y2",
                marker_color="lightgray",
                opacity=0.6,
                text=sprints_values,                  # <- valores como texto
                textposition='outside',               # <- arriba de la barra
                textfont=dict(color='black', size=11), # <- estilo del texto
                hovertemplate='<b>%{x}</b><br>Nº de Sprints: %{y}<extra></extra>'
            ))

            # --- Línea azul = Vel Máx del día (km/h), eje izquierdo ---
            fig2.add_trace(go.Scatter(
                x=players_x,
                y=speed_day,
                name='Vel Máx (día)',
                mode='lines+markers+text',  # línea + marcador + texto
                line=dict(color='darkblue', width=2),
                marker=dict(color='darkblue', size=6),
                text=np.round(speed_day, 1),
                textposition='bottom center',
                textfont=dict(color='darkblue', size=11),
                hovertemplate='<b>%{x}</b><br>Vel Máx día: %{y:.1f} km/h<extra></extra>',
                yaxis='y1'
            ))

            # --- Línea roja = % cumplimiento, eje izquierdo ---
            fig2.add_trace(go.Scatter(
                x=players_x,
                y=speed_pct,
                mode='lines+markers+text',
                name=f"Cumplimiento vs 94min ({selected_statistic})",
                text=speed_pct_text,
                textposition='top center',
                textfont=dict(color='red', size=11),
                marker=dict(color='red', size=8),
                line=dict(color='red'),
                hovertemplate='<b>%{x}</b><br>Cumplimiento: %{y:.0f}%<extra></extra>',  
                yaxis='y1'
            ))

            # --- Layout ---
            fig2.update_layout(
                title='<b>Velocidad Máxima y Nº de Sprints: Día vs 94min</b>',
                xaxis=dict(title='Jugador', categoryorder='array', categoryarray=players_x),
                yaxis=dict(title='Vel Máx Día (km/h) y % Cumplimiento', side='left'),
                yaxis2=dict(title='Nº de Sprints', overlaying='y', side='right'),
                plot_bgcolor='white',
                paper_bgcolor='white',
                font=dict(family='Arial', size=12),
                height=500,
                margin=dict(t=70, b=50, l=60, r=60),
                barmode="overlay"
            )


            # ================= FIGURA 3: ACC y DCC =================
            # --- Definir zonas ---
            dcc_zonas = [
                "Acceleration Zones  [-50, 0]% Cnt",
                "Acceleration Zones  [-60, -50]% Cnt",
                "Acceleration Zones  [-70, -60]% Cnt",
                "Acceleration Zones  [-80, -70]% Cnt",
                "Acceleration Zones  [-90, -80]% Cnt",
                "Acceleration Zones  [-100, -90]% Cnt"
            ]

            df_players['ACC Count'] = df_players[zonas_sprints].sum(axis=1)
            df_players['DCC Count'] = df_players[dcc_zonas].sum(axis=1)

            df_accdcc = df_players.groupby('Player', as_index=False).agg({
                'ACC Count': 'sum',
                'DCC Count': 'sum'
            })

            # --- Obtener referencias 94min ---
            df_stats = calculate_player_statistics_94min()
            try:
                df_stats = df_stats.to_pandas()
            except AttributeError:
                pass

            if 'estadistica' in df_stats.columns:
                df_stats = df_stats[df_stats['estadistica'] == selected_statistic]

            cols_needed = ['Player', 'Accelerations_94min', 'Decelerations_94min']
            df_stats = df_stats[[c for c in cols_needed if c in df_stats.columns]].copy()

            # --- Merge y calcular % ---
            df_merge = pd.merge(df_accdcc, df_stats, on='Player', how='left')

            for col in ['ACC Count', 'DCC Count', 'Accelerations_94min', 'Decelerations_94min']:
                df_merge[col] = pd.to_numeric(df_merge[col], errors='coerce')

            df_merge['ACC_pct'] = np.where(
                df_merge['Accelerations_94min'] > 0,
                df_merge['ACC Count'] / df_merge['Accelerations_94min'] * 100,
                np.nan
            )
            df_merge['DCC_pct'] = np.where(
                df_merge['Decelerations_94min'] > 0,
                df_merge['DCC Count'] / df_merge['Decelerations_94min'] * 100,
                np.nan
            )

            # --- Ordenar por promedio de % ---
            df_merge['AVG_pct'] = df_merge[['ACC_pct', 'DCC_pct']].mean(axis=1, skipna=True)
            df_merge = df_merge.sort_values('AVG_pct', ascending=False, na_position='last').reset_index(drop=True)

            players_order = df_merge['Player'].tolist()

            # --- Graficar líneas + barras ---
            fig3 = go.Figure()

            # Barras verdes = ACC Count
            fig3.add_trace(go.Bar(
                x=players_order,
                y=df_merge['ACC Count'],
                name="ACC Count",
                yaxis="y2",
                marker_color="lightgreen",
                opacity=0.7,
                text=df_merge['ACC Count'],
                textposition="outside",
                textfont=dict(color="darkgreen", size=11),
                hovertemplate="<b>%{x}</b><br>ACC Count: %{y}<extra></extra>"
            ))

            # Barras rojas = DCC Count
            fig3.add_trace(go.Bar(
                x=players_order,
                y=df_merge['DCC Count'],
                name="DCC Count",
                yaxis="y2",
                marker_color="lightcoral",
                opacity=0.7,
                text=df_merge['DCC Count'],
                textposition="outside",
                textfont=dict(color="darkred", size=11),
                hovertemplate="<b>%{x}</b><br>DCC Count: %{y}<extra></extra>"
            ))

            # Línea verde = ACC (%)
            fig3.add_trace(go.Scatter(
                x=players_order,
                y=df_merge['ACC_pct'],
                mode='lines+markers+text',
                name=f"ACC vs 94min ({selected_statistic})",
                line=dict(color='green', width=2),
                marker=dict(color='green', size=6),
                text=df_merge['ACC_pct'].fillna(0).astype(int).astype(str) + "%",
                textposition='top center',
                textfont=dict(color='green', size=11),
                hovertemplate='<b>%{x}</b><br>Cumplimiento ACC: %{y:.1f}%<extra></extra>',
                yaxis='y1'
            ))

            # Línea roja = DCC (%)
            fig3.add_trace(go.Scatter(
                x=players_order,
                y=df_merge['DCC_pct'],
                mode='lines+markers+text',
                name=f"DCC vs 94min ({selected_statistic})",
                line=dict(color='red', width=2),
                marker=dict(color='red', size=6),
                text=df_merge['DCC_pct'].fillna(0).astype(int).astype(str) + "%",
                textposition='top center',
                textfont=dict(color='red', size=11),
                hovertemplate='<b>%{x}</b><br>Cumplimiento DCC: %{y:.0f}%<extra></extra>',
                yaxis='y1'
            ))

            # Layout
            fig3.update_layout(
                title='<b>Aceleraciones y Deceleraciones: Día vs 94min</b>',
                xaxis=dict(
                    title='Jugador',
                    categoryorder='array',
                    categoryarray=players_order
                ),
                yaxis=dict(
                    title='Cumplimiento (%)',
                    side='left',
                    showgrid=True,
                    range=[0, 120]
                ),
                yaxis2=dict(
                    title='Recuento ACC/DCC',
                    overlaying='y',
                    side='right',
                    showgrid=False
                ),
                barmode='group',  # para que las barras ACC y DCC queden una al lado de la otra
                plot_bgcolor='white',
                paper_bgcolor='white',
                height=500,
                font=dict(family='Arial', size=12, color='#2c3e50'),
                margin=dict(t=60, b=40, l=80, r=80)
            )

            return [fig1, fig2, fig3, fig_hsr]

        except Exception as e:
            print(f"Error en nuevos gráficos: {e}")
            return [empty_fig, empty_fig, empty_fig, empty_fig]

# ============================================================================
# GRÁFICOS: Scatter Plot
# ============================================================================
# ...existing code...

# Callback para poblar los dropdowns de columnas numéricas
    @app.callback(
    [Output('dropdown-x-axis', 'options'),
     Output('dropdown-y-axis', 'options')],
    [Input('date-selector', 'date')]
)
    
    
    def update_scatter_dropdowns(selected_date):
        if not selected_date:
            return [[], []]

        df_fecha, formatted_date = format_and_filter_date_graphs(selected_date)
        if df_fecha is None or df_fecha.height == 0:
            return [[], []]

        df = df_fecha.to_pandas()
        df = df[df['Player'] != 'TEAM'].copy()
        print(df.columns)


        # Traemos las métricas 94min y las unimos
        df_94_stats = calculate_player_statistics_94min()
        try:
            df_94_stats = df_94_stats.to_pandas()
        except AttributeError:
            pass

        print(df_94_stats.columns)

        COLUMNS_ALLOWED = [
            "Distance (m)",
            "Max Speed (km/h)",
            "ACC Count",
            "DCC Count"

]
        # Filtrar por la estadística seleccionada (ej: mean)
        df_94_stats = df_94_stats[df_94_stats['estadistica'] == "mean"]
        df_94_stats = df_94_stats.drop(columns=["estadistica"], errors="ignore")

        # Merge por Player
        df_merge = pd.merge(df, df_94_stats, on="Player", how="left")

        # Columnas numéricas que estén en la lista permitida o terminen en _94min
        numeric_cols = [
            col for col in df_merge.columns
            if (col in COLUMNS_ALLOWED) or col.endswith("_94min")
        ]

        options = [{'label': col, 'value': col} for col in numeric_cols]
        return options, options

    # Callback para el scatter plot personalizado
    @app.callback(
    Output('grafico-nuevo4', 'figure'),
    [Input('dropdown-x-axis', 'value'),
     Input('dropdown-y-axis', 'value'),
     Input('date-selector', 'date')]
)
    def actualizar_scatter(col_x, col_y, selected_date):
        empty_fig = go.Figure()
        if not col_x or not col_y or not selected_date:
            return empty_fig

        df_fecha, formatted_date = format_and_filter_date_graphs(selected_date)
        if df_fecha is None or df_fecha.height == 0:
            return empty_fig

        df = df_fecha.to_pandas()
        df = df[df['Player'] != 'TEAM'].copy()

        # Métricas 94min
        df_94_stats = calculate_player_statistics_94min()
        try:
            df_94_stats = df_94_stats.to_pandas()
        except AttributeError:
            pass
        df_94_stats = df_94_stats[df_94_stats['estadistica'] == "mean"]
        df_94_stats = df_94_stats.drop(columns=["estadistica"], errors="ignore")

        df_merge = pd.merge(df, df_94_stats, on="Player", how="left")

        if col_x not in df_merge.columns or col_y not in df_merge.columns:
            return empty_fig

        # --- Medias ---
        mean_x = df_merge[col_x].mean()
        mean_y = df_merge[col_y].mean()

        # --- Límites de los ejes ---
        min_x, max_x = df_merge[col_x].min(), df_merge[col_x].max()
        min_y, max_y = df_merge[col_y].min(), df_merge[col_y].max()

        fig = go.Figure()

        # --- Cuadrantes coloreados ---
        # Arriba + Derecha (verde pastel)
        fig.add_shape(type="rect",
            x0=mean_x, x1=max_x, y0=mean_y, y1=max_y,
            fillcolor="rgba(144, 238, 144, 0.3)", line=dict(width=0))  
        # Arriba + Izquierda (amarillo pastel)
        fig.add_shape(type="rect",
            x0=min_x, x1=mean_x, y0=mean_y, y1=max_y,
            fillcolor="rgba(255, 255, 150, 0.3)", line=dict(width=0))  
        # Abajo + Derecha (amarillo pastel)
        fig.add_shape(type="rect",
            x0=mean_x, x1=max_x, y0=min_y, y1=mean_y,
            fillcolor="rgba(255, 255, 150, 0.3)", line=dict(width=0))  
        # Abajo + Izquierda (rojo pastel)
        fig.add_shape(type="rect",
            x0=min_x, x1=mean_x, y0=min_y, y1=mean_y,
            fillcolor="rgba(255, 182, 193, 0.3)", line=dict(width=0))  

        # --- Puntos con nombre ---
        fig.add_trace(go.Scatter(
            x=df_merge[col_x],
            y=df_merge[col_y],
            mode='markers+text',
            text=df_merge['Player'],
            textposition="top center",
            marker=dict(size=14, color='darkblue', opacity=0.9),
            name="Jugadores",
            hovertemplate="<b>%{text}</b><br>"
                        + f"{col_x}: " + "%{x:.2f}<br>"
                        + f"{col_y}: " + "%{y:.2f}<extra></extra>"
        ))

        # --- Líneas de media ---
        fig.add_shape(type="line",
            x0=mean_x, x1=mean_x, y0=min_y, y1=max_y,
            line=dict(color="red", dash="dash"))
        fig.add_shape(type="line",
            x0=min_x, x1=max_x, y0=mean_y, y1=mean_y,
            line=dict(color="red", dash="dash"))

        # --- Layout ---
        fig.update_layout(
            title=f"<b>Scatter Plot: {col_x} vs {col_y}</b>",
            height=600,
            margin=dict(t=60, b=40, l=60, r=40),
            plot_bgcolor='white',
            paper_bgcolor='white',
            font=dict(family="Arial", size=12),
            showlegend=False
        )

        return fig



            
            
    # ============================================================================
    # CALLBACKS - Graficos
    # ============================================================================
    @app.callback(
        [Output('grafico-distance', 'figure'),
        #  Output('grafico-hsr', 'figure'),
        #  Output('grafico-acc', 'figure'),
        #  Output('grafico-dcc', 'figure'),
         
        #  Output('grafico-velocidad', 'figure'),
        Output('grafico-posiciones', 'figure')],
            
        [Input('date-selector', 'date'),
         Input('statistic-selector', 'value')]
    )
    def update_graficos(selected_date, selected_statistic):
        """
        Actualiza todos los gráficos basados en la fecha y estadística seleccionadas.
        
        Esta función callback genera seis gráficos de rendimiento deportivo que visualizan
        diferentes métricas de los jugadores para la fecha seleccionada, incluyendo
        distancias por zonas de velocidad, HSR, aceleraciones, desaceleraciones,
        velocidad máxima y comparaciones por posición.
        
        Args:
            selected_date (str): Fecha seleccionada en formato YYYY-MM-DD
            selected_statistic (str): Estadística seleccionada para el análisis
        
        Returns:
            tuple: Seis figuras de Plotly para los gráficos:
                  - fig_distance: Gráfico de barras apiladas de distancia por zonas de velocidad
                  - fig_hsr: Gráfico de barras horizontales de distancia en alta velocidad
                  - fig_acc: Gráfico de barras de aceleraciones por zona de intensidad
                  - fig_dcc: Gráfico de barras de desaceleraciones por zona de intensidad
                  - fig_velocidad: Gráfico de barras de velocidad máxima por jugador
                  - fig_posiciones: Gráfico de barras agrupadas de métricas por posición
        
        Note:
            Todos los gráficos incluyen líneas de promedio donde es relevante y
            utilizan esquemas de colores consistentes para facilitar la interpretación
        """
        
        # Figura vacía para casos de error
        empty_fig = {
            'data': [],
            'layout': {
                'title': 'Sin datos disponibles',
                'plot_bgcolor': '#f8f9fa',
                'paper_bgcolor': '#f8f9fa',
                'font': {'color': '#000000'},
                'margin': dict(t=40, b=40, l=40, r=40)
            }
        }
        
        try:
            if not selected_date:
                return [empty_fig] * 6
            
            # Obtener datos filtrados usando la función auxiliar existente
            path_to_parquet = os.path.join(DATA_GPS_PATH, 'df_gps.parquet')
            if not os.path.exists(path_to_parquet):
                print(f"Archivo parquet no existe: {path_to_parquet}")
                return None
        
            df = pl.read_parquet(path_to_parquet)
            
            result = format_and_filter_date_graphs(selected_date)
            if result is None or result[0] is None:
                return [empty_fig] * 6
            
            df_fecha, formatted_date = result
            
            if df_fecha.height == 0:
                return [empty_fig] * 6
            
            # Convertir a pandas para facilitar manipulación
            df_pandas = df_fecha.to_pandas()

            # Verificar si tenemos las columnas necesarias
            required_columns = [
                'Player',
                'Speed Zones (m) [0.0, 45.0]% (m)',
                'Speed Zones (m) [45.0, 65.0]% (m)',
                'Speed Zones (m) [65.0, 75.0]% (m)',
                'Speed Zones (m) [75.0, 85.0]% (m)',
                'Speed Zones (m) [85.0, 95.0]% (m)',
                'Speed Zones (m) [95.0, 100.0]% (m)',
                'Abs HSR(m)',
                'Acceleration Zones  [0, 50]% Cnt',
                'Acceleration Zones  [50, 60]% Cnt',
                'Acceleration Zones  [-50, 0]% Cnt',
                'Acceleration Zones  [-60, -50]% Cnt',
                'MAX Speed(km/h)'
            ]
            
            missing_columns = [col for col in required_columns if col not in df_pandas.columns]
            if missing_columns:
                print(f"Columnas faltantes: {missing_columns}")
                return [empty_fig] * 6
            
            # ============================================================================
            # GRÁFICO 1: DISTANCE
            # ============================================================================
            
            # Filtrar solo jugadores
            df_players = df_pandas[df_pandas['Player'] != 'TEAM'].copy()

            if len(df_players) == 0:
                fig_distance = empty_fig
            else:
                # Convertir a formato largo
                speed_zone_cols = [
                    "Speed Zones (m) [0.0, 6.0]km/h (m)",
                    "Speed Zones (m) [6.0, 12.0]km/h (m)",
                    "Speed Zones (m) [12.0, 18.0]km/h (m)",
                    "Speed Zones (m) [18.0, 21.0]km/h (m)",
                    "Speed Zones (m) [21.0, 24.0]km/h (m)",
                    "Speed Zones (m) [24.0, 27.0]km/h (m)",
                    "Speed Zones (m) [27.0, 30.0]km/h (m)",
                    "Speed Zones (m) [30.0, 50.0]km/h (m)"
                ]

                df_melted = df_players.melt(
                    id_vars=['Player'],
                    value_vars=speed_zone_cols,
                    var_name='Zona',
                    value_name='Distancia'
                )

                # Renombrar zonas para la leyenda y asignar colores
                zona_map = {
                    "Speed Zones (m) [0.0, 6.0]km/h (m)": "[0.0, 6.0]",
                    "Speed Zones (m) [6.0, 12.0]km/h (m)": "[6.0, 12.0]",
                    "Speed Zones (m) [12.0, 18.0]km/h (m)": "[12.0, 18.0]",
                    "Speed Zones (m) [18.0, 21.0]km/h (m)": "[18.0, 21.0]",
                    "Speed Zones (m) [21.0, 24.0]km/h (m)": "[21.0, 24.0]",
                    "Speed Zones (m) [24.0, 27.0]km/h (m)": "[24.0, 27.0]",
                    "Speed Zones (m) [27.0, 30.0]km/h (m)": "[27.0, 30.0]",
                    "Speed Zones (m) [30.0, 50.0]km/h (m)": "[30.0, 50.0]"
                }
                df_melted['Zona'] = df_melted['Zona'].replace(zona_map)

                color_map = {
                    "[0.0, 6.0]": "#e4dcc6",
                    "[6.0, 12.0]": "#d9cdb2",
                    "[12.0, 18.0]": "#cfc09e",
                    "[18.0, 21.0]": "#c3b89a",
                    "[21.0, 24.0]": "#FF0000",
                    "[24.0, 27.0]": "#CF0000",
                    "[27.0, 30.0]": "#9F0000",
                    "[30.0, 50.0]": "#6F0000"
                }

                # Crear gráfico de barras apiladas
                fig_distance = px.bar(
                    df_melted,
                    x="Player",
                    y="Distancia",
                    color="Zona",
                    title="<b>Distancia por Zonas de Velocidad</b>",
                    color_discrete_map=color_map,
                    labels={
                        "Distancia": "Distancia (m)",
                        "Player": "Jugador",
                        "Zona": "Zona de Velocidad"
                    }
                )

                # Ajustes de layout
                fig_distance.update_layout(
                    plot_bgcolor="white",
                    paper_bgcolor="white",
                    font=dict(family="Arial, sans-serif", size=12, color="#2c3e50"),
                    title=dict(x=0.5, font=dict(size=16, color="#2c3e50")),
                    margin=dict(t=80, b=120, l=60, r=50),
                    barmode="stack",
                    height=550,
                    legend=dict(
                        orientation="h",
                        yanchor="top",
                        y=1.5,
                        xanchor="center",
                        x=0.5
                    )
                )

                fig_distance.update_xaxes(
                    title_font=dict(size=14, color="#2c3e50"),
                    tickfont=dict(size=11, color="#2c3e50"),
                    gridcolor="#ecf0f1",
                    showgrid=True
                )

                fig_distance.update_yaxes(
                    title_font=dict(size=14, color="#2c3e50"),
                    tickfont=dict(size=11, color="#2c3e50"),
                    gridcolor="#ecf0f1",
                    showgrid=True
                )
            
            # ============================================================================
            # GRÁFICO 2: HSR
            # ============================================================================
            
            # if len(df_players) == 0:
            #     fig_hsr = empty_fig
            # else:
            #     # Eliminar NaNs en la métrica
            #     df_hsr = df_players.dropna(subset=["Abs HSR(m)"])
                
            #     if len(df_hsr) == 0:
            #         fig_hsr = empty_fig
            #     else:
            #         # Agrupar por jugador y sumar la métrica
            #         df_agrupado = df_hsr.groupby("Player", as_index=False)["Abs HSR(m)"].sum()
                    
            #         # Calcular promedio
            #         promedio_hsr = df_agrupado["Abs HSR(m)"].mean()
                    
            #         fig_hsr = px.bar(
            #             df_agrupado,
            #             y="Player",
            #             x="Abs HSR(m)",
            #             orientation="h",
            #             title="<b>Distancia en Alta Velocidad (HSR)</b>",
            #             color_discrete_sequence=["#525252"],
            #             labels={
            #                 "Abs HSR(m)": "HSR (m)",
            #                 "Player": "Jugador"
            #             }
            #         )
                    
            #         # Añadir línea del promedio
            #         fig_hsr.add_shape(
            #             type="line",
            #             x0=promedio_hsr,
            #             x1=promedio_hsr,
            #             y0=0,
            #             y1=1,
            #             line=dict(color="#e74c3c", width=3, dash="dash"),
            #             xref="x",
            #             yref="paper"
            #         )
                    
            #         fig_hsr.add_annotation(
            #             x=promedio_hsr,
            #             y=1.05,
            #             xref="x",
            #             yref="paper",
            #             text=f"<b>Promedio: {float(promedio_hsr):.1f} m</b>",
            #             showarrow=False,
            #             font=dict(color="#e74c3c", size=12),
            #             bgcolor="white",
            #             bordercolor="#e74c3c",
            #             borderwidth=1
            #         )
                    
            #         fig_hsr.update_layout(
            #             plot_bgcolor="white",
            #             paper_bgcolor="white",
            #             font=dict(family="Arial, sans-serif", size=12, color="#2c3e50"),
            #             title=dict(x=0.5, font=dict(size=16, color="#2c3e50")),
            #             margin=dict(t=70, b=50, l=120, r=50),
            #             height=500
            #         )
                    
            #         fig_hsr.update_xaxes(
            #             title_font=dict(size=14, color="#2c3e50"),
            #             tickfont=dict(size=11, color="#2c3e50"),
            #             gridcolor="#ecf0f1",
            #             showgrid=True
            #         )
                    
            #         fig_hsr.update_yaxes(
            #             title_font=dict(size=14, color="#2c3e50"),
            #             tickfont=dict(size=11, color="#2c3e50"),
            #             gridcolor="#ecf0f1",
            #             showgrid=True
            #         )
            
            # ============================================================================
            # GRÁFICO 3: ACC
            # ============================================================================
            
            # if len(df_players) == 0:
            #     fig_acc = empty_fig
            # else:
            #     # Convertir a formato largo
            #     df_melted_acc = df_players.melt(
            #         id_vars=['Player'],
            #         value_vars=[
            #             "Acceleration Zones  [0, 50]% Cnt",
            #             "Acceleration Zones  [50, 60]% Cnt"
            #         ],
            #         var_name='Zona',
            #         value_name='Cuenta'
            #     )
                
            #     # Renombrar zonas
            #     df_melted_acc['Zona'] = df_melted_acc['Zona'].replace({
            #         "Acceleration Zones  [0, 50]% Cnt": "[Z1]",
            #         "Acceleration Zones  [50, 60]% Cnt": "[Z2]"
            #     })
                
            #     fig_acc = px.bar(
            #         df_melted_acc,
            #         y="Player",
            #         x="Cuenta",
            #         color="Zona",
            #         orientation="h",
            #         title="<b>Aceleraciones por Zona de Intensidad</b>",
            #         color_discrete_map={
            #             "[Z1]": "#38a838",
            #             "[Z2]": "#01fa16"
            #         },
            #         labels={
            #             "Cuenta": "Número de Aceleraciones",
            #             "Player": "Jugador",
            #             "Zona": "Zona de Aceleración"
            #         }
            #     )
                
            #     fig_acc.update_layout(
            #         plot_bgcolor="white",
            #         paper_bgcolor="white",
            #         font=dict(family="Arial, sans-serif", size=12, color="#2c3e50"),
            #         title=dict(x=0.5, font=dict(size=16, color="#2c3e50")),
            #         margin=dict(t=80, b=80, l=120, r=50),
            #         height=550,
            #         legend=dict(
            #             orientation="h",
            #             yanchor="top",
            #             y=-0.15,
            #             xanchor="center",
            #             x=0.5
            #         )
            #     )
                
            #     fig_acc.update_xaxes(
            #         title_font=dict(size=14, color="#2c3e50"),
            #         tickfont=dict(size=11, color="#2c3e50"),
            #         gridcolor="#ecf0f1",
            #         showgrid=True
            #     )
                
            #     fig_acc.update_yaxes(
            #         title_font=dict(size=14, color="#2c3e50"),
            #         tickfont=dict(size=11, color="#2c3e50"),
            #         gridcolor="#ecf0f1",
            #         showgrid=True
            #     )
            
            # # ============================================================================
            # # GRÁFICO 4: DCC
            # # ============================================================================
            
            # if len(df_players) == 0:
            #     fig_dcc = empty_fig
            # else:
            #     # Convertir a formato largo
            #     df_melted_dcc = df_players.melt(
            #         id_vars=['Player'],
            #         value_vars=[
            #             "Acceleration Zones  [-50, 0]% Cnt",
            #             "Acceleration Zones  [-60, -50]% Cnt"
            #         ],
            #         var_name='Zona',
            #         value_name='Cuenta'
            #     )
                
            #     # Renombrar zonas
            #     df_melted_dcc['Zona'] = df_melted_dcc['Zona'].replace({
            #         "Acceleration Zones  [-50, 0]% Cnt": "[Z1]",
            #         "Acceleration Zones  [-60, -50]% Cnt": "[Z2]"
            #     })
                
            #     fig_dcc = px.bar(
            #         df_melted_dcc,
            #         y="Player",
            #         x="Cuenta",
            #         color="Zona",
            #         orientation="h",
            #         title="<b>Desaceleraciones por Zona de Intensidad</b>",
            #         color_discrete_map={
            #             "[Z1]": "#a83838",
            #             "[Z2]": "#fa0101"
            #         },
            #         labels={
            #             "Cuenta": "Número de Desaceleraciones",
            #             "Player": "Jugador",
            #             "Zona": "Zona de Desaceleración"
            #         }
            #     )
                
            #     fig_dcc.update_layout(
            #         plot_bgcolor="white",
            #         paper_bgcolor="white",
            #         font=dict(family="Arial, sans-serif", size=12, color="#2c3e50"),
            #         title=dict(x=0.5, font=dict(size=16, color="#2c3e50")),
            #         margin=dict(t=80, b=80, l=120, r=50),
            #         height=550,
            #         legend=dict(
            #             orientation="h",
            #             yanchor="top",
            #             y=-0.15,
            #             xanchor="center",
            #             x=0.5
            #         )
            #     )
                
            #     fig_dcc.update_xaxes(
            #         title_font=dict(size=14, color="#2c3e50"),
            #         tickfont=dict(size=11, color="#2c3e50"),
            #         gridcolor="#ecf0f1",
            #         showgrid=True
            #     )
                
            #     fig_dcc.update_yaxes(
            #         title_font=dict(size=14, color="#2c3e50"),
            #         tickfont=dict(size=11, color="#2c3e50"),
            #         gridcolor="#ecf0f1",
            #         showgrid=True
            #     )
            
            # ============================================================================
            # GRÁFICO 5: VELOCIDAD
            # ============================================================================
            
            # if len(df_players) == 0:
            #     fig_velocidad = empty_fig
            # else:
            #     # Eliminar NaNs en la métrica
            #     df_velocidad = df_players.dropna(subset=["MAX Speed(km/h)"])
                
            #     if len(df_velocidad) == 0:
            #         fig_velocidad = empty_fig
            #     else:
            #         # Agrupar por jugador y calcular máximo
            #         df_agrupado_vel = df_velocidad.groupby("Player", as_index=False)["MAX Speed(km/h)"].max()
                    
            #         # Calcular promedio
            #         promedio_velocidad = df_agrupado_vel["MAX Speed(km/h)"].mean()
                    
            #         fig_velocidad = px.bar(
            #             df_agrupado_vel,
            #             y="Player",
            #             x="MAX Speed(km/h)",
            #             orientation="h",
            #             title="<b>Velocidad Máxima por Jugador</b>",
            #             color_discrete_sequence=["#12A7C2"],
            #             labels={
            #                 "MAX Speed(km/h)": "Velocidad Máxima (km/h)",
            #                 "Player": "Jugador"
            #             }
            #         )
                    
            #         # Añadir línea del promedio
            #         fig_velocidad.add_shape(
            #             type="line",
            #             x0=promedio_velocidad,
            #             x1=promedio_velocidad,
            #             y0=0,
            #             y1=1,
            #             line=dict(color="#e74c3c", width=3, dash="dash"),
            #             xref="x",
            #             yref="paper"
            #         )
                    
            #         fig_velocidad.add_annotation(
            #             x=promedio_velocidad,
            #             y=1.05,
            #             xref="x",
            #             yref="paper",
            #             text=f"<b>Promedio: {float(promedio_velocidad):.1f} km/h</b>",
            #             showarrow=False,
            #             font=dict(color="#e74c3c", size=12),
            #             bgcolor="white",
            #             bordercolor="#e74c3c",
            #             borderwidth=1
            #         )
                    
            #         fig_velocidad.update_layout(
            #             plot_bgcolor="white",
            #             paper_bgcolor="white",
            #             font=dict(family="Arial, sans-serif", size=12, color="#2c3e50"),
            #             title=dict(x=0.5, font=dict(size=16, color="#2c3e50")),
            #             margin=dict(t=70, b=50, l=120, r=50),
            #             height=500
            #         )
                    
            #         fig_velocidad.update_xaxes(
            #             title_font=dict(size=14, color="#2c3e50"),
            #             tickfont=dict(size=11, color="#2c3e50"),
            #             gridcolor="#ecf0f1",
            #             showgrid=True
            #         )
                    
            #         fig_velocidad.update_yaxes(
            #             title_font=dict(size=14, color="#2c3e50"),
            #             tickfont=dict(size=11, color="#2c3e50"),
            #             gridcolor="#ecf0f1",
            #             showgrid=True
            #         )
            
            # ============================================================================
            # GRÁFICO 6: POSICIONES
            # ============================================================================
            
            # Verificar si tenemos columna Position
            if 'Position' not in df_pandas.columns:
                fig_posiciones = empty_fig
            else:
                # Filtrar jugadores válidos con posición
                df_pos = df_players.dropna(subset=['Position'])
                
                if len(df_pos) == 0:
                    fig_posiciones = empty_fig
                else:
                    # Agrupar por posición y calcular promedio
                    df_agrupado_pos = df_pos.groupby("Position", as_index=False).agg({
                        "Speed Zones (m) [0.0, 45.0]% (m)": "mean",
                        "Speed Zones (m) [45.0, 65.0]% (m)": "mean",
                        "Speed Zones (m) [65.0, 75.0]% (m)": "mean",
                        "Speed Zones (m) [75.0, 85.0]% (m)": "mean",
                        "Speed Zones (m) [85.0, 95.0]% (m)": "mean",
                        "Speed Zones (m) [95.0, 100.0]% (m)": "mean",
                        "Abs HSR(m)": "mean",
                        "Acceleration Zones  [0, 50]% Cnt": "mean",
                        "Acceleration Zones  [50, 60]% Cnt": "mean",
                        "Acceleration Zones  [-50, 0]% Cnt": "mean",
                        "Acceleration Zones  [-60, -50]% Cnt": "mean",
                        "MAX Speed(km/h)": "mean"
                    })
                    
                    # Crear métricas finales
                    df_agrupado_pos["Distance"] = (
                        df_agrupado_pos["Speed Zones (m) [0.0, 45.0]% (m)"] +
                        df_agrupado_pos["Speed Zones (m) [45.0, 65.0]% (m)"] +
                        df_agrupado_pos["Speed Zones (m) [65.0, 75.0]% (m)"] +
                        df_agrupado_pos["Speed Zones (m) [75.0, 85.0]% (m)"] +
                        df_agrupado_pos["Speed Zones (m) [85.0, 95.0]% (m)"] +
                        df_agrupado_pos["Speed Zones (m) [95.0, 100.0]% (m)"]
                    )
                    
                    df_agrupado_pos["Acc"] = (
                        df_agrupado_pos["Acceleration Zones  [0, 50]% Cnt"] +
                        df_agrupado_pos["Acceleration Zones  [50, 60]% Cnt"]
                    )
                    
                    df_agrupado_pos["Decc"] = (
                        df_agrupado_pos["Acceleration Zones  [-50, 0]% Cnt"] +
                        df_agrupado_pos["Acceleration Zones  [-60, -50]% Cnt"]
                    )
                    
                    df_agrupado_pos.rename(columns={"Abs HSR(m)": "HSR", "MAX Speed(km/h)": "Max Speed"}, inplace=True)
                    
                    # Seleccionar columnas finales
                    df_final_pos = df_agrupado_pos[["Position", "Distance", "HSR", "Acc", "Decc", "Max Speed"]]
                    
                    # Transformar a formato largo
                    df_melted_pos = df_final_pos.melt(
                        id_vars="Position",
                        var_name="Métrica",
                        value_name="Valor"
                    )
                    
                    fig_posiciones = px.bar(
                        df_melted_pos,
                        x="Position",
                        y="Valor",
                        color="Métrica",
                        barmode="group",
                        title="<b>Análisis por Posición - Métricas Promedio</b>",
                        color_discrete_map={
                            "Distance": "#6e6e6e",
                            "HSR": "#000000",
                            "Acc": "#33b300",
                            "Decc": "#bb0404",
                            "Max Speed": "#0092cc"
                        },
                        text="Valor",
                        labels={
                            "Position": "Posición",
                            "Valor": "Valor Promedio",
                            "Métrica": "Métrica"
                        }
                    )
                    
                    fig_posiciones.update_traces(
                        texttemplate='%{text:.0f}',
                        textposition="outside",
                        textangle=0,
                        textfont=dict(size=10, color="#2c3e50")
                    )
                    
                    fig_posiciones.update_layout(
                        plot_bgcolor="white",
                        paper_bgcolor="white",
                        font=dict(family="Arial, sans-serif", size=12, color="#2c3e50"),
                        title=dict(x=0.5, font=dict(size=16, color="#2c3e50")),
                        margin=dict(t=80, b=80, l=50, r=50),
                        height=550,
                        legend=dict(
                            orientation="h",
                            yanchor="top",
                            y=-0.15,
                            xanchor="center",
                            x=0.5
                        )
                    )
                    
                    fig_posiciones.update_xaxes(
                        title_font=dict(size=14, color="#2c3e50"),
                        tickfont=dict(size=11, color="#2c3e50"),
                        gridcolor="#ecf0f1",
                        showgrid=True
                    )
                    
                    fig_posiciones.update_yaxes(
                        title_font=dict(size=14, color="#2c3e50"),
                        tickfont=dict(size=11, color="#2c3e50"),
                        gridcolor="#ecf0f1",
                        showgrid=True
                    )
            
            return [fig_distance, #fig_hsr,  fig_acc, fig_dcc,  fig_velocidad, fig_posiciones
                    fig_posiciones]
            
        except Exception as e:
            print(f"Error al generar gráficos: {e}")
            import traceback
            traceback.print_exc()
            return [empty_fig] * 6


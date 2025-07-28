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
# LAYOUT DE LA PÁGINA
# ============================================================================

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
            
            html.Div(id='players-table-output'),
            
            # Seção de gráficos
            html.Div([
                html.H4('Gráficos de Análisis', className="section-title", style={'margin-top': '30px'}),
                
                # Primera fila de gráficos - Distance e HSR
                html.Div([
                    html.Div([
                        html.Div([
                            dcc.Graph(id='grafico-distance')
                        ], className="graph-box")
                    ], style={'width': '48%', 'display': 'inline-block', 'margin-right': '2%'}),
                    
                    html.Div([
                        html.Div([
                            dcc.Graph(id='grafico-hsr')
                        ], className="graph-box")
                    ], style={'width': '48%', 'display': 'inline-block'})
                ], style={'margin-bottom': '20px'}),
                
                # Segunda fila de gráficos - ACC, DCC e Velocidad
                html.Div([
                    html.Div([
                        html.Div([
                            dcc.Graph(id='grafico-acc')
                        ], className="graph-box")
                    ], style={'width': '32%', 'display': 'inline-block', 'margin-right': '2%'}),
                    
                    html.Div([
                        html.Div([
                            dcc.Graph(id='grafico-dcc')
                        ], className="graph-box")
                    ], style={'width': '32%', 'display': 'inline-block', 'margin-right': '2%'}),
                    
                    html.Div([
                        html.Div([
                            dcc.Graph(id='grafico-velocidad')
                        ], className="graph-box")
                    ], style={'width': '32%', 'display': 'inline-block'})
                ], style={'margin-bottom': '20px'}),
                
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
        
        # Obtener fechas ordenadas cronológicamente en formato dd/mm/aaaa
        fechas_ordenadas = get_sorted_dates()
        
        if not fechas_ordenadas:
            return dash.no_update, None, None, []
        
        # Configurar límites del calendario (convertir a datetime.date para el DatePickerSingle)
        try:
            min_date = datetime.strptime(fechas_ordenadas[0], '%d/%m/%Y').date()
            max_date = datetime.strptime(fechas_ordenadas[-1], '%d/%m/%Y').date()
        except:
            min_date = max_date = None
        
        # Si no hay trigger (inicialización de la página), retornar la fecha más reciente
        if not ctx.triggered:
            # Convertir la fecha más reciente (última en la lista) a formato YYYY-MM-DD
            try:
                latest_date_dt = datetime.strptime(fechas_ordenadas[-1], '%d/%m/%Y')
                latest_date_formatted = latest_date_dt.strftime('%Y-%m-%d')
            except:
                latest_date_formatted = fechas_ordenadas[-1]
            return latest_date_formatted, min_date, max_date, []
        
        # Si es solo inicialización del selector, solo configurar calendario
        if ctx.triggered[0]['prop_id'] == 'date-selector.id':
            return dash.no_update, min_date, max_date, []
        
        # Manejar navegación con botones
        if not current_date:
            # Si no hay fecha actual, retornar la primera fecha disponible
            return fechas_ordenadas[0], min_date, max_date, []
        
        # Convertir current_date (formato aaaa-mm-dd del DatePickerSingle) a dd/mm/aaaa
        try:
            if isinstance(current_date, str):
                if '-' in current_date:
                    current_dt = datetime.strptime(current_date, '%Y-%m-%d')
                    current_date_formatted = current_dt.strftime('%d/%m/%Y')
                else:
                    current_date_formatted = current_date
            else:
                current_date_formatted = current_date.strftime('%d/%m/%Y')
        except:
            current_date_formatted = fechas_ordenadas[0]
        
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
        
        Args:
            selected_date (str): Fecha seleccionada en formato YYYY-MM-DD
            selected_statistic (str): Código de la estadística seleccionada 
                                    ('mean', 'median', 'max', 'min', 'p75', 'p90', 'p95')
        
        Returns:
            html.Div: Componente HTML con la información de la sesión formateada,
                     incluyendo título, detalles de la sesión y estadística seleccionada
        
        Note:
            Maneja errores de datos faltantes y muestra mensajes informativos apropiados
        """
        if not selected_date:
            return html.Div("Selecciona una fecha para ver la información de la sesión.", 
                          className="info-message")
        
        try:
            path_to_parquet = os.path.join(DATA_GPS_PATH, 'df_gps.parquet')
            if not os.path.exists(path_to_parquet):
                return html.Div("No se encontró el archivo de datos.", 
                              className="error-message")
            
            df = pl.read_parquet(path_to_parquet)
            
            result = format_and_filter_date(selected_date)
            if result is None or result[0] is None:
                return html.Div(f"No se encontraron datos para la fecha {selected_date}.", 
                              className="warning-message")
            
            df_fecha, formatted_date = result
            
            if df_fecha.height == 0:
                return html.Div(f"No se encontraron datos para la fecha {selected_date}.", 
                              className="warning-message")
            
            
            # Obtener información básica de la sesión
            num_jugadores = df_fecha.select('Player').n_unique()
            
            # Calcular duración total de drills si la columna existe
            if 'Drills Duration' in df_fecha.columns:
                duration = df_fecha.select('Drills Duration').row(0)[0]
            
            # Filtrar Match Days excluyendo 'Rehab'
            match_days_filtered = df_fecha.filter(pl.col('Match Day') != 'Rehab').select('Match Day').unique().to_series().to_list()
            
            # Crear información base
            session_info = [
                html.H5(f"Información de la Sesión - {formatted_date}", className="session-title"),
                html.P(f"Número de jugadores: {num_jugadores}", className="session-detail"),
                html.P(f"Duración total: {duration}", className="session-detail"),
                html.P(f"Match Days: {', '.join(match_days_filtered)}", className="session-detail")
            ]
            
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
    # CALLBACKS - Tabla
    # ============================================================================

    
    @app.callback(
        Output('players-table-output', 'children'),
        [Input('date-selector', 'date'),
         Input('statistic-selector', 'value'),
         Input('zscore-state-store', 'data')
        ]
    )
    def update_players_table(selected_date, selected_statistic, zscore_state):
        """
        Actualiza la tabla de jugadores usando la función get_combined_table_with_reference
        con gradiente de cores baseado nos z-scores.
        
        Esta función callback utiliza get_combined_table_with_reference para
        generar una tabla que combina valores absolutos con z-scores para jugadores,
        equipos y posiciones, aplicando un gradiente de cores baseado nos valores z-score.
        
        Args:
            selected_date (str): Fecha seleccionada en formato YYYY-MM-DD
            selected_statistic (str): Estadística seleccionada para el análisis
        
        Returns:
            html.Div: Componente HTML con la tabla de datos formateada que incluye
                     valores absolutos y z-scores para jugadores, equipos y posiciones
                     con gradiente de cores baseado nos z-scores
        """
        
        if not selected_date:
            return html.Div("Selecciona una fecha para ver los datos de los jugadores.", 
                          className="info-message")
        
        if not selected_statistic:
            return html.Div("Selecciona una estadística para ver los datos.", 
                          className="info-message")
        
        try:
            # Convertir fecha al formato correcto (dd/mm/aaaa)
            result = format_and_filter_date(selected_date)
            if result is None or result[0] is None:
                return html.Div(f"No se encontraron datos para la fecha {selected_date}.", 
                              className="warning-message")
            
            df_fecha, formatted_date = result
            
            # Determinar o tipo de referência baseado no estado do Z-Score
            if zscore_state is None:
                zscore_state = {'active': True}
            
            is_zscore_active = zscore_state.get('active', True)
            valor_referencia = "zscore" if is_zscore_active else "diferencia"
            
            # Usar la función get_combined_table_with_reference
            tabla_combinada = get_combined_table_with_reference(
                fecha=formatted_date, 
                valor_referencia=valor_referencia, 
                estadistica=selected_statistic
            )
            
            if tabla_combinada is None:
                return html.Div("No se pudieron obtener los datos combinados para la fecha seleccionada.", 
                              className="warning-message")
            
            # Obter matriz de z-scores para estilização
            zscore_matrix = get_zscore_matrix_for_styling(
                fecha=formatted_date
            )
            
            # Convertir el DataFrame de Polars a formato compatible con Dash DataTable
            data_for_table = tabla_combinada.to_dicts()
            
            # Obtener las columnas
            columns = tabla_combinada.columns
            
            # Renombrar la columna Player a Player/Team/Position para mayor claridad
            columns_renamed = []
            for col in columns:
                if col == 'Player':
                    columns_renamed.append('Player/Team/Position')
                else:
                    columns_renamed.append(col)
            
            # Actualizar los datos con el nuevo nombre de columna y agregar tipo de entidad
            for record in data_for_table:
                if 'Player' in record:
                    player_name = record['Player']
                    record['Player/Team/Position'] = player_name
                    record.pop('Player')
                    
                    # Identificar tipo de entidad para estilos condicionais
                    if player_name.startswith('TEAM'):
                        record['_entity_type'] = 'team'
                    elif player_name.startswith('POS_'):
                        record['_entity_type'] = 'position'
                    else:
                        record['_entity_type'] = 'player'
            
            # Gerar estilos condicionais baseados nos z-scores (sempre aplicar gradiente)
            zscore_styles = []
            if zscore_matrix is not None:
                zscore_styles = generate_color_styles_from_zscore(zscore_matrix)
            
            # Estilos condicionais para diferentes tipos de entidades na coluna Player/Team/Position
            entity_styles = [
                # Jogadores - Amarelo claro
                {
                    'if': {
                        'filter_query': '{_entity_type} = player',
                        'column_id': 'Player/Team/Position'
                    },
                    'backgroundColor': '#fff9c4',  # Amarelo claro
                    'color': '#856404',  # Texto amarelo escuro
                    'fontWeight': 'bold'
                },
                # Equipos - Azul claro
                {
                    'if': {
                        'filter_query': '{_entity_type} = team',
                        'column_id': 'Player/Team/Position'
                    },
                    'backgroundColor': '#cce7ff',  # Azul claro
                    'color': '#004085',  # Texto azul escuro
                    'fontWeight': 'bold'
                },
                # Posições - Verde claro
                {
                    'if': {
                        'filter_query': '{_entity_type} = position',
                        'column_id': 'Player/Team/Position'
                    },
                    'backgroundColor': '#d4edda',  # Verde claro
                    'color': '#155724',  # Texto verde escuro
                    'fontWeight': 'bold'
                }
            ]
            
            # Combinar estilos de entidades com estilos de z-score
            all_styles = entity_styles + zscore_styles
            
            # Crear la tabla con estilos básicos, cabeçalho fixo e cores condicionais
            combined_table = dash_table.DataTable(
                id='combined-table-with-zscore',
                data=data_for_table,
                columns=[
                    {"name": col, "id": col, "type": "text"}
                    for col in columns_renamed if col != '_entity_type'  # Excluir coluna auxiliar
                ],
                style_table={
                    'overflowX': 'auto',
                    'maxHeight': '800px',
                    'overflowY': 'auto'
                },
                style_cell={
                    'textAlign': 'left',
                    'padding': '8px',
                    'fontFamily': 'Arial, sans-serif',
                    'fontSize': '13px',
                    'border': '1px solid #ddd',
                    'whiteSpace': 'normal',
                    'height': 'auto'
                },
                style_header={
                    'backgroundColor': '#e8f4fd',
                    'fontWeight': 'bold',
                    'textAlign': 'center',
                    'border': '1px solid #ddd',
                    'position': 'sticky',
                    'top': '0',
                    'zIndex': '1'
                },
                style_data={
                    'backgroundColor': 'white',
                    'border': '1px solid #ddd'
                },
                # Aplicar todos os estilos condicionais (entidades + gradiente z-score)
                style_data_conditional=all_styles,
                sort_action="native",
                filter_action="native",
                page_action="none",
                export_format="xlsx",
                export_headers="display",
                fixed_rows={'headers': True}
            )
            
            # Contar registros por tipo
            num_jugadores = len([d for d in data_for_table if not d['Player/Team/Position'].startswith(('TEAM', 'POS_'))])
            num_equipos = len([d for d in data_for_table if d['Player/Team/Position'].startswith('TEAM')])
            num_posiciones = len([d for d in data_for_table if d['Player/Team/Position'].startswith('POS_')])
            
            info_text = f"Jugadores: {num_jugadores} | Equipos: {num_equipos} | Posiciones: {num_posiciones}"
            
            # Definir título baseado no estado do Z-Score
            if is_zscore_active:
                table_title = 'Tabla Combinada - Valores Absolutos con Z-Scores'
            else:
                table_title = 'Tabla Combinada - Valores Absolutos con Diferencias %'
            
            # Legenda sempre mostra o gradiente de z-scores
            legend_content = html.Div([
                html.P("Gradiente de cores baseado nos Z-Scores:", style={'margin-bottom': '5px', 'font-weight': 'bold'}),
                html.Div([
                    html.Span("Z-Score baixo", style={'color': '#1a365d', 'font-weight': 'bold', 'margin-right': '10px'}),
                    html.Div(style={
                        'display': 'inline-block',
                        'width': '200px',
                        'height': '20px',
                        'background': 'linear-gradient(to right, #1a365d, #ffffff, #8b0000)',
                        'border': '1px solid #ccc',
                        'margin': '0 10px',
                        'vertical-align': 'middle'
                    }),
                    html.Span("Z-Score alto", style={'color': '#8b0000', 'font-weight': 'bold', 'margin-left': '10px'})
                ], style={'display': 'flex', 'align-items': 'center', 'justify-content': 'center'})
            ], style={'text-align': 'center', 'margin': '10px 0', 'padding': '10px', 'background-color': '#f8f9fa', 'border-radius': '5px'})
            
            # Adicionar informação sobre o tipo de dados mostrados
            if not is_zscore_active:
                additional_info = html.Div([
                    html.P("Mostrando diferencias porcentuales respecto a la media del equipo", 
                           style={'margin': '5px 0', 'padding': '5px', 'background-color': '#e9ecef', 
                                  'border-radius': '3px', 'text-align': 'center', 'font-style': 'italic'})
                ])
            else:
                additional_info = html.Div()
            
            return html.Div([
                html.H5(table_title, className="section-subtitle"),
                html.P(f"Mostrando {info_text} | Estadística: {selected_statistic}", className="table-info"),
                # Legenda baseada no estado
                legend_content,
                # Informação adicional sobre o tipo de dados
                additional_info,
                # Container para tabela com botões alinhados
                html.Div([
                    # Container para botões na parte superior direita
                    html.Div([
                        html.Button(
                            "Z Score",
                            id="zscore-toggle-btn",
                            className="zscore-toggle-btn-gray",
                            style={
                                'background-color': '#6c757d',
                                'color': 'white',
                                'border': '1px solid #6c757d',
                                'border-radius': '4px',
                                'padding': '8px 16px',
                                'cursor': 'pointer',
                                'font-size': '14px',
                                'margin-bottom': '2px'
                            }
                        )
                    ], style={
                        'display': 'flex', 
                        'justify-content': 'flex-end', 
                        'margin-bottom': '1px'
                    }),
                    # Tabela (com botão Export nativo que aparecerá próximo ao Z-Score)
                    combined_table
                ], style={'width': '100%'})
            ], className="combined-stats-table-container")
            
        except Exception as e:
            return html.Div(f"Error al cargar tabla de jugadores: {str(e)}", 
                          className="error-message")
    
    # ============================================================================
    # CALLBACKS - Tarjetas
    # ============================================================================
    
    # Callback para popular as opções do dropdown tarjetas-columns-selector
    @app.callback(
        Output('tarjetas-columns-selector', 'options'),
        [Input('date-selector', 'date')]
    )
    def update_tarjetas_columns_options(selected_date):
        """
        Atualiza as opções do dropdown tarjetas-columns-selector com todas as colunas disponíveis.
        
        Args:
            selected_date (str): Data selecionada em formato YYYY-MM-DD
            
        Returns:
            list: Lista de opções para o dropdown com todas as colunas de métricas disponíveis
        """
        if not selected_date:
            return []
        
        try:
            # Obter dados de jogadores para a data selecionada
            df_players = get_players_data(selected_date)
                      
            exclude_cols = ['Player', 'Position', 'Team', 'Match Day', 'Estadistica', 'Date']
            all_columns = [col for col in df_players.columns if col not in exclude_cols]
            
            # Criar opções para o dropdown ordenadas alfabeticamente
            options = [{'label': col, 'value': col} for col in sorted(all_columns)]
            
            return options
            
        except Exception as e:
            print(f"Erro ao atualizar opções do dropdown de colunas: {e}")
            # Fallback para colunas básicas
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
        [Input('date-selector', 'date')]
    )
    def update_cards_view_options(selected_date):
        """
        Atualiza as opções do dropdown cards-view-selector com base na data selecionada.
        
        Args:
            selected_date (str): Fecha seleccionada en formato YYYY-MM-DD
        
        Returns:
            list: Lista de opções para o dropdown (Equipo e posições disponíveis)
        """
        if not selected_date:
            return [{'label': 'Equipo', 'value': 'Equipo'}]
        
        try:
            
            # Obtener datos de jugadores para la fecha seleccionada
            df_players = get_players_data(selected_date)
            
            if df_players is None:
                return [{'label': 'Equipo', 'value': 'Equipo'}]
            
            # Criar opções básicas
            options = [{'label': 'Equipo', 'value': 'Equipo'}]
            
            # Adicionar posições únicas disponíveis
            if 'Position' in df_players.columns:
                positions = df_players['Position'].unique().to_list()
                for position in sorted(positions):
                    if position and str(position) != 'null':
                        options.append({
                            'label': f'Posición: {position}',
                            'value': f'Position_{position}'
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
        de las métricas para el equipo completo o por posición específica.
        
        Args:
            selected_date (str): Fecha seleccionada en formato YYYY-MM-DD
            selected_statistic (str): Estadística seleccionada para el análisis
            selected_view (str): Vista seleccionada ('Equipo' o 'Position_X')
            selected_columns (list): Lista de columnas de métricas a mostrar en las tarjetas
        
        Returns:
            html.Div: Componente HTML con tarjetas de métricas que incluyen:
                     - Título descriptivo de la vista
                     - Tarjetas individuales para cada métrica seleccionada
                     - Valores absolutos de las métricas
        """
        
        if not selected_date or not selected_statistic:
            return html.Div()
        
        try:
            # Obtener datos de jugadores para la fecha seleccionada
            df_players = get_players_data(selected_date)
            
            if df_players is None:
                return html.Div("No se encontraron datos para la fecha seleccionada.", 
                              className="info-message")
            
            # Determinar qué datos usar basado en selected_view
            if selected_view == 'Equipo':
                # Calcular estadísticas del equipo usando calcular_metricas
                columnas_numericas = [col for col in df_players.columns if col not in ['Player', 'Position']]
                team_stats = calcular_metricas(df_players, columnas_numericas, selected_statistic)
                # Crear DataFrame para el equipo
                team_stats['Player'] = "TEAM"
                df_to_use = pl.DataFrame([team_stats])
                title_prefix = f'Métricas del Equipo - {selected_statistic}'
                
            elif selected_view and selected_view.startswith('Position_'):
                # Extraer la posición del valor selected_view
                position = selected_view.replace('Position_', '')
                
                # Filtrar jugadores por posición
                df_position_players = df_players.filter(pl.col('Position') == position)
                
                if df_position_players.height == 0:
                    return html.Div(f"No se encontraron jugadores para la posición {position}.", 
                                  className="info-message")
                
                # Calcular estadísticas de la posición usando calcular_metricas
                columnas_numericas = [col for col in df_position_players.columns if col not in ['Player', 'Position']]
                position_stats = calcular_metricas(df_position_players, columnas_numericas, selected_statistic)
                
                # Crear DataFrame para la posición
                position_stats['Player'] = f"POS_{position}"
                df_to_use = pl.DataFrame([position_stats])
                title_prefix = f'Métricas de la Posición {position} - {selected_statistic}'
                
            else:
                # Fallback a equipo
                columnas_numericas = [col for col in df_players.columns if col not in ['Player', 'Position']]
                team_stats = calcular_metricas(df_players, columnas_numericas, selected_statistic)
                # Crear DataFrame para el equipo
                team_stats['Player'] = "TEAM"
                df_to_use = pl.DataFrame([team_stats])
                title_prefix = f'Métricas del Equipo - {selected_statistic}'
            
            if df_to_use.height == 0:
                return html.Div("No hay datos disponibles para esta selección.", 
                              className="info-message")
            
            # Convertir a pandas para facilitar el manejo
            df_pandas = df_to_use.to_pandas()
            
            # Obtener columnas de métricas (excluyendo columnas de identificación)
            exclude_cols = ['Player', 'Position', 'Team', 'Match Day', 'Estadistica']
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
                            if 'percentage' in col.lower() or '%' in col:
                                formatted_value = f'{metric_value_num:.2f}%'
                            elif 'speed' in col.lower() or 'km/h' in col:
                                formatted_value = f'{metric_value_num:.2f} km/h'
                            elif 'distance' in col.lower() or '(m)' in col:
                                formatted_value = f'{metric_value_num:.0f} m'
                            elif 'cnt' in col.lower() or 'count' in col.lower():
                                formatted_value = f'{metric_value_num:.0f}'
                            else:
                                formatted_value = f'{metric_value_num:.2f}'
                        except (ValueError, TypeError):
                            formatted_value = str(metric_value)
                     
                    # Crear tarjeta usando estilos CSS definidos
                    card_content = [
                         # Título da métrica usando classe card-title
                         html.H6(col, className="card-title"),
                         
                         # Container de valores usando classe card-values-container
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
    # CALLBACKS - Graficos
    # ============================================================================
    @app.callback(
        [Output('grafico-distance', 'figure'),
         Output('grafico-hsr', 'figure'),
         Output('grafico-acc', 'figure'),
         Output('grafico-dcc', 'figure'),
         Output('grafico-velocidad', 'figure'),
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
            
            result = format_and_filter_date(selected_date)
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
            
            # Filtrar solo jugadores (no TEAM)
            df_players = df_pandas[df_pandas['Player'] != 'TEAM'].copy()
            
            if len(df_players) == 0:
                fig_distance = empty_fig
            else:
                # Convertir a formato largo
                df_melted = df_players.melt(
                    id_vars=['Player'],
                    value_vars=[
                        "Speed Zones (m) [0.0, 45.0]% (m)",
                        "Speed Zones (m) [45.0, 65.0]% (m)",
                        "Speed Zones (m) [65.0, 75.0]% (m)",
                        "Speed Zones (m) [75.0, 85.0]% (m)",
                        "Speed Zones (m) [85.0, 95.0]% (m)",
                        "Speed Zones (m) [95.0, 100.0]% (m)"
                    ],
                    var_name='Zona',
                    value_name='Distancia'
                )
                
                # Renombrar zonas para la leyenda
                df_melted['Zona'] = df_melted['Zona'].replace({
                    "Speed Zones (m) [0.0, 45.0]% (m)": "Z1",
                    "Speed Zones (m) [45.0, 65.0]% (m)": "Z2",
                    "Speed Zones (m) [65.0, 75.0]% (m)": "Z3",
                    "Speed Zones (m) [75.0, 85.0]% (m)": "Z4",
                    "Speed Zones (m) [85.0, 95.0]% (m)": "Z5",
                    "Speed Zones (m) [95.0, 100.0]% (m)": "Z6"
                })
                
                fig_distance = px.bar(
                    df_melted,
                    y="Player",
                    x="Distancia",
                    color="Zona",
                    orientation="h",
                    title="<b>Distancia por Zonas de Velocidad</b>",
                    color_discrete_map={
                        "Z1": "#e4dcc6",
                        "Z2": "#d9cdb2",
                        "Z3": "#cfc09e",
                        "Z4": "#c3b89a",
                        "Z5": "#FF0000",
                        "Z6": "#CF0000"
                    },
                    labels={
                        "Distancia": "Distancia (m)",
                        "Player": "Jugador",
                        "Zona": "Zona de Velocidad"
                    }
                )
                
                fig_distance.update_layout(
                    plot_bgcolor="white",
                    paper_bgcolor="white",
                    font=dict(family="Arial, sans-serif", size=12, color="#2c3e50"),
                    title=dict(x=0.5, font=dict(size=16, color="#2c3e50")),
                    margin=dict(t=80, b=80, l=120, r=50),
                    barmode="stack",
                    height=550,
                    legend=dict(
                        orientation="h",
                        yanchor="top",
                        y=-0.15,
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
            
            if len(df_players) == 0:
                fig_hsr = empty_fig
            else:
                # Eliminar NaNs en la métrica
                df_hsr = df_players.dropna(subset=["Abs HSR(m)"])
                
                if len(df_hsr) == 0:
                    fig_hsr = empty_fig
                else:
                    # Agrupar por jugador y sumar la métrica
                    df_agrupado = df_hsr.groupby("Player", as_index=False)["Abs HSR(m)"].sum()
                    
                    # Calcular promedio
                    promedio_hsr = df_agrupado["Abs HSR(m)"].mean()
                    
                    fig_hsr = px.bar(
                        df_agrupado,
                        y="Player",
                        x="Abs HSR(m)",
                        orientation="h",
                        title="<b>Distancia en Alta Velocidad (HSR)</b>",
                        color_discrete_sequence=["#525252"],
                        labels={
                            "Abs HSR(m)": "HSR (m)",
                            "Player": "Jugador"
                        }
                    )
                    
                    # Añadir línea del promedio
                    fig_hsr.add_shape(
                        type="line",
                        x0=promedio_hsr,
                        x1=promedio_hsr,
                        y0=0,
                        y1=1,
                        line=dict(color="#e74c3c", width=3, dash="dash"),
                        xref="x",
                        yref="paper"
                    )
                    
                    fig_hsr.add_annotation(
                        x=promedio_hsr,
                        y=1.05,
                        xref="x",
                        yref="paper",
                        text=f"<b>Promedio: {float(promedio_hsr):.1f} m</b>",
                        showarrow=False,
                        font=dict(color="#e74c3c", size=12),
                        bgcolor="white",
                        bordercolor="#e74c3c",
                        borderwidth=1
                    )
                    
                    fig_hsr.update_layout(
                        plot_bgcolor="white",
                        paper_bgcolor="white",
                        font=dict(family="Arial, sans-serif", size=12, color="#2c3e50"),
                        title=dict(x=0.5, font=dict(size=16, color="#2c3e50")),
                        margin=dict(t=70, b=50, l=120, r=50),
                        height=500
                    )
                    
                    fig_hsr.update_xaxes(
                        title_font=dict(size=14, color="#2c3e50"),
                        tickfont=dict(size=11, color="#2c3e50"),
                        gridcolor="#ecf0f1",
                        showgrid=True
                    )
                    
                    fig_hsr.update_yaxes(
                        title_font=dict(size=14, color="#2c3e50"),
                        tickfont=dict(size=11, color="#2c3e50"),
                        gridcolor="#ecf0f1",
                        showgrid=True
                    )
            
            # ============================================================================
            # GRÁFICO 3: ACC
            # ============================================================================
            
            if len(df_players) == 0:
                fig_acc = empty_fig
            else:
                # Convertir a formato largo
                df_melted_acc = df_players.melt(
                    id_vars=['Player'],
                    value_vars=[
                        "Acceleration Zones  [0, 50]% Cnt",
                        "Acceleration Zones  [50, 60]% Cnt"
                    ],
                    var_name='Zona',
                    value_name='Cuenta'
                )
                
                # Renombrar zonas
                df_melted_acc['Zona'] = df_melted_acc['Zona'].replace({
                    "Acceleration Zones  [0, 50]% Cnt": "[Z1]",
                    "Acceleration Zones  [50, 60]% Cnt": "[Z2]"
                })
                
                fig_acc = px.bar(
                    df_melted_acc,
                    y="Player",
                    x="Cuenta",
                    color="Zona",
                    orientation="h",
                    title="<b>Aceleraciones por Zona de Intensidad</b>",
                    color_discrete_map={
                        "[Z1]": "#38a838",
                        "[Z2]": "#01fa16"
                    },
                    labels={
                        "Cuenta": "Número de Aceleraciones",
                        "Player": "Jugador",
                        "Zona": "Zona de Aceleración"
                    }
                )
                
                fig_acc.update_layout(
                    plot_bgcolor="white",
                    paper_bgcolor="white",
                    font=dict(family="Arial, sans-serif", size=12, color="#2c3e50"),
                    title=dict(x=0.5, font=dict(size=16, color="#2c3e50")),
                    margin=dict(t=80, b=80, l=120, r=50),
                    height=550,
                    legend=dict(
                        orientation="h",
                        yanchor="top",
                        y=-0.15,
                        xanchor="center",
                        x=0.5
                    )
                )
                
                fig_acc.update_xaxes(
                    title_font=dict(size=14, color="#2c3e50"),
                    tickfont=dict(size=11, color="#2c3e50"),
                    gridcolor="#ecf0f1",
                    showgrid=True
                )
                
                fig_acc.update_yaxes(
                    title_font=dict(size=14, color="#2c3e50"),
                    tickfont=dict(size=11, color="#2c3e50"),
                    gridcolor="#ecf0f1",
                    showgrid=True
                )
            
            # ============================================================================
            # GRÁFICO 4: DCC
            # ============================================================================
            
            if len(df_players) == 0:
                fig_dcc = empty_fig
            else:
                # Convertir a formato largo
                df_melted_dcc = df_players.melt(
                    id_vars=['Player'],
                    value_vars=[
                        "Acceleration Zones  [-50, 0]% Cnt",
                        "Acceleration Zones  [-60, -50]% Cnt"
                    ],
                    var_name='Zona',
                    value_name='Cuenta'
                )
                
                # Renombrar zonas
                df_melted_dcc['Zona'] = df_melted_dcc['Zona'].replace({
                    "Acceleration Zones  [-50, 0]% Cnt": "[Z1]",
                    "Acceleration Zones  [-60, -50]% Cnt": "[Z2]"
                })
                
                fig_dcc = px.bar(
                    df_melted_dcc,
                    y="Player",
                    x="Cuenta",
                    color="Zona",
                    orientation="h",
                    title="<b>Desaceleraciones por Zona de Intensidad</b>",
                    color_discrete_map={
                        "[Z1]": "#a83838",
                        "[Z2]": "#fa0101"
                    },
                    labels={
                        "Cuenta": "Número de Desaceleraciones",
                        "Player": "Jugador",
                        "Zona": "Zona de Desaceleración"
                    }
                )
                
                fig_dcc.update_layout(
                    plot_bgcolor="white",
                    paper_bgcolor="white",
                    font=dict(family="Arial, sans-serif", size=12, color="#2c3e50"),
                    title=dict(x=0.5, font=dict(size=16, color="#2c3e50")),
                    margin=dict(t=80, b=80, l=120, r=50),
                    height=550,
                    legend=dict(
                        orientation="h",
                        yanchor="top",
                        y=-0.15,
                        xanchor="center",
                        x=0.5
                    )
                )
                
                fig_dcc.update_xaxes(
                    title_font=dict(size=14, color="#2c3e50"),
                    tickfont=dict(size=11, color="#2c3e50"),
                    gridcolor="#ecf0f1",
                    showgrid=True
                )
                
                fig_dcc.update_yaxes(
                    title_font=dict(size=14, color="#2c3e50"),
                    tickfont=dict(size=11, color="#2c3e50"),
                    gridcolor="#ecf0f1",
                    showgrid=True
                )
            
            # ============================================================================
            # GRÁFICO 5: VELOCIDAD
            # ============================================================================
            
            if len(df_players) == 0:
                fig_velocidad = empty_fig
            else:
                # Eliminar NaNs en la métrica
                df_velocidad = df_players.dropna(subset=["MAX Speed(km/h)"])
                
                if len(df_velocidad) == 0:
                    fig_velocidad = empty_fig
                else:
                    # Agrupar por jugador y calcular máximo
                    df_agrupado_vel = df_velocidad.groupby("Player", as_index=False)["MAX Speed(km/h)"].max()
                    
                    # Calcular promedio
                    promedio_velocidad = df_agrupado_vel["MAX Speed(km/h)"].mean()
                    
                    fig_velocidad = px.bar(
                        df_agrupado_vel,
                        y="Player",
                        x="MAX Speed(km/h)",
                        orientation="h",
                        title="<b>Velocidad Máxima por Jugador</b>",
                        color_discrete_sequence=["#12A7C2"],
                        labels={
                            "MAX Speed(km/h)": "Velocidad Máxima (km/h)",
                            "Player": "Jugador"
                        }
                    )
                    
                    # Añadir línea del promedio
                    fig_velocidad.add_shape(
                        type="line",
                        x0=promedio_velocidad,
                        x1=promedio_velocidad,
                        y0=0,
                        y1=1,
                        line=dict(color="#e74c3c", width=3, dash="dash"),
                        xref="x",
                        yref="paper"
                    )
                    
                    fig_velocidad.add_annotation(
                        x=promedio_velocidad,
                        y=1.05,
                        xref="x",
                        yref="paper",
                        text=f"<b>Promedio: {float(promedio_velocidad):.1f} km/h</b>",
                        showarrow=False,
                        font=dict(color="#e74c3c", size=12),
                        bgcolor="white",
                        bordercolor="#e74c3c",
                        borderwidth=1
                    )
                    
                    fig_velocidad.update_layout(
                        plot_bgcolor="white",
                        paper_bgcolor="white",
                        font=dict(family="Arial, sans-serif", size=12, color="#2c3e50"),
                        title=dict(x=0.5, font=dict(size=16, color="#2c3e50")),
                        margin=dict(t=70, b=50, l=120, r=50),
                        height=500
                    )
                    
                    fig_velocidad.update_xaxes(
                        title_font=dict(size=14, color="#2c3e50"),
                        tickfont=dict(size=11, color="#2c3e50"),
                        gridcolor="#ecf0f1",
                        showgrid=True
                    )
                    
                    fig_velocidad.update_yaxes(
                        title_font=dict(size=14, color="#2c3e50"),
                        tickfont=dict(size=11, color="#2c3e50"),
                        gridcolor="#ecf0f1",
                        showgrid=True
                    )
            
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
            
            return [fig_distance, fig_hsr, fig_acc, fig_dcc, fig_velocidad, fig_posiciones]
            
        except Exception as e:
            print(f"Error al generar gráficos: {e}")
            import traceback
            traceback.print_exc()
            return [empty_fig] * 6

    # ============================================================================
    # CALLBACK - Z-Score Toggle Button
    # ============================================================================
    
    @app.callback(
        [Output('zscore-toggle-btn', 'children'),
         Output('zscore-toggle-btn', 'style'),
         Output('zscore-state-store', 'data')],
        [Input('zscore-toggle-btn', 'n_clicks')],
        [State('zscore-state-store', 'data')]
    )
    def toggle_zscore_button(n_clicks, current_state):
        """
        Controla o estado do botão Z-Score, alternando entre ativo e inativo.
        
        Args:
            n_clicks (int): Número de cliques no botão
            current_state (dict): Estado atual do Z-Score
            
        Returns:
            tuple: (texto do botão, estilo do botão, novo estado)
        """
        if n_clicks is None:
            n_clicks = 0
        
        # Determinar o novo estado
        if current_state is None:
            current_state = {'active': True}
        
        # Alternar estado a cada clique
        new_active = not current_state.get('active', True) if n_clicks > 0 else current_state.get('active', True)
        
        # Definir texto e estilo baseado no estado
        if new_active:
            button_text = "Z Score"
            button_style = {
                'background-color': '#6c757d',
                'color': 'white',
                'border': '1px solid #6c757d',
                'border-radius': '4px',
                'padding': '8px 16px',
                'cursor': 'pointer',
                'font-size': '14px'
            }
        else:
            button_text = "Diferencia %"
            button_style = {
                'background-color': '#6c757d',
                'color': 'white',
                'border': '1px solid #6c757d',
                'border-radius': '4px',
                'padding': '8px 16px',
                'cursor': 'pointer',
                'font-size': '14px'
            }
        
        return button_text, button_style, {'active': new_active}

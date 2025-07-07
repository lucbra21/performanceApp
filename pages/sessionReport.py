# ============================================================================
# IMPORTACIONES
# ============================================================================

# Importações de Dash
from dash import html, dcc, Output, Input, State, callback_context, dash_table
import dash

# Importaciones del sistema y utilidades
import os
import datetime
import polars as pl
import pandas as pd
from utils.utils import DATA_GPS_PATH, calcular_estadisticas

# ============================================================================
# ESTILOS PARA DATATABLES - CENTRALIZADOS PARA MEJOR ORGANIZACIÓN
# ============================================================================

# Estilos para tabla de jugadores
PLAYERS_TABLE_STYLES = {
    'style_table': {
        'overflowX': 'auto',
        'maxHeight': '600px',
        'overflowY': 'auto'
    },
    'style_cell': {
        'textAlign': 'left',
        'padding': '10px',
        'fontFamily': 'Arial, sans-serif',
        'fontSize': '14px',
        'border': '1px solid #ddd'
    },
    'style_header': {
        'backgroundColor': '#f8f9fa',
        'fontWeight': 'bold',
        'textAlign': 'center',
        'border': '1px solid #ddd'
    },
    'style_data': {
        'backgroundColor': 'white',
        'border': '1px solid #ddd'
    },
    'style_data_conditional': [
        {
            'if': {'row_index': 'odd'},
            'backgroundColor': '#f8f9fa'
        }
    ]
}

# Estilos para tabla combinada de estadísticas
COMBINED_TABLE_STYLES = {
    'style_table': {
        'overflowX': 'auto',
        'maxHeight': '600px',
        'overflowY': 'auto'
    },
    'style_cell': {
        'textAlign': 'left',
        'padding': '8px',
        'fontFamily': 'Arial, sans-serif',
        'fontSize': '13px',
        'border': '1px solid #ddd'
    },
    'style_header': {
        'backgroundColor': '#e8f4fd',
        'fontWeight': 'bold',
        'textAlign': 'center',
        'border': '1px solid #ddd'
    },
    'style_data': {
        'backgroundColor': 'white',
        'border': '1px solid #ddd'
    },
    'style_data_conditional': [
        {
            'if': {'row_index': 'odd'},
            'backgroundColor': '#f8f9fa'
        },
        {
            'if': {'filter_query': '{_tipo_interno} = JugadorIndividual'},
            'backgroundColor': '#e8f5e8'
        },
        {
            'if': {'filter_query': '{_tipo_interno} = Jugador'},
            'backgroundColor': '#f0f8ff'
        },
        {
            'if': {'filter_query': '{_tipo_interno} = Equipo'},
            'backgroundColor': '#e8f4fd'
        },
        {
            'if': {'filter_query': '{_tipo_interno} = Posición'},
            'backgroundColor': '#fff3cd'
        }
    ]
}



# ============================================================================
# FUNCIONES AUXILIARES
# ============================================================================

def get_sorted_dates():
        """Función auxiliar para obtener fechas ordenadas cronológicamente del archivo parquet"""
        try:
            path_to_parquet = os.path.join(DATA_GPS_PATH, 'df_gps.parquet')
            if not os.path.exists(path_to_parquet):
                return []
                
            df = pl.read_parquet(path_to_parquet)
            if df.height == 0 or 'Date' not in df.columns:
                return []
                
            # Obtener fechas únicas del DataFrame
            fechas_raw = df.select('Date').unique()['Date'].to_list()
            
            # Convertir todas las fechas al formato dd/mm/aaaa y ordenar cronológicamente
            fechas_datetime = []
            for fecha in fechas_raw:
                try:
                    if isinstance(fecha, str):
                        # Convertir diferentes formatos a dd/mm/aaaa
                        if '/' in fecha:
                            # Ya está en formato dd/mm/aaaa
                            fecha_dt = datetime.datetime.strptime(fecha, '%d/%m/%Y')
                            fecha_formatted = fecha
                        elif '-' in fecha:
                            # Convertir de aaaa-mm-dd a dd/mm/aaaa
                            fecha_dt = datetime.datetime.strptime(fecha, '%Y-%m-%d')
                            fecha_formatted = fecha_dt.strftime('%d/%m/%Y')
                        else:
                            continue
                        fechas_datetime.append((fecha_dt, fecha_formatted))
                    else:
                        # Si es datetime object, convertir a string dd/mm/aaaa
                        fecha_formatted = fecha.strftime('%d/%m/%Y')
                        fechas_datetime.append((fecha, fecha_formatted))
                except Exception as e:
                    print(f"Error procesando fecha {fecha}: {e}")
                    continue
            
            # Ordenar cronológicamente y extraer solo las fechas en formato dd/mm/aaaa
            fechas_datetime.sort(key=lambda x: x[0])
            fechas_ordenadas = [fecha_str for fecha_dt, fecha_str in fechas_datetime]
            
            return fechas_ordenadas
            
        except Exception as e:
            print(f"Error obteniendo fechas del parquet: {e}")
            return []

def get_latest_date_for_picker():
    """Obtiene la fecha más reciente en formato YYYY-MM-DD para el DatePickerSingle"""
    try:
        fechas_ordenadas = get_sorted_dates()
        if not fechas_ordenadas:
            return None
        
        # Obtener la última fecha (más reciente) y convertir a formato YYYY-MM-DD
        latest_date_str = fechas_ordenadas[-1]  # Última fecha en formato dd/mm/aaaa
        latest_date_dt = datetime.datetime.strptime(latest_date_str, '%d/%m/%Y')
        return latest_date_dt.strftime('%Y-%m-%d')  # Formato para DatePickerSingle
        
    except Exception as e:
        print(f"Error obteniendo fecha más reciente: {e}")
        return None
    

def get_columns_of_interest():
    """Carga las columnas de interés desde el archivo de texto"""
    try:
        columns_file = os.path.join(DATA_GPS_PATH, 'Columnas_interés.txt')
        if not os.path.exists(columns_file):
            return []
        
        with open(columns_file, 'r', encoding='utf-8') as f:
            columns = [line.strip() for line in f.readlines() if line.strip()]
        
        return columns
        
    except Exception as e:
        print(f"Error al cargar columnas de interés: {e}")
        return []

def format_and_filter_date(selected_date):
    """
    Formatea la fecha seleccionada y filtra el dataframe para esa fecha.
    """
    
    path_to_parquet = os.path.join(DATA_GPS_PATH, 'df_gps.parquet')
    if not os.path.exists(path_to_parquet):
        print(f"Archivo parquet no existe: {path_to_parquet}")
        return None
        
    df = pl.read_parquet(path_to_parquet)
    df_filtered = (df.filter(pl.col('Match Day') != 'Rehab')
                      .filter(pl.col('Player') != 'TEAM')
                      .filter(pl.col('Team ') != 'TEAM')
                      .filter(pl.col('Selection') == 'Drills')
                      .with_columns(
                          pl.when(pl.col('Team ').str.contains('Sporting'))
                          .then(pl.lit('Sporting de Gijón'))
                          .otherwise(pl.col('Team '))
                          .alias('Team ')
                      ))
    
    # Convertir la fecha seleccionada al formato correcto
    if isinstance(selected_date, str):
        try:
            selected_dt = datetime.datetime.strptime(selected_date, '%Y-%m-%d')
            formatted_date = selected_dt.strftime('%d/%m/%Y')
        except:
            formatted_date = selected_date
    else:
        formatted_date = selected_date
        
    #print(f"Buscando datos para fecha: {formatted_date}")
    
    # Filtrar datos para la fecha formateada
    df_fecha = df_filtered.filter(pl.col('Date') == formatted_date)
    #print(f"Encontradas {df_fecha.height} filas para la fecha {formatted_date}")
    
    if df_fecha is None or df_fecha.height == 0:
        print("No se encontraron datos para ningún formato de fecha")
        # Mostrar algunas fechas disponibles para debug
        available_dates = df.select('Date').unique().limit(5)['Date'].to_list()
        print(f"Fechas disponibles (muestra): {available_dates}")
        return None
        
    return df_fecha, formatted_date


def filter_and_get_players_data(selected_date):
    """Filtra los datos GPS por fecha y aplica los filtros especificados"""
    try:
        path_to_parquet = os.path.join(DATA_GPS_PATH, 'df_gps.parquet')
        if not os.path.exists(path_to_parquet):
            print(f"Archivo parquet no existe: {path_to_parquet}")
            return None
        
        df = pl.read_parquet(path_to_parquet)
        #print(f"DataFrame cargado con {df.height} filas y {len(df.columns)} columnas")
        
        df_fecha, formatted_date = format_and_filter_date(selected_date)
        
        #print(f"Datos encontrados para la fecha: {df_fecha.height} filas")
        # Aplicar filtros como en utils.py
        df_filtered = (df_fecha.filter(pl.col('Match Day') != 'Rehab')
                      .filter(pl.col('Player') != 'TEAM')
                      .filter(pl.col('Team ') != 'TEAM')
                      .filter(pl.col('Selection') == 'Drills')
                      .with_columns(
                          pl.when(pl.col('Team ').str.contains('Sporting'))
                          .then(pl.lit('Sporting de Gijón'))
                          .otherwise(pl.col('Team '))
                          .alias('Team ')
                      ))
        #print(f"Después de filtros: {df_filtered.height} filas")
        
        # Obtener columnas de interés
        columns_of_interest = get_columns_of_interest()
        #print(f"Columnas de interés: {columns_of_interest}")
        
        # Seleccionar columnas básicas + columnas de interés que existan en el DataFrame
        basic_columns = ['Player']
        available_columns = [col for col in columns_of_interest if col in df_filtered.columns]
        #print(f"Columnas disponibles en DataFrame: {available_columns}")
        
        selected_columns = basic_columns + available_columns
        #print(f"Columnas seleccionadas: {selected_columns}")
        
        if df_filtered.height > 0:
            result_df = df_filtered.select(selected_columns)
            #print(f"DataFrame resultado: {result_df.height} filas, {len(result_df.columns)} columnas")
            return result_df
        else:
            print("DataFrame filtrado está vacío")
            return None
            
    except Exception as e:
        print(f"Error al filtrar datos: {e}")
        import traceback
        traceback.print_exc()
        return None


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
        
        # Container unificado para información de sesión y tabla de jugadores
        html.Div([
            html.Div(id='session-info-output'),
            html.Div(id='players-table-output'),
            html.Div(id='team-diff-cards-output')
        ], className="session-and-players-container")
    ])
])

# ============================================================================
# CALLBACKS
# ============================================================================

def register_callbacks(app):
    """Registra todos los callbacks de la página"""
    
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
        """Función unificada para manejar navegación de fechas y configuración del calendario"""
        ctx = dash.callback_context
        
        # Obtener fechas ordenadas cronológicamente en formato dd/mm/aaaa
        fechas_ordenadas = get_sorted_dates()
        
        if not fechas_ordenadas:
            return dash.no_update, None, None, []
        
        # Configurar límites del calendario (convertir a datetime.date para el DatePickerSingle)
        try:
            min_date = datetime.datetime.strptime(fechas_ordenadas[0], '%d/%m/%Y').date()
            max_date = datetime.datetime.strptime(fechas_ordenadas[-1], '%d/%m/%Y').date()
        except:
            min_date = max_date = None
        
        # Si no hay trigger (inicialización de la página), retornar la fecha más reciente
        if not ctx.triggered:
            # Convertir la fecha más reciente (última en la lista) a formato YYYY-MM-DD
            try:
                latest_date_dt = datetime.datetime.strptime(fechas_ordenadas[-1], '%d/%m/%Y')
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
                    current_dt = datetime.datetime.strptime(current_date, '%Y-%m-%d')
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
                current_dt = datetime.datetime.strptime(current_date_formatted, '%d/%m/%Y')
                fechas_dt = [datetime.datetime.strptime(f, '%d/%m/%Y') for f in fechas_ordenadas]
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
            new_date_dt = datetime.datetime.strptime(new_date, '%d/%m/%Y')
            new_date_formatted = new_date_dt.strftime('%Y-%m-%d')
        except:
            new_date_formatted = new_date
        
        return new_date_formatted, min_date, max_date, []
    
    
    @app.callback(
        Output('session-info-output', 'children'),
        [Input('date-selector', 'date'),
         Input('statistic-selector', 'value')]
    )
    def update_session_info(selected_date, selected_statistic):
        """Actualiza la información de la sesión basada en la fecha y estadística seleccionadas"""
        if not selected_date:
            return html.Div("Selecciona una fecha para ver la información de la sesión.", 
                          className="info-message")
        
        try:
            path_to_parquet = os.path.join(DATA_GPS_PATH, 'df_gps.parquet')
            if not os.path.exists(path_to_parquet):
                return html.Div("No se encontró el archivo de datos.", 
                              className="error-message")
            
            df = pl.read_parquet(path_to_parquet)
            
            df_fecha, formatted_date = format_and_filter_date(selected_date)

            
            if df_fecha is None or df_fecha.height == 0:
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
    
    @app.callback(
        Output('players-table-output', 'children'),
        [Input('date-selector', 'date'),
         Input('statistic-selector', 'value')]
    )
    def update_players_table(selected_date, selected_statistic):
        """Actualiza la tabla de jugadores con datos de jugadores, equipos y posiciones"""
        
        if not selected_date:
            return html.Div("Selecciona una fecha para ver los datos de los jugadores.", 
                          className="info-message")
        
        if not selected_statistic:
            return html.Div("Selecciona una estadística para ver los datos.", 
                          className="info-message")
        
        try:
            # Convertir fecha al formato correcto para calcular_estadisticas
            df_fecha, formatted_date = format_and_filter_date(selected_date)
            
            # Obtener columnas de interés
            columnas_interes = get_columns_of_interest()
            
            # Obtener datos individuales de jugadores para la fecha seleccionada
            df_individual_players = filter_and_get_players_data(formatted_date)
            #print(df_individual_players)
            
            # Calcular estadísticas para la fecha y estadística seleccionadas
            df_players, df_position, df_team = calcular_estadisticas( fecha=formatted_date, columnas_interes=columnas_interes, estadistica=selected_statistic)
            
            # Crear dataframe combinado con todos los datos
            combined_data_all = []
            combined_info = []
            
            # Agregar datos individuales de jugadores 
            if df_individual_players is not None and df_individual_players.height > 0:
                df_individual_pandas = df_individual_players.to_pandas()
                
                # Renombrar columna Player a Player/Team/Position
                if 'Player' in df_individual_pandas.columns:
                    df_individual_pandas = df_individual_pandas.rename(columns={'Player': 'Player/Team/Position'})

                # Agregar identificador interno para estilos
                df_individual_pandas['_tipo_interno'] = 'JugadorIndividual'
                
                # Redondear valores numéricos
                numeric_cols_individual = df_individual_pandas.select_dtypes(include=['float64', 'int64']).columns
                for col in numeric_cols_individual:
                    df_individual_pandas[col] = df_individual_pandas[col].round(2)
                combined_data_all.extend(df_individual_pandas.to_dict('records'))
                combined_info.append(f"Jugadores Individuales: {df_individual_players.height} registros")
            
            # Agregar datos de equipos
            if df_team is not None and df_team.height > 0:
                df_team_pandas = df_team.to_pandas()
                
                # Renombrar columna Team a Player/Team/Position
                if 'Team' in df_team_pandas.columns:
                    df_team_pandas = df_team_pandas.rename(columns={'Team': 'Player/Team/Position'})
                
                # Filtrar solo columnas de interés y Player/Team/Position
                available_columns = ['Player/Team/Position'] + [col for col in columnas_interes if col in df_team_pandas.columns]
                df_team_pandas = df_team_pandas[available_columns]
                
                # Agregar identificador interno para estilos
                df_team_pandas['_tipo_interno'] = 'Equipo'
                
                # Redondear valores numéricos
                numeric_cols_team = df_team_pandas.select_dtypes(include=['float64', 'int64']).columns
                for col in numeric_cols_team:
                    df_team_pandas[col] = df_team_pandas[col].round(2)
                combined_data_all.extend(df_team_pandas.to_dict('records'))
                combined_info.append(f"Equipos: {df_team.height} registros")
            
            # Agregar datos de posiciones
            if df_position is not None and df_position.height > 0:
                df_position_pandas = df_position.to_pandas()
                
                # Renombrar columna Position a Player/Team/Position
                if 'Position' in df_position_pandas.columns:
                    df_position_pandas = df_position_pandas.rename(columns={'Position': 'Player/Team/Position'})
                
                # Filtrar solo columnas de interés y Player/Team/Position
                available_columns = ['Player/Team/Position'] + [col for col in columnas_interes if col in df_position_pandas.columns]
                df_position_pandas = df_position_pandas[available_columns]
                
                # Agregar identificador interno para estilos
                df_position_pandas['_tipo_interno'] = 'Posición'
                
                # Redondear valores numéricos
                numeric_cols_position = df_position_pandas.select_dtypes(include=['float64', 'int64']).columns
                for col in numeric_cols_position:
                    df_position_pandas[col] = df_position_pandas[col].round(2)
                combined_data_all.extend(df_position_pandas.to_dict('records'))
                combined_info.append(f"Posiciones: {df_position.height} registros")
            
            # Crear tabla única con todos los dados
            if combined_data_all:
                # Obtener todas las columnas únicas
                all_columns = set()
                for record in combined_data_all:
                    all_columns.update(record.keys())
                
                # Reorganizar columnas para que 'Player/Team/Position' esté al principio, excluyendo el identificador interno
                columns_order = ['Player/Team/Position'] + [col for col in sorted(all_columns) if col not in ['Player/Team/Position', '_tipo_interno']]
                
                # Completar registros faltantes con valores vacíos
                for record in combined_data_all:
                    for col in columns_order:
                        if col not in record:
                            record[col] = 'NULL'
                
                # Determinar columnas numéricas
                numeric_columns_combined = []
                if combined_data_all:
                    sample_record = combined_data_all[0]
                    for col in columns_order:
                        if col not in ['Player/Team/Position', '_tipo_interno'] and col in sample_record:
                            try:
                                float(sample_record[col])
                                numeric_columns_combined.append(col)
                            except (ValueError, TypeError):
                                pass
                
                # Filtrar datos para mostrar solo las columnas visibles
                filtered_data_all = []
                for record in combined_data_all:
                    filtered_record = {col: record.get(col, '') for col in columns_order}
                    
                    # Mantener _tipo_interno para estilos pero no mostrarlo
                    filtered_record['_tipo_interno'] = record.get('_tipo_interno', '')
                    filtered_data_all.append(filtered_record)
                
                combined_table = dash_table.DataTable(
                    id='combined-all-stats-table',
                    data=filtered_data_all,
                    columns=[
                        {"name": col, "id": col, "type": "numeric" if col in numeric_columns_combined else "text"}
                        for col in columns_order
                    ],
                    **COMBINED_TABLE_STYLES,
                    sort_action="native",
                    filter_action="native",
                    page_action="none",
                    export_format="xlsx",
                    export_headers="display"
                )
                
                # Crear información de resumen
                combined_info_text = ' | '.join(combined_info)
                
                return html.Div([
                    html.H5('Datos Combinados - Jugadores, Equipos y Posiciones', className="section-subtitle"),
                    html.P(f"Mostrando {combined_info_text}", className="table-info"),
                    combined_table
                ], className="combined-stats-table-container")
            else:
                return html.Div("No se encontraron datos para la fecha y estadística seleccionadas.", 
                              className="warning-message")
            
        except Exception as e:
            return html.Div(f"Error al cargar tabla de jugadores: {str(e)}", 
                          className="error-message")
    
    @app.callback(
        Output('team-diff-cards-output', 'children'),
        [Input('date-selector', 'date'),
         Input('statistic-selector', 'value')]
    )
    def update_team_diff_cards(selected_date, selected_statistic):
        """Crea cartões mostrando as colunas com 'Diff' do df_team"""
        
        if not selected_date or not selected_statistic:
            return html.Div()
        
        try:
            # Convertir fecha al formato correcto para calcular_estadisticas
            df_fecha, formatted_date = format_and_filter_date(selected_date)
            # Obtener columnas de interés
            columnas_interes = get_columns_of_interest()
            
             # Carregar dados originais do arquivo parquet
            try:
                
                        match_day_value = df_fecha['Match Day'].unique().to_list()[0]
                        #print(match_day_value)
                        
                        # Agora carregar e filtrar df_team_estadisticas
                        df_team_estadisticas_path = os.path.join(DATA_GPS_PATH, '../processed/df_team_estadisticas.parquet')
                        if os.path.exists(df_team_estadisticas_path):
                            df_team_estadisticas = pl.read_parquet(df_team_estadisticas_path)
                            # Filtrar dados originais pelo Match Day correto e Estatística
                            df_team_estadisticas_filtered = df_team_estadisticas.filter(
                                (pl.col('Match Day') == match_day_value) & 
                                (pl.col('Estadistica') == selected_statistic)
                            )          
                        else:
                            df_team_estadisticas_filtered = None

            except Exception as e:
                print(f"Error cargando archivo original: {e}")
                df_team_estadisticas_filtered = None
            
            # print(df_team_estadisticas_filtered)
            # print("df_team_estadisticas_filtered.height = ", df_team_estadisticas_filtered.height )
            
            # Calcular estadísticas para la fecha y estadística seleccionadas
            df_players, df_position, df_team = calcular_estadisticas(
                fecha=formatted_date, 
                columnas_interes=columnas_interes, 
                estadistica=selected_statistic
            )
            
            if df_team is None or df_team.height == 0:
                return html.Div()
            
            # Convertir a pandas para facilitar el manejo
            df_team_pandas = df_team.to_pandas()
            
            # Encontrar columnas que terminan con ' diff'
            diff_columns = [col for col in df_team_pandas.columns if col.endswith(' diff')]
            
            if not diff_columns:
                return html.Div()
            
            # Crear cartões para cada coluna diff
            cards = []
            
            # Título da seção
            cards.append(
                html.H5('Diferencias Porcentuales del Equipo', 
                        className="section-subtitle", 
                        style={'margin-top': '30px', 'margin-bottom': '20px'})
            )
            
            # Container para os cartões
            cards_container = []
            
            for col in diff_columns:
                # Obter o nome da métrica original (sem ' diff')
                metric_name = col.replace(' diff', '')
                
                # Obter o valor da diferença (assumindo que há apenas uma linha de equipe)
                if len(df_team_pandas) > 0:
                    diff_value = df_team_pandas[col].iloc[0]
                    # print(col)
                    # Obter valor referencia da métrica correspondente
                    original_value = None
                    
                    if df_team_estadisticas_filtered is not None and df_team_estadisticas_filtered.height > 0:
                        try:
                            df_original_pandas = df_team_estadisticas_filtered.to_pandas()
                            if col in df_original_pandas.columns and len(df_original_pandas) > 0:
                                original_value = df_original_pandas[col].iloc[0]
                                if pd.isna(original_value):
                                    original_value = None
                        except Exception as e:
                            print(f"Error obteniendo valor original para {metric_name}: {e}")
                            original_value = None
                    # print("original_value = ", original_value)
                    
                    # Formatar valor da diferença
                    if pd.isna(diff_value):
                        formatted_diff_value = 'N/A'
                    else:
                        formatted_diff_value = f'{diff_value:.2f}%'
                    
                    # Formatar valor referencia
                    if pd.isna(original_value):
                            formatted_original_value = 'N/A'
                    else:
                        formatted_original_value = f'{original_value:.2f}%'
                    
                    # Calcular diferencia absoluta si hay valor original
                    formatted_difference = 'N/A'
                    difference_value = None  # Inicializar la variable

                    if not pd.isna(original_value) and not pd.isna(diff_value):
                        # Calcular el valor de referencia basado en la diferencia porcentual
                        difference_value = diff_value - original_value 
                        formatted_difference = f'{difference_value:.2f}%'
                     
                     # Determinar cor baseada no valor
                    if pd.isna(difference_value) or difference_value == 0 or difference_value is None:
                        card_color = '#f8f9fa'  # Cinza neutro
                        text_color = '#6c757d'
                    elif difference_value < 10:
                        card_color = '#d4edda'  # Verde claro
                        text_color = '#155724'
                    else:
                        card_color = '#f8d7da'  # Vermelho claro
                        text_color = '#721c24'
                        
                    # Crear cartão con layout vertical optimizado
                    card_content = [
                         # Título de la métrica
                         html.H5(metric_name, 
                                 style={'margin': '0 0 8px 0', 'font-weight': 'bold', 
                                        'color': text_color, 'text-align': 'center', 'font-size': '16px'}),
                         
                         # Sección de diferencia porcentual
                         html.Div([
                             html.P("Con respecto al partido", 
                                    style={'margin': '0 0 4px 0', 'font-size': '11px', 
                                           'color': text_color, 'font-style': 'italic', 'text-align': 'center'}),
                             html.Div([
                                 html.Span(formatted_diff_value, 
                                           style={'font-size': '20px', 'font-weight': 'bold', 'color': text_color})
                             ], style={'display': 'flex', 'align-items': 'center', 'justify-content': 'center'})
                         ], style={'margin-bottom': '12px'}),
                         
                         # Sección de valor original
                         html.Div([
                             html.P("Con respecto a la referencia", 
                                    style={'margin': '0 0 4px 0', 'font-size': '11px', 
                                           'color': text_color, 'font-style': 'italic', 'text-align': 'center'}),
                             html.P(formatted_original_value if original_value is not None else 'N/A', 
                                    style={'margin': '0 0 12px 0', 'font-size': '16px', 
                                           'color': text_color, 'font-weight': 'bold', 'text-align': 'center'})
                         ]),
                         
                         # Sección de diferencia absoluta
                         html.Div([
                             html.P("Diferencia", 
                                    style={'margin': '0 0 4px 0', 'font-size': '11px', 
                                           'color': text_color, 'font-style': 'italic', 'text-align': 'center'}),
                             html.P(formatted_difference, 
                                    style={'margin': '0', 'font-size': '14px', 
                                           'color': text_color, 'font-weight': 'bold', 'text-align': 'center'})
                         ])
                     ]
                     
                    card = html.Div(card_content, style={
                         'background-color': card_color,
                         'border': f'1px solid {text_color}',
                         'border-radius': '8px',
                         'padding': '12px',
                         'margin': '0',
                         'width': '100%',
                         'height': '100%',
                         'box-shadow': '0 2px 4px rgba(0,0,0,0.1)',
                         'display': 'flex',
                         'flex-direction': 'column',
                         'justify-content': 'space-between'
                     })
                    
                    cards_container.append(card)
            
            # Container com layout em grid 4x3
            if cards_container:
                 cards.append(
                     html.Div(cards_container, style={
                         'display': 'grid',
                         'grid-template-columns': 'repeat(4, 1fr)',
                         'grid-template-rows': 'repeat(3, 1fr)',
                         'gap': '15px',
                         'margin-top': '10px',
                         'max-width': '100%'
                     })
                 )
            
            return html.Div(cards)
            
        except Exception as e:
            return html.Div(f"Error al cargar cartões de diferencias: {str(e)}", 
                          className="error-message")
            
    

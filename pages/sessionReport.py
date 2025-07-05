# ============================================================================
# IMPORTACIONES
# ============================================================================

# Importações de Dash
from dash import html, dcc, Output, Input, State, callback_context, dash_table
import dash

# Importaciones del sistema y utilidades
import os
import datetime
import json
import polars as pl
from utils.utils import DATA_GPS_PATH

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

def save_last_selected_date(date_str):
    """Guarda la última fecha seleccionada en un archivo JSON"""
    try:
        last_date_file = os.path.join('data', 'last_selected_date.json')
        data = {
            'last_date': date_str,
            'timestamp': datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        }
        
        with open(last_date_file, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
            
    except Exception as e:
        print(f"Error guardando última fecha: {e}")

def load_last_selected_date():
    """Carga la última fecha seleccionada desde el archivo JSON"""
    try:
        last_date_file = os.path.join('data', 'last_selected_date.json')
        
        if not os.path.exists(last_date_file):
            return None
            
        with open(last_date_file, 'r', encoding='utf-8') as f:
            data = json.load(f)
            
        return data.get('last_date')
        
    except Exception as e:
        print(f"Error cargando última fecha: {e}")
        return None

def get_initial_date():
    """Obtiene la fecha inicial para el DatePickerSingle al cargar la página"""
    try:
        # Intentar cargar la última fecha seleccionada
        last_date = load_last_selected_date()
        
        if last_date:
            # Convertir la fecha guardada (dd/mm/aaaa) a formato aaaa-mm-dd para el DatePickerSingle
            if '/' in last_date:
                last_date_dt = datetime.datetime.strptime(last_date, '%d/%m/%Y')
                return last_date_dt.strftime('%Y-%m-%d')
            else:
                return last_date
        
        # Si no hay fecha guardada, usar la fecha más reciente disponible
        fechas_ordenadas = get_sorted_dates()
        if fechas_ordenadas:
            most_recent_date = fechas_ordenadas[-1]  # Última fecha en la lista ordenada
            most_recent_dt = datetime.datetime.strptime(most_recent_date, '%d/%m/%Y')
            return most_recent_dt.strftime('%Y-%m-%d')
        
        # Si no hay fechas disponibles, retornar None
        return None
        
    except Exception as e:
        print(f"Error obteniendo fecha inicial: {e}")
        return None

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


def filter_and_get_players_data(selected_date):
    """Filtra los datos GPS por fecha y aplica los filtros especificados"""
    try:
        path_to_parquet = os.path.join(DATA_GPS_PATH, 'df_gps.parquet')
        if not os.path.exists(path_to_parquet):
            return None
        
        df = pl.read_parquet(path_to_parquet)
        
        # Convertir la fecha seleccionada al formato correcto
        if isinstance(selected_date, str):
            try:
                selected_dt = datetime.datetime.strptime(selected_date, '%Y-%m-%d')
                date_formats = [
                    selected_dt.strftime('%d/%m/%Y'),
                    selected_dt.strftime('%Y-%m-%d'),
                    selected_date
                ]
            except:
                date_formats = [selected_date]
        else:
            date_formats = [selected_date]
        
        # Buscar datos para cualquiera de los formatos
        df_fecha = None
        for date_format in date_formats:
            df_temp = df.filter(pl.col('Date') == date_format)
            if df_temp.height > 0:
                df_fecha = df_temp
                break
        
        if df_fecha is None or df_fecha.height == 0:
            return None
        
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
        
        # Obtener columnas de interés
        columns_of_interest = get_columns_of_interest()
        
        # Seleccionar columnas básicas + columnas de interés que existan en el DataFrame
        basic_columns = ['Player']
        available_columns = [col for col in columns_of_interest if col in df_filtered.columns]
        
        selected_columns = basic_columns + available_columns
        
        if df_filtered.height > 0:
            result_df = df_filtered.select(selected_columns)
            return result_df
        else:
            return None
            
    except Exception as e:
        print(f"Error al filtrar datos: {e}")
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
                            date=get_initial_date(),
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
            html.Div(id='players-table-output')
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
        
        # Si no hay trigger o es solo inicialización del selector, cargar última fecha guardada
        if not ctx.triggered or ctx.triggered[0]['prop_id'] == 'date-selector.id':
            # Intentar cargar la última fecha seleccionada
            last_date = load_last_selected_date()
            if last_date:
                # Convertir la fecha guardada (dd/mm/aaaa) a formato aaaa-mm-dd para el DatePickerSingle
                try:
                    if '/' in last_date:
                        last_date_dt = datetime.datetime.strptime(last_date, '%d/%m/%Y')
                        last_date_formatted = last_date_dt.strftime('%Y-%m-%d')
                    else:
                        last_date_formatted = last_date
                    
                    # Verificar que la fecha esté en las fechas disponibles
                    if last_date in fechas_ordenadas:
                        return last_date_formatted, min_date, max_date, []
                except:
                    pass
            
            # Si no hay fecha guardada o no es válida, usar la fecha más reciente
            try:
                most_recent_date = fechas_ordenadas[-1]  # Última fecha en la lista ordenada
                most_recent_dt = datetime.datetime.strptime(most_recent_date, '%d/%m/%Y')
                most_recent_formatted = most_recent_dt.strftime('%Y-%m-%d')
                return most_recent_formatted, min_date, max_date, []
            except:
                return dash.no_update, min_date, max_date, []
        
        # Manejar navegación con botones
        if not current_date:
            # Si no hay fecha actual, retornar la primera fecha disponible
            first_date_dt = datetime.datetime.strptime(fechas_ordenadas[0], '%d/%m/%Y')
            first_date_formatted = first_date_dt.strftime('%Y-%m-%d')
            return first_date_formatted, min_date, max_date, []
        
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
        
        # Guardar la nueva fecha seleccionada
        save_last_selected_date(new_date)
        
        # Convertir la nueva fecha de dd/mm/aaaa a aaaa-mm-dd para el DatePickerSingle
        try:
            new_date_dt = datetime.datetime.strptime(new_date, '%d/%m/%Y')
            new_date_formatted = new_date_dt.strftime('%Y-%m-%d')
        except:
            new_date_formatted = new_date
        
        return new_date_formatted, min_date, max_date, []
    
    # Callback para guardar la fecha cuando se selecciona directamente en el DatePickerSingle
    @app.callback(
        Output('date-selector', 'style'),  # Output dummy para permitir el callback
        Input('date-selector', 'date'),
        prevent_initial_call=True
    )
    def save_selected_date(selected_date):
        """Guarda la fecha cuando se selecciona directamente en el DatePickerSingle"""
        if selected_date:
            # Convertir la fecha de aaaa-mm-dd a dd/mm/aaaa antes de guardar
            try:
                if isinstance(selected_date, str) and '-' in selected_date:
                    selected_dt = datetime.datetime.strptime(selected_date, '%Y-%m-%d')
                    date_to_save = selected_dt.strftime('%d/%m/%Y')
                else:
                    date_to_save = selected_date
                
                save_last_selected_date(date_to_save)
            except Exception as e:
                print(f"Error guardando fecha seleccionada: {e}")
        
        return {}  # Retornar estilo vacío (no cambia nada visualmente)
    
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
            
            # Convertir la fecha seleccionada al formato correcto
            if isinstance(selected_date, str):
                try:
                    selected_dt = datetime.datetime.strptime(selected_date, '%Y-%m-%d')
                    # Intentar diferentes formatos para comparar
                    date_formats = [
                        selected_dt.strftime('%d/%m/%Y'),
                        selected_dt.strftime('%Y-%m-%d'),
                        selected_date
                    ]
                except:
                    date_formats = [selected_date]
            else:
                date_formats = [selected_date]
            
            # Buscar datos para cualquiera de los formatos
            df_fecha = None
            for date_format in date_formats:
                df_temp = df.filter(pl.col('Date') == date_format)
                if df_temp.height > 0:
                    df_fecha = df_temp
                    break
            
            if df_fecha is None or df_fecha.height == 0:
                return html.Div(f"No se encontraron datos para la fecha {selected_date}.", 
                              className="warning-message")
            
            # Formatear la fecha al formato dd-mm-aaaa
            formatted_date = selected_date
            if isinstance(selected_date, str):
                try:
                    if '-' in selected_date:
                        date_obj = datetime.datetime.strptime(selected_date, '%Y-%m-%d')
                        formatted_date = date_obj.strftime('%d-%m-%Y')
                    elif '/' in selected_date:
                        # Si ya está en formato dd/mm/yyyy, convertir a dd-mm-yyyy
                        date_obj = datetime.datetime.strptime(selected_date, '%d/%m/%Y')
                        formatted_date = date_obj.strftime('%d-%m-%Y')
                except:
                    formatted_date = selected_date
            
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
        """Actualiza la tabla de jugadores y calcula estadísticas de equipa y posición"""
        if not selected_date:
            return html.Div("Selecciona una fecha para ver los datos de los jugadores.", 
                          className="info-message")
        
        if not selected_statistic:
            return html.Div("Selecciona una estadística para ver los datos.", 
                          className="info-message")
        
        try:
            # Import calcular_estadisticas function
            from utils.utils import calcular_estadisticas
            
            # Convert date to dd/mm/yyyy format for calcular_estadisticas
            if isinstance(selected_date, str):
                try:
                    if '-' in selected_date:
                        date_obj = datetime.datetime.strptime(selected_date, '%Y-%m-%d')
                        fecha_formatted = date_obj.strftime('%d/%m/%Y')
                    else:
                        fecha_formatted = selected_date
                except:
                    fecha_formatted = selected_date
            else:
                fecha_formatted = selected_date
            
            # Get columns of interest
            columnas_interes = get_columns_of_interest()
            
            # Calculate statistics using calcular_estadisticas with selected statistic
            df_jugadores, df_position, df_team = calcular_estadisticas(fecha=fecha_formatted, columnas_interes=columnas_interes, estadistica=selected_statistic)
            
            # Obtener datos filtrados para la tabla de jugadores
            df_filtered = filter_and_get_players_data(selected_date)
            
            if df_filtered is None or df_filtered.height == 0:
                return html.Div(f"No se encontraron datos de jugadores para la fecha {selected_date}.", 
                              className="warning-message")
            
            # Convertir a pandas para dash_table (más fácil de manejar)
            df_pandas = df_filtered.to_pandas()
            
            # Redondear valores numéricos para mejor visualización
            numeric_columns = df_pandas.select_dtypes(include=['float64', 'int64']).columns
            for col in numeric_columns:
                df_pandas[col] = df_pandas[col].round(2)
            
            # Crear la tabla de jugadores
            # Nota: Los estilos están definidos en style_sessionReport.css pero deben aplicarse via style_* 
            # ya que DataTable no soporta className para estilos personalizados
            players_table = dash_table.DataTable(
                id='players-data-table',
                data=df_pandas.to_dict('records'),
                columns=[
                    {"name": col, "id": col, "type": "numeric" if col in numeric_columns else "text"}
                    for col in df_pandas.columns
                ],
                **PLAYERS_TABLE_STYLES,
                sort_action="native",
                filter_action="native",
                page_action="none",
                export_format="xlsx",
                export_headers="display"
            )
            
            # Crear dataframe concatenado con jugadores, equipos y posiciones
            combined_data_all = []
            combined_info = []
            
            # Agregar datos de jugadores
            if df_jugadores is not None and df_jugadores.height > 0:
                df_players_pandas = df_filtered.to_pandas()
                
                # Renombrar columna Player a Player/Team/Position
                if 'Player' in df_players_pandas.columns:
                    df_players_pandas = df_players_pandas.rename(columns={'Player': 'Player/Team/Position'})
                
                # Agregar identificador interno para estilos
                df_players_pandas['_tipo_interno'] = 'Jugador'
                # Redondear valores numéricos
                numeric_cols_players = df_players_pandas.select_dtypes(include=['float64', 'int64']).columns
                for col in numeric_cols_players:
                    df_players_pandas[col] = df_players_pandas[col].round(2)
                combined_data_all.extend(df_players_pandas.to_dict('records'))
                combined_info.append(f"Jugadores: {df_filtered.height} registros")
            
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
            
            # Crear tabla única con todos los datos
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
                            record[col] = ''
                
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
            else:
                combined_table = None
            
            # Crear información de resumen
            stats_info = []
            
            # Crear layout de retorno con tabla única
            if combined_table is not None:
                statistic_labels = {
                    'mean': 'Media',
                    'median': 'Mediana', 
                    'max': 'Máximo',
                    'min': 'Mínimo',
                    'p75': 'Percentil 75',
                    'p90': 'Percentil 90',
                    'p95': 'Percentil 95'
                }
                combined_info_text = ' | '.join(combined_info)
                
                # Crear cartões para colunas de interesse mostrando valores _Diff
                cards_container = []
                if columnas_interes:
                    # Buscar valores das colunas _Diff nos dataframes originais
                    diff_values = {}
                    
                    # Buscar nos dados de jogadores
                    if df_jugadores is not None and df_jugadores.height > 0:
                        df_players_pandas = df_jugadores.to_pandas()
                        for col in columnas_interes:
                            diff_col = f"{col} diff"
                            if diff_col in df_players_pandas.columns:
                                values = df_players_pandas[diff_col].dropna().tolist()
                                if values:
                                    if col not in diff_values:
                                        diff_values[col] = []
                                    diff_values[col].extend(values)
                    
                    # Buscar nos dados de equipos
                    if df_team is not None and df_team.height > 0:
                        df_team_pandas = df_team.to_pandas()
                        for col in columnas_interes:
                            diff_col = f"{col} diff"
                            if diff_col in df_team_pandas.columns:
                                values = df_team_pandas[diff_col].dropna().tolist()
                                if values:
                                    if col not in diff_values:
                                        diff_values[col] = []
                                    diff_values[col].extend(values)
                    
                    # Buscar nos dados de posições
                    if df_position is not None and df_position.height > 0:
                        df_position_pandas = df_position.to_pandas()
                        for col in columnas_interes:
                            diff_col = f"{col} diff"
                            if diff_col in df_position_pandas.columns:
                                values = df_position_pandas[diff_col].dropna().tolist()
                                if values:
                                    if col not in diff_values:
                                        diff_values[col] = []
                                    diff_values[col].extend(values)
                    
                    # Buscar valores da estatística selecionada para cada coluna
                    selected_stat_values = {}
                    
                    # Filtrar dados pela estatística selecionada
                    if df_jugadores is not None and df_jugadores.height > 0:
                        df_players_filtered = df_jugadores.filter(pl.col('Estadistica') == selected_statistic)
                        if df_players_filtered.height > 0:
                            df_players_pandas = df_players_filtered.to_pandas()
                            for col in columnas_interes:
                                diff_col = f"{col} diff"
                                if diff_col in df_players_pandas.columns:
                                    values = df_players_pandas[diff_col].dropna().tolist()
                                    if values:
                                        if col not in selected_stat_values:
                                            selected_stat_values[col] = []
                                        selected_stat_values[col].extend(values)
                    
                    if df_team is not None and df_team.height > 0:
                        df_team_filtered = df_team.filter(pl.col('Estadistica') == selected_statistic)
                        if df_team_filtered.height > 0:
                            df_team_pandas = df_team_filtered.to_pandas()
                            for col in columnas_interes:
                                diff_col = f"{col} diff"
                                if diff_col in df_team_pandas.columns:
                                    values = df_team_pandas[diff_col].dropna().tolist()
                                    if values:
                                        if col not in selected_stat_values:
                                            selected_stat_values[col] = []
                                        selected_stat_values[col].extend(values)
                    
                    if df_position is not None and df_position.height > 0:
                        df_position_filtered = df_position.filter(pl.col('Estadistica') == selected_statistic)
                        if df_position_filtered.height > 0:
                            df_position_pandas = df_position_filtered.to_pandas()
                            for col in columnas_interes:
                                diff_col = f"{col} diff"
                                if diff_col in df_position_pandas.columns:
                                    values = df_position_pandas[diff_col].dropna().tolist()
                                    if values:
                                        if col not in selected_stat_values:
                                            selected_stat_values[col] = []
                                        selected_stat_values[col].extend(values)
                    
                    # Crear cartões para cada coluna que tem valores da estatística selecionada
                    cards = []
                    for col in sorted(columnas_interes):
                        if col in selected_stat_values and selected_stat_values[col]:
                            values = selected_stat_values[col]
                            # Calcular apenas a estatística selecionada
                            if selected_statistic == 'mean':
                                stat_value = sum(values) / len(values)
                                stat_label = 'Media'
                            elif selected_statistic == 'median':
                                sorted_values = sorted(values)
                                n = len(sorted_values)
                                if n % 2 == 0:
                                    stat_value = (sorted_values[n//2-1] + sorted_values[n//2]) / 2
                                else:
                                    stat_value = sorted_values[n//2]
                                stat_label = 'Mediana'
                            elif selected_statistic == 'max':
                                stat_value = max(values)
                                stat_label = 'Máximo'
                            elif selected_statistic == 'min':
                                stat_value = min(values)
                                stat_label = 'Mínimo'
                            elif selected_statistic == 'p75':
                                sorted_values = sorted(values)
                                index = int(0.75 * (len(sorted_values) - 1))
                                stat_value = sorted_values[index]
                                stat_label = 'Percentil 75'
                            elif selected_statistic == 'p90':
                                sorted_values = sorted(values)
                                index = int(0.90 * (len(sorted_values) - 1))
                                stat_value = sorted_values[index]
                                stat_label = 'Percentil 90'
                            elif selected_statistic == 'p95':
                                sorted_values = sorted(values)
                                index = int(0.95 * (len(sorted_values) - 1))
                                stat_value = sorted_values[index]
                                stat_label = 'Percentil 95'
                            else:
                                continue
                            

                            
                            # Determinar classe de cor baseada no valor
                            if stat_value > 100:
                                color_class = 'positive'  # Verde para valores acima de 100%
                            elif stat_value < 100:
                                color_class = 'negative'  # Vermelho para valores abaixo de 100%
                            else:
                                color_class = 'neutral'  # Laranja para 100%
                            
                            card = html.Div([
                                html.H6(col, className="card-title"),
                                html.Div([
                                    html.Div([
                                        html.Span(f"Sesión:", className="session-label"),
                                        html.Span(f"{stat_value:.1f}%", className=f"stat-value {color_class}")
                                    ], className="card-values-container"),
                                    
                                ])
                            ], className="metric-card")
                            cards.append(card)
                    
                    if cards:
                        cards_container = html.Div([
                            html.H5("Resumen de Métricas", className="metrics-section-subtitle"),
                            html.Div(cards, className="metrics-cards-container")
                        ])
                
                return html.Div([
                    # Resumen de estadísticas calculadas
                    html.Div(stats_info, className="statistics-summary") if stats_info else html.Div(),
                    
                    # Tabla única con todos los datos
                    html.Div([
                        html.P(f"Mostrando {combined_info_text}", className="table-info"),
                        combined_table
                    ], className="stats-table-container"),
                    
                    # Container de cartões com métricas
                    cards_container
                ])
            else:
                # Fallback si no hay datos
                return html.Div([
                    html.Div(stats_info, className="statistics-summary") if stats_info else html.Div(),
                    html.Div([
                        html.H5('Datos de Jugadores', className="section-subtitle"),
                        html.P(f"Mostrando {df_filtered.height} registros de jugadores", className="table-info"),
                        players_table
                    ], className="players-main-table")
                ])
            
        except Exception as e:
            return html.Div(f"Error al cargar tabla de jugadores: {str(e)}", 
                          className="error-message")
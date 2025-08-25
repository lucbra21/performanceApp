import polars as pl
import numpy as np
from datetime import datetime
import os
import json


# ============================================================================
# CONFIGURACIÓN DE RUTAS DEL PROYECTO
# ============================================================================

# Obtener la ruta base del proyecto basada en la ubicación de este archivo
BASE_PATH = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_GPS_PATH = os.path.join(BASE_PATH, 'data', 'gps')

# ============================================================================
# FUNCIONES AUXILIARES
# ============================================================================

def ensure_dir(directory):
    """
    Asegura que un directorio existe, creándolo si es necesario.
    
    Función auxiliar para garantizar que las carpetas de datos procesados
    existan antes de intentar guardar archivos.
    
    Args:
        directory (str): Ruta del directorio a verificar/crear
    """
    if not os.path.exists(directory):
        os.makedirs(directory)
        
        
# ============================================================================
# FUNCIONES DE CARGA Y FILTRADO DE DATOS
# ============================================================================

def load_gps_data():
    """
    Carga los datos GPS desde el archivo parquet principal.
    
    Returns:
        pl.DataFrame: DataFrame con los datos GPS o None si no existe el archivo
    """
    path_to_parquet = os.path.join(DATA_GPS_PATH, 'df_gps.parquet')
    if not os.path.exists(path_to_parquet):
        print(f"Archivo parquet no existe: {path_to_parquet}")
        return None
    return pl.read_parquet(path_to_parquet)


def apply_standard_filters(df):
    """
    Aplica filtros estándar a los datos GPS para limpiar y preparar los datos.
    
    Filtros aplicados:
    - Excluye sesiones de rehabilitación (Match Day != 'Rehab')
    - Excluye registros de equipo completo (Player != 'TEAM')
    - Excluye registros de equipo completo (Team != 'TEAM')
    - Solo incluye ejercicios específicos (Selection == 'Drills')
    - Normaliza el nombre del equipo Sporting
    
    Args:
        df (pl.DataFrame): DataFrame con datos GPS sin filtrar
        
    Returns:
        pl.DataFrame: DataFrame filtrado y limpio
    """
    return (df.filter(pl.col('Match Day') != 'Rehab')
              .filter(pl.col('Player') != 'TEAM')
              .filter(pl.col('Team ') != 'TEAM')
              .filter(pl.col('Selection') == 'Drills')
              .with_columns(
                  pl.when(pl.col('Team ').str.contains('Sporting'))
                  .then(pl.lit('Sporting de Gijón'))
                  .otherwise(pl.col('Team '))
                  .alias('Team ')
              ))


def load_metrics_mapping():
    """
    Carrega o mapeamento de métricas do arquivo JSON.
    
    Returns:
        dict: Dicionário com o mapeamento de métricas ou None se houver erro.
    """
    try:
        mapping_path = os.path.join(os.path.dirname(__file__), '..', 'data', 'gps', 'metrics_mapping.json')
        with open(mapping_path, 'r', encoding='utf-8') as f:
            return json.load(f)
    except Exception as e:
        print(f"Erro ao carregar mapeamento de métricas: {e}")
        return None

def get_calculation_method(metrics_mapping, column_name):
    """
    Obtém o método de cálculo para uma coluna específica.
    
    Args:
        metrics_mapping (dict): Mapeamento de métricas carregado do JSON.
        column_name (str): Nome da coluna.
    
    Returns:
        str: Método de cálculo ('sum', 'max', 'mean', 'accelerations_minus_decelerations') ou 'sum' como padrão.
    """
    if not metrics_mapping:
        return 'sum'
    
    for metric_key, metric_data in metrics_mapping.get('metrics', {}).items():
        if metric_data.get('project_name') == column_name:
            calc_methods = metric_data.get('calculation_methods', {})
            method_data = calc_methods.get('calculate_metrics_for_94min', {})
            return method_data.get('method', 'sum')
    
    return 'sum'  # Padrão se não encontrar

def get_columns_of_interest():
    """
    Carga las columnas de interés desde el archivo de configuración.
    
    Lee el archivo 'Columnas_interés.txt' que contiene las métricas GPS
    que se utilizarán en los análisis y reportes.
    
    Returns:
        list: Lista de nombres de columnas de interés o lista vacía si hay error
    """
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





# ============================================================================
# FUNCIONES DE PROCESAMIENTO DE MÉTRICAS
# ============================================================================

def add_per_minute_metrics(df, session_minutes):
    """
    Calcula y añade métricas por minuto al DataFrame.
    
    Convierte métricas absolutas en métricas relativas por minuto para permitir
    comparaciones entre sesiones de diferente duración.
    
    Métricas calculadas:
    - Explosive Dist (m)/min: Distancia explosiva por minuto
    - Abs HSR(m)/Min: Distancia de alta velocidad por minuto  
    - Distance (m)/min: Distancia total por minuto
    
    Args:
        df (pl.DataFrame): DataFrame con datos GPS
        session_minutes (str): Duración de la sesión en formato HH:MM:SS
        
    Returns:
        tuple: (DataFrame modificado, lista de columnas actualizada)
    """
    # Crear una copia del DataFrame para no modificar el original
    df_modified = df.clone()
    
    # Convertir session_minutes de formato HH:MM:SS a minutos
    time_parts = session_minutes.split(':')
    minutes = int(time_parts[0]) * 60 + int(time_parts[1]) + int(time_parts[2]) / 60
    session_minutes = round(minutes, 2)
    #print("session_minutes = ", session_minutes)
    
    # Crear nueva lista para evitar modificar la original
    new_columns = get_columns_of_interest().copy()
    
    # Calcular y añadir métricas por minuto si existen las columnas base
    if 'Explosive Dist (m)' in df_modified.columns:
        df_modified = df_modified.with_columns(
            (pl.col('Explosive Dist (m)') / session_minutes).alias('Explosive Dist (m)/min')
        )
        new_columns.append('Explosive Dist (m)/min')
    else: 
        print("No existe la columna 'Explosive Dist (m)'")
        
    if 'Abs HSR(m)' in df_modified.columns:
        df_modified = df_modified.with_columns(
            (pl.col('Abs HSR(m)') / session_minutes).alias('Abs HSR(m)/Min')
        )
        new_columns.append('Abs HSR(m)/Min')
    else: 
        print("No existe la columna 'Abs HSR(m)'")
        
    if 'Distance (m)' in df_modified.columns:
        df_modified = df_modified.with_columns(
            (pl.col('Distance (m)') / session_minutes).alias('Distance (m)/min')
        )
        new_columns.append('Distance (m)/min')
    else: 
        print("No existe la columna 'Distance (m)'")
        
    # Ordenar alfabéticamente
    return df_modified, sorted(new_columns)


# ============================================================================
# FUNCIONES DE MANEJO DE FECHAS
# ============================================================================

def get_sorted_dates():
    """
    Obtiene todas las fechas disponibles en los datos GPS ordenadas cronológicamente.
    
    Procesa diferentes formatos de fecha y los convierte al formato estándar dd/mm/aaaa.
    Útil para poblar selectores de fecha en la interfaz de usuario.
    
    Returns:
        list: Lista de fechas en formato dd/mm/aaaa ordenadas cronológicamente
    """
    try:
        df = load_gps_data()
        if df is None or df.height == 0 or 'Date' not in df.columns:
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
                        fecha_dt = datetime.strptime(fecha, '%d/%m/%Y')
                        fecha_formatted = fecha
                    elif '-' in fecha:
                        # Convertir de aaaa-mm-dd a dd/mm/aaaa
                        fecha_dt = datetime.strptime(fecha, '%Y-%m-%d')
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
    """
    Obtiene la fecha más reciente disponible en formato compatible con DatePickerSingle.
    
    Convierte la fecha más reciente del formato dd/mm/aaaa al formato YYYY-MM-DD
    que requiere el componente DatePickerSingle de Dash.
    
    Returns:
        str: Fecha más reciente en formato YYYY-MM-DD o None si no hay fechas
    """
    try:
        fechas_ordenadas = get_sorted_dates()
        if not fechas_ordenadas:
            return None
        
        # Obtener la última fecha (más reciente) y convertir a formato YYYY-MM-DD
        latest_date_str = fechas_ordenadas[-1]  # Última fecha en formato dd/mm/aaaa
        latest_date_dt = datetime.strptime(latest_date_str, '%d/%m/%Y')
        return latest_date_dt.strftime('%Y-%m-%d')  # Formato para DatePickerSingle
        
    except Exception as e:
        print(f"Error obteniendo fecha más reciente: {e}")
        return None
    
def format_and_filter_date(selected_date):
    """
    Formatea una fecha seleccionada y filtra los datos GPS para esa fecha específica.
    
    Convierte la fecha del formato de entrada (YYYY-MM-DD) al formato interno (dd/mm/aaaa)
    y filtra el DataFrame para obtener solo los datos de esa fecha.
    
    Args:
        selected_date (str): Fecha en formato YYYY-MM-DD o dd/mm/aaaa
        
    Returns:
        tuple: (DataFrame filtrado para la fecha, fecha formateada) o (None, None) si no hay datos
    """
    
    df = load_gps_data()
    if df is None:
        return None, None
        
    df_filtered = apply_standard_filters(df)
    
    # Convertir la fecha seleccionada al formato correcto
    if isinstance(selected_date, str):
        try:
            selected_dt = datetime.strptime(selected_date, '%Y-%m-%d')
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
        #print(f"Fechas disponibles (muestra): {available_dates}")
        return None, None
        
    return df_fecha, formatted_date


def get_players_data(selected_date):
    """
    Procesa los datos de las columnas de interés de jugadores para una fecha específica.

    
    Función principal para obtener datos de jugadores listos para análisis.
    Aplica todos los filtros necesarios, calcula métricas por minuto y 
    selecciona solo las columnas de interés.
    
    Args:
        selected_date (str): Fecha en formato YYYY-MM-DD o dd/mm/aaaa
        
    Returns:
        pl.DataFrame: DataFrame con datos de jugadores procesados o None si no hay datos
    """
    try:
        result = format_and_filter_date(selected_date)
        
        # Verificar si se encontraron datos para la fecha
        if result is None or result[0] is None:
            print(f"No se encontraron datos para la fecha: {selected_date}")
            return None
        
        df_fecha, formatted_date = result
        
        if df_fecha is None:
            print(f"No se encontraron datos para la fecha: {selected_date}")
            return None
        

        # Obtener columnas de interés y calcular métricas por minuto
        session_minutes = df_fecha.select('Drills Duration').row(0)[0]
        df, columns_of_interest = add_per_minute_metrics(df_fecha, session_minutes)
        #print(f"Columnas de interés: {columns_of_interest}")
        
        # Seleccionar columnas básicas + columnas de interés que existan en el DataFrame
        basic_columns = ['Player', 'Position']
        available_columns = [col for col in columns_of_interest if col in df.columns]
        #print(f"Columnas disponibles en DataFrame: {available_columns}")
        
        selected_columns = basic_columns + available_columns
        #print(f"Columnas seleccionadas: {selected_columns}")
        
        if df.height > 0:
            result_df = df.select(selected_columns)
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


def get_specific_md_data(match_day_target, exclude_date=None):
    """
    Obtiene datos acumulados de un Match Day específico.
    
    Filtra todos los datos GPS para obtener solo las sesiones del mismo tipo
    de Match Day (ej: MD-1, MD+1, etc.), excluyendo opcionalmente una fecha específica.
    Útil para comparaciones históricas y cálculo de z-scores.
    
    Args:
        match_day_target (str): Match Day objetivo para filtrar (ej: 'MD-1', 'MD+1')
        exclude_date (str, optional): Fecha a excluir en formato dd/mm/aaaa
    
    Returns:
        pl.DataFrame: DataFrame con datos históricos del mismo Match Day o None si no hay datos
    """
    # Cargar datos GPS
    df = load_gps_data()
    if df is None:
        print("Error loading GPS data")
        return None
    
    # Aplicar filtros estándar
    df = apply_standard_filters(df)
    
    # Filtrar por Match Day objetivo
    df_md = df.filter(pl.col('Match Day') == match_day_target)
    
    # Excluir fecha específica si se proporciona
    if exclude_date is not None:
        df_md = df_md.filter(pl.col('Date') != exclude_date)
    
    return df_md


def obtener_datos_fecha_con_md_anterior(fecha):
    """
    Filtra los datos GPS por una fecha específica y los combina con datos del MD anterior.
    
    Esta función toma una fecha específica, obtiene los datos de esa fecha y busca
    la fecha más reciente anterior con Match Day = 'MD' para realizar comparaciones.
    Utiliza la función filter_and_get_players_data para procesar los datos.
    
    Args:
        fecha (str): Fecha específica en formato dd/mm/aaaa para análisis comparativo
        
    Returns:
        pl.DataFrame: DataFrame combinado con datos de la fecha específica y del MD anterior,
                     o None si no se encuentran datos válidos
    """
    try:
        # Cargar datos GPS completos
        df = load_gps_data()
        if df is None:
            print("Error al cargar datos GPS")
            return None
            
        # Verificar que existen las columnas necesarias
        if 'Date' not in df.columns or 'Match Day' not in df.columns:
            print("Faltan columnas requeridas (Date o Match Day)")
            return None
            
        # Obtener los datos de la fecha específica
        df_fecha = df.filter(pl.col('Date') == fecha)
        if df_fecha.height == 0:
            print(f"No se encontraron datos para la fecha: {fecha}")
            return None
            
        # Obtener el Match Day para la fecha especificada
        match_day_especifico = df_fecha['Match Day'][0]
        print(f"Match Day de la fecha {fecha}: {match_day_especifico}")
        
        # Encontrar la fecha más reciente anterior con Match Day == 'MD'
        # Convertir la columna Date a datetime para comparación y ordenación correcta
        df_with_datetime = df.with_columns(
            pl.col('Date').str.strptime(pl.Date, format='%d/%m/%Y').alias('Date_dt')
        )
        
        # Convertir la fecha objetivo a datetime para comparación
        fecha_dt = datetime.strptime(fecha, '%d/%m/%Y').date()
        
        # Filtrar datos anteriores a la fecha especificada con Match Day == 'MD'
        df_md_anteriores = df_with_datetime.filter(
            (pl.col('Date_dt') < fecha_dt) & 
            (pl.col('Match Day') == 'MD')
        ).sort('Date_dt', descending=True)  # Ordenar por fecha descendente para obtener la más reciente
        
        if df_md_anteriores.height == 0:
            print(f"No se encontró ningún MD anterior a la fecha {fecha}")
            return None
        
        # Obtener la fecha MD más reciente antes de la fecha especificada
        fecha_md_anterior = df_md_anteriores['Date'][0]
        print(f"Fecha MD anterior encontrada: {fecha_md_anterior}")
        
        # Crear DataFrame combinado con las filas de ambas fechas
        df_combinado = df.filter(
            (pl.col('Date') == fecha) | 
            (pl.col('Date') == fecha_md_anterior)
        )
        
        if df_combinado.height == 0:
            print("No se pudieron combinar los datos de ambas fechas")
            return None
            
        # Aplicar filtros estándar para limpiar los datos
        df_combinado = apply_standard_filters(df_combinado)
        
        # Calcular métricas por minuto usando los datos de la fecha específica
        df_fecha_actual = df_combinado.filter(pl.col('Date') == fecha)
        if df_fecha_actual.height > 0:
            session_minutes = df_fecha_actual.select('Drills Duration').row(0)[0]
            df_combinado, columnas_interes = add_per_minute_metrics(df_combinado, session_minutes)
        else:
            print("No se pudieron obtener los minutos de sesión")
            return None
            
        print(f"Datos combinados exitosamente: {df_combinado.height} registros")
        print(f"Fechas incluidas: {sorted(df_combinado['Date'].unique().to_list())}")
        
        return df_combinado
        
    except Exception as e:
        print(f"Error al obtener datos de fecha con MD anterior: {str(e)}")
        import traceback
        traceback.print_exc()
        return None
    
    
# ============================================================================
# FUNCIONES DE CÁLCULO DE ESTADÍSTICAS COMPARATIVAS
# ============================================================================


def calcular_metricas(df, columnas, estadistica):
    """
    Función auxiliar para calcular estadísticas específicas sobre columnas dadas.
    
    Calcula una estadística específica (media, mediana, máximo, etc.) para todas
    las columnas especificadas en un DataFrame.
    
    Args:
        df (pl.DataFrame): DataFrame con los datos a analizar
        columnas (list): Lista de nombres de columnas para calcular estadísticas
        estadistica (str): Tipo de estadística ('mean', 'median', 'max', 'min', 'p75', 'p90', 'p95')
        
    Returns:
        dict: Diccionario con el valor de la estadística para cada columna
    """
    resultado = {}
    for columna in columnas:
        if columna not in df.columns:
            continue
            
        if estadistica == "mean":
            valor = df[columna].mean()
        elif estadistica == "median":
            valor = df[columna].median()
        elif estadistica == "max":
            valor = df[columna].max()
        elif estadistica == "min":
            valor = df[columna].min()
        elif estadistica == "p75":
            valor = df[columna].quantile(0.75)
        elif estadistica == "p90":
            valor = df[columna].quantile(0.90)
        elif estadistica == "p95":
            valor = df[columna].quantile(0.95)
        else:
            continue
            
        resultado[columna] = valor
    return resultado


def calcular_estadisticas_por_matchday():
    """
    Calcula estadísticas para cada Match Day de todos los datos históricos.
    
    Esta función procesa todos los datos GPS disponibles, los filtra, obtiene las columnas
    de interés, calcula métricas por minuto para cada Match Day y luego agrupa los datos
    por Match Day calculando estadísticas descriptivas para jugadores, posiciones y equipos.
    
    Returns:
        tuple: (df_jugadores, df_posiciones, df_equipos) con estadísticas calculadas por Match Day
    """
        # Cargar datos GPS usando función auxiliar
    df = load_gps_data()
    if df is None:
        print("Error al cargar datos GPS")
        return None, None, None
        
    try:
        if df.height == 0:
            print("DataFrame está vacío después del filtrado")
            return None, None, None
        
        # Aplicar filtros estándar
        df = apply_standard_filters(df)
        
        # Obtener columnas de interés base
        columnas_interes = get_columns_of_interest().copy()
        
        # Calcular métricas por minuto de forma eficiente 
        # Converter Drills Duration para minutos para todo el DataFrame de una vez
        df = df.with_columns([
            pl.col('Drills Duration').str.split(':').list.eval(
                pl.element().cast(pl.Int32)
            ).list.eval(
                pl.when(pl.int_range(pl.len()) == 0).then(pl.element() * 60)  # horas * 60
                .when(pl.int_range(pl.len()) == 1).then(pl.element())         # minutos
                .when(pl.int_range(pl.len()) == 2).then(pl.element() / 60)    # segundos / 60
                .otherwise(0)
            ).list.sum().alias('session_minutes')
        ])
        
        # Calcular métricas por minuto para todo el DataFrame de una vez
        if 'Explosive Dist (m)' in df.columns:
            df = df.with_columns(
                (pl.col('Explosive Dist (m)') / pl.col('session_minutes')).alias('Explosive Dist (m)/min')
            )
            columnas_interes.append('Explosive Dist (m)/min')
        
        if 'Abs HSR(m)' in df.columns:
            df = df.with_columns(
                (pl.col('Abs HSR(m)') / pl.col('session_minutes')).alias('Abs HSR(m)/Min')
            )
            columnas_interes.append('Abs HSR(m)/Min')
        
        if 'Distance (m)' in df.columns:
            df = df.with_columns(
                (pl.col('Distance (m)') / pl.col('session_minutes')).alias('Distance (m)/min')
            )
            columnas_interes.append('Distance (m)/min')
        
        # Ordenar columnas de interés
        columnas_interes = sorted(columnas_interes)

        # Obtener valores únicos para agrupación
        match_days = df['Match Day'].unique().to_list()
        jugadores = df['Player'].unique().to_list()
        posiciones = df['Position'].unique().to_list() 
        equipos = df['Team '].unique().to_list()

        # Estadísticas a calcular
        estadisticas = ["mean", "median", "max", "min", "p75", "p90", "p95"]
        
        # Inicializar listas de resultados
        resultados_jugadores = []
        resultados_position = []
        resultados_team = []

        # Calcular estadísticas para jugadores por Match Day
        for jugador in jugadores:
            df_jugador = df.filter(pl.col('Player') == jugador)
            posicion_jugador = df_jugador['Position'][0] if df_jugador.height > 0 else "Desconocida"
            
            for match_day in match_days:
                df_match = df_jugador.filter(pl.col('Match Day') == match_day)
                if df_match.height == 0:
                    continue
                    
                for estadistica in estadisticas:
                    registro = {
                        "Player": jugador,
                        "Position": posicion_jugador,
                        "Match Day": match_day,
                        "Estadistica": estadistica
                    }
                    registro.update(calcular_metricas(df_match, columnas_interes, estadistica))
                    resultados_jugadores.append(registro)

        # Calcular estadísticas para posiciones por Match Day
        for posicion in posiciones:
            df_posicion = df.filter(pl.col('Position') == posicion)
            for match_day in match_days:
                df_match = df_posicion.filter(pl.col('Match Day') == match_day)
                if df_match.height == 0:
                    continue
                    
                for estadistica in estadisticas:
                    registro = {
                        "Position": posicion,
                        "Match Day": match_day,
                        "Estadistica": estadistica
                    }
                    registro.update(calcular_metricas(df_match, columnas_interes, estadistica))
                    resultados_position.append(registro)

        # Calcular estadísticas para equipos por Match Day
        for equipo in equipos:
            df_equipo = df.filter(pl.col('Team ') == equipo)
            for match_day in match_days:
                df_match = df_equipo.filter(pl.col('Match Day') == match_day)
                if df_match.height == 0:
                    continue
                    
                for estadistica in estadisticas:
                    registro = {
                        "Team": equipo,
                        "Match Day": match_day,
                        "Estadistica": estadistica
                    }
                    registro.update(calcular_metricas(df_match, columnas_interes, estadistica))
                    resultados_team.append(registro)

        # Crear DataFrames con los resultados
        df_estadisticas = pl.DataFrame(resultados_jugadores)
        df_estadisticas_position = pl.DataFrame(resultados_position)
        df_estadisticas_team = pl.DataFrame(resultados_team)

        # # Guardar resultados en archivos parquet
        # output_path = os.path.join(DATA_PROCESSED_PATH, 'df_jugadores_estadisticas.parquet')
        # output_path_position = os.path.join(DATA_PROCESSED_PATH, 'df_position_estadisticas.parquet')
        # output_path_team = os.path.join(DATA_PROCESSED_PATH, 'df_team_estadisticas.parquet')

        # df_estadisticas.write_parquet(output_path)
        # df_estadisticas_position.write_parquet(output_path_position)
        # df_estadisticas_team.write_parquet(output_path_team)
        
        print("Estadísticas por Match Day calculadas y guardadas exitosamente")
        return df_estadisticas, df_estadisticas_position, df_estadisticas_team

    except Exception as e:
        print(f"Error al calcular estadísticas por Match Day: {str(e)}")
        return None, None, None



def calcular_zscore_fecha_vs_matchday_acumulado(fecha):
    """
    Calcula el z-score comparando los datos de una fecha específica con los datos 
    acumulados históricos del mismo Match Day.
    
    Esta función utiliza get_table_data para obtener los valores de la fecha específica
    y get_specific_md_data para obtener los datos históricos acumulados del mismo Match Day,
    calculando el z-score para cada métrica de cada jugador, equipo y posición.
    Incluye métricas por minuto en el cálculo.
    
    Args:
        fecha (str): Fecha específica en formato dd/mm/aaaa para calcular z-scores
        
    Returns:
        dict: Diccionario con z-scores organizados por tipo de entidad:
              {
                'jugadores': {nombre_jugador: {'z_scores': {metrica: zscore, ...}, 'valores_actuales': {...}}},
                'equipos': {nombre_equipo: {'z_scores': {metrica: zscore, ...}, 'valores_actuales': {...}}},
                'posiciones': {nombre_posicion: {'z_scores': {metrica: zscore, ...}, 'valores_actuales': {...}}}
              }
              Retorna None si no se pueden calcular los z-scores
    """
    try:
        # Cargar datos GPS para obtener el Match Day de la fecha específica
        df = load_gps_data()
        if df is None:
            print("Error al cargar datos GPS")
            return None
            
        # Aplicar filtros estándar
        df = apply_standard_filters(df)
        
        # Obtener el Match Day para la fecha especificada
        df_fecha_info = df.filter(pl.col('Date') == fecha)
        if df_fecha_info.height == 0:
            print(f"No se encontraron datos para la fecha: {fecha}")
            return None
            
        match_day_especifico = df_fecha_info['Match Day'][0]

        
        # Obtener tabla con datos de la fecha específica usando diferentes estadísticas
        # Esta función ya incluye métricas por minuto
        tabla_actual_mean = get_table_data(fecha, "median")
        if tabla_actual_mean is None:
            print(f"No se pudieron obtener datos actuales para la fecha: {fecha}")
            return None
            
        # Separar jugadores, equipo y posiciones de la tabla actual
        jugadores_actuales = tabla_actual_mean.filter(
            ~pl.col('Player').str.starts_with('TEAM') & 
            ~pl.col('Player').str.starts_with('POS_')
        )
        
        equipo_actual = tabla_actual_mean.filter(pl.col('Player').str.starts_with('TEAM'))
        posiciones_actuales = tabla_actual_mean.filter(pl.col('Player').str.starts_with('POS_'))
        
        
        # Obtener datos históricos del mismo Match Day (excluyendo la fecha actual)
        df_historico_raw = get_specific_md_data(match_day_especifico, exclude_date=fecha)
        if df_historico_raw is None or df_historico_raw.height == 0:
            print(f"No se encontraron datos históricos para Match Day: {match_day_especifico}")
            return None
        
        # Aplicar métricas por minuto a los datos históricos
        # Procesar cada fecha histórica por separado para calcular métricas por minuto correctamente
        fechas_historicas = df_historico_raw['Date'].unique().to_list()
        df_historico_list = []
        
        for fecha_hist in fechas_historicas:
            df_fecha_hist = df_historico_raw.filter(pl.col('Date') == fecha_hist)
            if df_fecha_hist.height > 0:
                # Obtener duración de la sesión para esta fecha
                session_minutes = df_fecha_hist.select('Drills Duration').row(0)[0]
                # Aplicar métricas por minuto
                df_fecha_hist_con_metricas, _ = add_per_minute_metrics(df_fecha_hist, session_minutes)
                df_historico_list.append(df_fecha_hist_con_metricas)
        
        # Combinar todos los datos históricos con métricas por minuto
        if df_historico_list:
            df_historico = pl.concat(df_historico_list)
        else:
            print(f"No se pudieron procesar datos históricos para Match Day: {match_day_especifico}")
            return None
        
        # Obtener columnas numéricas (excluyendo Player)
        columnas_numericas = [col for col in jugadores_actuales.columns if col not in ['Player']]
        
        # Inicializar diccionario de resultados
        resultados = {
            'jugadores': {},
            'equipos': {},
            'posiciones': {}
        }
        
        # CALCULAR Z-SCORES PARA JUGADORES
        # Los datos ya incluyen métricas por minuto calculadas anteriormente
        for row in jugadores_actuales.iter_rows(named=True):
            jugador = row['Player']
            
            # Datos históricos del mismo jugador en el mismo Match Day
            df_jugador_historico = df_historico.filter(pl.col('Player') == jugador)
            if df_jugador_historico.height == 0:
                print(f"No hay datos históricos para el jugador: {jugador}")
                continue
            
            valores_actuales = {}
            z_scores = {}
            
            for columna in columnas_numericas:
                if columna not in df_jugador_historico.columns:
                    continue
                    
                # Valor actual del jugador (incluye métricas por minuto)
                valor_actual = row[columna]
                valores_actuales[columna] = valor_actual
                
                # Valores históricos del jugador (incluyen métricas por minuto)
                valores_historicos = df_jugador_historico[columna].to_list()
                if len(valores_historicos) < 2:  # Necesitamos al menos 2 valores para calcular std
                    z_scores[columna] = None
                    continue
                    
                media_historica = np.mean(valores_historicos)
                std_historica = np.std(valores_historicos, ddof=1)  # Usar ddof=1 para muestra
                
                # Calcular z-score
                if std_historica > 0:
                    z_score = (valor_actual - media_historica) / std_historica
                    z_scores[columna] = z_score
                else:
                    z_scores[columna] = None
            
            resultados['jugadores'][jugador] = {
                'z_scores': z_scores,
                'valores_actuales': valores_actuales
            }
        

        # CALCULAR Z-SCORES PARA EQUIPOS
        # Los datos ya incluyen métricas por minuto calculadas anteriormente
        if equipo_actual.height > 0:
            equipo_row = equipo_actual.row(0, named=True)
            
            # Obtener datos históricos del equipo agrupados por fecha
            fechas_historicas = df_historico['Date'].unique().to_list()
            valores_historicos_por_fecha = {}
            
            for fecha_hist in fechas_historicas:
                df_fecha_hist = df_historico.filter(pl.col('Date') == fecha_hist)
                valores_historicos_por_fecha[fecha_hist] = {}
                
                for columna in columnas_numericas:
                    if columna in df_fecha_hist.columns:
                        valores_historicos_por_fecha[fecha_hist][columna] = df_fecha_hist[columna].mean()
            
            valores_actuales = {}
            z_scores = {}
            
            for columna in columnas_numericas:
                # Valor actual del equipo (incluye métricas por minuto)
                valor_actual = equipo_row[columna]
                valores_actuales[columna] = valor_actual
                
                # Valores históricos del equipo (promedios por fecha, incluyen métricas por minuto)
                valores_hist = [valores_historicos_por_fecha[f][columna] 
                               for f in fechas_historicas 
                               if columna in valores_historicos_por_fecha[f]]
                
                if len(valores_hist) < 2:
                    z_scores[columna] = None
                    continue
                    
                media_historica = np.mean(valores_hist)
                std_historica = np.std(valores_hist, ddof=1)
                
                if std_historica > 0:
                    z_score = (valor_actual - media_historica) / std_historica
                    z_scores[columna] = z_score
                else:
                    z_scores[columna] = None
            
            resultados['equipos']['TEAM'] = {
                'z_scores': z_scores,
                'valores_actuales': valores_actuales
            }
        
        # CALCULAR Z-SCORES PARA POSICIONES
        # Los datos ya incluyen métricas por minuto calculadas anteriormente
        for row in posiciones_actuales.iter_rows(named=True):
            posicion_name = row['Player']  # Formato: POS_posicion
            posicion = posicion_name.replace('POS_', '')
            
            # Obtener datos históricos de la posición agrupados por fecha
            fechas_historicas = df_historico['Date'].unique().to_list()
            valores_historicos_por_fecha = {}
            
            for fecha_hist in fechas_historicas:
                df_posicion_hist = df_historico.filter(
                    (pl.col('Date') == fecha_hist) & 
                    (pl.col('Position') == posicion)
                )
                if df_posicion_hist.height > 0:
                    valores_historicos_por_fecha[fecha_hist] = {}
                    
                    for columna in columnas_numericas:
                        if columna in df_posicion_hist.columns:
                            valores_historicos_por_fecha[fecha_hist][columna] = df_posicion_hist[columna].mean()
            
            valores_actuales = {}
            z_scores = {}
            
            for columna in columnas_numericas:
                # Valor actual de la posición (incluye métricas por minuto)
                valor_actual = row[columna]
                valores_actuales[columna] = valor_actual
                
                # Valores históricos de la posición (promedios por fecha, incluyen métricas por minuto)
                valores_hist = [valores_historicos_por_fecha[f][columna] 
                               for f in fechas_historicas 
                               if f in valores_historicos_por_fecha and columna in valores_historicos_por_fecha[f]]
                
                if len(valores_hist) < 2:
                    z_scores[columna] = None
                    continue
                    
                media_historica = np.mean(valores_hist)
                std_historica = np.std(valores_hist, ddof=1)
                
                if std_historica > 0:
                    z_score = (valor_actual - media_historica) / std_historica
                    z_scores[columna] = z_score
                else:
                    z_scores[columna] = None
            
            resultados['posiciones'][posicion] = {
                'z_scores': z_scores,
                'valores_actuales': valores_actuales
            }
        
        return resultados
        
    except Exception as e:
        print(f"Error al calcular z-scores: {str(e)}")
        import traceback
        traceback.print_exc()
        return None



##################################################
# FUNCIONES PARA LA PÁGINA SESSION REPORT
##################################################

def get_table_data(selected_date, selected_statistic):
    """
    Procesa los datos de las columnas de interés de la tabla de la página Session Report para una fecha específica
    (jugadores + equipos + posiciones)
    
    Args:
        selected_date (str): Fecha seleccionada para procesar los datos
        selected_statistic (str): Estadística seleccionada para agrupar los datos ('mean', 'median', etc)

    Returns:
        pl.DataFrame: DataFrame combinado con datos de jugadores, equipo y posiciones
    """
    
    # Obtener datos de jugadores para la fecha seleccionada
    df_players = get_players_data(selected_date)
    
    if df_players is None:
        return None
    
    # Team 
    
    # Calcular estadísticas del equipo usando calcular_metricas
    columnas_numericas = [col for col in df_players.columns if col not in ['Player', 'Position']]
    team_stats = calcular_metricas(df_players, columnas_numericas, selected_statistic)
    
    # Crear DataFrame para el equipo
    team_stats['Player'] = "TEAM"
    df_team = pl.DataFrame([team_stats])
    
    
    #Position 
    
    # Obtener datos de posiciones agrupados por la estadística seleccionada
    positions = df_players.select('Position').unique()['Position'].to_list()
    position_data = []
    
    for pos in positions:
        df_pos = df_players.filter(pl.col('Position') == pos)
        pos_stats = calcular_metricas(df_pos, columnas_numericas, selected_statistic)
        pos_stats['Player'] = f"POS_{pos}"
        position_data.append(pos_stats)
    
    # Crear DataFrame de posiciones si hay datos
    df_position = pl.DataFrame(position_data) if position_data else None
    
    # Combinar todos los DataFrames
    # Remover la columna Position del DataFrame de jugadores para compatibilidad
    df_players_clean = df_players.drop('Position').sort('Player')
    
    # Obtener el orden de columnas del DataFrame de jugadores (sin Position)
    column_order = df_players_clean.columns
    
    # Concatenar en el orden deseado: jugadores -> equipo -> posiciones
    dfs_to_concat = [df_players_clean]
    
    if df_team is not None:
        # Reordenar columnas del DataFrame del equipo para que coincidan
        df_team = df_team.select(column_order)
        dfs_to_concat.append(df_team)
        
    if df_position is not None:
        # Reordenar columnas del DataFrame de posiciones para que coincidan
        df_position = df_position.select(column_order)
        dfs_to_concat.append(df_position)
    
    result_df = pl.concat(dfs_to_concat)
    
    return result_df


def get_combined_table_with_reference(fecha, valor_referencia="zscore", estadistica="median"):
    """
    Retorna una tabla combinada donde cada celda contiene el valor absoluto 
    más el valor de referencia.
    
    Esta función combina los datos de get_table_data con los valores de referencia
    calculados (como z-scores) para crear una tabla donde cada celda muestra
    tanto el valor absoluto como el valor de referencia formateado.
    
    Args:
        fecha (str): Fecha específica en formato dd/mm/aaaa
        valor_referencia (str): Tipo de valor de referencia a calcular. 
                               Opciones: "zscore" (default)
        estadistica (str): Estadística a usar para get_table_data. 
                          Opciones: "mean", "median", "max", "min", "quantile"
                          Default: "median"
        
    Returns:
        pl.DataFrame: DataFrame con columnas combinadas donde cada celda contiene
                     el valor absoluto y el valor de referencia formateado.
                     Formato para z-score: "valor_absoluto (Z=z_score_valor)"
                     Retorna None si hay errores en el procesamiento
    """
    try:

        # OBTENER DATOS ABSOLUTOS
        tabla_absoluta = get_table_data(fecha, estadistica)
        if tabla_absoluta is None:
            print(f"No se pudieron obtener datos absolutos para la fecha: {fecha}")
            return None
         
        # OBTENER VALORES DE REFERENCIA
        if valor_referencia.lower() == "zscore":
            datos_referencia = calcular_zscore_fecha_vs_matchday_acumulado(fecha)
            if datos_referencia is None:
                print(f"No se pudieron calcular z-scores para la fecha: {fecha}")
                return None
        else:
            # ADICIONAR AQUI DIFERENÇA PERCENTUAL
            print(f"Valor de referencia '{valor_referencia}' no soportado actualmente")
            return None
        
        
        # COMBINAR DATOS ABSOLUTOS CON VALORES DE REFERENCIA
        
        # Crear una copia de la tabla absoluta para modificar
        tabla_combinada = tabla_absoluta.clone()
        
        # Obtener columnas numéricas (excluyendo Player)
        columnas_numericas = [col for col in tabla_combinada.columns if col not in ['Player']]
        
        # Procesar cada fila de la tabla
        filas_procesadas = []
        
        for row in tabla_combinada.iter_rows(named=True):
            player_name = row['Player']
            fila_procesada = {'Player': player_name}
            
            # Determinar el tipo de entidad (jugador, equipo o posición)
            if player_name.startswith('TEAM'):
                # Es un equipo
                entidad_tipo = 'equipos'
                entidad_key = 'TEAM'
            elif player_name.startswith('POS_'):
                # Es una posición
                entidad_tipo = 'posiciones'
                entidad_key = player_name.replace('POS_', '')
            else:
                # Es un jugador
                entidad_tipo = 'jugadores'
                entidad_key = player_name
            
            # Obtener los datos de referencia para esta entidad
            datos_entidad = None
            if entidad_tipo in datos_referencia and entidad_key in datos_referencia[entidad_tipo]:
                datos_entidad = datos_referencia[entidad_tipo][entidad_key]
            
            # Procesar cada columna numérica
            for columna in columnas_numericas:
                valor_absoluto = row[columna]
                
                # Formatear el valor absoluto
                if isinstance(valor_absoluto, (int, float)) and not np.isnan(valor_absoluto):
                    if abs(valor_absoluto) >= 1000:
                        valor_abs_str = f"{valor_absoluto:.0f}"
                    elif abs(valor_absoluto) >= 10:
                        valor_abs_str = f"{valor_absoluto:.1f}"
                    else:
                        valor_abs_str = f"{valor_absoluto:.2f}"
                else:
                    valor_abs_str = "N/A"
                
                # Obtener el valor de referencia
                valor_ref_str = ""
                if datos_entidad and 'z_scores' in datos_entidad:
                    z_scores = datos_entidad['z_scores']
                    if columna in z_scores and z_scores[columna] is not None:
                        z_score = z_scores[columna]
                        if valor_referencia.lower() == "zscore":
                            valor_ref_str = f"     Z={z_score:.2f}"
                
                # Combinar valor absoluto con valor de referencia
                valor_combinado = valor_abs_str + valor_ref_str
                fila_procesada[columna] = valor_combinado
            
            filas_procesadas.append(fila_procesada)
        
        # Crear el DataFrame final
        tabla_final = pl.DataFrame(filas_procesadas)
        
        print(f"Tabla combinada generada exitosamente con {tabla_final.height} filas")
        return tabla_final
        
    except Exception as e:
        print(f"Error al generar tabla combinada: {str(e)}")
        import traceback
        traceback.print_exc()
        return None


def get_zscore_matrix_for_styling(fecha):
    """
    Extrae una matriz de z-scores organizada para aplicar estilos de color en DataTable.
    
    Esta función utiliza los z-scores ya calculados por calcular_zscore_fecha_vs_matchday_acumulado
    y los organiza en una estructura optimizada para generar estilos condicionales en Dash DataTable.
    La matriz resultante mapea cada fila (jugador/equipo/posición) con cada columna y su z-score correspondiente.
    
    Args:
        fecha (str): Fecha específica en formato dd/mm/aaaa para obtener z-scores
        
    Returns:
        dict: Diccionario con estructura {row_index: {column_name: zscore_value}}
              donde row_index corresponde al índice de la fila en la tabla y 
              column_name es el nombre de la columna métrica.
              Retorna None si no se pueden obtener los z-scores.
              
    Example:
        {
            0: {'Distance (m)': 1.5, 'HSR': -0.8, 'Explosive Dist (m)': 2.1},
            1: {'Distance (m)': -1.2, 'HSR': 0.3, 'Explosive Dist (m)': -0.5},
            ...
        }
    """
    try:
        # Obtener datos de z-scores calculados
        datos_zscore = calcular_zscore_fecha_vs_matchday_acumulado(fecha)
        if datos_zscore is None:
            print(f"No se pudieron obtener z-scores para la fecha: {fecha}")
            return None
        
        # Obtener tabla de datos para conocer el orden de filas
        tabla_datos = get_table_data(fecha, "median")
        if tabla_datos is None:
            print(f"No se pudieron obtener datos de tabla para la fecha: {fecha}")
            return None
        
        # Obtener columnas numéricas (excluyendo Player)
        columnas_numericas = [col for col in tabla_datos.columns if col not in ['Player']]
        
        # Inicializar matriz de z-scores
        zscore_matrix = {}
        
        # Procesar cada fila de la tabla para mantener el mismo orden
        for row_index, row in enumerate(tabla_datos.iter_rows(named=True)):
            player_name = row['Player']
            
            # Determinar el tipo de entidad y clave para buscar en datos_zscore
            if player_name.startswith('TEAM'):
                entidad_tipo = 'equipos'
                entidad_key = 'TEAM'
            elif player_name.startswith('POS_'):
                entidad_tipo = 'posiciones'
                entidad_key = player_name.replace('POS_', '')
            else:
                entidad_tipo = 'jugadores'
                entidad_key = player_name
            
            # Inicializar diccionario para esta fila
            zscore_matrix[row_index] = {}
            
            # Obtener z-scores para esta entidad
            datos_entidad = None
            if entidad_tipo in datos_zscore and entidad_key in datos_zscore[entidad_tipo]:
                datos_entidad = datos_zscore[entidad_tipo][entidad_key]
            
            # Procesar cada columna numérica
            for columna in columnas_numericas:
                zscore_value = None
                
                if datos_entidad and 'z_scores' in datos_entidad:
                    z_scores = datos_entidad['z_scores']
                    if columna in z_scores and z_scores[columna] is not None:
                        zscore_value = z_scores[columna]
                
                # Almacenar el z-score (puede ser None si no existe)
                zscore_matrix[row_index][columna] = zscore_value
        
        print(f"Matriz de z-scores generada exitosamente para {len(zscore_matrix)} filas")
        return zscore_matrix
        
    except Exception as e:
        print(f"Error al generar matriz de z-scores: {str(e)}")
        import traceback
        traceback.print_exc()
        return None


def generate_color_styles_from_zscore(zscore_matrix):
    """
    Genera estilos condicionales para DataTable basados en una matriz de z-scores.
    
    Esta función toma una matriz de z-scores y genera una lista de estilos condicionales
    que aplican un gradiente de colores desde azul oscuro (z-scores bajos) hasta 
    rojo oscuro (z-scores altos). Los valores nulos no reciben coloración.
    
    Args:
        zscore_matrix (dict): Matriz de z-scores con estructura {row_index: {column_name: zscore_value}}
        
    Returns:
        list: Lista de diccionarios con estilos condicionales para Dash DataTable.
              Cada elemento tiene la estructura:
              {
                  'if': {'row_index': row, 'column_id': column},
                  'backgroundColor': color_hex,
                  'color': text_color_hex
              }
              
    Example:
        [
            {'if': {'row_index': 0, 'column_id': 'Distance (m)'}, 
             'backgroundColor': '#ff4444', 'color': 'white'},
            {'if': {'row_index': 0, 'column_id': 'HSR'}, 
             'backgroundColor': '#4444ff', 'color': 'white'},
            ...
        ]
    """
    try:
        if zscore_matrix is None:
            return []
        
        # Recopilar todos los z-scores válidos para normalización
        all_zscores = []
        for row_data in zscore_matrix.values():
            for zscore in row_data.values():
                if zscore is not None and not np.isnan(zscore):
                    all_zscores.append(zscore)
        
        if not all_zscores:
            print("No hay z-scores válidos para generar colores")
            return []
        
        # Calcular rango absoluto máximo para normalización simétrica
        # Esto garantiza que z-score 0.0 siempre esté en el centro (blanco)
        max_abs_zscore = max(abs(min(all_zscores)), abs(max(all_zscores)))
        
        # Evitar división por cero
        if max_abs_zscore == 0:
            max_abs_zscore = 1
        
        # Generar estilos condicionales
        conditional_styles = []
        
        for row_index, row_data in zscore_matrix.items():
            for column_name, zscore_value in row_data.items():
                if zscore_value is not None and not np.isnan(zscore_value):
                    # Normalizar z-score de forma simétrica alrededor de 0
                    # Rango: [-1, 1] donde 0 = neutro, -1 = azul máximo, +1 = rojo máximo
                    normalized_score = zscore_value / max_abs_zscore
                    
                    # Limitar a rango [-1, 1]
                    normalized_score = max(-1, min(1, normalized_score))
                    
                    # Generar color basado en z-score normalizado
                    if normalized_score < 0:
                        # Z-score negativo: gradiente de azul oscuro a blanco
                        # normalized_score va de -1 a 0
                        intensity = abs(normalized_score)  # 0 a 1
                        red_green = int(255 * (1 - intensity))  # 255 a 0
                        blue = 255  # Siempre azul máximo
                        bg_color = f'rgb({red_green}, {red_green}, {blue})'
                        text_color = 'white' if intensity > 0.5 else 'black'
                    elif normalized_score > 0:
                        # Z-score positivo: gradiente de blanco a rojo oscuro
                        # normalized_score va de 0 a 1
                        intensity = normalized_score  # 0 a 1
                        red = 255  # Siempre rojo máximo
                        green_blue = int(255 * (1 - intensity))  # 255 a 0
                        bg_color = f'rgb({red}, {green_blue}, {green_blue})'
                        text_color = 'white' if intensity > 0.5 else 'black'
                    else:
                        # Z-score exactamente 0: color neutro (blanco)
                        bg_color = 'rgb(255, 255, 255)'
                        text_color = 'black'
                    
                    # Agregar estilo condicional
                    conditional_styles.append({
                        'if': {
                            'row_index': row_index,
                            'column_id': column_name
                        },
                        'backgroundColor': bg_color,
                        'color': text_color
                    })
        
        print(f"Generados {len(conditional_styles)} estilos condicionales")
        return conditional_styles
        
    except Exception as e:
        print(f"Error al generar estilos de color: {str(e)}")
        import traceback
        traceback.print_exc()
        return []


def calculate_metcrics_for_94min():
    """
    Carga el dataset principal usando load_gps_data, filtra por Match Day "MD" y aplica filtros adicionales.
    No se usa get_specific_md_data porque no se filtra por 'Drills' en la columna Selection.
    Luego, para cada fecha única, suma la duración de los drills por jugador.

    Returns:
        dict: Diccionario con fechas como claves y otro diccionario como valor,
              donde las claves son jugadores y los valores la suma de Drills Duration.
    """
    df = load_gps_data()
    if df is None:
        print("No se pudo cargar el dataset principal.")
        return None

    # Carregar o mapeamento de métricas
    metrics_mapping = load_metrics_mapping()
    
    # Usar todas as colunas do arquivo Columnas_interés.txt
    columns_of_interest = [
        'Distance (m)',
        'Speed Zones (m) [0.0, 6.0]km/h (m)',
        'Abs HSR(m)',
        'HSR Rel (m)',
        'Sprint Abs (m)',
        'Sprint Rel (m)',
        'Explosive Dist (m)',
        'MAX Speed(km/h)',
        'Max Acceleration',
        'Accelerations',
        'Decelerations',
        'Dif. ACC/DEC',
        'Step Balance (%)',
        'Total impacts'
    ]
                           
    # Filtrar para mantener solo las columnas de interés
    columns = ['Date', 'Match Day', 'Team ', 'Selection', 'Player', 'Drills Duration'] + columns_of_interest
    df = df.select(columns)
    if df is None:
        print("No se pudo cargar el dataset principal.")
        return None

    # Filtrar por Match Day objetivo
    df_md = df.filter(pl.col('Match Day') == "MD")

    # Aplicar filtros adicionales
    df_filtered = (df_md.filter(pl.col('Match Day') != 'Rehab')
                        .filter(pl.col('Player') != 'TEAM')
                        .filter(pl.col('Team ') != 'TEAM')
                        .filter(pl.col('Selection').is_in(['Fútbol_11v11_105x68m', 'Fútbol_10v10_105x68m']))
                        .with_columns(
                            pl.when(pl.col('Team ').str.contains('Sporting'))
                            .then(pl.lit('Sporting de Gijón'))
                            .otherwise(pl.col('Team '))
                            .alias('Team ')
                        ))

    # Obtener todas las fechas únicas de todos los partidos
    unique_dates = df_filtered.select('Date').unique().to_series().to_list()

    # Lista para acumular los DataFrames de cada jugador
    result_dataframes = []

    # Loop por cada fecha
    for date in unique_dates:
        df_date = df_filtered.filter(pl.col('Date') == date)
        players = df_date.select('Player').unique().to_series().to_list()
        
        for player in players:
            df_player = df_date.filter(pl.col('Player') == player)
            # Convertir 'Drills Duration' de HH:MM:SS a minutos
            durations = df_player.select('Drills Duration').to_series().to_list()
            total_minutes = 0
            for dur in durations:
                 if isinstance(dur, str):
                     parts = dur.split(':')
                     if len(parts) == 3:
                         h, m, s = parts
                         total_minutes += int(h)*60 + int(m) + int(s)/60

            # Aplicar regra de três simples se total_minutes > 50
            if total_minutes > 50:
                # Calcular métricas usando métodos específicos para cada coluna
                metrics_94min = {}
                
                for col in columns_of_interest:
                    if col in df_player.columns:
                        method = get_calculation_method(metrics_mapping, col)
                        
                        if method == 'sum':
                            total_metric = df_player[col].sum()
                            adjusted_metric = total_metric * 94 / total_minutes
                        elif method == 'max':
                            adjusted_metric = df_player[col].max()
                        elif method == 'mean':
                            adjusted_metric = df_player[col].mean()
                        elif method == 'accelerations_minus_decelerations':
                            # Caso especial para Dif. ACC/DEC
                            if 'Accelerations' in df_player.columns and 'Decelerations' in df_player.columns:
                                acc_total = df_player['Accelerations'].sum()
                                dec_total = df_player['Decelerations'].sum()
                                acc_adjusted = acc_total * 94 / total_minutes
                                dec_adjusted = dec_total * 94 / total_minutes
                                adjusted_metric = acc_adjusted - dec_adjusted
                            else:
                                adjusted_metric = 0
                        else:
                            # Método padrão: sum
                            total_metric = df_player[col].sum()
                            adjusted_metric = total_metric * 94 / total_minutes
                        
                        metrics_94min[f"{col}_94min"] = adjusted_metric
                
                # Criar DataFrame com Player, Date e métricas ajustadas
                if metrics_94min:  # Solo si hay métricas calculadas
                    player_data = {
                        'Player': [player],
                        'Date': [date]
                    }
                    player_data.update({k: [v] for k, v in metrics_94min.items()})
                    
                    df_player_result = pl.DataFrame(player_data)
                    result_dataframes.append(df_player_result)

    # Concatenar todos los DataFrames acumulados
    if result_dataframes:
        df_final = pl.concat(result_dataframes, how="vertical")
        return df_final
    else:
        # Retornar DataFrame vacío con las columnas esperadas si no hay datos
        empty_columns = ['Player', 'Date'] + [f"{col}_94min" for col in columns_of_interest]
        return pl.DataFrame({col: [] for col in empty_columns})


def calculate_player_statistics_94min():
    """
    Recibe el DataFrame generado por calculate_metcrics_for_94min y calcula todas las estadísticas
    agrupadas por jugador para cada métrica. Incluye una columna 'estadistica' que identifica
    el tipo de estadística calculada.
    
    Returns:
        pl.DataFrame: DataFrame con columnas 'Player', 'estadistica' y métricas calculadas por jugador
    """
    
    df_94min = calculate_metcrics_for_94min()

    # Verificar si el DataFrame está vacío
    if df_94min.is_empty():
        print("El DataFrame de entrada está vacío.")
        return pl.DataFrame()
    
    # Obtener las columnas de métricas (todas las que terminan en '_94min')
    metric_columns = [col for col in df_94min.columns if col.endswith('_94min')]
    
    if not metric_columns:
        print("No se encontraron columnas de métricas en el DataFrame.")
        return pl.DataFrame()
    
    # Diccionario para mapear nombres de estadísticas a funciones de Polars
    stat_functions = {
        'median': pl.median,
        'mean': pl.mean,
        'max': pl.max,
        'min': pl.min,
        'p75': lambda col: pl.col(col).quantile(0.75),
        'p90': lambda col: pl.col(col).quantile(0.90), 
        'p99': lambda col: pl.col(col).quantile(0.99)
    }
    
    # Lista para almacenar los DataFrames de cada estadística
    result_dataframes = []
    
    try:
        # Calcular cada estadística por separado
        for stat_name, agg_function in stat_functions.items():
            # Agrupar por jugador y calcular la estadística para cada métrica
            df_stat = df_94min.group_by('Player').agg([
                agg_function(col)for col in metric_columns
            ])
            
            # Agregar columna con el nombre de la estadística
            df_stat = df_stat.with_columns(
                pl.lit(stat_name).alias('estadistica')
            )
            
            # Reordenar columnas para que 'estadistica' esté después de 'Player'
            columns_order = ['Player', 'estadistica'] + [col for col in df_stat.columns if col not in ['Player', 'estadistica']]
            df_stat = df_stat.select(columns_order)
            
            result_dataframes.append(df_stat)
        
        # Concatenar todos los DataFrames
        df_final = pl.concat(result_dataframes, how="vertical")
        
        # Ordenar por jugador y estadística para mejor presentación
        df_final = df_final.sort(['Player', 'estadistica'])
        
        return df_final
        
    except Exception as e:
        print(f"Error al calcular estadísticas: {str(e)}")
        return pl.DataFrame()


def calculate_percentage_difference_vs_reference(selected_date, reference_statistic='median'):
    """
    Calcula la diferencia porcentual entre las métricas de un día específico
    y las estadísticas de referencia (calculadas con calculate_player_statistics).
    
    Args:
        selected_date (str): Fecha específica para obtener datos del día
        reference_statistic (str): Estadística de referencia ('median', 'mean', 'max', 'min', 'p75', 'p90', 'p99')
                                  Por defecto es 'median'
    
    Returns:
        pl.DataFrame: DataFrame con columnas 'Player' y diferencias porcentuales para cada métrica
    """
    
    columns_of_interest = [
        'Distance (m)',
        'Speed Zones (m) [0.0, 6.0]km/h (m)',
        'Abs HSR(m)',
        'HSR Rel (m)',
        'Sprint Abs (m)',
        'Sprint Rel (m)',
        'Explosive Dist (m)',
        'MAX Speed(km/h)',
        'Max Acceleration',
        'Accelerations',
        'Decelerations',
        'Dif. ACC/DEC',
        'Step Balance (%)',
        'Total impacts'
    ]  
    
    try:
        # Obtener datos del día específico
        df_day = get_players_data(selected_date)
        if df_day is None or df_day.is_empty():
            print(f"No se encontraron datos para la fecha {selected_date}.")
            return pl.DataFrame()
        
        # Obtener estadísticas de referencia (todas las estadísticas)
        df_reference = calculate_player_statistics_94min()
        if df_reference is None or df_reference.is_empty():
            print("No se pudieron obtener estadísticas de referencia.")
            return pl.DataFrame()
        
        # Filtrar por la estadística de referencia seleccionada
        df_reference_filtered = df_reference.filter(
            pl.col('estadistica') == reference_statistic
        )
        
        if df_reference_filtered.is_empty():
            print(f"No se encontraron datos para la estadística '{reference_statistic}'.")
            return pl.DataFrame()
        
         # Obtener las columnas de métricas de la sesión
        session_metric_columns = columns_of_interest
        
        # Obtener las columnas de métricas de referencia 
        ref_metric_columns = [col + '_94min' for col in columns_of_interest]
        
        # Identificar los jugadores presentes en df_day
        players_in_day = df_day.select('Player').unique().to_series().to_list()
        
        # Filtrar df_reference para incluir solo los jugadores del día
        df_reference_players = df_reference_filtered.filter(
            pl.col('Player').is_in(players_in_day)
        )
        
        if df_reference_players.is_empty():
            print(f"No se encontraron jugadores coincidentes entre el día {selected_date} y las estadísticas de referencia.")
            return pl.DataFrame()
        
        # Crear DataFrame base con jugadores y columnas de interés
        # Seleccionar solo las columnas necesarias del día
        df_day_selected = df_day.select(['Player'] + session_metric_columns)
        
        # Seleccionar solo las columnas necesarias de referencia
        df_reference_selected = df_reference_players.select(['Player'] + ref_metric_columns)
        
        # Paso 4: Realizar join y calcular diferencias porcentuales
        df_joined = df_day_selected.join(
            df_reference_selected,
            on='Player',
            how='inner'
        )
        
        # Calcular diferencias porcentuales para cada métrica
        percentage_diff_expressions = [pl.col('Player')]
        
        for i, col in enumerate(session_metric_columns):
            ref_col = ref_metric_columns[i]
            # Calcular diferencia porcentual: ((valor_día - valor_referencia) / valor_referencia) * 100
            percentage_diff_expr = (
                ((pl.col(col) / pl.col(ref_col)) * 100).round(2)

            ).alias(f'{col}_diff_pct')
            percentage_diff_expressions.append(percentage_diff_expr)
        
        # Crear DataFrame final con diferencias porcentuales
        df_result = df_joined.select(percentage_diff_expressions)
        
        return df_result
        
    except Exception as e:
        print(f"Error al calcular diferencias porcentuales: {str(e)}")
        return pl.DataFrame()
        
        
        


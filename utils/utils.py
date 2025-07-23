import polars as pl
import numpy as np
from datetime import datetime
import os

# ============================================================================
# CONFIGURACIÓN DE RUTAS DEL PROYECTO
# ============================================================================

# Obtener la ruta base del proyecto basada en la ubicación de este archivo
BASE_PATH = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_GPS_PATH = os.path.join(BASE_PATH, 'data', 'gps')
DATA_PROCESSED_PATH = os.path.join(BASE_PATH, 'data', 'processed')

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


def filter_and_get_players_data(selected_date):
    """
    Filtra y procesa los datos GPS de jugadores para una fecha específica.
    
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
        
        #print(f"Datos encontrados para la fecha: {df_fecha.height} filas")
        df_filtered = apply_standard_filters(df_fecha)
        #print(f"Después de filtros: {df_filtered.height} filas")
        
        # Obtener columnas de interés y calcular métricas por minuto
        session_minutes = df_fecha.select('Drills Duration').row(0)[0]
        df_filtered, columns_of_interest = add_per_minute_metrics(df_filtered, session_minutes)
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
# FUNCIONES DE CÁLCULO DE ESTADÍSTICAS COMPARATIVAS
# ============================================================================
        
def calcular_estadisticas(fecha=None, columnas_interes=None, estadistica=None):
    """
    Calcula estadísticas comparativas para jugadores, posiciones y equipos por Match Day.
    
    Función principal para generar análisis estadísticos comparativos. Puede trabajar
    con todos los datos históricos o filtrar por una fecha específica para comparar
    con el Match Day de referencia más cercano.
    
    Args:
        fecha (str, optional): Fecha específica en formato dd/mm/aaaa para análisis comparativo
        columnas_interes (list, optional): Lista de columnas a analizar. Si None, usa todas las de interés
        estadistica (str, optional): Estadística específica a calcular ('mean', 'median', etc.)
        
    Returns:
        tuple: (df_jugadores, df_posiciones, df_equipos) con estadísticas calculadas
    """
    # Asegurar que el directorio de datos procesados existe
    ensure_dir(DATA_PROCESSED_PATH)
    
    # Cargar datos GPS usando función auxiliar
    df = load_gps_data()
    if df is None:
        print("Error loading GPS data")
        return None, None, None
        
    try:
        df = df.filter(pl.col('Match Day') != 'Rehab')
        
        if df.height == 0:
            print("DataFrame is empty")
            return None, None, None
        
        # # Crear backup
        # backup_path = os.path.join(DATA_GPS_PATH, 'df_gps_backup.parquet')
        # df.write_parquet(backup_path)
        
        # Filtrar por fecha si se proporciona
        if fecha is not None:
            if 'Date' not in df.columns or 'Match Day' not in df.columns:
                print("Required date columns missing")
                return None, None, None
                
            # Obtener los datos de fecha específica
            df_fecha = df.filter(pl.col('Date') == fecha)
            if df_fecha.height == 0:
                print(f"No data found for date {fecha}")
                return None, None, None
                
            # Obtener el Match Day para la fecha especificada
            match_day_especifico = df_fecha['Match Day'][0]
            
            # Encontrar la primera fecha con Match Day == 'MD' antes de la fecha especificada
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
            ).sort('Date_dt', descending=True)  # Ordenar por fecha datetime descendente para obtener la más reciente
            
            if df_md_anteriores.height == 0:
                print(f"No MD found before date {fecha}")
                return None, None, None
            
            # Obtener la fecha MD más reciente antes de la fecha especificada
            fecha_md = df_md_anteriores['Date'][0]
            
            # Crear nuevo dataframe con las filas de ambas fechas
            df = df.filter(
                (pl.col('Date') == fecha) | 
                (pl.col('Date') == fecha_md)
            )
            
            #print(f"Using data from MD date: {fecha_md} and target date: {fecha}")
            
        # Aplicar filtros estándar
        df = apply_standard_filters(df)
        
        # Obtener columnas de interés y calcular métricas por minuto
        # Si hay fecha específica, usar los datos de esa fecha para session_minutes
        if fecha is not None:
            df_fecha_actual = df.filter(pl.col('Date') == fecha)
            if df_fecha_actual.height > 0:
                session_minutes = df_fecha_actual.select('Drills Duration').row(0)[0]
            else:
                session_minutes = 90  # Valor por defecto si no se encuentra
            df, columnas_interes = add_per_minute_metrics(df, session_minutes)
        else:
            # Cuando fecha es None, calcular métricas por minuto para cada Match Day por separado
            columnas_interes = get_columns_of_interest().copy()
            
            # Obtener valores únicos de Match Day
            match_days_temp = df['Match Day'].unique().to_list()
            
            # Procesar cada Match Day por separado
            df_list = []
            for match_day in match_days_temp:
                df_match_day = df.filter(pl.col('Match Day') == match_day)
                if df_match_day.height == 0:
                    continue
                
                # Calcular la suma total de minutos para este Match Day
                # Convertir cada duración de HH:MM:SS a minutos y sumar
                total_minutes = 0
                for duration in df_match_day['Drills Duration'].to_list():
                    time_parts = duration.split(':')
                    minutes = int(time_parts[0]) * 60 + int(time_parts[1]) + int(time_parts[2]) / 60
                    total_minutes += minutes
                
                session_minutes_str = f"{int(total_minutes//60):02d}:{int(total_minutes%60):02d}:00"
                #print(f"Match Day {match_day}: Total session minutes = {session_minutes_str} ({total_minutes} minutes)")
                
                # Aplicar métricas por minuto usando el total de minutos para este Match Day
                df_match_day_processed, _ = add_per_minute_metrics(df_match_day, session_minutes_str)
                df_list.append(df_match_day_processed)
            
            # Concatenar todos los DataFrames procesados
            if df_list:
                df = pl.concat(df_list)
                # Actualizar columnas de interés con las nuevas columnas por minuto
                if 'Explosive Dist (m)' in df.columns:
                    columnas_interes.append('Explosive Dist (m)/min')
                if 'Abs HSR(m)' in df.columns:
                    columnas_interes.append('Abs HSR(m)/Min')
                if 'Distance (m)' in df.columns:
                    columnas_interes.append('Distance (m)/min')
                columnas_interes = sorted(columnas_interes)
            else:
                print("No se pudieron procesar datos para ningún Match Day")
                return None, None, None

        # Obtener valores únicos
        match_days = df['Match Day'].unique().to_list()
        jugadores = df['Player'].unique().to_list()
        posiciones = df['Position'].unique().to_list() 
        equipos = df['Team '].unique().to_list()

        # Estadísticas a calcular
        if estadistica is not None:
            estadisticas = [estadistica]  # Solo calcular la estadística seleccionada
        else:
            estadisticas = ["mean", "median", "max", "min", "p75", "p90", "p95"]  # Calcular todas las estadísticas
        
        # Inicializar listas de resultados
        resultados_jugadores = []
        resultados_position = []
        resultados_team = []

        # Calcular estadísticas para cada grupo
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

        # Crear DataFrames
        df_estadisticas = pl.DataFrame(resultados_jugadores)
        df_estadisticas_position = pl.DataFrame(resultados_position)
        df_estadisticas_team = pl.DataFrame(resultados_team)

        # Calcular diferencias porcentuales
        df_estadisticas = calcular_diferencia_porcentual(df_estadisticas)
        df_estadisticas_position = calcular_diferencia_porcentual(df_estadisticas_position)
        df_estadisticas_team = calcular_diferencia_porcentual(df_estadisticas_team)

        # Guardar resultados o filtrar por Match Day específico si se proporcionó fecha
        if fecha is not None:
            # # Filtrar resultados para devolver solo los datos del Match Day específico
            # df_estadisticas_filtrado = df_estadisticas.filter(pl.col('Match Day') == match_day_especifico)
            # df_estadisticas_position_filtrado = df_estadisticas_position.filter(pl.col('Match Day') == match_day_especifico)
            # df_estadisticas_team_filtrado = df_estadisticas_team.filter(pl.col('Match Day') == match_day_especifico)
            
            #print(f"Returning statistics for specific Match Day: {match_day_especifico}")
            
            #return df_estadisticas_filtrado, df_estadisticas_position_filtrado, df_estadisticas_team_filtrado
            return df_estadisticas, df_estadisticas_position, df_estadisticas_team

        else:
            # Nomenclatura original para estadísticas generales
            output_path = os.path.join(DATA_PROCESSED_PATH, 'df_jugadores_estadisticas.parquet')
            output_path_position = os.path.join(DATA_PROCESSED_PATH, 'df_position_estadisticas.parquet')
            output_path_team = os.path.join(DATA_PROCESSED_PATH, 'df_team_estadisticas.parquet')

            df_estadisticas.write_parquet(output_path)
            df_estadisticas_position.write_parquet(output_path_position)
            df_estadisticas_team.write_parquet(output_path_team)
            
            print("Estadísticas generales guardadas con nomenclatura estándar")
            return df_estadisticas, df_estadisticas_position, df_estadisticas_team

    except Exception as e:
        print(f"Error calculating statistics: {str(e)}")
        return None, None, None

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

def calcular_diferencia_porcentual(df):
    """
    Calcula diferencias porcentuales entre Match Days no-MD y MD de referencia.
    
    Para cada entidad (jugador, posición o equipo), compara sus valores en sesiones
    de entrenamiento con sus valores en partidos (MD), calculando la diferencia
    porcentual como: (abs(valor_entrenamiento) / abs(valor_partido)) * 100
    
    Args:
        df (pl.DataFrame): DataFrame con estadísticas por Match Day
        
    Returns:
        pl.DataFrame: DataFrame original más filas adicionales con Match Day = 'diferencia'
                     conteniendo las diferencias porcentuales calculadas
    """
    if df is None or df.height == 0:
        print("El dataframe está vacío.")
        return df
    
    # Obtener las columnas de métricas (todas las columnas excepto Player, Position, Team, Match Day y Estadistica)
    columnas_metricas = [col for col in df.columns if col not in ['Player', 'Position', 'Team', 'Match Day', 'Estadistica']]
    
    # Determinar si el dataframe tiene la columna 'Player', 'Position' o 'Team'
    tiene_player = 'Player' in df.columns
    tiene_position = 'Position' in df.columns
    tiene_team = 'Team' in df.columns
    
    # Determinar las columnas de agrupación
    columnas_agrupacion = []
    if tiene_player:
        columnas_agrupacion.append('Player')
    if tiene_position:
        columnas_agrupacion.append('Position')
    if tiene_team:
        columnas_agrupacion.append('Team')
    
    # Añadir 'Estadistica' a las columnas de agrupación
    columnas_agrupacion.append('Estadistica')
    
    # Lista para almacenar las nuevas filas de diferencias
    nuevas_filas = []
    
    # Agrupar por las columnas de agrupación
    grupos = df.group_by(columnas_agrupacion)
    
    # Para cada grupo, calcular las diferencias porcentuales
    for grupo_valores, grupo_df in grupos:
        # Verificar si existe el Match Day 'MD' en este grupo
        df_md = grupo_df.filter(pl.col('Match Day') == 'MD')
        if df_md.height == 0:
            continue
        
        # Obtener los valores de referencia (MD)
        valores_md = df_md.row(0, named=True)
        
        # Obtener todos los Match Days que no sean 'MD'
        df_no_md = grupo_df.filter(pl.col('Match Day') != 'MD')
        
        # Para cada Match Day que no sea 'MD', calcular las diferencias
        for fila_no_md in df_no_md.iter_rows(named=True):
            # Crear una nueva fila para las diferencias
            nueva_fila = {}
            
            # Copiar las columnas de agrupación excepto 'Match Day'
            for col in columnas_agrupacion:
                if col != 'Match Day':  # No copiar Match Day ya que lo estableceremos como 'diferencia'
                    nueva_fila[col] = fila_no_md[col]
            
            # Copiar también las columnas de identificación que no están en columnas_agrupacion
            if tiene_player and 'Player' not in nueva_fila:
                nueva_fila['Player'] = fila_no_md['Player']
            if tiene_position and 'Position' not in nueva_fila:
                nueva_fila['Position'] = fila_no_md['Position']
            if tiene_team and 'Team' not in nueva_fila:
                nueva_fila['Team'] = fila_no_md['Team']
            
            # Establecer Match Day como 'diferencia'
            nueva_fila['Match Day'] = 'diferencia'
            
            # Calcular diferencias porcentuales para cada métrica
            for columna in columnas_metricas:
                if columna in fila_no_md and columna in valores_md:
                    valor_no_md = fila_no_md[columna]
                    valor_md = valores_md[columna]
                    
                    # Verificar si alguno de los valores es None o si el valor MD es cero
                    if valor_no_md is None or valor_md is None or valor_md == 0:
                        nueva_fila[columna] = None
                    else:
                        # Calcular la diferencia porcentual: (abs(valor_no_MD) / abs(valor_MD)) * 100
                        diff_porcentual = (abs(valor_no_md) / abs(valor_md)) * 100
                        nueva_fila[columna] = round(diff_porcentual, 2)
                else:
                    nueva_fila[columna] = None
            
            nuevas_filas.append(nueva_fila)
    
    # Si hay nuevas filas, añadirlas al dataframe original
    if nuevas_filas:
        # Asegurar que todas las nuevas filas tengan las mismas columnas que el DataFrame original
        columnas_originales = df.columns
        for fila in nuevas_filas:
            for col in columnas_originales:
                if col not in fila:
                    fila[col] = None
        
        df_diferencias = pl.DataFrame(nuevas_filas)
        # Reordenar las columnas del DataFrame de diferencias para que coincidan con el original
        df_diferencias = df_diferencias.select(columnas_originales)
        df_resultado = pl.concat([df, df_diferencias], how="vertical")
    else:
        df_resultado = df
    
    return df_resultado

# ============================================================================
# FUNCIONES DE ANÁLISIS COMPARATIVO CON DATOS HISTÓRICOS
# ============================================================================

def get_historical_md_data(match_day_target, exclude_date=None):
    """
    Obtiene datos históricos para un Match Day específico.
    
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


def calcular_comparacion_fecha_md(df_fecha, estadistica_seleccionada, columnas_interes=None):
    """
    Calcula diferencias porcentuales y z-scores comparando datos de una fecha específica
    con el historial del mismo Match Day.
    
    Función principal para análisis comparativo que:
    1. Detecta automáticamente el tipo de datos (jugador, equipo o posición)
    2. Obtiene datos históricos del mismo Match Day
    3. Calcula diferencias porcentuales y z-scores para cada métrica
    4. Retorna resultados organizados por entidad
    
    Args:
        df_fecha (pl.DataFrame): DataFrame con datos de la fecha específica a analizar
        estadistica_seleccionada (str): Estadística de referencia ('mean', 'median', etc.)
        columnas_interes (list, optional): Columnas a analizar. Si None, usa todas las de interés
    
    Returns:
        dict: Diccionario con resultados de comparación organizados por entidad
              Cada entrada contiene diferencias porcentuales y z-scores por métrica
    """
    if df_fecha is None or df_fecha.height == 0:
        print("df_fecha está vacío")
        return {}
    
    # Detectar el tipo de datos basado en las columnas disponibles
    data_type = "unknown"
    entity_column = None
    
    if 'Player' in df_fecha.columns:
        data_type = "player"
        entity_column = 'Player'
        print("Detectado: Datos de jugadores individuales")
    elif 'Team' in df_fecha.columns:
        data_type = "team"
        entity_column = 'Team'
        print("Detectado: Datos de equipo")
    elif 'Position' in df_fecha.columns and 'Player' not in df_fecha.columns:
        data_type = "position"
        entity_column = 'Position'
        print("Detectado: Datos por posición")
    else:
        print("Error: No se pudo detectar el tipo de datos (Player, Team o Position)")
        return {}
    
    # Obtener el Match Day y la fecha del df_fecha
    match_day_target = df_fecha['Match Day'][0]
    fecha_target = df_fecha['Date'][0] if 'Date' in df_fecha.columns else None
    
    print(f"Obteniendo datos históricos para Match Day: {match_day_target}")
    
    # Obtener datos históricos del mismo Match Day (excluyendo la fecha actual)
    df_md_filtrado = get_historical_md_data(match_day_target, exclude_date=fecha_target)
    
    if df_md_filtrado is None or df_md_filtrado.height == 0:
        print(f"No se encontraron datos históricos para Match Day: {match_day_target}")
        return {}
    
    print(f"Encontrados {df_md_filtrado.height} registros históricos para Match Day: {match_day_target}")
    
    # Aplicar métricas por minuto si es necesario
    if fecha_target is not None:
        # Usar los datos de la fecha específica para session_minutes
        session_minutes = df_fecha.select('Drills Duration').row(0)[0]
        df_fecha, columnas_interes = add_per_minute_metrics(df_fecha, session_minutes)
        df_md_filtrado, _ = add_per_minute_metrics(df_md_filtrado, session_minutes)
    
    # Obtener columnas de interés si no se proporcionan
    if columnas_interes is None:
        columnas_interes = get_columns_of_interest()
        # Añadir columnas por minuto si existen
        if 'Explosive Dist (m)/min' in df_fecha.columns:
            columnas_interes.append('Explosive Dist (m)/min')
        if 'Abs HSR(m)/Min' in df_fecha.columns:
            columnas_interes.append('Abs HSR(m)/Min')
        if 'Distance (m)/min' in df_fecha.columns:
            columnas_interes.append('Distance (m)/min')
        columnas_interes = sorted(columnas_interes)
    
    # Filtrar solo las columnas que existen en ambos DataFrames
    columnas_disponibles = [col for col in columnas_interes 
                           if col in df_fecha.columns and col in df_md_filtrado.columns]
    
    if not columnas_disponibles:
        print("No hay columnas de interés disponibles en ambos DataFrames")
        return {}
    
    # Procesar según el tipo de datos
    if data_type == "player":
        return _procesar_datos_jugadores(df_fecha, df_md_filtrado, estadistica_seleccionada, 
                                       columnas_disponibles, match_day_target, fecha_target)
    elif data_type == "team":
        return _procesar_datos_equipo(df_fecha, df_md_filtrado, estadistica_seleccionada, 
                                    columnas_disponibles, match_day_target, fecha_target)
    elif data_type == "position":
        return _procesar_datos_posicion(df_fecha, df_md_filtrado, estadistica_seleccionada, 
                                      columnas_disponibles, match_day_target, fecha_target)
    
    return {}


def _procesar_datos_jugadores(df_fecha, df_md_filtrado, estadistica_seleccionada, 
                            columnas_disponibles, match_day_target, fecha_target):
    """
    Procesa y compara datos de jugadores individuales con su historial.
    
    Para cada jugador en los datos de la fecha específica, busca sus datos históricos
    del mismo Match Day y calcula las métricas comparativas correspondientes.
    
    Args:
        df_fecha (pl.DataFrame): Datos de jugadores de la fecha específica
        df_md_filtrado (pl.DataFrame): Datos históricos del mismo Match Day
        estadistica_seleccionada (str): Estadística de referencia para comparaciones
        columnas_disponibles (list): Lista de columnas métricas a analizar
        match_day_target (str): Match Day objetivo
        fecha_target (str): Fecha específica analizada
    
    Returns:
        dict: Resultados de comparación organizados por jugador
    """
    jugadores_fecha = df_fecha['Player'].unique().to_list()
    resultados = {}
    
    for jugador in jugadores_fecha:
        # Obtener datos del jugador en df_fecha
        df_jugador_fecha = df_fecha.filter(pl.col('Player') == jugador)
        if df_jugador_fecha.height == 0:
            continue
        
        # Obtener datos del mismo jugador en df_md_filtrado
        df_jugador_md = df_md_filtrado.filter(pl.col('Player') == jugador)
        if df_jugador_md.height == 0:
            print(f"No se encontraron datos históricos del jugador {jugador} en Match Day {match_day_target}")
            continue
        
        # Calcular métricas para este jugador
        resultado_jugador = _calcular_metricas_entidad(
            df_jugador_fecha, df_jugador_md, estadistica_seleccionada, columnas_disponibles,
            entity_name=jugador, entity_type="Player", match_day_target=match_day_target, 
            fecha_target=fecha_target, df_fecha_completo=df_fecha
        )
        
        resultados[jugador] = resultado_jugador
    
    return resultados


def _procesar_datos_equipo(df_fecha, df_md_filtrado, estadistica_seleccionada, 
                         columnas_disponibles, match_day_target, fecha_target):
    """
    Procesa y compara datos de equipos con su historial.
    
    Para cada equipo en los datos de la fecha específica, calcula estadísticas
    agregadas y las compara con el historial del mismo equipo en el mismo Match Day.
    
    Args:
        df_fecha (pl.DataFrame): Datos de equipo de la fecha específica
        df_md_filtrado (pl.DataFrame): Datos históricos del mismo Match Day
        estadistica_seleccionada (str): Estadística de referencia para comparaciones
        columnas_disponibles (list): Lista de columnas métricas a analizar
        match_day_target (str): Match Day objetivo
        fecha_target (str): Fecha específica analizada
    
    Returns:
        dict: Resultados de comparación organizados por equipo
    """
    equipos_fecha = df_fecha['Team'].unique().to_list()
    resultados = {}
    
    for equipo in equipos_fecha:
        # Obtener datos del equipo en df_fecha
        df_equipo_fecha = df_fecha.filter(pl.col('Team') == equipo)
        if df_equipo_fecha.height == 0:
            continue
        
        # Para datos de equipo, necesitamos calcular estadísticas históricas del mismo equipo
        # Primero calculamos las estadísticas por fecha en los datos históricos
        df_equipo_md = df_md_filtrado.filter(pl.col('Team') == equipo) if 'Team' in df_md_filtrado.columns else df_md_filtrado
        
        if df_equipo_md.height == 0:
            print(f"No se encontraron datos históricos del equipo {equipo} en Match Day {match_day_target}")
            continue
        
        # Calcular métricas para este equipo
        resultado_equipo = _calcular_metricas_entidad(
            df_equipo_fecha, df_equipo_md, estadistica_seleccionada, columnas_disponibles,
            entity_name=equipo, entity_type="Team", match_day_target=match_day_target, 
            fecha_target=fecha_target, df_fecha_completo=df_fecha
        )
        
        resultados[equipo] = resultado_equipo
    
    return resultados


def _procesar_datos_posicion(df_fecha, df_md_filtrado, estadistica_seleccionada, 
                           columnas_disponibles, match_day_target, fecha_target):
    """
    Procesa y compara datos por posición con su historial.
    
    Para cada posición en los datos de la fecha específica, calcula estadísticas
    agregadas de todos los jugadores de esa posición y las compara con el historial.
    
    Args:
        df_fecha (pl.DataFrame): Datos por posición de la fecha específica
        df_md_filtrado (pl.DataFrame): Datos históricos del mismo Match Day
        estadistica_seleccionada (str): Estadística de referencia para comparaciones
        columnas_disponibles (list): Lista de columnas métricas a analizar
        match_day_target (str): Match Day objetivo
        fecha_target (str): Fecha específica analizada
    
    Returns:
        dict: Resultados de comparación organizados por posición
    """
    posiciones_fecha = df_fecha['Position'].unique().to_list()
    resultados = {}
    
    for posicion in posiciones_fecha:
        # Obtener datos de la posición en df_fecha
        df_posicion_fecha = df_fecha.filter(pl.col('Position') == posicion)
        if df_posicion_fecha.height == 0:
            continue
        
        # Para datos por posición, calculamos estadísticas históricas de la misma posición
        df_posicion_md = df_md_filtrado.filter(pl.col('Position') == posicion) if 'Position' in df_md_filtrado.columns else df_md_filtrado
        
        if df_posicion_md.height == 0:
            print(f"No se encontraron datos históricos de la posición {posicion} en Match Day {match_day_target}")
            continue
        
        # Calcular métricas para esta posición
        resultado_posicion = _calcular_metricas_entidad(
            df_posicion_fecha, df_posicion_md, estadistica_seleccionada, columnas_disponibles,
            entity_name=posicion, entity_type="Position", match_day_target=match_day_target, 
            fecha_target=fecha_target, df_fecha_completo=df_fecha
        )
        
        resultados[posicion] = resultado_posicion
    
    return resultados


def _calcular_metricas_entidad(df_entidad_fecha, df_entidad_md, estadistica_seleccionada, 
                             columnas_disponibles, entity_name, entity_type, 
                             match_day_target, fecha_target, df_fecha_completo):
    """
    Calcula métricas comparativas detalladas para una entidad específica.
    
    Función central que realiza todos los cálculos estadísticos para una entidad
    (jugador, equipo o posición), incluyendo diferencias porcentuales y z-scores.
    
    Args:
        df_entidad_fecha (pl.DataFrame): Datos de la entidad en la fecha específica
        df_entidad_md (pl.DataFrame): Datos históricos de la entidad en el mismo Match Day
        estadistica_seleccionada (str): Estadística de referencia ('mean', 'median', etc.)
        columnas_disponibles (list): Lista de columnas métricas a analizar
        entity_name (str): Nombre de la entidad (jugador, equipo o posición)
        entity_type (str): Tipo de entidad ('Player', 'Team', 'Position')
        match_day_target (str): Match Day objetivo
        fecha_target (str): Fecha específica analizada
        df_fecha_completo (pl.DataFrame): DataFrame completo de la fecha para contexto adicional
    
    Returns:
        dict: Diccionario con todas las métricas calculadas para la entidad
              Incluye diferencias porcentuales, z-scores y metadatos
    """
    # Inicializar resultado para esta entidad
    resultado_entidad = {
        entity_type: entity_name,
        'Match_Day': match_day_target,
        'Date': fecha_target,
        'diferencias_porcentuales': {},
        'z_scores': {}
    }
    
    # Añadir información adicional según el tipo
    if entity_type == "Player" and 'Position' in df_entidad_fecha.columns:
        resultado_entidad['Position'] = df_entidad_fecha['Position'][0]
    elif entity_type == "Position" and 'Team' in df_fecha_completo.columns:
        resultado_entidad['Team'] = df_fecha_completo['Team'][0] if df_fecha_completo.height > 0 else 'Desconocido'
    
    # Calcular métricas para cada columna de interés
    for columna in columnas_disponibles:
        try:
            # Obtener valor de la entidad en df_fecha (valor actual)
            valor_fecha = df_entidad_fecha[columna][0]
            
            # Calcular estadística del grupo MD (valor de referencia para diferencia porcentual)
            if estadistica_seleccionada == "mean":
                valor_referencia = df_entidad_md[columna].mean()
            elif estadistica_seleccionada == "median":
                valor_referencia = df_entidad_md[columna].median()
            elif estadistica_seleccionada == "max":
                valor_referencia = df_entidad_md[columna].max()
            elif estadistica_seleccionada == "min":
                valor_referencia = df_entidad_md[columna].min()
            elif estadistica_seleccionada == "p75":
                valor_referencia = df_entidad_md[columna].quantile(0.75)
            elif estadistica_seleccionada == "p90":
                valor_referencia = df_entidad_md[columna].quantile(0.90)
            elif estadistica_seleccionada == "p95":
                valor_referencia = df_entidad_md[columna].quantile(0.95)
            else:
                print(f"Estadística no reconocida: {estadistica_seleccionada}")
                continue
            
            # Calcular media y desviación estándar para z-score (siempre usar fórmula estándar)
            media_referencia = df_entidad_md[columna].mean()
            std_referencia = df_entidad_md[columna].std()
            
            # Verificar valores válidos
            if valor_fecha is None or valor_referencia is None:
                resultado_entidad['diferencias_porcentuales'][columna] = None
                resultado_entidad['z_scores'][columna] = None
                continue
            
            # Calcular diferencia porcentual
            if valor_referencia != 0:
                diff_porcentual = ((valor_fecha - valor_referencia) / abs(valor_referencia)) * 100
                resultado_entidad['diferencias_porcentuales'][columna] = round(diff_porcentual, 2)
            else:
                resultado_entidad['diferencias_porcentuales'][columna] = None
            
            # Calcular z-score usando fórmula padrão: (valor - media) / desvio_padrão
            if std_referencia is not None and std_referencia != 0:
                z_score = (valor_fecha - media_referencia) / std_referencia
                resultado_entidad['z_scores'][columna] = round(z_score, 2)
            else:
                resultado_entidad['z_scores'][columna] = None
            
        except Exception as e:
            print(f"Error calculando métricas para {entity_name} - {columna}: {e}")
            resultado_entidad['diferencias_porcentuales'][columna] = None
            resultado_entidad['z_scores'][columna] = None
    
    return resultado_entidad

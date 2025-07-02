import polars as pl
import numpy as np
from datetime import datetime
import os


# Obtener la ruta base del proyecto basada en la ubicación de este archivo
BASE_PATH = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_GPS_PATH = os.path.join(BASE_PATH, 'data', 'gps')
DATA_PROCESSED_PATH = os.path.join(BASE_PATH, 'data', 'processed')


# Asegúrese de que la carpeta de datos procesados existe
def ensure_dir(directory):
    if not os.path.exists(directory):
        os.makedirs(directory)
        
def calcular_estadisticas(fecha=None, columnas_interes=None, estadistica=None):
    """
    Calculates comparative statistics for each player, position and team by Match Day.
    If fecha is provided, filters data for that specific date's Week Team.
    
    Args:
        fecha (str, optional): Date in dd/mm/yyyy format to filter data
        columnas_interes (list, optional): List of columns to calculate statistics for
            
    Returns:
        tuple: (df_estadisticas, df_estadisticas_position, df_estadisticas_team) with calculated statistics
    """
    # Asegurar que el directorio de datos procesados existe
    ensure_dir(DATA_PROCESSED_PATH)
    
    # Verificar que el archivo parquet existe y cargarlo
    path_to_parquet = os.path.join(DATA_GPS_PATH, 'df_gps.parquet')
    if not os.path.exists(path_to_parquet) or os.path.getsize(path_to_parquet) == 0:
        print("df_gps.parquet does not exist or is empty")
        return None, None, None
        
    try:
        df = pl.read_parquet(path_to_parquet)
        if df.height == 0:
            print("DataFrame is empty")
            return None, None, None
            
        # # Crear backup
        # backup_path = os.path.join(DATA_GPS_PATH, 'df_gps_backup.parquet')
        # df.write_parquet(backup_path)
        
        # Filtrar por fecha si se proporciona
        if fecha is not None:
            if 'Date' not in df.columns or 'Week Team' not in df.columns:
                print("Required date columns missing")
                return None, None, None
                
            # Obtener los datos de fecha específica para encontrar el Match Day correspondiente
            df_fecha = df.filter(pl.col('Date') == fecha)
            if df_fecha.height == 0:
                print(f"No data found for date {fecha}")
                return None, None, None
                
            # Obtener el Match Day y Week Team para la fecha especificada
            match_day_especifico = df_fecha['Match Day'][0]
            week_team = df_fecha['Week Team'][0]
            
            # Filtrar por Week Team para incluir MD en cálculos de porcentaje
            df = df.filter(pl.col('Week Team') == week_team)
            print(f"Filtered data for Week Team: {week_team}, target Match Day: {match_day_especifico}")
            
        # Cargar columnas de interés
        if columnas_interes is None:
            path_to_txt = os.path.join(DATA_GPS_PATH, 'Columnas_interés.txt')
            with open(path_to_txt, 'r') as f:
                columnas_interes = [line.strip() for line in f.readlines()]

        # Aplicar filtros
        df = (df.filter(pl.col('Match Day') != 'Rehab')
              .filter(pl.col('Player') != 'TEAM')
              .filter(pl.col('Team ') != 'TEAM')
              .filter(pl.col('Selection') == 'Drills')
              .with_columns(
                  pl.when(pl.col('Team ').str.contains('Sporting'))
                  .then(pl.lit('Sporting de Gijón'))
                  .otherwise(pl.col('Team '))
                  .alias('Team ')
              ))

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
            # Filtrar resultados para devolver solo los datos del Match Day específico
            df_estadisticas_filtrado = df_estadisticas.filter(pl.col('Match Day') == match_day_especifico)
            df_estadisticas_position_filtrado = df_estadisticas_position.filter(pl.col('Match Day') == match_day_especifico)
            df_estadisticas_team_filtrado = df_estadisticas_team.filter(pl.col('Match Day') == match_day_especifico)
            
            print(f"Returning statistics for specific Match Day: {match_day_especifico}")
            return df_estadisticas_filtrado, df_estadisticas_position_filtrado, df_estadisticas_team_filtrado
        
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
    """Función auxiliar para calcular estadísticas para columnas dadas"""
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
    if df is None or df.height == 0:
        print("El dataframe está vacío.")
        return df
    
    # Obtener las columnas de métricas (todas las columnas excepto Player, Position, Match Day y Estadistica)
    columnas_metricas = [col for col in df.columns if col not in ['Player', 'Position', 'Team', 'Match Day', 'Estadistica']]
    
    # Crear una copia del dataframe original
    df_resultado = df.clone()
    
    # Para cada combinación de jugador/posición/equipo y estadística
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
    
    # Para cada combinación de columnas de agrupación y estadística
    grupos = df.group_by(columnas_agrupacion)
    
    # Crear un diccionario para almacenar los valores de referencia (MD) para cada grupo y métrica
    valores_referencia = {}
    
    # Para cada grupo, obtener los valores de referencia (MD)
    for grupo_valores, grupo_df in grupos:
        # Verificar si existe el Match Day 'MD' en este grupo
        if 'MD' not in grupo_df['Match Day'].to_list():
            continue
        
        # Obtener los valores de referencia (MD)
        df_md = grupo_df.filter(pl.col('Match Day') == 'MD')
        
        # Crear una clave para este grupo
        clave_grupo = tuple(grupo_valores)
        
        # Almacenar los valores de referencia para cada métrica
        valores_referencia[clave_grupo] = {}
        for columna in columnas_metricas:
            if columna in df_md.columns:
                valores_referencia[clave_grupo][columna] = df_md[columna][0]
    
    # Para cada columna de métrica, crear una nueva columna con la diferencia porcentual
    for columna in columnas_metricas:
        # Nombre de la nueva columna
        nueva_columna = f"{columna} diff"
        
        # Crear una lista para almacenar los valores de diferencia porcentual
        valores_diff = []
        
        # Para cada fila del dataframe
        for fila in df_resultado.iter_rows(named=True):
            # Extraer los valores necesarios
            valores_grupo = {}
            for col in columnas_agrupacion:
                valores_grupo[col] = fila[col]
            match_day = fila['Match Day']
            
            # Si la columna no existe en esta fila, añadir None
            if columna not in fila:
                valores_diff.append(None)
                continue
                
            valor = fila[columna]
            
            # Si es el Match Day de referencia, la diferencia es 0%
            if match_day == 'MD':
                valores_diff.append(0.0)
                continue
            
            # Crear la clave para buscar en el diccionario de valores de referencia
            clave_grupo = tuple(valores_grupo[col] for col in columnas_agrupacion)
            
            # Verificar si existe el valor de referencia para este grupo y métrica
            if clave_grupo not in valores_referencia or columna not in valores_referencia[clave_grupo]:
                valores_diff.append(None)
                continue
            
            # Obtener el valor de referencia
            valor_referencia = valores_referencia[clave_grupo][columna]
            
            # Si el valor de referencia es cero, no se puede calcular el porcentaje
            if valor_referencia == 0:
                valores_diff.append(None)
                continue
            
            # Calcular la diferencia porcentual y redondear a dos decimales
            diff_porcentual = (valor / valor_referencia) * 100
            valores_diff.append(round(diff_porcentual, 2))
        
        # Añadir la nueva columna al dataframe resultado
        df_resultado = df_resultado.with_columns(pl.Series(nueva_columna, valores_diff))
    
    return df_resultado

# df, df1, df2 = calcular_estadisticas("30/11/2023")

# print(df1)

import polars as pl
import numpy as np
from datetime import datetime
import os

# Definir caminhos absolutos para as pastas de dados
# Obtener la ruta base del proyecto basada en la ubicación de este archivo
BASE_PATH = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_GPS_PATH = os.path.join(BASE_PATH, 'data', 'gps')
DATA_PROCESSED_PATH = os.path.join(BASE_PATH, 'data', 'processed')

# Garantir que a pasta de dados processados exista
def ensure_dir(directory):
    if not os.path.exists(directory):
        os.makedirs(directory)

def calcular_estadisticas():
    """
    Calcula estadísticas comparativas para cada jugador, posición y equipo por Match Day en las columnas de interés.
    Retorna tres dataframes con las estadísticas: media, mediana, máximo, mínimo, percentil 75, 90 y 95.
    - df_estadisticas: agrupado por jugador
    - df_estadisticas_position: agrupado por posición
    - df_estadisticas_team: agrupado por equipo
    """
    
    
    
    # Garantir que a pasta de dados processados exista
    ensure_dir(DATA_PROCESSED_PATH)
    
    # Cargar archivo CSV utilizando una ruta relativa
    path_to_csv = os.path.join(DATA_GPS_PATH, 'df_gps.csv')
    df = pl.read_csv(path_to_csv)
    
    # Cargar columnas de interés 
    path_to_txt = os.path.join(DATA_GPS_PATH, 'Columnas_interés.txt')
    with open(path_to_txt, 'r') as f:
        columnas_interes = [line.strip() for line in f.readlines()]
        #print(columnas_interes)

    # Lista de estadísticas a calcular
    estadisticas = ["mean", "median", "max", "min", "p75", "p90", "p95"]
    
    # Borrar filas que no contienen 'Rehab' en la columna 'Match Day'
    df = df.filter(pl.col('Match Day') != 'Rehab')
    
    # Borrar filas que contienen 'TEAM' en la columna 'Player'
    df = df.filter(pl.col('Player') != 'TEAM')
    
    # Borrar filas que contienen 'TEAM' en la columna 'Team'
    df = df.filter(pl.col('Team ') != 'TEAM')
    
    # Normalizar los valores de la columna 'Team '
    df = df.with_columns(
    pl.when(pl.col('Team ').str.contains('Sporting'))
    .then(pl.lit('Sporting de Gijón'))
    .otherwise(pl.col('Team '))
    .alias('Team ')
    )
    
    # Filtrar filas que contienen 'Drills' en la columna 'Selection'
    df = df.filter(pl.col('Selection') == 'Drills')
    
    # Obtener lista de match days, jugadores, posiciones y equipos únicos
    match_days = df['Match Day'].unique().to_list()
    jugadores = df['Player'].unique().to_list()
    posiciones = df['Position'].unique().to_list()
    equipos = df['Team '].unique().to_list()
    
    # Crear listas para almacenar los resultados
    resultados_jugadores = []
    resultados_position = []
    resultados_team = []
    
    
    ################### Jugadores ########################
    
    # Para cada jugador, match day y estadística, calcular los valores para las columnas de interés
    for jugador in jugadores:
        # Filtrar datos del jugador
        df_jugador = df.filter(pl.col('Player') == jugador)
        
        # Obtener la posición del jugador (tomamos la primera que aparece, asumiendo que es constante)
        if df_jugador.height > 0 and 'Position' in df_jugador.columns:
            posicion_jugador = df_jugador['Position'][0]
        else:
            posicion_jugador = "Desconocida"
        
        for match_day in match_days:
            # Filtrar datos del jugador para el match day específico
            df_jugador_match = df_jugador.filter(pl.col('Match Day') == match_day)
            
            # Si no hay datos para este jugador en este match day, continuar con el siguiente
            if df_jugador_match.height == 0:
                continue
            
            # Para cada estadística, crear un registro con todas las columnas de interés
            for estadistica in estadisticas:
                # Inicializar un diccionario para este registro
                registro = {
                    "Player": jugador,
                    "Position": posicion_jugador,
                    "Match Day": match_day,
                    "Estadistica": estadistica
                }
                
                # Calcular la estadística para cada columna de interés y añadirla al registro
                for columna in columnas_interes:
                    # Verificar si la columna existe en el dataframe
                    if columna not in df_jugador_match.columns:
                        continue
                    
                    # Calcular la estadística correspondiente
                    if estadistica == "mean":
                        valor = df_jugador_match[columna].mean()
                    elif estadistica == "median":
                        valor = df_jugador_match[columna].median()
                    elif estadistica == "max":
                        valor = df_jugador_match[columna].max()
                    elif estadistica == "min":
                        valor = df_jugador_match[columna].min()
                    elif estadistica == "p75":
                        valor = df_jugador_match[columna].quantile(0.75)
                    elif estadistica == "p90":
                        valor = df_jugador_match[columna].quantile(0.90)
                    elif estadistica == "p95":
                        valor = df_jugador_match[columna].quantile(0.95)
                    else:
                        continue
                    
                    # Añadir el valor calculado al registro usando el nombre de la columna como clave
                    registro[columna] = valor
                
                # Añadir el registro completo a la lista de resultados
                resultados_jugadores.append(registro)
    
    
    ################### Posición ########################
    
    # Para cada posición, match day y estadística, calcular los valores para las columnas de interés
    for posicion in posiciones:
        # Filtrar datos de la posición
        df_posicion = df.filter(pl.col('Position') == posicion)
        
        for match_day in match_days:
            # Filtrar datos de la posición para el match day específico
            df_posicion_match = df_posicion.filter(pl.col('Match Day') == match_day)
            
            # Si no hay datos para esta posición en este match day, continuar con el siguiente
            if df_posicion_match.height == 0:
                continue
            
            # Para cada estadística, crear un registro con todas las columnas de interés
            for estadistica in estadisticas:
                # Inicializar un diccionario para este registro
                registro = {
                    "Position": posicion,
                    "Match Day": match_day,
                    "Estadistica": estadistica
                }
                
                # Calcular la estadística para cada columna de interés y añadirla al registro
                for columna in columnas_interes:
                    # Verificar si la columna existe en el dataframe
                    if columna not in df_posicion_match.columns:
                        continue
                    
                    # Calcular la estadística correspondiente
                    if estadistica == "mean":
                        valor = df_posicion_match[columna].mean()
                    elif estadistica == "median":
                        valor = df_posicion_match[columna].median()
                    elif estadistica == "max":
                        valor = df_posicion_match[columna].max()
                    elif estadistica == "min":
                        valor = df_posicion_match[columna].min()
                    elif estadistica == "p75":
                        valor = df_posicion_match[columna].quantile(0.75)
                    elif estadistica == "p90":
                        valor = df_posicion_match[columna].quantile(0.90)
                    elif estadistica == "p95":
                        valor = df_posicion_match[columna].quantile(0.95)
                    else:
                        continue
                    
                    # Añadir el valor calculado al registro usando el nombre de la columna como clave
                    registro[columna] = valor
                
                # Añadir el registro completo a la lista de resultados por posición
                resultados_position.append(registro)
                
  
    ################### Equipo ########################
    
    # Para cada equipo, match day y estadística, calcular los valores para las columnas de interés
    for equipo in equipos:
        # Filtrar datos del equipo
        df_equipo = df.filter(pl.col('Team ') == equipo)
        
        for match_day in match_days:
            # Filtrar datos del equipo para el match day específico
            df_equipo_match = df_equipo.filter(pl.col('Match Day') == match_day)
            
            # Si no hay datos para este equipo en este match day, continuar con el siguiente
            if df_equipo_match.height == 0:
                continue
            
            # Para cada estadística, crear un registro con todas las columnas de interés
            for estadistica in estadisticas:
                # Inicializar un diccionario para este registro
                registro = {
                    "Team": equipo,
                    "Match Day": match_day,
                    "Estadistica": estadistica
                }
                
                # Calcular la estadística para cada columna de interés y añadirla al registro
                for columna in columnas_interes:
                    # Verificar si la columna existe en el dataframe
                    if columna not in df_equipo_match.columns:
                        continue
                    
                    # Calcular la estadística correspondiente
                    if estadistica == "mean":
                        valor = df_equipo_match[columna].mean()
                    elif estadistica == "median":
                        valor = df_equipo_match[columna].median()
                    elif estadistica == "max":
                        valor = df_equipo_match[columna].max()
                    elif estadistica == "min":
                        valor = df_equipo_match[columna].min()
                    elif estadistica == "p75":
                        valor = df_equipo_match[columna].quantile(0.75)
                    elif estadistica == "p90":
                        valor = df_equipo_match[columna].quantile(0.90)
                    elif estadistica == "p95":
                        valor = df_equipo_match[columna].quantile(0.95)
                    else:
                        continue
                    
                    # Añadir el valor calculado al registro usando el nombre de la columna como clave
                    registro[columna] = valor
                
                # Añadir el registro completo a la lista de resultados por equipo
                resultados_team.append(registro)
    
    
    # Crear los dataframes a partir de las listas de resultados
    df_estadisticas = pl.DataFrame(resultados_jugadores)
    df_estadisticas_position = pl.DataFrame(resultados_position)
    df_estadisticas_team = pl.DataFrame(resultados_team)
        
    # Guardar los dataframes de estadísticas en archivos CSV
    output_path = os.path.join(DATA_PROCESSED_PATH, 'df_jugadores_estadisticas.csv')
    output_path_position = os.path.join(DATA_PROCESSED_PATH, 'df_position_estadisticas.csv')
    output_path_team = os.path.join(DATA_PROCESSED_PATH, 'df_team._estadisticas.csv')
    
    df_estadisticas.write_csv(output_path)
    df_estadisticas_position.write_csv(output_path_position)
    df_estadisticas_team.write_csv(output_path_team)
    
    return df_estadisticas, df_estadisticas_position, df_estadisticas_team

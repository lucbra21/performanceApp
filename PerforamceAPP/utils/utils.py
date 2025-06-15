import polars as pl
import numpy as np
from datetime import datetime

def calcular_estadisticas_comparativas(df_principal, fecha_filtro, columnas_interes, estadistica='mean'):
    """
    Calcula estadísticas para variables de un DataFrame y las compara con el mismo DataFrame filtrado por fecha.
    
    Args:
        df_principal (polars.DataFrame): DataFrame principal con los datos
        fecha_filtro (str): Fecha en formato dd/mm/aaaa para filtrar el DataFrame
        columnas_interes (list): Lista de columnas para las cuales calcular estadísticas
        estadistica (str): Estadística a calcular ('mean', 'median', 'sum', 'max', 'min', 'p75', 'p90', 'p95', 'p99')
        
    Returns:
        tuple: (DataFrame filtrado, diccionario con diferencias porcentuales por jugador, 
                diccionario con diferencias porcentuales por equipo) 
    """
    # Crear una copia del DataFrame principal para no modificarlo
    df = df_principal.clone()
    
    # Borrar filas que no contienen 'Rehab' en la columna 'Match Day'
    df = df.filter(pl.col('Match Day') != 'Rehab')
    
    # Borrar filas que contienen 'TEAM' en la columna 'Player'
    df = df.filter(pl.col('Player') != 'TEAM')
    
    # Borrar filas que contienen 'TEAM' en la columna 'Team '
    df = df.filter(pl.col('Team ') != 'TEAM')
    
    # Normalizar los valores de la columna 'Team '
    df = df.with_columns(
    pl.when(pl.col('Team ').str.contains('Sporting'))
    .then(pl.lit('Sporting'))
    .otherwise(pl.col('Team '))
    .alias('Team ')
    )
    
    # Filtrar filas que contienen 'Drills' en la columna 'Selection'
    df = df.filter(pl.col('Selection') == 'Drills')
        
    # Calcular estadísticas para las columnas de interés
    if estadistica == 'mean':
        stats_jugador = df.group_by(['Player', 'Match Day']).agg(
            [pl.col(col).mean() for col in columnas_interes]
        )
        stats_equipo = df.group_by(['Team ', 'Match Day']).agg(
            [pl.col(col).mean() for col in columnas_interes]
        )
    elif estadistica == 'median':
        stats_jugador = df.group_by(['Player', 'Match Day']).agg(
            [pl.col(col).median() for col in columnas_interes]
        )
        stats_equipo = df.group_by(['Team ', 'Match Day']).agg(
            [pl.col(col).median() for col in columnas_interes]
        )
    elif estadistica == 'sum':
        stats_jugador = df.group_by(['Player', 'Match Day']).agg(
            [pl.col(col).sum() for col in columnas_interes]
        )
        stats_equipo = df.group_by(['Team ', 'Match Day']).agg(
            [pl.col(col).sum() for col in columnas_interes]
        )
    elif estadistica == 'max':
        stats_jugador = df.group_by(['Player', 'Match Day']).agg(
            [pl.col(col).max() for col in columnas_interes]
        )
        stats_equipo = df.group_by(['Team ', 'Match Day']).agg(
            [pl.col(col).max() for col in columnas_interes]
        )
    elif estadistica == 'min':
        stats_jugador = df.group_by(['Player', 'Match Day']).agg(
            [pl.col(col).min() for col in columnas_interes]
        )
        stats_equipo = df.group_by(['Team ', 'Match Day']).agg(
            [pl.col(col).min() for col in columnas_interes]
        )
    elif estadistica == 'p75':
        stats_jugador = df.group_by(['Player', 'Match Day']).agg(
            [pl.col(col).quantile(0.75) for col in columnas_interes]
        )
        stats_equipo = df.group_by(['Team ', 'Match Day']).agg(
            [pl.col(col).quantile(0.75) for col in columnas_interes]
        )
    elif estadistica == 'p90':
        stats_jugador = df.group_by(['Player', 'Match Day']).agg(
            [pl.col(col).quantile(0.90) for col in columnas_interes]
        )
        stats_equipo = df.group_by(['Team ', 'Match Day']).agg(
            [pl.col(col).quantile(0.90) for col in columnas_interes]
        )
    elif estadistica == 'p95':
        stats_jugador = df.group_by(['Player', 'Match Day']).agg(
            [pl.col(col).quantile(0.95) for col in columnas_interes]
        )
        stats_equipo = df.group_by(['Team ', 'Match Day']).agg(
            [pl.col(col).quantile(0.95) for col in columnas_interes]
        )
    elif estadistica == 'p99':
        stats_jugador = df.group_by(['Player', 'Match Day']).agg(
            [pl.col(col).quantile(0.99) for col in columnas_interes]
        )
        stats_equipo = df.group_by(['Team ', 'Match Day']).agg(
            [pl.col(col).quantile(0.99) for col in columnas_interes]
        )
    else:
        # Si la estadística no está en las opciones, usar mean por defecto
        stats_jugador = df.group_by(['Player', 'Match Day']).agg(
            [pl.col(col).mean() for col in columnas_interes]
        )
        stats_equipo = df.group_by(['Team ', 'Match Day']).agg(
            [pl.col(col).mean() for col in columnas_interes]
        )
    
    #save df into csv file
    # stats_equipo.write_csv('stats_equipo.csv')
    # stats_jugador.write_csv('stats_jugador.csv')
    
    # Convertir la fecha de filtro al formato correcto
    try:
        # Intentar filtrar directamente por la fecha como string
        df_filtrado = df.filter(pl.col('Date') == fecha_filtro)
        
        # Si no hay resultados, intentar otras variaciones de formato
        if df_filtrado.is_empty():
            # Imprimir valores únicos de Date para depuración
            print(f"Valores únicos en la columna Date: {df.select('Date').unique().to_series().to_list()}")
            print(f"Buscando fecha: {fecha_filtro}")
            
            # Intentar con trim para eliminar espacios
            df_filtrado = df.filter(pl.col('Date').str.strip() == fecha_filtro.strip())
            
        # Verificar si se encontraron resultados
        if not df_filtrado.is_empty():
            print(f"Se encontraron {df_filtrado.shape[0]} filas para la fecha {fecha_filtro}")
        else:
            print(f"No se encontraron filas para la fecha {fecha_filtro}")
            
    except Exception as e:
        print(f'Error al filtrar por fecha: {e}')
        df_filtrado = pl.DataFrame()

           
    
    # Filtrar filas que no contienen 'Rehab' en la columna 'Match Day'
    if not df_filtrado.is_empty():
        # Borrar filas que no contienen 'Rehab' en la columna 'Match Day'
        df_filtrado = df_filtrado.filter(pl.col('Match Day') != 'Rehab')
        
        # Borrar filas que contienen 'TEAM' en la columna 'Player'
        df_filtrado = df_filtrado.filter(pl.col('Player') != 'TEAM')
        
        # Borrar filas que contienen 'TEAM' en la columna 'Team '
        df_filtrado = df_filtrado.filter(pl.col('Team ') != 'TEAM')
        
        # Normalizar los valores de la columna 'Team '
        df_filtrado = df_filtrado.with_columns(
        pl.when(pl.col('Team ').str.contains('Sporting'))
        .then(pl.lit('Sporting'))
        .otherwise(pl.col('Team '))
        .alias('Team ')
        )
        
        # Filtrar filas que contienen 'Drills' en la columna 'Selection'
        df_filtrado = df_filtrado.filter(pl.col('Selection') == 'Drills')
        
        # Almacenar valor de Match Day
        matchday = df_filtrado.get_column('Match Day')[0]

        
        #save df into csv file
        #df_filtrado.write_csv('df_filtrado.csv')
        

        # stats_filtrado_equipo = df_filtrado_equipo.group_by('Team ').agg(
        #     [pl.col(col) for col in columnas_interes]
        # )
    
    # Crear diccionarios para almacenar las diferencias porcentuales
    diferencias_jugador = {}
    diferencias_equipo = {}
    
    # Calcular diferencias porcentuales para cada jugador y cada columna de interés
    if not df_filtrado.is_empty():
        for jugador in df_filtrado.get_column('Player').to_list():
            # Obtener estadísticas generales para este jugador 
            stats_generales_df = stats_jugador.filter((pl.col('Player') == jugador) & (pl.col('Match Day') == matchday))
            stats_generales = {}
        

            for col in columnas_interes:
                stats_generales[col] = stats_generales_df.get_column(col)[0]
            
            # Obtener estadísticas filtradas para este jugador
            stats_especificas_df = df_filtrado.filter(pl.col('Player') == jugador)
            stats_especificas = {}
            
            # if jugador == 'Nacho Méndez':
            #     #save df into csv file
            #     stats_especificas_df.write_csv('stats_especificas_df.csv')
            
            for col in columnas_interes:
                stats_especificas[col] = stats_especificas_df.get_column(col)[0]

            
            # Calcular diferencias porcentuales
            for columna in columnas_interes:
                if columna not in diferencias_jugador:
                    diferencias_jugador[columna] = {}
                
                # Evitar división por cero
                if stats_generales[columna] != 0:
                    diferencia = ((stats_especificas[columna] - stats_generales[columna]) / stats_generales[columna]) * 100
                else:
                    diferencia = float('nan')
                
                diferencias_jugador[columna][jugador] = diferencia
        
        # # Calcular diferencias porcentuales para cada equipo y cada columna de interés
        # for equipo in stats_filtrado_equipo.get_column('Team ').to_list():
        #     # Obtener estadísticas generales para este equipo (promedio de todos los Match Day)
        #     stats_generales_df = stats_equipo.filter(pl.col('Team ') == equipo)
        #     stats_generales = {}
            
        #     for col in columnas_interes:
        #         stats_generales[col] = stats_generales_df.get_column(col).mean()
            
        #     # Obtener estadísticas filtradas para este equipo
        #     stats_especificas_df = stats_filtrado_equipo.filter(pl.col('Team ') == equipo)
        #     stats_especificas = {}
            
        #     for col in columnas_interes:
        #         stats_especificas[col] = stats_especificas_df.get_column(col)[0]
            
        #     # Calcular diferencias porcentuales
        #     for columna in columnas_interes:
        #         if columna not in diferencias_equipo:
        #             diferencias_equipo[columna] = {}
                
        #         # Evitar división por cero
        #         if stats_generales[columna] != 0:
        #             diferencia = ((stats_especificas[columna] - stats_generales[columna]) / stats_generales[columna]) * 100
        #         else:
        #             diferencia = float('nan')
                
        #         diferencias_equipo[columna][equipo] = diferencia
    
    # Seleccionar solo las columnas requeridas: Date, Player, Match Day y columnas de interés
    if not df_filtrado.is_empty():
        columnas_a_mantener = ['Date', 'Match Day', 'Player'] + columnas_interes
        df_filtrado = df_filtrado.select(columnas_a_mantener)
    
    return df_filtrado, diferencias_jugador


# Ejemplo de uso:
import os
#Carregar o arquivo CSV usando caminho relativo
path_to_csv = os.path.join('..', 'data', 'gps', 'df_gps.csv')
df = pl.read_csv(path_to_csv)

# Definir as colunas de interesse
colunas_interes = ['Distance (m)', 'HIBD (m)']

# Chamar a função
df_filtrado, dif_jugador= calcular_estadisticas_comparativas(
    df_principal=df,
    fecha_filtro='23/08/2023',  # Ajuste esta data para uma que exista no seu conjunto de dados
    columnas_interes=colunas_interes,
    estadistica='p95'  # Testando com percentil 95
)

#save df into csv file
df_filtrado.write_csv('df_filtrado.csv')


# Imprimir resultados
# print("DataFrame filtrado:")
# print(df_filtrado)

print("\nDiferenças percentuais por jogador:")
for columna, valores in dif_jugador.items():
    print(f"\n{columna}:")
    for jugador, diferencia in valores.items():
        print(f"  {jugador}: {diferencia:.2f}%")

# print("\nDiferenças percentuais por equipe:")
# for columna, valores in dif_equipo.items():
#     print(f"\n{columna}:")
#     for equipo, diferencia in valores.items():
#         print(f"  {equipo}: {diferencia:.2f}%")
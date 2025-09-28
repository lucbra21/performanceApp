"""
=============================================================================
MÓDULO DE PROCESAMIENTO DE MÉTRICAS DEPORTIVAS (metrics.py)
=============================================================================

Este módulo contiene funciones especializadas para el procesamiento, cálculo y 
análisis de métricas de rendimiento deportivo. Incluye funcionalidades para 
métricas por minuto, estadísticas comparativas, z-scores y análisis de partidos.

FUNCIONES DISPONIBLES:
=============================================================================

CÁLCULO DE MÉTRICAS POR MINUTO:
- add_per_minute_metrics(): Añade métricas calculadas por minuto basándose en 
  la duración total de los entrenamientos y la configuración del archivo 
  metrics_mapping.json

CÁLCULO DE ESTADÍSTICAS BÁSICAS:
- calcular_metricas(): Función auxiliar que calcula estadísticas específicas 
  (media, mediana, máximo, mínimo, percentiles) sobre columnas dadas de un DataFrame

ANÁLISIS POR MATCH DAY:
- calcular_estadisticas_por_matchday(): Calcula estadísticas agrupadas por Match Day 
  para jugadores individuales, equipos y posiciones, procesando todos los datos 
  históricos disponibles

ANÁLISIS DE Z-SCORES:
- calcular_zscore_fecha_vs_matchday_acumulado(): Calcula z-scores comparando los 
  datos de una fecha específica con los datos históricos acumulados del mismo 
  Match Day para jugadores, equipos y posiciones

MÉTRICAS AJUSTADAS A 94 MINUTOS:
- calculate_metrics_for_94min(): Calcula métricas ajustadas a 94 minutos para 
  datos de partidos, aplicando regla de tres simple cuando la duración supera 
  los 50 minutos
- calculate_player_statistics_94min(): Calcula estadísticas (media, mediana, 
  percentiles) para métricas de 94 minutos agrupadas por jugador, posición y equipo

ANÁLISIS COMPARATIVO:
- calculate_percentage_difference_vs_reference(): Calcula diferencias porcentuales 
  entre las métricas de un día específico y las estadísticas de referencia ajustadas a 94 minutos

DEPENDENCIAS:
=============================================================================
- config.py: Para rutas base, configuración de directorios y funciones de utilidad
- data_access.py: Para carga y filtrado de datos GPS y de entrenamiento
- pages/sessionReport_utils.py: Para obtención de datos de tabla específicos
- metrics_mapping.json: Para configuración de cálculos por minuto y métodos de cálculo
"""

import os
import json
import polars as pl
import numpy as np 

# Importación de funciones de utilidad desde el módulo config.py
from .config import BASE_PATH, REFERENCES_PATH, ensure_dir
# Importación de todas las funciones de acceso a datos desde data_access.py
from .data_access import *


#### CÁLCULO DE MÉTRICAS POR MINUTO #### 

def add_per_minute_metrics(df):
    """
    PROPÓSITO:
    Añade métricas calculadas por minuto al DataFrame de entrenamiento y una columna 
    de Total_Minutes, basándose en la configuración definida en metrics_mapping.json.
    
    QUÉ HACE EXACTAMENTE:
    1. Verifica que el DataFrame no esté vacío
    2. Carga la configuración de metrics_mapping.json para determinar qué métricas 
       deben calcularse por minuto
    3. Convierte la columna 'Drills Duration' de formato HH:MM:SS a minutos totales
    4. Para cada métrica configurada con per_minute_calculation=true, calcula el 
       valor por minuto dividiendo la métrica total entre los minutos totales
    5. Añade las nuevas columnas con sufijo '_per_min' al DataFrame
    
    CÓMO LO HACE:
    - Utiliza Polars para manipulación eficiente de datos
    - Aplica str.split(':') para separar horas, minutos y segundos
    - Convierte cada componente a enteros y calcula minutos totales usando 
      list.eval() con condiciones when().then().otherwise()
    - Itera sobre la configuración de métricas para identificar cuáles calcular
    - Crea expresiones de Polars para calcular métricas por minuto usando 
      división simple: métrica_total / minutos_totales
    
    ARCHIVOS UTILIZADOS:
    - metrics_mapping.json (desde config/): Configuración de métricas y cálculos
    
    Args:
        df (pl.DataFrame): DataFrame con datos de entrenamiento que debe contener 
                          la columna 'Drills Duration' en formato HH:MM:SS
    
    Returns:
        pl.DataFrame: DataFrame original con columnas adicionales:
                     - 'Total Minutes': Duración total en minutos
                     - Columnas '_per_min': Métricas calculadas por minuto para 
                       cada métrica configurada con per_minute_calculation=true
    """
    # Verificación inicial: retornar DataFrame vacío si no hay datos
    if df.is_empty():
        return df
    
    # Cargar el mapeo de métricas para obtener la configuración per_minute_calculation
    # Este archivo define qué métricas deben tener cálculo por minuto
    metrics_mapping_path = os.path.join(BASE_PATH, "config", "metrics_mapping.json")
    try:
        with open(metrics_mapping_path, 'r', encoding='utf-8') as f:
            metrics_mapping = json.load(f)
    except Exception as e:
        print(f"Error al cargar metrics_mapping.json: {e}")
        return df
    
    # Convertir 'Drills Duration' de HH:MM:SS a minutos usando operaciones de Polars
    # El proceso: dividir por ':', convertir a enteros, aplicar conversión a minutos
    df_with_minutes = df.with_columns([
        pl.col('Drills Duration').str.split(':').list.eval(
            pl.element().cast(pl.Int32)  # Convertir cada elemento a entero
        ).list.eval(
            # Aplicar conversión: horas*60 + minutos + segundos/60
            pl.when(pl.int_range(pl.len()) == 0).then(pl.element() * 60)  # Horas a minutos
            .when(pl.int_range(pl.len()) == 1).then(pl.element())         # Minutos
            .when(pl.int_range(pl.len()) == 2).then(pl.element() / 60)    # Segundos a minutos
            .otherwise(0)
        ).list.sum().alias('Total Minutes')  # Sumar todos los componentes
    ])
    
    # Obtener columnas que deben tener cálculo por minuto según configuración
    per_minute_expressions = []
    for metric_key, metric_config in metrics_mapping.get('metrics', {}).items():
        # Verificar si esta métrica debe tener cálculo por minuto
        if metric_config.get('per_minute_calculation', True):
            project_name = metric_config.get('project_name', '')
            if project_name in df.columns:
                per_minute_col = f"{project_name}_per_min"
                # Crear expresión para calcular métrica por minuto
                per_minute_expressions.append(
                    (pl.col(project_name) / pl.col('Total Minutes')).alias(per_minute_col)
                )
    
    # Aplicar cálculos por minuto si hay expresiones definidas
    if per_minute_expressions:
        df_with_per_minute = df_with_minutes.with_columns(per_minute_expressions)
        return df_with_per_minute
    
    # Retornar DataFrame con columna Total Minutes si no hay métricas por minuto
    return df_with_minutes



#### CÁLCULO DE ESTADÍSTICAS BÁSICAS ####

def calcular_metricas(df, columnas, estadistica):
    """
    PROPÓSITO:
    Función auxiliar que calcula una estadística específica (media, mediana, máximo, 
    mínimo, percentiles) para todas las columnas especificadas en un DataFrame.
    
    QUÉ HACE EXACTAMENTE:
    1. Itera sobre cada columna especificada en la lista de columnas
    2. Verifica que la columna exista en el DataFrame antes de procesarla
    3. Aplica la función estadística correspondiente según el parámetro estadistica
    4. Almacena el resultado en un diccionario con el nombre de la columna como clave
    5. Retorna un diccionario con todos los valores calculados
    
    CÓMO LO HACE:
    - Utiliza métodos nativos de Polars para cálculos estadísticos eficientes
    - Implementa condicionales para seleccionar la función estadística apropiada
    - Para percentiles, utiliza el método quantile() con los valores correspondientes
    - Maneja casos donde la columna no existe saltándola sin generar errores
    
    ARCHIVOS UTILIZADOS:
    - Ninguno (función auxiliar que opera directamente sobre DataFrames)
    
    Args:
        df (pl.DataFrame): DataFrame con los datos a analizar
        columnas (list): Lista de nombres de columnas para calcular estadísticas
        estadistica (str): Tipo de estadística a calcular. Opciones válidas:
                          'mean', 'median', 'max', 'min', 'p75', 'p90', 'p95'
        
    Returns:
        dict: Diccionario donde las claves son nombres de columnas y los valores 
              son los resultados de la estadística calculada para cada columna
    """
    resultado = {}
    
    # Iterar sobre cada columna especificada
    for columna in columnas:
        # Verificar que la columna existe en el DataFrame
        if columna not in df.columns:
            continue
            
        # Aplicar la función estadística correspondiente usando métodos de Polars
        if estadistica == "mean":
            valor = df[columna].mean()          # Media aritmética
        elif estadistica == "median":
            valor = df[columna].median()        # Mediana (percentil 50)
        elif estadistica == "max":
            valor = df[columna].max()           # Valor máximo
        elif estadistica == "min":
            valor = df[columna].min()           # Valor mínimo
        elif estadistica == "p75":
            valor = df[columna].quantile(0.75)  # Percentil 75
        elif estadistica == "p90":
            valor = df[columna].quantile(0.90)  # Percentil 90
        elif estadistica == "p95":
            valor = df[columna].quantile(0.95)  # Percentil 95
        else:
            # Si la estadística no es reconocida, saltar esta columna
            continue
            
        # Almacenar el resultado en el diccionario
        resultado[columna] = valor
        
    return resultado



#### ANÁLISIS POR MATCH DAY ####

def calcular_estadisticas_por_matchday(save=False):
    """
    PROPÓSITO:
    Calcula estadísticas descriptivas para cada Match Day de todos los datos históricos 
    disponibles, agrupando por jugadores individuales, equipos y posiciones.
    
    QUÉ HACE EXACTAMENTE:
    1. Carga el dataset de entrenamiento usando load_dataset_entrenamiento() desde data_access.py
    2. Obtiene las columnas de interés y las métricas por minuto disponibles
    3. Identifica todos los valores únicos de Match Day, jugadores, posiciones y equipos
    4. Para cada combinación de jugador/equipo/posición y Match Day, calcula 7 estadísticas:
       media, mediana, máximo, mínimo, percentil 75, percentil 90 y percentil 95
    5. Organiza los resultados con nomenclatura específica: jugadores individuales, 
       'TEAM' para equipos, y 'POS_' como prefijo para posiciones
    6. Aplica ordenación personalizada y opcionalmente guarda los resultados
    
    CÓMO LO HACE:
    - Utiliza load_dataset_entrenamiento() para obtener datos filtrados y procesados
    - Aplica get_columns_of_interest() para obtener métricas base más columnas '_per_min'
    - Itera sobre cada jugador, equipo y posición para calcular estadísticas por Match Day
    - Utiliza calcular_metricas() como función auxiliar para los cálculos estadísticos
    - Implementa ordenación personalizada: individuales alfabético + TEAM + posiciones
    - Guarda resultados en formato Parquet en data/processed/references/ si save=True
    
    ARCHIVOS UTILIZADOS:
    - data_access.py: load_dataset_entrenamiento(), get_columns_of_interest()
    - config.py: BASE_PATH, ensure_dir() para manejo de directorios
    
    Args:
        save (bool): Si True, guarda el DataFrame resultante como 'estadisticas_matchday.parquet'
                    en la carpeta data/processed/references/. Por defecto False.
        
    Returns:
        pl.DataFrame: DataFrame con columnas:
                     - 'Player': Nombre del jugador, 'TEAM' para equipo, 'POS_posición' para posiciones
                     - 'Match Day': Día de entrenamiento específico
                     - 'Estadistica': Tipo de estadística calculada
                     - Columnas de métricas: Valores calculados para cada métrica
                     Retorna None si hay errores en el procesamiento
    """
    # Cargar datos GPS usando función auxiliar desde data_access.py
    df = load_dataset_entrenamiento()

    if df is None:
        print("Error al cargar datos GPS")
        return None, None, None
        
    try:
        # Verificar que el DataFrame no esté vacío después del filtrado
        if df.height == 0:
            print("DataFrame está vacío después del filtrado")
            return None, None, None

        # Obtener columnas de interés base desde data_access.py
        columnas_interes = get_columns_of_interest()
        # Añadir columnas de métricas por minuto (sufijo '_per_min')
        per_minute_columns = [col for col in df.columns if col.endswith('_per_min')]
        columnas_interes.extend(per_minute_columns)

        # Obtener valores únicos para agrupación usando métodos de Polars
        match_days = df['Match Day'].unique().to_list()
        jugadores = df['Player'].unique().to_list()
        posiciones = df['Position'].unique().to_list() 
        equipos = df['Team'].unique().to_list()

        # Estadísticas a calcular para cada grupo y Match Day
        estadisticas = ["mean", "median", "max", "min", "p75", "p90", "p95"]
        
        # Inicializar lista de resultados combinados
        resultados_combinados = []

        # CALCULAR ESTADÍSTICAS PARA JUGADORES INDIVIDUALES POR MATCH DAY
        for jugador in jugadores:
            # Filtrar datos específicos del jugador
            df_jugador = df.filter(pl.col('Player') == jugador)
            # Obtener posición del jugador (primera ocurrencia)
            posicion_jugador = df_jugador['Position'][0] if df_jugador.height > 0 else "Desconocida"
            
            for match_day in match_days:
                # Filtrar por Match Day específico
                df_match = df_jugador.filter(pl.col('Match Day') == match_day)
                if df_match.height == 0:
                    continue  # Saltar si no hay datos para este Match Day
                    
                # Calcular cada estadística para este jugador y Match Day
                for estadistica in estadisticas:
                    registro = {
                        "Player": jugador,
                        "Position": posicion_jugador,
                        "Match Day": match_day,
                        "Estadistica": estadistica
                    }
                    # Usar función auxiliar para calcular métricas
                    registro.update(calcular_metricas(df_match, columnas_interes, estadistica))
                    resultados_combinados.append(registro)

        # CALCULAR ESTADÍSTICAS PARA EQUIPOS POR MATCH DAY (renombrar como TEAM)
        for equipo in equipos:
            df_equipo = df.filter(pl.col('Team') == equipo)
            for match_day in match_days:
                df_match = df_equipo.filter(pl.col('Match Day') == match_day)
                if df_match.height == 0:
                    continue
                    
                for estadistica in estadisticas:
                    registro = {
                        "Player": "TEAM",  # Nomenclatura estándar para equipos
                        "Match Day": match_day,
                        "Estadistica": estadistica
                    }
                    registro.update(calcular_metricas(df_match, columnas_interes, estadistica))
                    resultados_combinados.append(registro)

        # Calcular estadísticas para posiciones por Match Day (agregar prefijo POS_)
        for posicion in posiciones:
            df_posicion = df.filter(pl.col('Position') == posicion)
            for match_day in match_days:
                df_match = df_posicion.filter(pl.col('Match Day') == match_day)
                if df_match.height == 0:
                    continue
                    
                for estadistica in estadisticas:
                    registro = {
                        "Player": f"POS_{posicion}",  # Agregar prefijo POS_
                        "Match Day": match_day,
                        "Estadistica": estadistica
                    }
                    registro.update(calcular_metricas(df_match, columnas_interes, estadistica))
                    resultados_combinados.append(registro)

        # Crear DataFrame combinado
        if not resultados_combinados:
            print("No se generaron resultados")
            return None, None, None
            
        df_combined = pl.DataFrame(resultados_combinados)
        
        # Implementar ordenación personalizada: jugadores individuales (alfabético), TEAM, posiciones con POS_
        # Separar jugadores por tipo para ordenación
        individual_players = [p for p in df_combined['Player'].unique().to_list() 
                            if p != "TEAM" and not p.startswith("POS_")]
        team_players = [p for p in df_combined['Player'].unique().to_list() 
                       if p == "TEAM"]
        position_players = [p for p in df_combined['Player'].unique().to_list() 
                          if p.startswith("POS_")]
        
        # Ordenar cada grupo
        individual_players.sort()  # Alfabético
        position_players.sort()    # Alfabético por posición
        
        # Crear orden final: individuales + TEAM + posiciones
        sorted_players = individual_players + team_players + position_players
        
        # Crear mapeo de orden
        player_order = {player: i for i, player in enumerate(sorted_players)}
        
        # Aplicar ordenación
        df_estadisticas = df_combined.with_columns(
            pl.col('Player').map_elements(lambda x: player_order.get(x, 999), return_dtype=pl.Int32).alias('sort_order')
        ).sort(['sort_order', 'Match Day', 'Estadistica']).drop('sort_order')


        # Crear carpeta data/processed si no existe
        processed_path = os.path.join(BASE_PATH, 'data', 'processed','references')
        ensure_dir(processed_path)
        
        if save:
            # Guardar el DataFrame combinado
            output_path = os.path.join(processed_path, 'estadisticas_matchday.parquet')
            df_estadisticas.write_parquet(output_path)
            
            print(f"Estadísticas por Match Day calculadas y guardadas exitosamente en: {output_path}")
            
        return df_estadisticas

    except Exception as e:
        print(f"Error al calcular estadísticas por Match Day: {str(e)}")
        return None, None, None



# Z SCORE

def calcular_zscore_fecha_vs_matchday_acumulado(fecha):
    """
    Calcula el z-score comparando los datos de una fecha específica con los datos 
    acumulados históricos del mismo Match Day.
    
    Esta función realiza un análisis estadístico avanzado que permite evaluar el rendimiento
    de jugadores, equipos y posiciones en una fecha específica comparándolo con su
    rendimiento histórico en el mismo tipo de Match Day. Utiliza get_table_data para
    obtener los valores actuales y get_specific_md_data para los datos históricos,
    calculando z-scores normalizados que indican cuántas desviaciones estándar se
    encuentra cada métrica respecto a su media histórica.
    
    Funcionalidad:
    - Carga datos de entrenamiento para la fecha especificada
    - Identifica el Match Day correspondiente a la fecha
    - Obtiene datos actuales usando estadística mediana para mayor robustez
    - Separa datos en tres categorías: jugadores individuales, equipo y posiciones
    - Recupera datos históricos del mismo Match Day excluyendo la fecha actual
    - Calcula z-scores para todas las métricas numéricas incluyendo métricas por minuto
    - Maneja conversiones de tipos de datos para compatibilidad con diferentes sistemas
    
    Implementación:
    - Utiliza load_dataset_entrenamiento() para cargar datos base
    - Emplea get_table_data() con estadística "median" para datos actuales
    - Usa get_specific_md_data() para obtener datos históricos del Match Day
    - Aplica get_columns_of_interest() para identificar métricas relevantes
    - Incluye columnas que terminan en '_per_min' para métricas por minuto
    - Calcula z-score usando fórmula: (valor_actual - media_histórica) / desviación_estándar
    - Requiere mínimo 2 valores históricos para calcular desviación estándar válida
    

    Args:
        fecha (str): Fecha específica en formato dd/mm/aaaa para calcular z-scores.
                    Debe corresponder a una fecha existente en el dataset de entrenamiento.
        
    Returns:
        pl.DataFrame: DataFrame con z-scores calculados que contiene:
                     - Columna 'Player': Nombres de jugadores, 'TEAM' para equipo, 
                       'POS_posición' para posiciones específicas
                     - Columnas de métricas: Cada métrica como columna separada con 
                       valores de z-score correspondientes
                     - Valores None para métricas sin suficientes datos históricos
                     - None si ocurren errores en el procesamiento o no hay datos
    
    """
    try:
        from .pages.sessionReport_utils import get_table_data
        
        # Obtener datos de jugadores para la fecha especificada
        df = load_dataset_entrenamiento()
        if df is None or df.height == 0:
            print("Error al cargar datos de entreinamiento")
            return None
        
        df_fecha = df.filter(pl.col('Date') == fecha)
        if df_fecha is None or df_fecha.height == 0:
            print("Error al cargar datos de df_fecha")
            return None
        
        # Obtener el Match Day para la fecha especificada
        match_day_especifico = df_fecha['Match Day'][0]
        
        # Obtener tabla con datos de la fecha específica usando diferentes estadísticas
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
        df_historico = get_specific_md_data(match_day_especifico, exclude_date=fecha)
        if df_historico is None or df_historico.height == 0:
            print(f"No se encontraron datos históricos para Match Day: {match_day_especifico}")
            return None
        

        # Obtener columnas numéricas (excluyendo Player)
        columnas_metrics = get_columns_of_interest()
        per_minute_columns = [col for col in df_fecha.columns if col.endswith('_per_min')]
        columnas_numericas =  columnas_metrics + per_minute_columns
        #print(f"Columnas numéricas para z-score: {columnas_numericas}")
        
        # Listas para construir el DataFrame final
        players_list = []
        zscore_data = {}
        
        # Inicializar diccionario para almacenar z-scores por columna
        for columna in columnas_numericas:
            zscore_data[columna] = []
        
        # CALCULAR Z-SCORES PARA JUGADORES
        for row in jugadores_actuales.iter_rows(named=True):
            jugador = row['Player']
            
            # Datos históricos del mismo jugador en el mismo Match Day
            df_jugador_historico = df_historico.filter(pl.col('Player') == jugador)
            if df_jugador_historico.height == 0:
                print(f"No hay datos históricos para el jugador: {jugador}")
                continue
            
            # Agregar el nombre del jugador a la lista
            players_list.append(jugador)
            
            for columna in columnas_numericas:
                if columna not in df_jugador_historico.columns:
                    zscore_data[columna].append(None)
                    continue
                    
                # Valor actual del jugador y convertir a tipo Python nativo
                valor_actual = row[columna]
                # Convertir valores NumPy a tipos Python nativos
                if hasattr(valor_actual, 'item'):
                    valor_actual = valor_actual.item()
                else:
                    valor_actual = float(valor_actual) if valor_actual is not None else None
                
                # Valores históricos del jugador 
                valores_historicos = df_jugador_historico[columna].to_list()
                if len(valores_historicos) < 2:  # Necesitamos al menos 2 valores para calcular std
                    zscore_data[columna].append(None)
                    continue
                    
                media_historica = np.mean(valores_historicos)
                std_historica = np.std(valores_historicos, ddof=1)  # Usar ddof=1 para muestra
                
                # Calcular z-score y convertir a tipo Python nativo
                if std_historica > 0:
                    z_score = (valor_actual - media_historica) / std_historica
                    zscore_data[columna].append(float(z_score))  # Convertir a float Python nativo
                else:
                    zscore_data[columna].append(None)
        

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
            
            # Agregar el nombre del equipo a la lista
            players_list.append('TEAM')
            
            for columna in columnas_numericas:
                # Valor actual del equipo (incluye métricas por minuto) y convertir a tipo Python nativo
                valor_actual = equipo_row[columna]
                # Convertir valores NumPy a tipos Python nativos
                if hasattr(valor_actual, 'item'):
                    valor_actual = valor_actual.item()
                else:
                    valor_actual = float(valor_actual) if valor_actual is not None else None
                
                # Valores históricos del equipo (promedios por fecha, incluyen métricas por minuto)
                valores_hist = [valores_historicos_por_fecha[f][columna] 
                               for f in fechas_historicas 
                               if columna in valores_historicos_por_fecha[f]]
                
                if len(valores_hist) < 2:
                    zscore_data[columna].append(None)
                    continue
                    
                media_historica = np.mean(valores_hist)
                std_historica = np.std(valores_hist, ddof=1)
                
                if std_historica > 0:
                    z_score = (valor_actual - media_historica) / std_historica
                    zscore_data[columna].append(float(z_score))  # Convertir a float Python nativo
                else:
                    zscore_data[columna].append(None)
        
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
            
            # Agregar el nombre de la posición a la lista
            players_list.append(posicion_name)
            
            for columna in columnas_numericas:
                # Valor actual de la posición (incluye métricas por minuto) y convertir a tipo Python nativo
                valor_actual = row[columna]
                # Convertir valores NumPy a tipos Python nativos
                if hasattr(valor_actual, 'item'):
                    valor_actual = valor_actual.item()
                else:
                    valor_actual = float(valor_actual) if valor_actual is not None else None
                
                # Valores históricos de la posición (promedios por fecha, incluyen métricas por minuto)
                valores_hist = [valores_historicos_por_fecha[f][columna] 
                               for f in fechas_historicas 
                               if f in valores_historicos_por_fecha and columna in valores_historicos_por_fecha[f]]
                
                if len(valores_hist) < 2:
                    zscore_data[columna].append(None)
                    continue
                    
                media_historica = np.mean(valores_hist)
                std_historica = np.std(valores_hist, ddof=1)
                
                if std_historica > 0:
                    z_score = (valor_actual - media_historica) / std_historica
                    zscore_data[columna].append(float(z_score))  # Convertir a float Python nativo
                else:
                    zscore_data[columna].append(None)
        
        # Crear el DataFrame final con los z-scores
        # Agregar la columna Player al diccionario de datos
        zscore_data['Player'] = players_list
        
        # Crear el DataFrame con Player como primera columna
        df_zscore = pl.DataFrame(zscore_data).select(['Player'] + columnas_numericas)
        
        return df_zscore
    
        
    except Exception as e:
        print(f"Error al calcular z-scores: {str(e)}")
        import traceback
        traceback.print_exc()
        return None



#### MÉTRICAS A 94 MINUTOS ####

def calculate_metrics_for_94min(save=False, last_games=4):
    """
    Calcula métricas ajustadas a 94 minutos para análisis de rendimiento en partidos.
    
    Esta función procesa datos de partidos (Match Day "MD") para normalizar las métricas
    de rendimiento de jugadores a un tiempo estándar de 94 minutos, permitiendo
    comparaciones justas independientemente del tiempo real jugado.
    El GPS divide cada parte del juego. Esta función suma el tiempo de ambas partes para cada jugador,
    comprueba si es superior a 50 minutos y, si lo es, une (a partir del método de cálculo)
    los valores de ambas partes para todas las métricas. 
    
    Funcionalidad:
    - Carga dataset de partidos usando load_dataset_partidos()
    - Procesa cada fecha y jugador individualmente
    - Aplica filtro de tiempo mínimo (50 minutos) para validez estadística
    - Calcula métricas ajustadas usando métodos específicos por tipo de métrica
    - Genera DataFrames históricos completos y de referencia (últimos partidos)
    - Guarda resultados en formato CSV en carpeta data/processed/references
    
    Implementación:
    - Utiliza load_dataset_partidos() para obtener datos de partidos
    - Emplea get_columns_of_interest() para identificar métricas relevantes
    - Incluye columnas que terminan en '_per_min' automáticamente
    - Aplica get_calculation_method() para determinar método de cálculo por métrica
    - Usa regla de tres simples: métrica_ajustada = métrica_total * 94 / minutos_reales
    - Maneja casos especiales como 'Dif. ACC/DEC' con lógica específica
    - Ordena resultados por fecha para facilitar análisis temporal
    
    Métodos de cálculo por tipo de métrica:
    - 'sum': Suma total ajustada proporcionalmente (distancias, sprints, etc.)
    - 'max': Valor máximo (percentil 99) sin ajuste temporal (velocidades máximas)
    - 'accelerations_minus_decelerations': Diferencia entre aceleraciones y desaceleraciones ajustadas

    
    Args:
        save (bool, optional): Si True, guarda los DataFrames en archivos CSV.
                              Defaults to False.
        last_games (int, optional): Número de últimos partidos para DataFrame de referencia.
                                   Defaults to 4.
        
    Returns:
        dict: Diccionario con estructura:
              {
                  'historico_completo': pl.DataFrame con todas las métricas históricas,
                  'ultimos_partidos': pl.DataFrame con métricas de últimos N partidos,
                  'fechas_procesadas': list con fechas procesadas,
                  'jugadores_procesados': list con jugadores procesados
              }
              Retorna None si no se puede cargar el dataset o no hay datos válidos.
    
    
    """
    df = load_dataset_partidos()
    if df is None:
        print("No se pudo cargar el dataset principal.")
        return None

    columns_of_interest = get_columns_of_interest() 
    per_minute_columns = [col for col in df.columns if col.endswith('_per_min')]
    columns_of_interest.extend(per_minute_columns)

    # Obtener todas las fechas únicas de todos los partidos
    unique_dates = df.select('Date').unique().to_series().to_list()

    # Lista para acumular los DataFrames de cada jugador
    result_dataframes = []

    # Loop por cada fecha
    for date in unique_dates:
        df_date = df.filter(pl.col('Date') == date)
        players = df_date.select('Player').unique().to_series().to_list()
        
        for player in players:
            df_player = df_date.filter(pl.col('Player') == player)
            
            # Extrair o valor total de minutos como escalar
            total_minutes = round(df_player['Total Minutes'].sum(),2)

            
            # Aplicar regra de três simples se total_minutes > 50
            if total_minutes > 50:
                # Calcular métricas usando métodos específicos para cada coluna
                metrics_94min = {}
                
                for col in columns_of_interest:
                    if col in df_player.columns:
                        method = get_calculation_method(col)
                        
                        if method == 'sum':
                            total_metric = df_player[col].sum()
                            adjusted_metric = total_metric * 94 / total_minutes
                        elif method == 'max':
                            total_metric = df_player[col].quantile(0.99)
                            adjusted_metric = total_metric
                        elif method == 'mean':
                            total_metric = df_player[col].mean()
                            adjusted_metric = total_metric * 94 / total_minutes
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
                            # Método por defecto: sum
                            total_metric = df_player[col].sum()
                            adjusted_metric = total_metric * 94 / total_minutes
                        
                        metrics_94min[f"{col}_94min"] = adjusted_metric
                
                # Crear DataFrame con Player, Date y métricas ajustadas
                if metrics_94min:  # Solo si hay métricas calculadas
                    player_data = {
                        'Player': [player],
                        'Position': [df_player['Position'].first()],
                        'Date': [date],
                        'Total Minutes en Partido': [total_minutes]
                    }
                    player_data.update({k: [v] for k, v in metrics_94min.items()})
                    
                    df_player_result = pl.DataFrame(player_data)
                    result_dataframes.append(df_player_result)

    # Concatenar todos los DataFrames acumulados
    if result_dataframes:
        df_final = pl.concat(result_dataframes, how="vertical")
        
        # Ordenar por fecha para obtener los últimos partidos
        df_final_sorted = df_final.sort('Date', descending=True)
        
        # Crear carpeta data/processed/references si no existe
        references_path = os.path.join(BASE_PATH, 'data', 'processed', 'references')
        ensure_dir(references_path)
        
        
        
        # Obtener últimas fechas únicas para crear DataFrame de referencia
        unique_dates_sorted = df_final_sorted.select('Date').unique().sort('Date', descending=True)
        last_dates = unique_dates_sorted.head(last_games).to_series().to_list()
        
        # Filtrar DataFrame para últimos partidos
        df_last_games = df_final_sorted.filter(pl.col('Date').is_in(last_dates))
        
        
        
        if save:
            # Guardar DataFrame histórico completo
            historical_file = os.path.join(references_path, 'metrics_94min_historical.parquet')
            df_final_sorted.write_parquet(historical_file)
            
            # Guardar DataFrame de los últimos partidos
            last_games_file = os.path.join(references_path, f'metrics_94min_last_{last_games}_games.parquet')
            df_last_games.write_parquet(last_games_file)
            
            print(f"DataFrames salvos em {references_path}:")
            print(f"- Histórico completo: metrics_94min_historical.parquet ({len(df_final_sorted)} registros)")
            print(f"- Últimos {last_games} jogos: metrics_94min_last_{last_games}_games.parquet ({len(df_last_games)} registros)")
        
        return df_final_sorted
    else:
        # Retornar DataFrame vacío con las columnas esperadas si no hay datos
        empty_columns = ['Player', 'Date'] + [f"{col}_94min" for col in columns_of_interest]
        return pl.DataFrame({col: [] for col in empty_columns})
    

def calculate_player_statistics_94min(save=True, last_games=None):
    """
    Calcula estadísticas descriptivas completas para métricas de 94 minutos agrupadas 
    por jugador, posición y equipo.
    
    Esta función procesa los datos de métricas ajustadas a 94 minutos para generar
    un conjunto completo de estadísticas descriptivas que permiten analizar el
    rendimiento histórico de jugadores individuales, posiciones específicas y el
    equipo en general. Calcula múltiples estadísticas (media, mediana, percentiles,
    máximo, mínimo) para cada métrica, proporcionando una base sólida para análisis
    comparativos y evaluación de rendimiento.
    
    Funcionalidad:
    - Obtiene datos de métricas de 94 minutos usando calculate_metrics_for_94min()
    - Identifica automáticamente todas las columnas de métricas disponibles
    - Calcula 7 estadísticas descriptivas diferentes para cada métrica
    - Agrupa resultados en tres categorías: jugadores, posiciones y equipo
    - Aplica ordenación personalizada para facilitar la interpretación
    - Guarda resultados en formato Parquet para análisis posteriores
    
    Implementación:
    - Utiliza calculate_metrics_for_94min() para obtener datos base
    - Emplea funciones de agregación de Polars para cálculos estadísticos
    - Aplica group_by() para agrupar por jugador y posición
    - Usa quantile() para calcular percentiles específicos
    - Implementa lógica de renombrado y prefijos para identificar categorías
    - Ordena resultados con prioridad: jugadores individuales, equipo, posiciones

    
    Args:
        save (bool, optional): Si True, guarda los DataFrames en archivos Parquet.
                              Defaults to True.
        last_games (int, optional): Número de últimos juegos a considerar para el cálculo.
                                   Si None, considera todos los juegos disponibles.
                                   Defaults to None.
        
    Returns:
        pl.DataFrame: DataFrame con estadísticas calculadas que contiene:
                     - Columna 'Player': Nombres de jugadores, 'TEAM' para equipo,
                       'POS_posición' para posiciones específicas
                     - Columna 'estadistica': Tipo de estadística calculada
                     - Columnas de métricas: Cada métrica de 94 minutos como columna separada
                     - Ordenado por categoría y alfabéticamente dentro de cada categoría
                     - DataFrame vacío si ocurren errores en el procesamiento

    """
    
    # Obter dados de métricas para 94 minutos, passando o parâmetro last_games se fornecido
    if last_games is not None:
        df_94min = calculate_metrics_for_94min(save=False, last_games=last_games)
    else:
        df_94min = calculate_metrics_for_94min()

    # Verificar se o DataFrame está vazio
    if df_94min.is_empty():
        print("O DataFrame de entrada está vazio.")
        return pl.DataFrame()
    
    # Obter colunas de métricas (todas que terminam em '_94min')
    metric_columns = [col for col in df_94min.columns if col.endswith('_94min')]
    
    if not metric_columns:
        print("Não foram encontradas colunas de métricas no DataFrame.")
        return pl.DataFrame()
    
    # Definir funções estatísticas
    stat_functions = {
        'mean': pl.mean,
        'median': pl.median,
        'max': pl.max,
        'min': pl.min,
        'p75': lambda col: pl.col(col).quantile(0.75),
        'p90': lambda col: pl.col(col).quantile(0.90), 
        'p99': lambda col: pl.col(col).quantile(0.99)
    }
    
    try:
        # Função auxiliar para calcular estatísticas para um grupo
        def calculate_group_statistics(df, group_col, group_name_prefix=""):
            group_results = []
            for stat_name, agg_function in stat_functions.items():
                df_stat = df.group_by(group_col).agg([
                    agg_function(col) for col in metric_columns
                ])
                
                # Renomear coluna de agrupamento para 'Player' e adicionar prefixo se necessário
                if group_col != 'Player':
                    df_stat = df_stat.rename({group_col: 'Player'})
                    if group_name_prefix:
                        df_stat = df_stat.with_columns(
                            pl.concat_str([pl.lit(group_name_prefix), pl.col('Player')], separator="").alias('Player')
                        )
                
                # Adicionar coluna de estatística
                df_stat = df_stat.with_columns(pl.lit(stat_name).alias('estadistica'))
                
                # Reordenar colunas
                columns_order = ['Player', 'estadistica'] + metric_columns
                df_stat = df_stat.select(columns_order)
                
                group_results.append(df_stat)
            
            return group_results
        
        # Lista para armazenar todos os DataFrames
        all_results = []
        
        # 1. Estatísticas por jogador individual
        all_results.extend(calculate_group_statistics(df_94min, 'Player'))
        
        # 2. Estatísticas por posição (com prefixo POS_)
        all_results.extend(calculate_group_statistics(df_94min, 'Position', 'POS_'))
        
        # 3. Estatísticas para toda a equipe
        for stat_name, agg_function in stat_functions.items():
            df_team_stat = df_94min.select([
                agg_function(col).alias(col) for col in metric_columns
            ])
            
            df_team_stat = df_team_stat.with_columns([
                pl.lit('TEAM').alias('Player'),
                pl.lit(stat_name).alias('estadistica')
            ])
            
            columns_order = ['Player', 'estadistica'] + metric_columns
            df_team_stat = df_team_stat.select(columns_order)
            
            all_results.append(df_team_stat)
        
        # Concatenar todos os DataFrames
        df_final = pl.concat(all_results, how="vertical")
        
        # Implementar ordenação personalizada: jogadores alfabeticamente, TEAM, posições com POS_
        df_final = df_final.with_columns(
            pl.when(pl.col('Player') == 'TEAM')
            .then(pl.lit(2))  # TEAM no meio
            .when(pl.col('Player').str.starts_with('POS_'))
            .then(pl.lit(3))  # Posições por último
            .otherwise(pl.lit(1))  # Jogadores individuais primeiro
            .alias('order_priority')
        )
        
        # Ordenar por prioridade, depois por Player alfabeticamente, depois por estatística
        df_final = df_final.sort(['order_priority', 'Player', 'estadistica'])
        
        # Remover coluna auxiliar
        df_final = df_final.drop('order_priority')
        
        if save:
            # Criar pasta data/processed/references se não existir
            references_path = os.path.join(BASE_PATH, 'data', 'processed', 'references')
            ensure_dir(references_path)
        
        
            # Salvar DataFrame principal
            main_file = os.path.join(references_path, 'statistics_94.parquet')
            df_final.write_parquet(main_file)
            
            print(f"DataFrame salvo em {references_path}:")
            print(f"- statistics_94.parquet ({len(df_final)} registros)")
            
            # Se last_games foi especificado, salvar também com nome específico
            if last_games is not None:
                last_games_file = os.path.join(references_path, f'statistics_94_last{last_games}games.parquet')
                df_final.write_parquet(last_games_file)
                print(f"- statistics_94_last{last_games}games.parquet ({len(df_final)} registros)")
        
        return df_final
        
    except Exception as e:
        print(f"Erro ao calcular estatísticas: {str(e)}")
        return pl.DataFrame()



def calculate_percentage_difference_vs_reference(selected_date, num_games=None, reference_statistic='median'):
    """
    Calcula la diferencia porcentual entre las métricas de un día específico
    y las estadísticas de referencia (calculadas con calculate_player_statistics_94min).
    Incluye comparaciones individuales, por posición y por equipe.
    
    Args:
        selected_date (str): Fecha específica para obtener datos del día
        num_games (int, optional): Número de juegos para calcular la referencia
        reference_statistic (str): Estadística de referencia ('median', 'mean', 'max', 'min', 'p75', 'p90', 'p99')
                                  Por defecto es 'median'
    
    Returns:
        pl.DataFrame: DataFrame con columnas 'Player' y diferencias porcentuales para cada métrica
    """
    
    def _calculate_percentage_differences(df_day_data, df_reference_data, session_columns, ref_columns):
        """
        Función auxiliar para calcular diferencias porcentuales entre datos del día y referencia.
        
        Args:
            df_day_data: DataFrame con datos del día
            df_reference_data: DataFrame con datos de referencia
            session_columns: Lista de columnas de métricas de sesión
            ref_columns: Lista de columnas de métricas de referencia
            
        Returns:
            pl.DataFrame: DataFrame con diferencias porcentuales
        """
        # Realizar join entre datos del día y referencia
        df_joined = df_day_data.join(df_reference_data, on='Player', how='inner')
        
        if df_joined.is_empty():
            return pl.DataFrame()
        
        # Calcular diferencias porcentuales para cada métrica
        percentage_expressions = [pl.col('Player')]
        
        for session_col, ref_col in zip(session_columns, ref_columns):
            percentage_expr = (
                ((pl.col(session_col) / pl.col(ref_col)) * 100).round(2)
            ).alias(f'{session_col}_diff_pct')
            percentage_expressions.append(percentage_expr)
        
        return df_joined.select(percentage_expressions)
    
    try:
        from .pages.sessionReport_utils import get_table_data
        
        # Obtener datos del día específico
        df_day = get_table_data(selected_date, reference_statistic)
        if df_day is None or df_day.is_empty():
            print(f"No se encontraron datos para la fecha {selected_date}.")
            return pl.DataFrame()
        
        # Obtener estadísticas de referencia
        df_reference = calculate_player_statistics_94min(save=False, last_games=num_games)	
        if df_reference is None or df_reference.is_empty():
            print("No se pudieron obtener estadísticas de referencia.")
            return pl.DataFrame()
        
        # Filtrar por la estadística de referencia seleccionada
        df_reference_filtered = df_reference.filter(pl.col('estadistica') == reference_statistic)
        
        if df_reference_filtered.is_empty():
            print(f"No se encontraron datos para la estadística '{reference_statistic}'.")
            return pl.DataFrame()
        
        # Obtener columnas de interés
        columns_of_interest = get_columns_of_interest()
        per_minute_columns = [col for col in df_day.columns if col.endswith('_per_min')]
        columns_of_interest.extend(per_minute_columns)
        
        session_metric_columns = columns_of_interest
        ref_metric_columns = [col for col in df_reference.columns if col.endswith('94min')]
        
        # Lista para almacenar todos los resultados
        result_list = []
        
        # 1. COMPARACIONES INDIVIDUALES POR JUGADOR
        players_in_day = df_day.select('Player').unique().to_series().to_list()
        df_reference_players = df_reference_filtered.filter(pl.col('Player').is_in(players_in_day))
        
        if not df_reference_players.is_empty():
            df_day_selected = df_day.select(['Player'] + session_metric_columns)
            df_reference_selected = df_reference_players.select(['Player'] + ref_metric_columns)
            
            df_individual = _calculate_percentage_differences(
                df_day_selected, df_reference_selected, session_metric_columns, ref_metric_columns
            )
            
            if not df_individual.is_empty():
                result_list.append(df_individual)
        
        # 2. COMPARACIONES POR POSICIÓN
        if 'Position' in df_day.columns:
            positions_in_day = df_day.select('Position').unique().to_series().to_list()
            
            # Filtrar referencias por posiciones y agregar prefijo 'POS_'
            df_reference_positions = df_reference_filtered.filter(pl.col('Player').is_in(positions_in_day))
            
            if not df_reference_positions.is_empty():
                # Agrupar datos del día por posición y calcular promedios
                df_day_by_position = df_day.group_by('Position').agg([
                    pl.col(col).mean().alias(col) for col in session_metric_columns
                ]).with_columns(
                    pl.concat_str([pl.lit('POS_'), pl.col('Position')]).alias('Player')
                ).drop('Position')
                
                # Agregar prefijo 'POS_' a las referencias de posición
                df_reference_pos_selected = df_reference_positions.with_columns(
                    pl.concat_str([pl.lit('POS_'), pl.col('Player')]).alias('Player')
                ).select(['Player'] + ref_metric_columns)
                
                df_position = _calculate_percentage_differences(
                    df_day_by_position, df_reference_pos_selected, session_metric_columns, ref_metric_columns
                )
                
                if not df_position.is_empty():
                    result_list.append(df_position)
        
        # 3. COMPARACIÓN POR EQUIPE
        df_reference_team = df_reference_filtered.filter(pl.col('Player') == 'Team')
        
        if not df_reference_team.is_empty():
            # Calcular métricas promedio de la equipe para el día
            df_day_team = df_day.select([
                pl.col(col).mean().alias(col) for col in session_metric_columns
            ]).with_columns(pl.lit('TEAM').alias('Player'))
            
            # Renombrar 'Team' a 'TEAM' en referencias
            df_reference_team_selected = df_reference_team.with_columns(
                pl.lit('TEAM').alias('Player')
            ).select(['Player'] + ref_metric_columns)
            
            df_team = _calculate_percentage_differences(
                df_day_team, df_reference_team_selected, session_metric_columns, ref_metric_columns
            )
            
            if not df_team.is_empty():
                result_list.append(df_team)
        
        # Concatenar y ordenar resultados
        if result_list:
            df_final = pl.concat(result_list, how="vertical")
            
            # Aplicar ordenación consistente: jugadores individuales, TEAM, posiciones (POS_)
            individual_players = df_final.filter(
                ~pl.col('Player').str.starts_with('POS_') & (pl.col('Player') != 'TEAM')
            ).sort('Player')
            
            team_data = df_final.filter(pl.col('Player') == 'TEAM')
            
            position_data = df_final.filter(
                pl.col('Player').str.starts_with('POS_')
            ).sort('Player')
            
            # Concatenar en orden: jugadores individuales, TEAM, posiciones
            df_sorted_parts = [individual_players, team_data, position_data]
            df_final = pl.concat([df for df in df_sorted_parts if not df.is_empty()], how="vertical")
            
            return df_final
        else:
            print("No fue posible calcular ninguna diferencia porcentual.")
            return pl.DataFrame()
        
    except Exception as e:
        print(f"Error al calcular diferencias porcentuales: {str(e)}")
        return pl.DataFrame()
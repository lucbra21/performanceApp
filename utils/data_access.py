"""
Módulo de Acceso y Procesamiento de Datos (data_access.py)

Este módulo centraliza todas las funciones relacionadas con el acceso, carga, filtrado y 
procesamiento de datos GPS y de entrenamiento. Proporciona una interfaz unificada para 
la gestión de datasets, configuración de métricas y filtrado de datos.

DESCRIPCIÓN GENERAL:
    El módulo data_access.py es el núcleo del sistema de gestión de datos de la aplicación.
    Se encarga de cargar datos desde archivos Parquet y CSV, aplicar filtros estándar,
    crear y mantener datasets de entrenamiento y partidos, y proporcionar funciones
    de configuración para el mapeo de métricas y columnas.

FUNCIONES DISPONIBLES:

    FUNCIONES DE CARGA DE DATOS:
    - load_gps_data: Carga datos GPS desde archivos específicos o el archivo principal

    FUNCIONES DE CONFIGURACIÓN:
    - get_filter_columns: Obtiene columnas de filtro desde la configuración de métricas
    - get_columns_of_interest: Define las columnas de métricas de interés para análisis
    - get_calculation_method: Obtiene el método de cálculo específico para cada métrica
    - get_column_mapping: Proporciona el mapeo entre nombres originales y de proyecto

    FUNCIONES DE LIMPIEZA DE DATOS:
    - apply_standard_filters: Aplica filtros estándar predefinidos a los datasets

    FUNCIONES DE CREACIÓN DE DATASETS:
    - create_dataset_entrenamiento: Crea el dataset principal de entrenamiento con métricas por minuto
    - load_dataset_entrenamiento: Carga el dataset de entrenamiento existente
    - create_dataset_partidos: Crea el dataset específico para datos de partidos
    - load_dataset_partidos: Carga el dataset de partidos existente

    FUNCIONES DE FILTRADO DE DATOS:
    - filter_data_by_date: Filtra datos por fecha específica
    - get_players_data: Obtiene datos de jugadores para una fecha determinada
    - get_specific_md_data: Obtiene datos específicos de matchday con opciones de exclusión

"""

import os
import json
import polars as pl
from datetime import datetime

# Importaciones específicas de los módulos utils
from .config import *
from .date import format_date

#### Carga de datos ####

def load_gps_data(file_path=None):
    """
    Función de Carga de Datos GPS
    
    PROPÓSITO:
        Carga datos GPS desde archivos específicos o desde el archivo parquet principal
        del sistema. Proporciona una interfaz unificada para acceder a datos GPS
        independientemente de su ubicación o formato.
    
    FUNCIONALIDAD:
        Esta función implementa un sistema de carga dual que permite:
        1. Carga automática del archivo principal de datos GPS (df_gps.parquet)
        2. Carga específica de archivos individuales en formatos CSV o Parquet
        
        La función detecta automáticamente el formato del archivo y aplica
        el método de lectura apropiado, garantizando compatibilidad con
        múltiples fuentes de datos.
    
    IMPLEMENTACIÓN:
        - Si no se especifica file_path, carga el archivo principal desde DATA_GPS_PATH
        - Si se especifica file_path, detecta el formato (.parquet o .csv) y carga apropiadamente
        - Incluye manejo robusto de errores para archivos inexistentes o corruptos
        - Retorna DataFrame vacío en caso de error para mantener la estabilidad del sistema
    

    
    PARÁMETROS:
        file_path (str, optional): Ruta específica del archivo a cargar.
                                 Si es None, carga el archivo parquet principal
                                 desde data/gps/df_gps.parquet
    
    VALORES DE RETORNO:
        pl.DataFrame: DataFrame de Polars con los datos GPS cargados.
                     Retorna DataFrame vacío si ocurre algún error durante la carga.

    """
    if file_path is None:
        # Cargar el archivo parquet principal de data/gps
        gps_file_path = os.path.join(DATA_GPS_PATH, 'df_gps.parquet')
        
        if not os.path.exists(gps_file_path):
            print(f"No se encontró el archivo GPS principal: {gps_file_path}")
            return pl.DataFrame()
        
        try:
            return pl.read_parquet(gps_file_path)
        except Exception as e:
            print(f"Error al cargar el archivo GPS principal: {str(e)}")
            return pl.DataFrame()
    else:
        # Cargar archivo específico
        try:
            if file_path.endswith('.parquet'):
                return pl.read_parquet(file_path)
            elif file_path.endswith('.csv'):
                return pl.read_csv(file_path)
            else:
                print(f"Formato de archivo no soportado: {file_path}")
                return pl.DataFrame()
        except Exception as e:
            print(f"Error al cargar el archivo {file_path}: {str(e)}")
            return pl.DataFrame()


#### Configuración de columnas a partir del mapeo ####

def get_filter_columns(original_dataset = False):
    """
    Función de Configuración de Columnas de Filtro
    
    PROPÓSITO:
        Obtiene las columnas de filtro desde el archivo de configuración de métricas
        (metrics_mapping.json). Estas columnas se utilizan como base para filtrar
        y estructurar los datasets principales del sistema.
    
    FUNCIONALIDAD:
        Esta función lee la configuración de métricas y extrae específicamente
        las columnas marcadas como 'filter' en el campo column_type. Permite
        obtener tanto los nombres originales como los nombres de proyecto,
        proporcionando flexibilidad en el mapeo de columnas.
        
        La función soporta dos modos de operación:
        1. Modo proyecto (default): Retorna nombres de columnas del proyecto
        2. Modo original: Retorna nombres de columnas originales del dataset
    
    IMPLEMENTACIÓN:
        - Lee el archivo metrics_mapping.json desde el directorio config/
        - Filtra métricas por column_type = 'filter'
        - Extrae nombres según el parámetro original_dataset
        - Incluye manejo robusto de errores para archivos inexistentes o malformados
        - Retorna lista vacía en caso de error para mantener estabilidad
    

    PARÁMETROS:
        original_dataset (bool, optional): Determina el tipo de nombres a retornar.
                                         False (default): Retorna project_name
                                         True: Retorna original_column_name
    
    VALORES DE RETORNO:
        list: Lista de nombres de columnas de filtro según el modo especificado.
              Retorna lista vacía si ocurre algún error o no se encuentran columnas.

    """
    try:
        # Usar BASE_PATH para acceder al archivo en config/
        metrics_file = os.path.join(BASE_PATH, 'config', 'metrics_mapping.json')
        if not os.path.exists(metrics_file):
            return []
        
        with open(metrics_file, 'r', encoding='utf-8') as f:
            metrics_data = json.load(f)
        
        # Filtrar solo las columnas que son filtros
        columns = []
        
        if original_dataset:
            for metric_key, metric_info in metrics_data.get('metrics', {}).items():
                if metric_info.get('column_type') == 'filter':
                    original_column_name = metric_info.get('original_column_name', '')
                    if original_column_name:
                        columns.append(original_column_name)
        else:
            for metric_key, metric_info in metrics_data.get('metrics', {}).items():
                if metric_info.get('column_type') == 'filter':
                    project_name = metric_info.get('project_name', '')
                    if project_name:
                        columns.append(project_name)
        
        return columns
        
    except Exception as e:
        print(f"Error al cargar columnas de filtro: {e}")
        return []

def get_columns_of_interest(original_dataset = False):
    """
    Función de Configuración de Columnas de Métricas de Interés
    
    PROPÓSITO:
        Obtiene las columnas de métricas de interés desde el archivo de configuración
        (metrics_mapping.json). Estas columnas representan las métricas principales
        que se utilizan en análisis, reportes y cálculos estadísticos del sistema.
    
    FUNCIONALIDAD:
        Esta función lee la configuración de métricas y extrae específicamente
        las columnas marcadas como 'metric' en el campo column_type. Proporciona
        flexibilidad para obtener nombres originales o nombres de proyecto,
        permitiendo compatibilidad con diferentes fuentes de datos.
        
        La función opera en dos modos:
        1. Modo proyecto (default): Retorna nombres de columnas del proyecto
        2. Modo original: Retorna nombres de columnas originales del dataset fuente
    
    IMPLEMENTACIÓN:
        - Lee el archivo metrics_mapping.json desde el directorio config/
        - Filtra métricas por column_type = 'metric'
        - Extrae nombres según el parámetro original_dataset
        - Incluye manejo robusto de errores para archivos inexistentes o corruptos
        - Retorna lista vacía en caso de error para mantener estabilidad del sistema
    
    
    PARÁMETROS:
        original_dataset (bool, optional): Determina el tipo de nombres de columna a retornar.
                                         False (default): Retorna project_name de las métricas
                                         True: Retorna original_column_name de las métricas
    
    VALORES DE RETORNO:
        list: Lista de nombres de columnas de métricas según el modo especificado.
              Retorna lista vacía si ocurre algún error o no se encuentran métricas.

    """
    try:
        # Usar BASE_PATH para acceder al archivo en config/
        metrics_file = os.path.join(BASE_PATH, 'config', 'metrics_mapping.json')
        if not os.path.exists(metrics_file):
            return []
        
        with open(metrics_file, 'r', encoding='utf-8') as f:
            metrics_data = json.load(f)
        
        # Filtrar solo las columnas que son métricas
        columns = []
        
        if original_dataset:
            for metric_key, metric_info in metrics_data.get('metrics', {}).items():
                if metric_info.get('column_type') == 'metric':
                    original_column_name = metric_info.get('original_column_name', '')
                    if original_column_name:
                        columns.append(original_column_name)
        else:
            for metric_key, metric_info in metrics_data.get('metrics', {}).items():
                if metric_info.get('column_type') == 'metric':
                    project_name = metric_info.get('project_name', '')
                    if project_name:
                        columns.append(project_name)
        
        return columns
        
    except Exception as e:
        print(f"Error al cargar columnas de interés: {e}")
        return []


def get_calculation_method(column_name):
    """
    Función de Obtención de Método de Cálculo para Métricas
    
    PROPÓSITO:
        Obtiene el método de cálculo apropiado para una métrica específica desde
        el archivo de configuración de métricas. Cada métrica puede tener un método
        de cálculo diferente (suma, máximo, promedio, etc.) según su naturaleza.
    
    FUNCIONALIDAD:
        Esta función busca en la configuración de métricas el método de cálculo
        específico asociado a una columna/métrica determinada. Los métodos soportados
        incluyen operaciones estadísticas básicas y cálculos especializados como
        aceleraciones menos desaceleraciones.
        
        La función implementa un sistema de búsqueda que:
        1. Busca la métrica por su nombre de proyecto
        2. Extrae el método de cálculo configurado
        3. Ignora métodos marcados como 'none'
        4. Retorna un método por defecto si no encuentra configuración
    
    IMPLEMENTACIÓN:
        - Lee el archivo metrics_mapping.json desde el directorio config/
        - Itera sobre todas las métricas buscando coincidencia por nombre
        - Filtra métodos válidos (ignora 'none')
        - Retorna 'sum' como método por defecto para casos no configurados
        - Incluye manejo robusto de errores para archivos inexistentes

    
    PARÁMETROS:
        column_name (str): Nombre de la columna/métrica para la cual se busca
                          el método de cálculo. Debe coincidir con project_name
                          en la configuración de métricas.
    
    VALORES DE RETORNO:
        str: Método de cálculo específico para la métrica. Valores posibles:
             - 'sum': Suma de valores (método por defecto)
             - 'max': Valor máximo
             - 'mean': Promedio aritmético
             - 'accelerations_minus_decelerations': Cálculo especializado
             - Otros métodos definidos en la configuración

    """
    try:
        # Cargar el archivo de mapeo de métricas
        metrics_file = os.path.join(BASE_PATH, 'config', 'metrics_mapping.json')
        if not os.path.exists(metrics_file):
            return 'sum'  # Método por defecto
        
        with open(metrics_file, 'r', encoding='utf-8') as f:
            metrics_data = json.load(f)
        
        # Buscar el método de cálculo para la columna específica
        for metric_key, metric_info in metrics_data.get('metrics', {}).items():
            project_name = metric_info.get('project_name', '')
            calculation_method = metric_info.get('calculation_method', '')
            
            # Comparar por nombre del proyecto y ignorar métodos 'none'
            if project_name == column_name and calculation_method != 'none':
                return calculation_method
        
        # Si no se encuentra, retornar método por defecto
        return 'sum'
        
    except Exception as e:
        print(f"Error al obtener método de cálculo: {e}")
        return 'sum'
    

def get_column_mapping():
    """
    Función de Creación de Mapeo de Columnas
    
    PROPÓSITO:
        Crea un diccionario de mapeo que relaciona los nombres de columnas originales
        del dataset fuente con los nombres de columnas del proyecto. Este mapeo es
        fundamental para la traducción y estandarización de nombres de columnas.
    
    FUNCIONALIDAD:
        Esta función lee la configuración de métricas y construye un diccionario
        que permite la conversión bidireccional entre nombres originales y nombres
        de proyecto. Es esencial para mantener la compatibilidad entre diferentes
        fuentes de datos y la nomenclatura interna del sistema.
        
        La función procesa todas las métricas configuradas y extrae:
        1. Nombres de columnas originales (original_column_name)
        2. Nombres de columnas del proyecto (project_name)
        3. Crea el mapeo directo entre ambos
    
    IMPLEMENTACIÓN:
        - Lee el archivo metrics_mapping.json desde el directorio config/
        - Itera sobre todas las métricas en la configuración
        - Extrae pares original_column_name -> project_name
        - Filtra entradas con nombres válidos (no vacíos)
        - Construye y retorna diccionario de mapeo completo
        - Incluye manejo robusto de errores para archivos inexistentes
    
    PARÁMETROS:
        Ninguno: La función no requiere parámetros de entrada.
    
    VALORES DE RETORNO:
        dict: Diccionario con mapeo {original_column_name: project_name}.
              Las claves son nombres originales y los valores son nombres de proyecto.
              Retorna diccionario vacío si ocurre algún error.

    """
    try:
        # Cargar el archivo de configuración de métricas
        metrics_file = os.path.join(BASE_PATH, 'config', 'metrics_mapping.json')
        if not os.path.exists(metrics_file):
            return {}
        
        with open(metrics_file, 'r', encoding='utf-8') as f:
            metrics_data = json.load(f)
        
        # Crear diccionario de mapeo
        column_mapping = {}
        for metric_key, metric_info in metrics_data.get('metrics', {}).items():
            original_name = metric_info.get('original_column_name', '')
            project_name = metric_info.get('project_name', '')
            if original_name and project_name:
                column_mapping[original_name] = project_name
        
        return column_mapping
        
    except Exception as e:
        print(f"Error al cargar mapeo de columnas: {e}")
        return {}


#### Limpieza y estandarización de datos ####

def apply_standard_filters(df):
    """
    Función de Aplicación de Filtros Estándar de Limpieza (para entranamiento)
    
    PROPÓSITO:
        Aplica un conjunto predefinido de filtros estándar a los datos GPS
    
    FUNCIONALIDAD:
        Esta función implementa una serie de filtros secuenciales que limpian
        los datos GPS eliminando:
        1. Registros de rehabilitación (Match Day = 'Rehab')
        2. Registros genéricos de Match Day (Match Day = 'MD')
        3. Registros de equipo completo (Player = 'TEAM' o Team = 'TEAM')
        4. Registros que no son de ejercicios específicos (Selection != 'Drills')
        
        Además, estandariza los nombres de equipos para mantener consistencia.
    
    IMPLEMENTACIÓN:
        - Verifica si el DataFrame está vacío antes de procesar
        - Aplica filtros secuenciales usando operaciones de Polars
        - Estandariza nombres de equipos (ej: variaciones de 'Sporting' -> 'Sporting de Gijón')
        - Utiliza operaciones encadenadas para eficiencia
        - Retorna DataFrame filtrado manteniendo estructura original
    
    
    PARÁMETROS:
        df (pl.DataFrame): DataFrame de Polars con datos GPS sin filtrar.
                          Debe contener las columnas: 'Match Day', 'Player', 
                          'Team ', 'Selection' para aplicar filtros correctamente.
    
    VALORES DE RETORNO:
        pl.DataFrame: DataFrame filtrado con los mismos tipos de columnas que el original.
                     Retorna DataFrame vacío si el input está vacío.
                     Contiene solo registros que pasan todos los filtros estándar.
    
    """
    if df.is_empty():
        return df
    
    # Aplicar filtros estándar
    df_filtered = (df.filter(pl.col('Match Day') != 'Rehab')
                    .filter(pl.col('Match Day') != "MD")
                    .filter(pl.col('Player') != 'TEAM')
                    .filter(pl.col('Team ') != 'TEAM')
                    .filter(pl.col('Selection') == 'Drills')
                    .with_columns(
                        pl.when(pl.col('Team ').str.contains('Sporting'))
                        .then(pl.lit('Sporting de Gijón'))
                        .otherwise(pl.col('Team '))
                        .alias('Team ')
                    ))
    
    return df_filtered


#### Creación del conjunto de datos de entrenamiento ####

def create_dataset_entrenamiento():
    """
    Función de Creación del Dataset de Entrenamiento Completo
    
    PROPÓSITO:
        Crea un dataset completo y limpio para análisis de entrenamiento, combinando
        datos GPS filtrados con métricas originales y métricas calculadas por minuto.
        Este dataset sirve como base para todos los análisis.
    
    FUNCIONALIDAD:
        Esta función ejecuta un pipeline completo de procesamiento de datos que incluye:
        1. Carga de datos GPS desde el archivo Parquet principal
        2. Aplicación de filtros estándar para eliminar datos no válidos
        3. Obtención de columnas de filtro desde metrics_mapping.json
        4. Obtención de columnas de métricas desde metrics_mapping.json
        5. Cálculo de métricas por minuto usando add_per_minute_metrics
        6. Renombrado de columnas usando nombres de proyecto
        7. Guardado del dataset procesado como dataset_entrenamiento_cleaned.parquet
        8. Retorno del dataset completo listo para análisis
    
    IMPLEMENTACIÓN:
        - Utiliza load_gps_data() para cargar datos base
        - Aplica apply_standard_filters() para limpieza inicial
        - Obtiene configuración de columnas desde JSON
        - Integra métricas por minuto del módulo metrics
        - Maneja renombrado de columnas según mapeo de configuración
        - Guarda resultado en formato Parquet para reutilización
        - Incluye validación de columnas disponibles vs requeridas
        - Manejo robusto de errores con fallback a DataFrame vacío
    
    
    PARÁMETROS:
        Ninguno. La función obtiene toda la información necesaria de archivos
        de configuración y variables globales del módulo.
    
    VALORES DE RETORNO:
        pl.DataFrame: Dataset completo con estructura:
                     - Columnas de filtro (renombradas según proyecto)
                     - Columna 'Total Minutes' 
                     - Métricas originales (renombradas según proyecto)
                     - Métricas por minuto (sufijo '_per_min')
                     Retorna DataFrame vacío en caso de error o datos insuficientes.

    """
    try:
        
        from .metrics import add_per_minute_metrics
        
        # Cargar datos GPS desde el archivo parquet principal
        df = load_gps_data()
        
        if df.is_empty():
            print("No se pudieron cargar los datos GPS.")
            return pl.DataFrame()
        
        # Aplicar filtros estándar para limpiar los datos antes del procesamiento
        #print("Aplicando filtros estándar para limpiar los datos...")
        df_filtered = apply_standard_filters(df)
        
        if df_filtered.is_empty():
            print("No hay datos después de aplicar los filtros estándar.")
            return pl.DataFrame()
        
        #print(f"Datos después del filtrado: {df_filtered.height} registros")
        
        # Obtener las columnas de filtro desde la configuración (nombres originales)
        filter_columns = get_filter_columns(original_dataset=True)
        #print(f"Columnas de filtro encontradas: {filter_columns}")
        
        # Obtener las columnas de métricas desde la configuración (nombres originales)
        metric_columns = get_columns_of_interest(original_dataset=True)
        #print(f"Columnas de métricas encontradas: {metric_columns}")
        
        # Combinar todas las columnas necesarias (filtros + métricas)
        all_required_columns = filter_columns + metric_columns
        
        if not all_required_columns:
            print("No se pudieron obtener las columnas desde la configuración.")
            return df_filtered
        
        # Verificar que las columnas requeridas existen en el DataFrame
        available_columns = [col for col in all_required_columns if col in df_filtered.columns]
        missing_columns = [col for col in all_required_columns if col not in df_filtered.columns]
        
        if missing_columns:
            print(f"Advertencia: Las siguientes columnas no están disponibles en los datos: {missing_columns}")
        
        # Separar columnas de filtro y métricas disponibles
        available_filter_columns = [col for col in filter_columns if col in df_filtered.columns]
        available_metric_columns = [col for col in metric_columns if col in df_filtered.columns]
        
        #print(f"Columnas de filtro disponibles: {available_filter_columns}")
        #print(f"Columnas de métricas disponibles: {available_metric_columns}")
        
        # Aplicar o processamento de métricas por minuto
        df_with_per_minute = add_per_minute_metrics(df_filtered)
        
        # Crear DataFrame final solo con las columnas requeridas (filtros + métricas + métricas por minuto)
        # Obtener las columnas de métricas por minuto que fueron añadidas
        per_minute_columns = [col for col in df_with_per_minute.columns if col.endswith('_per_min')]
        minutes_column = [col for col in df_with_per_minute.columns if col.endswith('Total Minutes')]
        
        # Combinar todas las columnas finales: filtros + métricas originales + métricas por minuto
        final_columns = available_filter_columns + minutes_column + available_metric_columns + per_minute_columns
        
        # Seleccionar solo las columnas requeridas
        df_selected = df_with_per_minute.select(final_columns)
        
        # Obtener el mapeo de nombres de columnas
        column_mapping = get_column_mapping()
        
        # Renombrar las columnas usando los nombres de proyecto
        rename_expressions = []
        for col in df_selected.columns:
            if col in column_mapping:
                # Renombrar columna original al nombre del proyecto
                rename_expressions.append(pl.col(col).alias(column_mapping[col]))
            else:
                # Mantener el nombre original si no hay mapeo (ej: columnas _per_min, Total Minutes)
                rename_expressions.append(pl.col(col))
        
        # Aplicar el renombrado
        df_final = df_selected.select(rename_expressions)
        
        #print(f"Columnas renombradas usando nombres de proyecto: {list(df_final.columns)}")
        
        # Guardar el dataset limpo en formato parquet
        output_path = os.path.join(DATA_GPS_PATH, "dataset_entrenamiento_cleaned.parquet")
        
        # Crear el directorio si no existe
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        
        # Eliminar el archivo anterior si existe para garantizar reemplazo completo
        if os.path.exists(output_path):
            os.remove(output_path)
            print(f"Archivo anterior eliminado: {output_path}")
        
        # Guardar el dataset final con solo las columnas requeridas
        df_final.write_parquet(output_path)
        #print(f"Dataset guardado exitosamente en: {output_path}")
        
        # print(f"Dataset del proyecto creado exitosamente con {df_final.height} registros.")
        # print(f"Columnas totales en dataset final: {len(df_final.columns)}")
        
        return df_final
        
    except Exception as e:
        #print(f"Error al crear el dataset del proyecto: {str(e)}")
        return pl.DataFrame()


def load_dataset_entrenamiento():
    """
    Función de Carga del Dataset de Entrenamiento Procesado
    
    PROPÓSITO:
        Carga el dataset de entrenamiento previamente procesado y guardado por
        create_dataset_entrenamiento(). Este dataset contiene datos GPS limpios,
        filtrados y enriquecidos con métricas por minuto, listo para análisis.
    
    FUNCIONALIDAD:
        Esta función proporciona acceso rápido al dataset procesado sin necesidad
        de reejecutar todo el pipeline de procesamiento. Incluye:
        1. Verificación de existencia del archivo dataset_entrenamiento_cleaned.parquet
        2. Carga del dataset desde formato Parquet
        3. Validación de que el dataset no esté vacío
        4. Retorno del DataFrame completo listo para análisis
        
        El dataset cargado contiene:
        - Datos filtrados (sin registros TEAM, Rehab, etc.)
        - Columnas de filtro renombradas según proyecto
        - Métricas originales renombradas según proyecto
        - Métricas calculadas por minuto (sufijo '_per_min')
        - Columna 'Total Minutes'
    
    IMPLEMENTACIÓN:
        - Construye ruta del archivo usando DATA_GPS_PATH
        - Verifica existencia del archivo antes de cargar
        - Utiliza pl.read_parquet() para carga eficiente
        - Valida que el DataFrame no esté vacío
        - Manejo de errores con mensajes informativos
        - Retorna DataFrame vacío en caso de error
    
    
    PARÁMETROS:
        Ninguno. La función accede directamente al archivo procesado
        en la ubicación estándar del proyecto.
    
    VALORES DE RETORNO:
        pl.DataFrame: Dataset completo procesado con estructura:
                     - Columnas de filtro (renombradas según proyecto)
                     - Columna 'Total Minutes'
                     - Métricas originales (renombradas según proyecto)
                     - Métricas por minuto (sufijo '_per_min')
                     Retorna DataFrame vacío si hay error o archivo no existe.
    """
    try:
        # Caminho para o dataset limpo
        cleaned_dataset_path = os.path.join(DATA_GPS_PATH, 'dataset_entrenamiento_cleaned.parquet')
        
        # Verificar se o arquivo existe
        if not os.path.exists(cleaned_dataset_path):
            print(f"Dataset limpo não encontrado: {cleaned_dataset_path}")
            print("Execute a função create_dataset_entrenamiento() primeiro para gerar o dataset.")
            return pl.DataFrame()
        
        # Carregar o dataset limpo
        df_cleaned = pl.read_parquet(cleaned_dataset_path)
        
        if df_cleaned.is_empty():
            print("Dataset limpo está vazio.")
            return pl.DataFrame()
        
        #print(f"Dataset limpo carregado com sucesso: {df_cleaned.height} registros, {len(df_cleaned.columns)} colunas")
        return df_cleaned
        
    except Exception as e:
        print(f"Erro ao carregar o dataset limpo: {str(e)}")
        return pl.DataFrame()


#### Creación del conjunto de datos de los partidos ####

def create_dataset_partidos():
    """
    Función de Creación del Dataset de Partidos Completo
    
    PROPÓSITO:
        Crea un dataset específico para análisis de partidos, combinando datos GPS
        filtrados con métricas originales y métricas calculadas por minuto.
        Este dataset se enfoca exclusivamente en datos de partidos oficiales.
    
    FUNCIONALIDAD:
        Esta función ejecuta un pipeline de procesamiento similar a create_dataset_entrenamiento
        pero con filtros específicos para partidos:
        1. Carga de datos GPS desde el archivo Parquet principal
        2. Aplicación de filtros específicos para partidos (Match Day = "MD")
        3. Filtrado por tipos de juego oficial (Fútbol_11v11_105x68m, Fútbol_10v10_105x68m)
        4. Obtención de columnas de filtro y métricas desde metrics_mapping.json
        5. Cálculo de métricas por minuto usando add_per_minute_metrics
        6. Renombrado de columnas usando nombres de proyecto
        7. Guardado del dataset procesado como dataset_partidos_cleaned.parquet
        8. Retorno del dataset completo listo para análisis de partidos
    
    IMPLEMENTACIÓN:
        - Utiliza load_gps_data() para cargar datos base
        - Aplica filtros específicos para partidos (MD, Selection oficial)
        - Excluye datos de rehabilitación y registros de equipo
        - Estandariza nombres de equipos (Sporting -> Sporting de Gijón)
        - Obtiene configuración de columnas desde JSON
        - Integra métricas por minuto del módulo metrics
        - Maneja renombrado de columnas según mapeo de configuración
        - Guarda resultado en formato Parquet para reutilización
        - Incluye validación de columnas disponibles vs requeridas
    
    
    PARÁMETROS:
        Ninguno. La función obtiene toda la información necesaria de archivos
        de configuración y variables globales del módulo.
    
    VALORES DE RETORNO:
        pl.DataFrame: Dataset completo de partidos con estructura:
                     - Columnas de filtro (renombradas según proyecto)
                     - Columna 'Total Minutes'
                     - Métricas originales (renombradas según proyecto)
                     - Métricas por minuto (sufijo '_per_min')
                     Retorna DataFrame vacío en caso de error o datos insuficientes.
    
    """
    try:
        
        from .metrics import add_per_minute_metrics
        
        # Cargar datos GPS desde el archivo parquet principal
        df = load_gps_data()
        
        if df.is_empty():
            print("No se pudieron cargar los datos GPS.")
            return pl.DataFrame()
        

        # Aplicar filtros estándar
        df_filtered = (df.filter(pl.col('Match Day') != 'Rehab')
                        .filter(pl.col('Match Day') == "MD")
                        .filter(pl.col('Player') != 'TEAM')
                        .filter(pl.col('Team ') != 'TEAM')
                        .filter(pl.col('Selection').is_in(['Fútbol_11v11_105x68m', 'Fútbol_10v10_105x68m']))
                        .with_columns(
                            pl.when(pl.col('Team ').str.contains('Sporting'))
                            .then(pl.lit('Sporting de Gijón'))
                            .otherwise(pl.col('Team '))
                            .alias('Team ')
                        ))

        
        if df_filtered.is_empty():
            print("No hay datos después de aplicar los filtros estándar.")
            return pl.DataFrame()
        
        #print(f"Datos después del filtrado: {df_filtered.height} registros")
        
        # Obtener las columnas de filtro desde la configuración (nombres originales)
        filter_columns = get_filter_columns(original_dataset=True)
        #print(f"Columnas de filtro encontradas: {filter_columns}")
        
        # Obtener las columnas de métricas desde la configuración (nombres originales)
        metric_columns = get_columns_of_interest(original_dataset=True)
        #print(f"Columnas de métricas encontradas: {metric_columns}")
        
        # Combinar todas las columnas necesarias (filtros + métricas)
        all_required_columns = filter_columns + metric_columns
        
        if not all_required_columns:
            print("No se pudieron obtener las columnas desde la configuración.")
            return df_filtered
        
        # Verificar que las columnas requeridas existen en el DataFrame
        missing_columns = [col for col in all_required_columns if col not in df_filtered.columns]
        
        if missing_columns:
            print(f"Advertencia: Las siguientes columnas no están disponibles en los datos: {missing_columns}")
        
        # Separar columnas de filtro y métricas disponibles
        available_filter_columns = [col for col in filter_columns if col in df_filtered.columns]
        available_metric_columns = [col for col in metric_columns if col in df_filtered.columns]
        
        #print(f"Columnas de filtro disponibles: {available_filter_columns}")
        #print(f"Columnas de métricas disponibles: {available_metric_columns}")
        
        # Aplicar o processamento de métricas por minuto
        df_with_per_minute = add_per_minute_metrics(df_filtered)
        
        # Crear DataFrame final solo con las columnas requeridas (filtros + métricas + métricas por minuto)
        # Obtener las columnas de métricas por minuto que fueron añadidas
        per_minute_columns = [col for col in df_with_per_minute.columns if col.endswith('_per_min')]
        minutes_column = [col for col in df_with_per_minute.columns if col.endswith('Total Minutes')]
        
        # Combinar todas las columnas finales: filtros + métricas originales + métricas por minuto
        final_columns = available_filter_columns + minutes_column + available_metric_columns + per_minute_columns
        
        # Seleccionar solo las columnas requeridas
        df_selected = df_with_per_minute.select(final_columns)
        
        # Obtener el mapeo de nombres de columnas
        column_mapping = get_column_mapping()
        
        # Renombrar las columnas usando los nombres de proyecto
        rename_expressions = []
        for col in df_selected.columns:
            if col in column_mapping:
                # Renombrar columna original al nombre del proyecto
                rename_expressions.append(pl.col(col).alias(column_mapping[col]))
            else:
                # Mantener el nombre original si no hay mapeo (ej: columnas _per_min, Total Minutes)
                rename_expressions.append(pl.col(col))
        
        # Aplicar el renombrado
        df_final = df_selected.select(rename_expressions)
        
        #print(f"Columnas renombradas usando nombres de proyecto: {list(df_final.columns)}")
        
        # Guardar el dataset limpo en formato parquet
        output_path = os.path.join(DATA_GPS_PATH, "dataset_partidos_cleaned.parquet")
        
        # Crear el directorio si no existe
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        
        # Eliminar el archivo anterior si existe para garantizar reemplazo completo
        if os.path.exists(output_path):
            os.remove(output_path)
            print(f"Archivo anterior eliminado: {output_path}")
        
        # Guardar el dataset final con solo las columnas requeridas
        df_final.write_parquet(output_path)
        #print(f"Dataset guardado exitosamente en: {output_path}")
        
        # print(f"Dataset del proyecto creado exitosamente con {df_final.height} registros.")
        # print(f"Columnas totales en dataset final: {len(df_final.columns)}")
        
        return df_final
        
    except Exception as e:
        #print(f"Error al crear el dataset del proyecto: {str(e)}")
        return pl.DataFrame()


def load_dataset_partidos():
    """
    Función de Carga del Dataset de Partidos Procesado
    
    PROPÓSITO:
        Carga el dataset de partidos previamente procesado y guardado por
        create_dataset_partidos(). Este dataset contiene datos GPS limpios
        específicos de partidos oficiales, filtrados y enriquecidos con métricas
        por minuto, listo para análisis de rendimiento en competición.
    
    FUNCIONALIDAD:
        Esta función proporciona acceso rápido al dataset de partidos procesado
        sin necesidad de reejecutar todo el pipeline de procesamiento. Incluye:
        1. Verificación de existencia del archivo dataset_partidos_cleaned.parquet
        2. Carga del dataset desde formato Parquet
        3. Validación de que el dataset no esté vacío
        4. Retorno del DataFrame completo listo para análisis de partidos
        
        El dataset cargado contiene:
        - Datos filtrados específicos de partidos (Match Day = "MD")
        - Solo registros de juego oficial (Fútbol_11v11_105x68m, Fútbol_10v10_105x68m)
        - Columnas de filtro renombradas según proyecto
        - Métricas originales renombradas según proyecto
        - Métricas calculadas por minuto (sufijo '_per_min')
        - Columna 'Total Minutes'
    
    IMPLEMENTACIÓN:
        - Construye ruta del archivo usando DATA_GPS_PATH
        - Verifica existencia del archivo antes de cargar
        - Utiliza pl.read_parquet() para carga eficiente
        - Valida que el DataFrame no esté vacío
        - Manejo de errores con mensajes informativos
        - Retorna DataFrame vacío en caso de error
    

    
    PARÁMETROS:
        Ninguno. La función accede directamente al archivo procesado
        en la ubicación estándar del proyecto.
    
    VALORES DE RETORNO:
        pl.DataFrame: Dataset completo de partidos procesado con estructura:
                     - Columnas de filtro (renombradas según proyecto)
                     - Columna 'Total Minutes'
                     - Métricas originales (renombradas según proyecto)
                     - Métricas por minuto (sufijo '_per_min')
                     Retorna DataFrame vacío si hay error o archivo no existe.
    """
    try:
        # Caminho para o dataset limpo
        cleaned_dataset_path = os.path.join(DATA_GPS_PATH, 'dataset_partidos_cleaned.parquet')
        
        # Verificar se o arquivo existe
        if not os.path.exists(cleaned_dataset_path):
            print(f"Dataset limpo não encontrado: {cleaned_dataset_path}")
            print("Execute a função create_dataset_partidos() primeiro para gerar o dataset.")
            return pl.DataFrame()
        
        # Carregar o dataset limpo
        df_cleaned = pl.read_parquet(cleaned_dataset_path)
        
        if df_cleaned.is_empty():
            print("Dataset limpo está vazio.")
            return pl.DataFrame()
        
        #print(f"Dataset limpo carregado com sucesso: {df_cleaned.height} registros, {len(df_cleaned.columns)} colunas")
        return df_cleaned
        
    except Exception as e:
        print(f"Erro ao carregar o dataset limpo: {str(e)}")
        return pl.DataFrame()


#### Dataframes filtrados (a partir del conjunto de datos de entrenamiento) ####

def filter_data_by_date(selected_date):
    """
    Filtra el dataset limpio de entrenamiento para una fecha específica.
    
    Esta función carga el dataset limpio de entrenamiento y filtra los datos para obtener
    únicamente los registros correspondientes a la fecha especificada. Es útil para análisis
    de sesiones específicas de entrenamiento.
    
    Funcionalidad:
    - Carga el dataset limpio de entrenamiento usando load_dataset_entrenamiento()
    - Formatea la fecha de entrada al formato estándar usando format_date()
    - Filtra los datos por la columna 'Date' para la fecha especificada
    - Proporciona información de debug si no se encuentran datos
    - Muestra fechas disponibles como referencia en caso de no encontrar datos
    
    Implementación:
    - Utiliza Polars para operaciones eficientes de filtrado
    - Maneja diferentes formatos de fecha de entrada
    - Incluye validación de datos y manejo de errores
    - Proporciona retroalimentación informativa al usuario
    

    
    Args:
        selected_date (str): Fecha objetivo en formato YYYY-MM-DD o dd/mm/aaaa.
                           La función acepta múltiples formatos y los convierte
                           automáticamente al formato estándar.
        
    Returns:
        pl.DataFrame: DataFrame de Polars filtrado conteniendo únicamente los registros
                     de la fecha especificada. Incluye todas las columnas del dataset
                     limpio (datos filtrados, métricas originales y métricas por minuto).
                     Retorna DataFrame vacío si no se encuentran datos para la fecha
                     o si ocurre algún error.

    
    Ejemplo de uso:
        # Filtrar datos para una fecha específica
        df_fecha = filter_data_by_date('15/01/2024')
    """
    try:
        # Cargar el dataset limpio
        df = load_dataset_entrenamiento()
        if df is None or df.is_empty():
            print("No fue posible cargar el dataset limpio")
            return pl.DataFrame()
        
        # Formatear la fecha al formato estándar
        formatted_date = format_date(selected_date)
        
        # Filtrar datos para la fecha formateada
        df_fecha = df.filter(pl.col('Date') == formatted_date)
        
        if df_fecha.is_empty():
            print(f"No se encontraron datos para la fecha: {formatted_date}")
            # Mostrar algunas fechas disponibles para debug
            available_dates = df.select('Date').unique().limit(5)['Date'].to_list()
            print(f"Fechas disponibles (muestra): {available_dates}")
            return pl.DataFrame()
        
        print(f"Encontrados {df_fecha.height} registros para la fecha {formatted_date}")
        return df_fecha
        
    except Exception as e:
        print(f"Error al filtrar datos por fecha: {str(e)}")
        return pl.DataFrame()
    
    
def get_players_data(selected_date):
    """
    Procesa y filtra los datos de jugadores para una fecha específica de entrenamiento.
    
    Esta función obtiene los datos filtrados para una fecha específica y selecciona únicamente
    las columnas relevantes para el análisis de jugadores, incluyendo información básica,
    métricas de interés y métricas calculadas por minuto.
    
    Funcionalidad:
    - Utiliza filter_data_by_date() para obtener datos de la fecha especificada
    - Selecciona columnas básicas de identificación (Player, Position)
    - Incluye todas las columnas de interés definidas en metrics_mapping.json
    - Agrega métricas calculadas por minuto (columnas terminadas en '_min')
    - Valida la existencia de datos antes de procesarlos
    
    Implementación:
    - Combina columnas básicas, de interés y por minuto en una selección optimizada
    - Utiliza Polars para operaciones eficientes de selección de columnas
    - Incluye manejo robusto de errores con trazabilidad completa
    - Proporciona retroalimentación informativa sobre el procesamiento
    

    
    Args:
        selected_date (str): Fecha objetivo en formato YYYY-MM-DD o dd/mm/aaaa.
                           Se acepta cualquier formato compatible con format_date().
        
    Returns:
        pl.DataFrame or None: DataFrame de Polars conteniendo únicamente las columnas
                             relevantes para análisis de jugadores:
                             - Columnas básicas: Player, Position
                             - Columnas de interés: métricas definidas en configuración
                             - Métricas por minuto: columnas calculadas terminadas en '_min'
                             Retorna None si no se encuentran datos para la fecha
                             o si ocurre algún error durante el procesamiento.
    
    
    Ejemplo de uso:
        # Obtener datos de jugadores para una fecha específica
        df_players = get_players_data('15/01/2024')
        
    """
    try:
        df_fecha = filter_data_by_date(selected_date)
        
        # Verificar si se encontraron datos para la fecha        
        if df_fecha is None:
            print(f"No se encontraron datos para la fecha: {selected_date}")
            return None
        
        # Seleccionar columnas básicas + columnas de interés que existan en el DataFrame
        basic_columns = ['Player', 'Position']
        per_minute_columns = [col for col in df_fecha.columns if col.endswith('_min')]
        columns_of_interest = get_columns_of_interest()
        
        selected_columns = basic_columns + columns_of_interest + per_minute_columns
        #print(f"Columnas seleccionadas: {selected_columns}")
        
        if df_fecha.height > 0:
            result_df = df_fecha.select(selected_columns)
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
    Obtiene datos históricos acumulados de un Match Day específico para análisis comparativo.
    
    Esta función filtra el dataset limpio de entrenamiento para obtener únicamente los datos
    correspondientes a un tipo específico de Match Day (ej: MD-1, MD+1), excluyendo
    opcionalmente una fecha específica.
    
    Funcionalidad:
    - Carga el dataset limpio de entrenamiento completo
    - Filtra datos por el Match Day objetivo especificado
    - Excluye opcionalmente una fecha específica del análisis
    - Retorna datos históricos para análisis comparativo
    - Permite análisis de tendencias y patrones por tipo de sesión
    
    Implementación:
    - Utiliza load_dataset_entrenamiento() para cargar datos completos
    - Aplica filtros secuenciales usando Polars para eficiencia
    - Maneja casos donde no existen datos para el Match Day especificado
    - Incluye validación de datos de entrada
    
    
    Args:
        match_day_target (str): Tipo de Match Day objetivo para filtrar.
                               Ejemplos: 'MD-1', 'MD+1', 'MD-2', 'MD+2', etc.
                               Debe coincidir exactamente con los valores en la
                               columna 'Match Day' del dataset.
        exclude_date (str, optional): Fecha específica a excluir del análisis
                                     en formato dd/mm/aaaa. Útil para excluir
                                     la sesión actual al calcular referencias
                                     históricas. Por defecto es None.
    
    Returns:
        pl.DataFrame or None: DataFrame de Polars conteniendo todos los registros
                             históricos del Match Day especificado. Incluye todas
                             las columnas del dataset limpio (datos filtrados,
                             métricas originales y métricas por minuto).
                             Retorna None si no se pueden cargar los datos o
                             si no existen registros para el Match Day especificado.


    """
    # Cargar datos GPS
    df = load_dataset_entrenamiento()
    if df is None:
        print("Error loading GPS data")
        return None
    
    
    # Filtrar por Match Day objetivo
    df_md = df.filter(pl.col('Match Day') == match_day_target)
    
    # Excluir fecha específica si se proporciona
    if exclude_date is not None:
        df_md = df_md.filter(pl.col('Date') != exclude_date)
    
    return df_md

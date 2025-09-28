"""
Módulo de Configuración y Rutas del Sistema (config.py)

Este módulo centraliza la configuración de rutas y directorios del proyecto.
Proporciona constantes globales para las rutas principales
del sistema y funciones para la gestión automática de directorios.

FUNCIONES DISPONIBLES:
====================

1. ensure_dir(directory)
   - Propósito: Crear directorios de forma segura si no existen
   - Uso: Garantizar que las rutas necesarias estén disponibles antes de operaciones de archivo
   - Retorna: None (función de utilidad)

2. get_base_path()
   - Propósito: Obtener la ruta base del proyecto de forma consistente
   - Uso: Acceso centralizado a la ruta raíz del proyecto
   - Retorna: str con la ruta base absoluta

3. setup_project_paths()
   - Propósito: Inicializar toda la estructura de directorios del proyecto
   - Uso: Configuración inicial del sistema o verificación de integridad de directorios
   - Retorna: dict con todas las rutas principales del proyecto

CONSTANTES DE RUTAS:
===================
- BASE_PATH: Ruta raíz del proyecto
- DATA_GPS_PATH: Directorio de datos GPS
- DATA_RAW_PATH: Directorio de datos sin procesar
- DATA_PROCESSED_PATH: Directorio de datos procesados
- REFERENCES_PATH: Directorio de datos de referencia
- TEMP_PATH: Directorio temporal
- METRICS_MAPPING_PATH: Archivo de configuración de métricas
"""

import os
from pathlib import Path

# ============================================================================
# CONFIGURACIÓN DE RUTAS BASE DEL PROYECTO
# ============================================================================

# Ruta base del proyecto: Se calcula dinámicamente desde la ubicación actual del archivo
# Utiliza os.path.dirname dos veces para subir dos niveles desde utils/config.py hasta la raíz
BASE_PATH = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# Ruta del directorio de datos GPS: Contiene archivos parquet con datos de entrenamiento y partidos
DATA_GPS_PATH = os.path.join(BASE_PATH, 'data', 'gps')

# Ruta del directorio de datos sin procesar: Para archivos CSV originales importados
DATA_RAW_PATH = os.path.join(BASE_PATH, 'data', 'raw')

# Ruta del directorio de datos procesados: Para archivos parquet limpiados y transformados
DATA_PROCESSED_PATH = os.path.join(BASE_PATH, 'data', 'processed')

# Ruta del directorio de referencias: Para estadísticas históricas y datos de comparación
REFERENCES_PATH = os.path.join(BASE_PATH, 'data', 'processed', 'references')

# Ruta del directorio temporal: Para archivos temporales durante el procesamiento
TEMP_PATH = os.path.join(BASE_PATH, 'temp')

# Ruta del archivo de configuración de métricas: Define el mapeo de columnas y cálculos
METRICS_MAPPING_PATH = os.path.join(BASE_PATH, 'config', 'metrics_mapping.json')


# ============================================================================
# FUNCIONES DE GESTIÓN DE DIRECTORIOS
# ============================================================================

def ensure_dir(directory):
    """
    Crea un directorio de forma segura si no existe en el sistema de archivos.
    
    PROPÓSITO:
    Esta función garantiza que un directorio específico exista antes de intentar
    realizar operaciones de escritura de archivos. Es una función de utilidad
    fundamental para evitar errores de "directorio no encontrado".
    
    QUÉ HACE EXACTAMENTE:
    1. Verifica si el directorio especificado ya existe en el sistema
    2. Si no existe, crea el directorio y todos los directorios padre necesarios
    3. Si ya existe, no realiza ninguna acción (operación idempotente)
    
    CÓMO LO HACE:
    - Utiliza os.path.exists() para verificar la existencia del directorio
    - Emplea os.makedirs() para crear el directorio con todos sus padres
    - La función os.makedirs() crea automáticamente directorios intermedios si es necesario
    
    
    Args:
        directory (str): Ruta absoluta o relativa del directorio a crear
                        Ejemplo: '/path/to/directory' o 'data/processed'
    
    Returns:
        None: Esta función no retorna ningún valor, es una operación de efecto lateral
    
    Ejemplo de uso:
        ensure_dir('/path/to/new/directory')
        ensure_dir(DATA_GPS_PATH)
    """
    # Verificar si el directorio ya existe en el sistema de archivos
    if not os.path.exists(directory):
        # Crear el directorio y todos los directorios padre necesarios
        # makedirs() es equivalente a 'mkdir -p' en sistemas Unix
        os.makedirs(directory)


def get_base_path():
    """
    Obtiene la ruta base del proyecto de forma consistente y centralizada.
    
    PROPÓSITO:
    Proporcionar un punto de acceso único y consistente a la ruta raíz del proyecto,
    permitiendo que otros módulos obtengan la ruta base sin necesidad de calcularla
    independientemente.
    
    QUÉ HACE EXACTAMENTE:
    1. Retorna el valor de la constante BASE_PATH que fue calculada al importar el módulo
    2. Proporciona una interfaz de función para acceder a la ruta base
    3. Garantiza consistencia en el acceso a la ruta raíz del proyecto
    
    CÓMO LO HACE:
    - Simplemente retorna la variable global BASE_PATH
    - La ruta fue calculada dinámicamente al cargar el módulo usando os.path.dirname()
    - No realiza cálculos adicionales, solo proporciona acceso a la constante
    
    
    Returns:
        str: Ruta absoluta del directorio raíz del proyecto
             Ejemplo: '/home/user/performanceApp' o 'C:\\Users\\user\\performanceApp'
    
    Ejemplo de uso:
        base = get_base_path()
        config_path = os.path.join(base, 'config', 'settings.json')
    """
    # Retornar la ruta base calculada al importar el módulo
    return BASE_PATH


def setup_project_paths():
    """
    Configura y crea toda la estructura de directorios principales del proyecto.
    
    PROPÓSITO:
    Inicializar o verificar la integridad de toda la estructura de directorios
    necesaria para el funcionamiento del sistema de análisis de rendimiento.
    Es especialmente útil durante la configuración inicial o para garantizar
    que todos los directorios necesarios estén disponibles.
    
    QUÉ HACE EXACTAMENTE:
    1. Define un diccionario con todas las rutas principales del proyecto
    2. Itera sobre cada ruta y garantiza que el directorio correspondiente exista
    3. Utiliza la función ensure_dir() para crear directorios faltantes
    4. Retorna un diccionario completo con todas las rutas para uso posterior
    
    CÓMO LO HACE:
    - Construye un diccionario 'paths' con claves descriptivas y rutas completas
    - Utiliza las constantes globales definidas en este módulo (DATA_GPS_PATH, etc.)
    - Itera sobre paths.values() para aplicar ensure_dir() a cada directorio
    - La función ensure_dir() (definida en este archivo) crea directorios faltantes

    
    ESTRUCTURA DE DIRECTORIOS CREADA:
    - base/: Directorio raíz del proyecto
    - data/: Directorio principal de datos
    - data/gps/: Archivos parquet con datos GPS de entrenamientos y partidos
    - data/raw/: Archivos CSV originales sin procesar
    - data/processed/: Archivos parquet procesados y limpiados
    - data/processed/references/: Estadísticas históricas y datos de referencia
    - temp/: Archivos temporales durante el procesamiento
    
    Returns:
        dict: Diccionario con claves descriptivas y rutas absolutas
              Claves disponibles: 'base', 'data', 'gps', 'raw', 'processed', 'references', 'temp'
              Ejemplo: {'base': '/path/to/project', 'gps': '/path/to/project/data/gps', ...}
    
    Ejemplo de uso:
        paths = setup_project_paths()
        gps_dir = paths['gps']
        processed_dir = paths['processed']
    """
    # Definir diccionario con todas las rutas principales del proyecto
    # Cada clave proporciona un identificador descriptivo para la ruta correspondiente
    paths = {
        'base': BASE_PATH,                    # Directorio raíz del proyecto
        'data': os.path.join(BASE_PATH, 'data'),  # Directorio principal de datos
        'gps': DATA_GPS_PATH,                 # Datos GPS (entrenamientos y partidos)
        'raw': DATA_RAW_PATH,                 # Datos sin procesar (CSV originales)
        'processed': DATA_PROCESSED_PATH,     # Datos procesados (parquet limpiados)
        'references': REFERENCES_PATH,        # Datos de referencia y estadísticas históricas
        'temp': TEMP_PATH                     # Archivos temporales
    }
    
    # Crear todos los directorios si no existen
    # Itera sobre cada ruta en el diccionario y garantiza su existencia
    for path in paths.values():
        # Utilizar la función ensure_dir() definida en este mismo archivo
        # para crear el directorio de forma segura
        ensure_dir(path)
    
    # Retornar el diccionario completo para uso por otros módulos
    return paths
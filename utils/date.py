"""
=============================================================================
MÓDULO DE MANEJO DE FECHAS (date.py)
=============================================================================

Este módulo contiene funciones especializadas para el manejo, procesamiento y 
conversión de fechas.
Proporciona utilidades para trabajar con diferentes formatos de fecha y 
compatibilidad con componentes de interfaz de usuario de Dash.

FUNCIONES DISPONIBLES:
=============================================================================

OBTENCIÓN Y ORDENAMIENTO DE FECHAS:
- get_sorted_dates(): Obtiene fechas únicas ordenadas cronológicamente del dataset
- get_latest_date_for_picker(): Obtiene la fecha más reciente en formato YYYY-MM-DD
- get_first_date_for_picker(): Obtiene la fecha más antigua en formato YYYY-MM-DD
- get_date_range(): Obtiene el rango completo de fechas (inicio y fin)

CONVERSIÓN Y FORMATEO:
- format_date(): Convierte fechas entre diferentes formatos (YYYY-MM-DD ↔ dd/mm/aaaa)

"""

import polars as pl
from datetime import datetime, timedelta
from typing import List, Optional, Union


def get_sorted_dates(df: pl.DataFrame = None, date_column: str = 'Date', ascending: bool = True) -> List[str]:
    """
    PROPÓSITO:
    Obtiene fechas únicas ordenadas cronológicamente del dataset de entrenamiento.
    Esta función es fundamental para proporcionar listas de fechas disponibles
    para selección en la interfaz de usuario.
    
    QUÉ HACE EXACTAMENTE:
    1. Carga el dataset de entrenamiento desde el archivo parquet
    2. Extrae fechas únicas de la columna 'Date'
    3. Convierte diferentes formatos de fecha a un formato estándar (dd/mm/aaaa)
    4. Ordena las fechas cronológicamente
    5. Retorna una lista de fechas como strings en formato dd/mm/aaaa
    
    CÓMO LO HACE:
    - Utiliza load_dataset_entrenamiento() para cargar los datos
    - Aplica .select('Date').unique() para obtener fechas únicas
    - Itera sobre cada fecha para normalizar el formato
    - Maneja múltiples formatos de entrada (dd/mm/aaaa, aaaa-mm-dd, datetime objects)
    - Utiliza datetime.strptime() para parsing y strftime() para formateo
    - Ordena usando una tupla (datetime_object, string_formatted) como clave
    
    ARCHIVOS UTILIZADOS:
    - utils.data_access: load_dataset_entrenamiento()
    
    Args:
        df (pl.DataFrame): DataFrame con columna de fechas (no utilizado, se carga internamente)
        date_column (str): Nombre de la columna de fechas (por defecto 'Date')
        ascending (bool): Si True, ordena ascendente; si False, descendente (no implementado)
    
    Returns:
        List[str]: Lista de fechas ordenadas cronológicamente en formato dd/mm/aaaa
    """
    
    try:
        # Importación específica del módulo utils para cargar datos
        from utils.data_access import load_dataset_entrenamiento
        
        # Cargar el dataset completo de entrenamiento desde el archivo parquet
        df = load_dataset_entrenamiento()
        
        # Verificar que el DataFrame no esté vacío y contenga la columna de fechas
        if df.is_empty() or date_column not in df.columns:
            return []
    
        # Obtener fechas únicas del DataFrame usando Polars
        # .select('Date') selecciona solo la columna de fechas
        # .unique() elimina duplicados
        # .to_list() convierte a lista de Python
        fechas_raw = df.select('Date').unique()['Date'].to_list()
        
        # Lista para almacenar tuplas (datetime_object, string_formatted)
        # Esto permite ordenar cronológicamente mientras mantenemos el formato deseado
        fechas_datetime = []
        
        # Procesar cada fecha para normalizar el formato
        for fecha in fechas_raw:
            try:
                # Caso 1: La fecha es un string
                if isinstance(fecha, str):
                    # Subcase 1a: Formato dd/mm/aaaa (ya está en formato deseado)
                    if '/' in fecha:
                        fecha_dt = datetime.strptime(fecha, '%d/%m/%Y')
                        fecha_formatted = fecha  # Mantener formato original
                    # Subcase 1b: Formato aaaa-mm-dd (convertir a dd/mm/aaaa)
                    elif '-' in fecha:
                        fecha_dt = datetime.strptime(fecha, '%Y-%m-%d')
                        fecha_formatted = fecha_dt.strftime('%d/%m/%Y')
                    else:
                        # Formato no reconocido, saltar esta fecha
                        continue
                    
                    # Agregar tupla (datetime_object, string_formatted) para ordenamiento
                    fechas_datetime.append((fecha_dt, fecha_formatted))
                    
                # Caso 2: La fecha es un objeto datetime
                else:
                    # Convertir datetime object a string en formato dd/mm/aaaa
                    fecha_formatted = fecha.strftime('%d/%m/%Y')
                    fechas_datetime.append((fecha, fecha_formatted))
                    
            except Exception as e:
                # Manejo de errores para fechas individuales malformadas
                print(f"Error procesando fecha {fecha}: {e}")
                continue
        
        # Ordenar cronológicamente usando el datetime object como clave
        # La función lambda extrae el primer elemento de cada tupla (datetime_object)
        fechas_datetime.sort(key=lambda x: x[0])
        
        # Extraer solo las fechas formateadas (segundo elemento de cada tupla)
        # Esto produce la lista final de fechas en formato dd/mm/aaaa ordenadas cronológicamente
        fechas_ordenadas = [fecha_str for fecha_dt, fecha_str in fechas_datetime]
        
        return fechas_ordenadas
        
    except Exception as e:
        # Manejo de errores generales (problemas de carga de datos, etc.)
        print(f"Error obteniendo fechas del parquet: {e}")
        return []
   
   
def get_latest_date_for_picker():
    """
    PROPÓSITO:
    Obtiene la fecha más reciente disponible en el dataset, convertida al formato
    compatible con el componente DatePickerSingle de Dash. Esta función es esencial
    para establecer valores por defecto en la interfaz de usuario.
    
    QUÉ HACE EXACTAMENTE:
    1. Obtiene todas las fechas ordenadas del dataset usando get_sorted_dates()
    2. Selecciona la última fecha de la lista (la más reciente cronológicamente)
    3. Convierte la fecha del formato interno dd/mm/aaaa al formato YYYY-MM-DD
    4. Retorna la fecha en formato compatible con componentes Dash
    
    CÓMO LO HACE:
    - Llama a get_sorted_dates() para obtener fechas ordenadas cronológicamente
    - Utiliza indexación [-1] para obtener el último elemento (fecha más reciente)
    - Aplica datetime.strptime() para convertir string a objeto datetime
    - Utiliza strftime('%Y-%m-%d') para formatear al estándar ISO
    
    ARCHIVOS UTILIZADOS:
    - Función get_sorted_dates() del mismo módulo 
    
    Returns:
        str: Fecha más reciente en formato YYYY-MM-DD para DatePickerSingle, 
             o None si no hay fechas disponibles
    """
    try:
        # Obtener todas las fechas ordenadas cronológicamente
        fechas_ordenadas = get_sorted_dates()
        
        # Verificar que existan fechas en el dataset
        if not fechas_ordenadas:
            return None
        
        # Obtener la última fecha (más reciente) de la lista ordenada
        # fechas_ordenadas[-1] accede al último elemento de la lista
        latest_date_str = fechas_ordenadas[-1]  # Formato dd/mm/aaaa
        
        # Convertir de formato interno (dd/mm/aaaa) a objeto datetime
        latest_date_dt = datetime.strptime(latest_date_str, '%d/%m/%Y')
        
        # Formatear a formato ISO (YYYY-MM-DD) requerido por DatePickerSingle
        return latest_date_dt.strftime('%Y-%m-%d')
        
    except Exception as e:
        # Manejo de errores en conversión de fechas o acceso a datos
        print(f"Error obteniendo fecha más reciente: {e}")
        return None


def get_first_date_for_picker():
    """
    PROPÓSITO:
    Obtiene la fecha más antigua disponible en el dataset, convertida al formato
    compatible con el componente DatePickerRange de Dash.
    
    QUÉ HACE EXACTAMENTE:
    1. Obtiene todas las fechas ordenadas del dataset usando get_sorted_dates()
    2. Selecciona la primera fecha de la lista (la más antigua cronológicamente)
    3. Convierte la fecha del formato interno dd/mm/aaaa al formato YYYY-MM-DD
    4. Retorna la fecha en formato compatible con componentes Dash
    
    CÓMO LO HACE:
    - Llama a get_sorted_dates() para obtener fechas ordenadas cronológicamente
    - Utiliza indexación [0] para obtener el primer elemento (fecha más antigua)
    - Aplica datetime.strptime() para convertir string a objeto datetime
    - Utiliza strftime('%Y-%m-%d') para formatear al estándar ISO
    
    ARCHIVOS UTILIZADOS:
    - Función get_sorted_dates() del mismo módulo 
    
    Returns:
        str: Fecha más antigua en formato YYYY-MM-DD para DatePickerRange,
             o None si no hay fechas disponibles
    """
    try:
        # Obtener todas las fechas ordenadas cronológicamente
        fechas_ordenadas = get_sorted_dates()
        
        # Verificar que existan fechas en el dataset
        if not fechas_ordenadas:
            return None
        
        # Obtener la primera fecha (más antigua) de la lista ordenada
        # fechas_ordenadas[0] accede al primer elemento de la lista
        first_date_str = fechas_ordenadas[0]  # Formato dd/mm/aaaa
        
        # Convertir de formato interno (dd/mm/aaaa) a objeto datetime
        first_date_dt = datetime.strptime(first_date_str, '%d/%m/%Y')
        
        # Formatear a formato ISO (YYYY-MM-DD) requerido por DatePickerRange
        return first_date_dt.strftime('%Y-%m-%d')
        
    except Exception as e:
        # Manejo de errores en conversión de fechas o acceso a datos
        print(f"Error obteniendo fecha más antigua: {e}")
        return None
    
def get_date_range():
    """
    PROPÓSITO:
    Obtiene el rango completo de fechas disponibles en el dataset (fecha más antigua
    y más reciente) en formato compatible con el componente DatePickerRange de Dash.
    Esta función es esencial para establecer los límites completos de selección de fechas.
    
    QUÉ HACE EXACTAMENTE:
    1. Obtiene todas las fechas ordenadas del dataset usando get_sorted_dates()
    2. Selecciona la primera fecha (más antigua) y la última fecha (más reciente)
    3. Convierte ambas fechas del formato interno dd/mm/aaaa al formato YYYY-MM-DD
    4. Retorna una tupla con el rango completo de fechas
    
    CÓMO LO HACE:
    - Llama a get_sorted_dates() para obtener fechas ordenadas cronológicamente
    - Utiliza indexación [0] y [-1] para obtener primera y última fecha respectivamente
    - Aplica datetime.strptime() para convertir strings a objetos datetime
    - Utiliza strftime('%Y-%m-%d') para formatear ambas fechas al estándar ISO
    - Retorna una tupla (fecha_inicio, fecha_fin) para uso directo en DatePickerRange
    
    ARCHIVOS UTILIZADOS:
    - Función get_sorted_dates() del mismo módulo 
    
    Returns:
        tuple: (fecha_inicio, fecha_fin) en formato YYYY-MM-DD para DatePickerRange,
               o (None, None) si no hay fechas disponibles
    """
    try:
        # Obtener todas las fechas ordenadas cronológicamente
        fechas_ordenadas = get_sorted_dates()
        
        # Verificar que existan fechas en el dataset
        if not fechas_ordenadas:
            return None, None
        
        # Obtener primera y última fecha de la lista ordenada
        # fechas_ordenadas[0] = fecha más antigua
        # fechas_ordenadas[-1] = fecha más reciente
        first_date_str = fechas_ordenadas[0]   # Formato dd/mm/aaaa
        last_date_str = fechas_ordenadas[-1]   # Formato dd/mm/aaaa
        
        # Convertir ambas fechas de formato interno (dd/mm/aaaa) a objetos datetime
        first_date_dt = datetime.strptime(first_date_str, '%d/%m/%Y')
        last_date_dt = datetime.strptime(last_date_str, '%d/%m/%Y')
        
        # Formatear ambas fechas a formato ISO (YYYY-MM-DD) y retornar como tupla
        # Esta tupla puede ser usada directamente en DatePickerRange como min_date_allowed y max_date_allowed
        return (first_date_dt.strftime('%Y-%m-%d'), 
                last_date_dt.strftime('%Y-%m-%d'))
        
    except Exception as e:
        # Manejo de errores en conversión de fechas o acceso a datos
        print(f"Error obteniendo rango de fechas: {e}")
        return None, None


def format_date(selected_date):
    """
    PROPÓSITO:
    Convierte una fecha seleccionada entre diferentes formatos estándar, específicamente
    del formato YYYY-MM-DD (usado por componentes Dash) al formato interno dd/mm/aaaa
    del sistema. Esta función es crucial para la interoperabilidad entre la interfaz
    de usuario y el procesamiento interno de datos.
    
    QUÉ HACE EXACTAMENTE:
    1. Recibe una fecha en cualquier formato de string
    2. Intenta convertir de formato YYYY-MM-DD a dd/mm/aaaa
    3. Si ya está en formato dd/mm/aaaa, la valida y la retorna sin cambios
    4. Si no puede convertir, retorna la fecha original como string
    
    CÓMO LO HACE:
    - Verifica primero que la entrada sea un string
    - Intenta parsing con datetime.strptime() usando formato '%Y-%m-%d'
    - Si tiene éxito, reformatea usando strftime('%d/%m/%Y')
    - Si falla, intenta validar formato '%d/%m/%Y' existente
    - Como último recurso, convierte la entrada a string y la retorna
    
    ARCHIVOS UTILIZADOS:
    - Ninguno (función de conversión pura)
    
    Args:
        selected_date (str): Fecha en formato YYYY-MM-DD, dd/mm/aaaa u otro formato
        
    Returns:
        str: Fecha formateada en dd/mm/aaaa o la fecha original como string
             si no se puede convertir
    """
    # Verificar que la entrada sea un string, si no, convertir a string
    if not isinstance(selected_date, str):
        return str(selected_date)
    
    try:
        # Caso 1: Intentar convertir de formato ISO (YYYY-MM-DD) a formato interno (dd/mm/aaaa)
        # Este es el caso más común cuando se reciben fechas de componentes Dash
        selected_dt = datetime.strptime(selected_date, '%Y-%m-%d')
        return selected_dt.strftime('%d/%m/%Y')
        
    except ValueError:
        try:
            # Caso 2: Verificar si ya está en formato interno (dd/mm/aaaa)
            # Si el parsing es exitoso, significa que el formato es válido
            datetime.strptime(selected_date, '%d/%m/%Y')
            return selected_date  # Retornar sin cambios
            
        except ValueError:
            # Caso 3: Formato no reconocido o fecha inválida
            # Retornar la fecha original como string para evitar errores
            return selected_date



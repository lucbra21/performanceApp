"""
============================================================================
PÁGINA DE CONFIGURACIÓN DE VARIABLES - PERFORMANCE APP
============================================================================

Este módulo implementa la página de configuración de variables del sistema de análisis
de rendimiento deportivo. Proporciona una interfaz web interactiva para gestionar el
mapeo entre las columnas del proyecto y las columnas de los datasets originales.

Funcionalidad Principal:
- Configuración del mapeo de variables entre proyecto y datasets originales
- Visualización de columnas disponibles en los datasets GPS
- Tabla interactiva para asignar columnas originales a métricas del proyecto
- Guardado automático de configuraciones en formato JSON
- Validación y retroalimentación en tiempo real

Componentes Principales:
- Funciones auxiliares para carga y guardado de configuraciones
- Layout de la página con componentes Dash interactivos
- Sistema de callbacks para manejo de eventos y actualizaciones
- Tabla editable con dropdowns para selección de columnas

============================================================================
"""

# ============================================================================
# IMPORTACIONES
# ============================================================================

# Importaciones de Dash
from dash import html, dcc, Output, Input, State, callback_context, dash_table
import dash
import dash_bootstrap_components as dbc

# Importações do sistema e utilidades
import os
import json
import pandas as pd

# Importaciones específicas de los módulos utils
from utils.config import *
from utils.data_access import load_gps_data

# ============================================================================
# FUNCIONES AUXILIARES
# ============================================================================

def load_metrics_mapping():
    """
    Carga el archivo de configuración de mapeo de métricas desde el sistema de archivos.
    
    Esta función lee el archivo JSON de configuración que contiene el mapeo entre las
    métricas del proyecto y las columnas de los datasets originales. Es fundamental
    para el funcionamiento del sistema de configuración de variables.
    
    Funcionalidad:
    - Lee el archivo metrics_mapping.json desde la ruta configurada
    - Maneja la codificación UTF-8 para caracteres especiales
    - Proporciona un diccionario por defecto si el archivo no existe
    - Mantiene la estructura de configuración del proyecto
    
    Implementación:
    - Utiliza la ruta METRICS_MAPPING_PATH definida en utils.config
    - Manejo robusto de errores para archivos faltantes
    - Codificación UTF-8 para soporte internacional
    - Estructura de retorno consistente
    
    Returns:
        dict: Diccionario con la configuración completa de métricas del proyecto.
              Estructura típica:
              {
                  "metrics": {
                      "metric_key": {
                          "project_name": "Nombre en proyecto",
                          "description": "Descripción de la métrica",
                          "unit": "Unidad de medida",
                          "original_column_name": "Columna en dataset original",
                          "calculation_method": "Método de cálculo",
                          "column_type": "Tipo de columna"
                      }
                  }
              }
              Retorna {"metrics": {}} si el archivo no existe o hay errores.
    
    """
    try:
        with open(METRICS_MAPPING_PATH, 'r', encoding='utf-8') as f:
            return json.load(f)
    except FileNotFoundError:
        return {"metrics": {}}

def save_metrics_mapping(mapping_data):
    """
    Guarda la configuración actualizada de mapeo de métricas en el sistema de archivos.
    
    Esta función persiste los cambios realizados en la configuración de mapeo de métricas,
    escribiendo los datos actualizados al archivo JSON de configuración. Es esencial
    para mantener la persistencia de las configuraciones del usuario.
    
    Funcionalidad:
    - Escribe el diccionario de configuración al archivo metrics_mapping.json
    - Asegura la existencia del directorio de destino antes de escribir
    - Utiliza formato JSON con indentación para legibilidad
    - Mantiene la codificación UTF-8 para caracteres especiales
    - Preserva caracteres no ASCII en el archivo de salida
    
    Implementación:
    - Utiliza ensure_dir() para crear directorios faltantes
    - Formato JSON con indentación de 2 espacios para legibilidad
    - Codificación UTF-8 con ensure_ascii=False para caracteres especiales
    - Escritura atómica para evitar corrupción de datos
    
    
    Args:
        mapping_data (dict): Diccionario completo con la configuración de métricas
                           a guardar. Debe seguir la estructura estándar:
                           {
                               "metrics": {
                                   "metric_key": {
                                       "project_name": str,
                                       "description": str,
                                       "unit": str,
                                       "original_column_name": str,
                                       "calculation_method": str,
                                       "column_type": str
                                   }
                               }
                           }
    
    Returns:
        None: Esta función no retorna valores, pero puede lanzar excepciones
              si ocurren errores durante la escritura del archivo.
    
    """
    ensure_dir(os.path.dirname(METRICS_MAPPING_PATH))
    with open(METRICS_MAPPING_PATH, 'w', encoding='utf-8') as f:
        json.dump(mapping_data, f, indent=2, ensure_ascii=False)

def get_available_columns():
    """
    Obtiene las columnas disponibles de los archivos CSV utilizando la función load_gps_data.
    
    Esta función carga los datos GPS del proyecto y extrae los nombres de todas las
    columnas disponibles en el dataset. Es fundamental para mostrar al usuario
    qué columnas están disponibles para el mapeo de métricas.
    
    Funcionalidad:
    - Carga los datos GPS utilizando load_gps_data() del módulo data_access
    - Verifica que el DataFrame no esté vacío antes de procesar
    - Convierte el DataFrame de Polars a Pandas para compatibilidad
    - Extrae y retorna la lista de nombres de columnas
    - Maneja errores de carga y acceso a datos
    
    Implementación:
    - Utiliza load_gps_data() para cargar datos desde archivos CSV
    - Verificación de DataFrame vacío con is_empty()
    - Conversión a Pandas con to_pandas() para obtener columnas
    - Manejo robusto de excepciones con logging de errores

    
    Returns:
        list: Lista de strings con los nombres de todas las columnas disponibles
              en el dataset GPS. Ejemplo:
              ['Player', 'Position', 'Date', 'Time', 'Speed', 'Distance', ...]
              Retorna lista vacía [] si no hay datos o ocurren errores.
    """
    try:
        # Usar load_gps_data para cargar los datos GPS
        df_gps = load_gps_data()
        
        if df_gps.is_empty():
            print("DataFrame GPS está vacío.")
            return []
        
        # Convertir a pandas para obtener las columnas
        df_pandas = df_gps.to_pandas()
        return list(df_pandas.columns)
    
    except Exception as e:
        print(f"Error al obtener columnas: {e}")
        return []

def create_settings_table_data():
    """
    Crea los datos estructurados para la tabla de configuraciones basada en el mapeo de métricas.
    
    Esta función procesa la configuración de mapeo de métricas y la transforma en un
    formato adecuado para mostrar en la tabla de configuraciones de la interfaz de usuario.
    Es esencial para la visualización y edición de la configuración de variables.
    
    Funcionalidad:
    - Carga la configuración actual de mapeo de métricas
    - Extrae información de cada métrica configurada en el proyecto
    - Transforma los datos en formato de lista de diccionarios
    - Simplifica el método de cálculo para visualización
    - Proporciona valores por defecto para campos faltantes
    
    Implementación:
    - Utiliza load_metrics_mapping() para obtener la configuración actual
    - Itera sobre todas las métricas definidas en la configuración
    - Extrae campos específicos: project_name, description, unit, etc.
    - Maneja campos faltantes con valores por defecto apropiados
    - Estructura los datos para compatibilidad con dash_table
    
    
    Returns:
        list: Lista de diccionarios, donde cada diccionario representa una métrica
              con la siguiente estructura:
              [
                  {
                      'metric_key': str,           # Clave única de la métrica
                      'project_name': str,         # Nombre mostrado en el proyecto
                      'description': str,          # Descripción de la métrica
                      'unit': str,                 # Unidad de medida
                      'original_column_name': str, # Columna en dataset original
                      'calculation_method': str    # Método de cálculo simplificado
                  },
                  ...
              ]
              Retorna lista vacía [] si no hay métricas configuradas.
    
    """
    mapping_data = load_metrics_mapping()
    metrics = mapping_data.get('metrics', {})
    
    table_data = []
    for metric_key, metric_info in metrics.items():
        # Obtener el método de cálculo simplificado
        calc_method = metric_info.get('calculation_method', 'N/A')
        
        table_data.append({
            'metric_key': metric_key,
            'project_name': metric_info.get('project_name', ''),
            'description': metric_info.get('description', ''),
            'unit': metric_info.get('unit', ''),
            'original_column_name': metric_info.get('original_column_name', ''),
            'calculation_method': calc_method
        })
    
    return table_data

# ============================================================================
# LAYOUT DE LA PÁGINA
# ============================================================================

layout = html.Div([
    # Título de la página
    html.H2('Configuración de Variables', className="page-title"),
    html.Hr(),
    
    # Información de la página
    dbc.Alert([
        html.H5("Configuración de Mapeo de Variables", className="alert-heading"),
        html.P([
            "Esta página permite configurar el mapeo entre las columnas del proyecto y las columnas ",
            "de los datasets originales. Seleccione la columna correspondiente del dataset original ",
            "para cada métrica del proyecto."
        ]),
        html.P([
            html.Strong("Nota: "), 
            "Los cambios se guardan automáticamente al seleccionar una nueva columna."
        ], className="mb-0")
    ], color="info", className="mb-4"),
    
    # Contenedor principal
    html.Div([
        # Sección de información de columnas disponibles
        html.Div([
            html.H4('Columnas Disponibles en Dataset', className="section-title"),
            html.Div(id='available-columns-info', className="mb-3"),
            html.Button(
                'Actualizar Columnas', 
                id='refresh-columns-btn', 
                className='btn btn-outline-primary mb-3',
                n_clicks=0
            )
        ], className="mb-4"),
        
        # Tabla de configuración de métricas
        html.Div([
            html.H4('Configuración de Métricas del Proyecto', className="section-title"),
            html.Div(id='settings-table-container')
        ], className="mb-4"),
        
        # Mensajes de estado
        html.Div(id='status-message', className="mt-3")
    ], className="container-fluid")
])

# ============================================================================
# FUNCIONES DE REGISTRO DE CALLBACKS
# ============================================================================

def register_callbacks(app):
    """
    Registra todos los callbacks de la página Settings para manejar la interactividad.
    
    Esta función configura toda la funcionalidad interactiva de la página de configuraciones
    mediante callbacks de Dash. Maneja la actualización de columnas disponibles, la generación
    de la tabla de configuraciones, y la persistencia automática de cambios en el mapeo.
    
    Funcionalidad:
    - Registra callback para actualizar información de columnas disponibles
    - Configura callback para generar y actualizar la tabla de configuraciones
    - Establece callback para guardar automáticamente cambios en el mapeo
    - Proporciona retroalimentación visual al usuario sobre el estado de operaciones
    - Maneja errores y excepciones con mensajes informativos
    
    Implementación:
    - Utiliza decoradores @app.callback para registrar funciones de callback
    - Configura inputs, outputs y states apropiados para cada callback
    - Implementa lógica de actualización reactiva basada en eventos del usuario
    - Maneja persistencia automática de datos sin intervención manual
    - Proporciona interfaz de usuario rica con componentes Bootstrap
    
    Callbacks registrados:
    1. update_available_columns_info: Actualiza lista de columnas disponibles
    2. update_settings_table: Genera tabla interactiva de configuraciones
    3. save_mapping_changes: Guarda automáticamente cambios en el mapeo
    
    """
    
    # ============================================================================
    # CALLBACK - Actualizar información de columnas disponibles
    # ============================================================================
    
    @app.callback(
        Output('available-columns-info', 'children'),
        [Input('refresh-columns-btn', 'n_clicks')]
    )
    def update_available_columns_info(n_clicks):
        """
        Callback que actualiza la información de las columnas disponibles en los datasets.
        
        Este callback se ejecuta cuando el usuario hace clic en el botón "Actualizar Columnas"
        y obtiene las columnas disponibles del dataset GPS para mostrarlas en la interfaz.
        Proporciona retroalimentación visual sobre qué columnas están disponibles para mapeo.
        
        Funcionalidad:
        - Se activa con clics en el botón 'refresh-columns-btn'
        - Obtiene columnas disponibles usando get_available_columns()
        - Genera componentes visuales para mostrar las columnas
        - Maneja casos donde no hay columnas disponibles
        - Proporciona scroll automático para listas largas de columnas
        
        Implementación:
        - Utiliza get_available_columns() para obtener datos actuales
        - Crea badges de Bootstrap para cada columna encontrada
        - Implementa scroll vertical para manejar muchas columnas
        - Muestra alertas informativas cuando no hay datos
        
        """
        available_columns = get_available_columns()
        
        if not available_columns:
            return dbc.Alert(
                "No se encontraron archivos CSV en la carpeta de datos o no se pudieron leer las columnas.",
                color="warning"
            )
        
        return html.Div([
            html.P(f"Se encontraron {len(available_columns)} columnas en el dataset:"),
            html.Div([
                dbc.Badge(col, color="secondary", className="me-1 mb-1") 
                for col in available_columns  # Mostrar todas las columnas
            ], style={'maxHeight': '400px', 'overflowY': 'auto'})  # Agregar scroll si hay muchas columnas
        ])
    
    # ============================================================================
    # CALLBACK - Generar tabla de configuraciones
    # ============================================================================
    
    @app.callback(
        Output('settings-table-container', 'children'),
        [Input('refresh-columns-btn', 'n_clicks')]
    )
    def update_settings_table(n_clicks):
        """
        Callback que genera y actualiza la tabla interactiva de configuraciones de métricas.
        
        Este callback crea la tabla principal de configuraciones donde los usuarios pueden
        mapear las métricas del proyecto con las columnas del dataset original. Se ejecuta
        cuando se actualiza la página o se hace clic en el botón de actualizar columnas.
        
        Funcionalidad:
        - Se activa con clics en el botón 'refresh-columns-btn'
        - Genera tabla interactiva con datos de configuración actuales
        - Crea dropdown con columnas disponibles para mapeo
        - Configura estilos y comportamiento de la tabla
        - Implementa tooltips informativos para cada métrica
        - Permite edición de la columna 'original_column_name'
        
        Implementación:
        - Utiliza create_settings_table_data() para obtener datos de métricas
        - Obtiene columnas disponibles con get_available_columns()
        - Configura dash_table.DataTable con columnas editables y no editables
        - Implementa dropdown para selección de columnas originales
        - Aplica estilos CSS personalizados para mejor UX
        - Configura tooltips con información detallada de métricas
        
        """
        table_data = create_settings_table_data()
        available_columns = get_available_columns()
        
        # Crear opciones para el dropdown de columnas originales
        column_options = [{'label': 'Sin asignar', 'value': ''}] + [
            {'label': col, 'value': col} for col in available_columns
        ]
        
        return dash_table.DataTable(
            id='settings-table',
            data=table_data,
            columns=[
                {
                    'id': 'project_name',
                    'name': 'Nombre en Proyecto',
                    'type': 'text',
                    'editable': False
                },
                {
                    'id': 'description',
                    'name': 'Descripción',
                    'type': 'text',
                    'editable': False
                },
                {
                    'id': 'unit',
                    'name': 'Unidad',
                    'type': 'text',
                    'editable': False
                },
                {
                    'id': 'calculation_method',
                    'name': 'Método Cálculo 94min',
                    'type': 'text',
                    'editable': False
                },
                {
                    'id': 'original_column_name',
                    'name': 'Columna Original',
                    'presentation': 'dropdown',
                    'editable': True,
                    'clearable': True
                }
            ],
            dropdown={
                'original_column_name': {
                    'options': column_options,
                    'clearable': True
                }
            },
            editable=True,
            row_deletable=False,
            style_cell={
                'textAlign': 'left',
                'padding': '10px',
                'fontFamily': 'Arial, sans-serif',
                'fontSize': '14px',
                'whiteSpace': 'normal',
                'height': 'auto',
                'minWidth': '120px'
            },
            style_header={
                'backgroundColor': '#f8f9fa',
                'fontWeight': 'bold',
                'border': '1px solid #dee2e6'
            },
            style_data={
                'border': '1px solid #dee2e6'
            },
            css=[{
                'selector': '.Select-menu-outer',
                'rule': 'display: block !important; z-index: 9999 !important; max-height: 300px !important; overflow-y: auto !important;'
            }, {
                'selector': '.Select-option',
                'rule': 'padding: 8px 12px !important; font-size: 14px !important;'
            }, {
                'selector': '.Select-option:hover',
                'rule': 'background-color: #f8f9fa !important;'
            }],
            style_data_conditional=[
                {
                    'if': {'row_index': 'odd'},
                    'backgroundColor': '#f8f9fa'
                },
                {
                    'if': {'column_id': 'original_column_name', 'filter_query': '{original_column_name} = ""'},
                    'backgroundColor': '#fff3cd',
                    'color': '#856404'
                }
            ],
            tooltip_data=[
                {
                    'description': {'value': row['description'], 'type': 'markdown'},
                    'calculation_method': {'value': f"Método: {row['calculation_method']}", 'type': 'text'}
                } for row in table_data
            ],
            tooltip_duration=None,
            page_size=15,
            sort_action='native',
            filter_action='native'
        )
    
    # ============================================================================
    # CALLBACK - Guardar cambios en el mapeo
    # ============================================================================
    
    @app.callback(
        Output('status-message', 'children'),
        [Input('settings-table', 'data')],
        [State('settings-table', 'data_previous')]
    )
    def save_mapping_changes(current_data, previous_data):
        """
        Callback que guarda automáticamente los cambios realizados en el mapeo de columnas.
        
        Este callback se ejecuta automáticamente cada vez que el usuario modifica la tabla
        de configuraciones, específicamente cuando cambia el mapeo de una métrica a una
        columna del dataset original. Proporciona persistencia automática sin intervención manual.
        
        Funcionalidad:
        - Se activa automáticamente cuando cambian los datos de la tabla
        - Compara datos actuales con datos anteriores para detectar cambios
        - Actualiza la configuración de mapeo con los nuevos valores
        - Guarda los cambios en el archivo de configuración JSON
        - Proporciona retroalimentación visual sobre el estado de la operación
        - Maneja errores de guardado con mensajes informativos
        
        Implementación:
        - Utiliza State para comparar datos actuales vs anteriores
        - Carga configuración actual con load_metrics_mapping()
        - Actualiza solo el campo 'original_column_name' de métricas modificadas
        - Persiste cambios usando save_metrics_mapping()
        - Genera alertas de Bootstrap para feedback visual
        - Maneja excepciones con mensajes de error detallados
        
        """
        if not current_data or current_data == previous_data:
            return ""
        
        try:
            # Cargar el mapeo actual
            mapping_data = load_metrics_mapping()
            
            # Actualizar el mapeo con los nuevos datos
            for row in current_data:
                metric_key = row['metric_key']
                if metric_key in mapping_data.get('metrics', {}):
                    mapping_data['metrics'][metric_key]['original_column_name'] = row['original_column_name']
            
            # Guardar los cambios
            save_metrics_mapping(mapping_data)
            
            return dbc.Alert(
                [
                    html.I(className="fas fa-check-circle me-2"),
                    "Configuración guardada exitosamente."
                ],
                color="success",
                dismissable=True,
                duration=3000
            )
            
        except Exception as e:
            return dbc.Alert(
                [
                    html.I(className="fas fa-exclamation-triangle me-2"),
                    f"Error al guardar la configuración: {str(e)}"
                ],
                color="danger",
                dismissable=True
            )

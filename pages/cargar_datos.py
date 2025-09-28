"""
============================================================================
PÁGINA DE CARGA DE DATOS - cargar_datos.py
============================================================================

DESCRIPCIÓN GENERAL:
Esta página permite la gestión completa de archivos GPS en formato Excel (.xlsx) para el 
sistema de análisis de rendimiento deportivo. Proporciona funcionalidades para cargar, 
visualizar, editar y eliminar archivos de datos GPS, así como actualizar las referencias 
del sistema y mantener un historial de todas las operaciones realizadas.

FUNCIONES DISPONIBLES:

FUNCIONES DE UTILIDAD PARA EL HISTORIAL:
- load_file_history(): Carga el historial de archivos desde el archivo JSON
- add_history_entry(): Añade una nueva entrada al historial con timestamp
- clear_processed_files(): Elimina todos los archivos de la carpeta processed

FUNCIONES DE INTERFAZ:
- generate_history_component(): Genera el componente HTML del historial de archivos
- update_references(): Ejecuta el recálculo completo de referencias y datasets

CALLBACKS PRINCIPALES:
- save_file(): Procesa la subida de archivos y actualiza el Parquet consolidado
- toggle_edit_modal(): Controla la visibilidad del modal de edición de archivos
- confirm_edit(): Procesa la confirmación de eliminación de archivos seleccionados
- handle_update_references(): Maneja la actualización de referencias del sistema

FUNCIONALIDADES PRINCIPALES:
- Carga de archivos Excel GPS con validación de formato
- Consolidación automática en formato Parquet para mejor rendimiento
- Historial completo de operaciones (carga/eliminación) con timestamps
- Modal de edición para selección y eliminación de archivos
- Sistema de copias de seguridad automáticas
- Actualización completa de datasets y referencias del sistema
- Interfaz intuitiva con mensajes de estado en tiempo real

"""

# ============================================================================
# IMPORTACIONES
# ============================================================================

# Importaciones de Dash
from dash import html, dcc, Output, Input, State, callback_context
import dash

# Importaciones del sistema y utilidades
import os
import base64
import re
import polars as pl
import datetime
import json

# Importaciones específicas de los módulos utils
from utils.metrics import *
from utils.data_access import *


# ============================================================================
# FUNCIONES DE UTILIDAD PARA EL HISTORIAL DE ARCHIVOS
# ============================================================================

# Función para cargar el historial de archivos
def load_file_history():
    """
    PROPÓSITO:
    Carga el historial completo de operaciones de archivos desde el sistema de almacenamiento JSON.
    
    FUNCIONALIDAD:
    - Crea automáticamente la carpeta 'data' si no existe
    - Busca y lee el archivo 'file_history.json' que contiene el registro de operaciones
    - Maneja errores de lectura devolviendo una lista vacía como fallback
    - Proporciona un historial persistente de todas las cargas y eliminaciones de archivos
    
    IMPLEMENTACIÓN:
    - Utiliza os.path.join() para construir rutas de manera compatible con el sistema operativo
    - Emplea os.makedirs() con exist_ok=True para crear directorios de forma segura
    - Implementa manejo de excepciones robusto para archivos corruptos o inexistentes
    - Lee el archivo JSON completo en memoria y lo convierte a lista de Python
    
    """
    # Define la carpeta donde se guardará el historial
    data_folder = os.path.join(os.path.dirname(__file__), '..', 'data')
    os.makedirs(data_folder, exist_ok=True)
    history_path = os.path.join(data_folder, 'file_history.json')
    
    if os.path.exists(history_path):
        try:
            with open(history_path, 'r') as f:
                return json.load(f)
        except Exception:
            return []
    return []

# Función para añadir una entrada al historial
def add_history_entry(action, filename):
    """
    PROPÓSITO:
    Registra una nueva operación en el historial de archivos con timestamp automático.
    
    FUNCIONALIDAD:
    - Carga el historial existente utilizando load_file_history()
    - Crea una nueva entrada con la acción realizada, nombre del archivo y timestamp
    - Añade la nueva entrada al final del historial (orden cronológico)
    - Guarda el historial actualizado de vuelta al archivo JSON
    - Mantiene un registro completo y persistente de todas las operaciones
    
    IMPLEMENTACIÓN:
    - Utiliza datetime.datetime.now() para generar timestamps precisos
    - Formatea la fecha en formato "YYYY-MM-DD HH:MM:SS" para legibilidad
    - Construye un diccionario con la estructura estándar del historial
    - Emplea json.dump() para serializar y guardar los datos actualizados
    - Reutiliza la lógica de load_file_history() para mantener consistencia
    
    """
    history = load_file_history()
    entry = {
        'action': action,
        'filename': filename,
        'timestamp': datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    }
    history.append(entry)
    
    # Define la carpeta y guarda el historial actualizado
    data_folder = os.path.join(os.path.dirname(__file__), '..', 'data')
    history_path = os.path.join(data_folder, 'file_history.json')
    with open(history_path, 'w') as f:
        json.dump(history, f)

# Función para limpiar los archivos de la carpeta processed
def clear_processed_files():
    """
    PROPÓSITO:
    Elimina todos los archivos de la carpeta 'processed' para limpiar datos pre-calculados.
    
    FUNCIONALIDAD:
    - Localiza la carpeta 'data/processed' del proyecto
    - Lista todos los archivos contenidos en la carpeta
    - Elimina cada archivo individualmente con manejo de errores
    - Proporciona retroalimentación detallada sobre el proceso de eliminación
    - Prepara el sistema para recálculos completos de estadísticas y referencias
    
    IMPLEMENTACIÓN:
    - Utiliza os.path.exists() para verificar la existencia de la carpeta
    - Emplea os.listdir() para obtener la lista completa de archivos
    - Aplica os.path.isfile() para filtrar solo archivos (no directorios)
    - Usa os.remove() para eliminar cada archivo de forma segura
    - Implementa manejo de excepciones individual para cada archivo
    - Genera mensajes informativos en consola para seguimiento del proceso
    
    """
    processed_folder = os.path.join(os.path.dirname(__file__), '..', 'data', 'processed')
    
    if os.path.exists(processed_folder):
        try:
            # Lista todos los archivos en la carpeta processed
            files_to_remove = []
            for filename in os.listdir(processed_folder):
                file_path = os.path.join(processed_folder, filename)
                if os.path.isfile(file_path):
                    files_to_remove.append(file_path)
            
            # Elimina cada archivo
            for file_path in files_to_remove:
                try:
                    os.remove(file_path)
                    print(f"Archivo eliminado: {os.path.basename(file_path)}")
                except Exception as e:
                    print(f"Error al eliminar {os.path.basename(file_path)}: {str(e)}")
            
            if files_to_remove:
                print(f"Se eliminaron {len(files_to_remove)} archivos de la carpeta processed.")
            else:
                print("No había archivos para eliminar en la carpeta processed.")
                
        except Exception as e:
            print(f"Error al acceder a la carpeta processed: {str(e)}")
    else:
        print("La carpeta processed no existe.")
        
# ============================================================================
# FUNCIONES DE INTERFAZ
# ============================================================================

# Función para generar el componente de historial de archivos
def generate_history_component(history=None):
    """
    PROPÓSITO:
    Genera el componente HTML visual que muestra el historial de operaciones de archivos.
    
    FUNCIONALIDAD:
    - Carga automáticamente el historial si no se proporciona como parámetro
    - Crea elementos HTML estructurados para mostrar cada entrada del historial
    - Ordena las entradas cronológicamente (más recientes primero)
    - Añade iconos visuales para diferenciar tipos de operaciones (✅ para carga, ❌ para eliminación)
    - Formatea la información de manera legible con nombres de archivo y timestamps
    - Maneja el caso de historial vacío con mensaje informativo
    
    IMPLEMENTACIÓN:
    - Utiliza reversed() para mostrar las entradas más recientes al principio
    - Construye elementos html.Div() anidados para estructura jerárquica
    - Aplica clases CSS específicas para estilizado consistente
    - Emplea html.Span() para elementos de texto con estilos diferenciados
    - Crea una estructura de componente reutilizable y modular
    
    """
    if history is None:
        history = load_file_history()
    
    if not history:
        return html.Div("No hay historial de archivos disponible.", className="no-files-msg")
    
    history_items = []
    for entry in reversed(history):  # Mostrar los más recientes primero
        icon = "✅" if entry['action'] == "upload" else "❌"
        history_items.append(
            html.Div([
                html.Div([
                    html.Span(f"{icon} ", style={'marginRight': '5px'}),
                    html.Span(f"{entry['action'].capitalize()}: {entry['filename']}")
                ], className="file-name"),
                html.Div(entry['timestamp'], className="file-date")
            ], className="file-history-item")
        )
    
    return html.Div([
        html.Div("Histórico de Arquivos", className="historico-title"),
        html.Div(history_items, id="files-history")
    ], className="history-component")



def update_references():
    """
    PROPÓSITO:
    Ejecuta secuencialmente todas las funciones de cálculo de referencias para pre-calcular 
    y almacenar las tablas de referencia del sistema, además de crear el dataset principal del proyecto.
    
    FUNCIONALIDAD:
    - Crea los datasets principales de partidos y entrenamiento como base del sistema
    - Ejecuta en orden específico las funciones de cálculo de métricas y estadísticas
    - Calcula métricas para períodos de 94 minutos con diferentes configuraciones
    - Genera estadísticas de jugadores individuales para análisis comparativo
    - Calcula estadísticas agrupadas por jornada (matchday) para seguimiento temporal
    - Genera tablas comparativas consolidadas para la interfaz de usuario
    - Valida cada paso del proceso para asegurar integridad de datos
    - Proporciona retroalimentación detallada sobre el progreso y posibles errores
    
    IMPLEMENTACIÓN:
    - Utiliza bloques try-catch para manejo robusto de errores
    - Ejecuta funciones en secuencia específica respetando dependencias de datos
    - Valida que cada dataset generado no esté vacío antes de continuar
    - Emplea print() para logging detallado del progreso en consola
    - Retorna tupla con estado de éxito y mensaje descriptivo
    - Maneja excepciones con mensajes de error informativos

    """
    try:
        # 0. Crear el dataset principal del proyecto primero
        print("=== CREANDO DATASET DE PARTIDOS ===")
        dataset_partidos = create_dataset_partidos()
        if dataset_partidos is None or dataset_partidos.is_empty():
            return False, "Error al crear el dataset de los partidos"
        
        print("=== CREANDO DATASET DE ENTRENAMIENTO ===")
        dataset_entrenamiento = create_dataset_entrenamiento()
        if dataset_entrenamiento is None or dataset_entrenamiento.is_empty():
            return False, "Error al crear el dataset de entrenamiento"
        
        print("Dataset principal creado exitosamente. Procediendo con las referencias...")
        
          # 1. calculate_metrics_for_94min(4)
        result3 = calculate_metrics_for_94min(save=True)
        if result3 is None or result3.is_empty():
            return False, "Error en calculate_metrics_for_94min(4)"
        
        # 2. calculate_metrics_for_94min()
        result4 = calculate_metrics_for_94min(save=True)
        if result4 is None or result4.is_empty():
            return False, "Error en calculate_metrics_for_94min()"
       
       
        # 3. calculate_player_statistics_94min(4)
        result1 = calculate_player_statistics_94min(4)
        if result1 is None or result1.is_empty():
            return False, "Error en calculate_player_statistics_94min(4)"
        
        # 4. calculate_player_statistics_94min()
        result2 = calculate_player_statistics_94min()
        if result2 is None or result2.is_empty():
            return False, "Error en calculate_player_statistics_94min()"
        
     
        # 5. calcular_estadisticas_por_matchday()
        result5 = calcular_estadisticas_por_matchday(save=True)
        if result5 is None:
            return False, "Error en calcular_estadisticas_por_matchday()"
        
        
        print("=== ACTUALIZACIÓN DE REFERENCIAS COMPLETADA ===")
        return True, "Dataset principal creado y referencias actualizadas correctamente. Todas las tablas de referencia han sido pre-calculadas y almacenadas."
        
    except Exception as e:
        error_msg = f"Error durante la actualización de referencias: {str(e)}"
        print(error_msg)
        return False, error_msg

# ============================================================================
# LAYOUT DE LA PÁGINA
# ============================================================================
"""
ESTRUCTURA DEL LAYOUT DE LA PÁGINA CARGAR DATOS:

PROPÓSITO:
Define la estructura visual completa de la página de carga de datos, organizando todos los 
componentes de interfaz de usuario de manera jerárquica y funcional.

COMPONENTES PRINCIPALES:

1. ENCABEZADO DE PÁGINA:
   - Título principal "Cargar Datos" con estilo page-title
   - Línea divisoria horizontal para separación visual

2. CONTENEDOR PRINCIPAL (cargar-datos-container):
   - Cabecera de acciones (cargar-datos-header):
     * Etiqueta "GPS" identificativa del tipo de datos
     * Grupo de botones de acción alineados a la derecha:
       - Upload: Componente dcc.Upload para subir archivos XLSX
       - EDIT: Botón para abrir modal de edición/eliminación de archivos
       - DOWNLOAD: Botón para descargar datos procesados
       - ATUALIZAR REFERÊNCIAS: Botón para ejecutar cálculos de referencias

3. ÁREA DE MENSAJES DE ESTADO:
   - Contenedor dinámico para mostrar mensajes informativos, de éxito o error
   - Mensaje inicial por defecto sobre instrucciones de uso

4. MODAL DE EDICIÓN (edit-modal):
   - Overlay modal inicialmente oculto para gestión de archivos
   - Estructura completa con header, body y footer
   - Botones de confirmación y cancelación de acciones
   - Sistema de cierre con botón X y botones de acción

5. ÁREA DE CONTENIDO PRINCIPAL:
   - Línea divisoria para separación visual
   - Contenedor del historial de archivos que muestra dinámicamente
     las operaciones realizadas (carga/eliminación) con timestamps


"""

layout = html.Div([
    # Título de la página
    html.H2('Cargar Datos', className="page-title"),
    html.Hr(),
    
    # Contenedor principal
    html.Div([
        # Cabecera con botones
        html.Div([
            html.Div("GPS", className="gps-label"),
            html.Div([
                # Componente para subir archivos XLSX
                dcc.Upload(
                    id='upload-data',
                    children=html.Button('Upload', className='btn-upload', style={
                                'cursor': 'pointer',
                                'display': 'inline-block'
                    }),
                    multiple=False,
                    accept='.xlsx',
                    style={'display': 'inline-block'}
                ),
                # Botón para editar archivos
                html.Button(
                    'EDIT', 
                    id='edit-files-btn', 
                    n_clicks=0, 
                    className='btn-edit'
                ),
                # Botão para descargar archivos
                html.Button('DOWNLOAD', id='btn-download', className='btn-download'),
                # Botão para atualizar referências
                html.Button('ATUALIZAR REFERÊNCIAS', id='update-references-btn', className='btn-update-references')
            ],  className="buttons-right"),
        ], className="cargar-datos-header"),
        
        # Contenedor de mensajes de estado
        html.Div(id='status-messages', children=[html.Div("Sube o ajusta el número de ficheros almacenados.", className="info-msg")]),
        
        # Modal para edición de archivos (inicialmente oculto)
        html.Div([
            html.Div([
                html.Div([
                    html.H3("Seleccionar Archivos para Eliminar", className="modal-title"),
                    html.Button("×", id="close-modal-btn", className="modal-close-btn"),
                ], className="modal-header"),
                html.Div(id="modal-content", className="modal-body"),
                html.Div([
                    html.Button("Confirmar", id="confirm-edit-btn", className="btn-confirm"),
                    html.Button("Cancelar", id="cancel-edit-btn", className="btn-cancel")
                ], className="modal-footer")
            ], className="modal-content-wrapper")
        ], id="edit-modal", className="modal-overlay", style={"display": "none"}),
        
        # Línea divisoria
        html.Hr(className="divider-line"),
        
        # Área de contenido
        html.Div([
            # Contenedor para mostrar el historial de archivos
            html.Div(id='file-history', children=[
                # Inicialmente cargamos el historial
                generate_history_component()
            ]),    
        ], className="cargar-datos-content")
    ], className="cargar-datos-container"),
    
])

# ============================================================================
# CALLBACKS
# ============================================================================

def register_callbacks(app):
    """
    PROPÓSITO:
    Registra todos los callbacks de la página cargar_datos.py en la aplicación Dash,
    estableciendo la interactividad completa de la interfaz de usuario.
    
    FUNCIONALIDAD:
    - Define y registra múltiples callbacks para diferentes componentes de la página
    - Gestiona la subida y procesamiento de archivos XLSX
    - Controla la visibilidad y contenido del modal de edición
    - Maneja la eliminación de archivos del dataset consolidado
    - Gestiona la actualización de referencias y cálculos del sistema
    - Proporciona retroalimentación visual mediante mensajes de estado
    
    IMPLEMENTACIÓN:
    - Utiliza decoradores @app.callback para definir cada callback
    - Emplea Input, Output y State de Dash para manejar interacciones
    - Implementa manejo robusto de errores con try-catch
    - Utiliza callback_context para identificar qué elemento disparó el callback
    - Actualiza múltiples componentes de la interfaz simultáneamente
    
   
    CALLBACKS INCLUIDOS:
    1. save_file: Procesa subida de archivos y actualiza dataset consolidado
    2. toggle_edit_modal: Controla visibilidad del modal de edición
    3. confirm_edit: Procesa eliminación de archivos seleccionados
    4. handle_update_references: Ejecuta actualización de referencias del sistema
    """
    
    # ========================================================================
    # Callback para subir archivos y actualizar el Parquet consolidado
    # ========================================================================
    @app.callback(
        [Output('status-messages', 'children'),
         Output('file-history', 'children', allow_duplicate=True)],
        Input('upload-data', 'contents'),
        State('upload-data', 'filename'),
        prevent_initial_call=True
    )
    def save_file(contents, filename):
        """
        PROPÓSITO:
        Procesa la subida de archivos XLSX, los integra al dataset consolidado Parquet
        y actualiza el historial de operaciones sin guardar archivos Excel individuales.
        
        FUNCIONALIDAD:
        - Valida que el archivo subido sea de formato XLSX válido
        - Decodifica el contenido del archivo desde base64 y lo procesa en memoria
        - Normaliza el nombre del archivo según el patrón de fechas del sistema
        - Verifica que el archivo no exista previamente en el dataset consolidado
        - Crea copias de seguridad automáticas antes de modificar datos existentes
        - Concatena los nuevos datos con el dataset existente manejando tipos de datos
        - Registra la operación en el historial de archivos con timestamp
        - Proporciona retroalimentación visual sobre el éxito o fallo de la operación
        - Implementa recuperación automática desde copias de seguridad en caso de error
        
        IMPLEMENTACIÓN:
        - Utiliza regex para extraer y reformatear fechas del nombre del archivo
        - Emplea polars para lectura eficiente de Excel y manipulación de dataframes
        - Maneja la concatenación diagonal para datasets con esquemas ligeramente diferentes
        - Convierte tipos de datos numéricos a float64 para consistencia
        - Utiliza shutil para operaciones de copia de seguridad y restauración
        - Implementa validación de integridad de archivos Parquet existentes
        
        """
        
        # Si no hay archivo subido, no hacer nada
        if contents is None or filename is None:
            return [html.Div("No se ha seleccionado ningún archivo.", className="error-msg")], dash.no_update

        # Si el nombre del archivo no tiene la extensión .xlsx, añadirla
        if not filename.endswith('.xlsx'):
            return [html.Div("El archivo no tiene la extensión .xlsx.", className="error-msg")], dash.no_update

        # Cambiar el nombre del archivo ("dd_mm_aaaa_al_dd_mm_aaaa.xlsx")
        match = re.search(r'(\d{2})-(\d{2})-(\d{4}).*?(\d{2})-(\d{2})-(\d{4})', filename)
        filename_original = filename
        if match:
            day1, month1, year1, day2, month2, year2 = match.groups()
            filename = f"{day1}_{month1}_{year1}_al_{day2}_{month2}_{year2}.xlsx"
   
        # Define la carpeta donde se guardará el archivo Parquet consolidado
        gps_folder = os.path.join(os.path.dirname(__file__), '..', 'data', 'gps')
        os.makedirs(gps_folder, exist_ok=True)
        merge_path = os.path.join(gps_folder, 'df_gps.parquet')
        backup_path = os.path.join(gps_folder, 'df_gps_backup.parquet')
        
        try:
            # Decodifica el archivo subido y lo lee directamente en memoria sin guardarlo
            _, content_string = contents.split(',')
            decoded = base64.b64decode(content_string)
            
            # Lee el archivo XLSX desde memoria y añade la columna 'File Name'
            import io
            df_new = pl.read_excel(io.BytesIO(decoded))
            df_new = df_new.with_columns(pl.lit(filename).alias('File Name'))
            
            # Crear una copia de seguridad del Parquet actual si existe
            if os.path.exists(merge_path) and os.path.getsize(merge_path) > 0:
                try:
                    # Leer el archivo Parquet actual para verificar que sea válido
                    df_actual = pl.read_parquet(merge_path)
                    if df_actual.height > 0:
                        # Verificar si el archivo ya existe en el dataframe
                        if filename in df_actual['File Name'].unique():
                            return [html.Div(f"El archivo '{filename}' ya existe en el dataframe.", className="error-msg")], dash.no_update
                        
                        # Crear copia de seguridad
                        import shutil
                        shutil.copy2(merge_path, backup_path)
                        print(f"Se ha creado una copia de seguridad del archivo")
                        
                        # Concatenar el nuevo dataframe con el existente
                        try:                        
                            # Intentar convertir todas las columnas numéricas a float
                            numeric_cols = [col for col in df_actual.columns if df_actual[col].dtype in [pl.Int64, pl.Float64]]
                            
                            # Convertir columnas numéricas a float en ambos dataframes
                            for col in numeric_cols:
                                if col in df_actual.columns and col in df_new.columns:
                                    df_actual = df_actual.with_columns(pl.col(col).cast(pl.Float64))
                                    df_new = df_new.with_columns(pl.col(col).cast(pl.Float64))
                            
                            # Intentar concatenar nuevamente
                            df_merge = pl.concat([df_actual, df_new], how='diagonal')
                        except Exception as concat_error:
                            print(f"Error en la concatenación: {str(concat_error)}")
                            # En caso de error en la concatenación, usar solo el nuevo dataframe
                            df_merge = df_new
                        
                except Exception as e:
                    print(f"Error al leer el Parquet actual o crear copia de seguridad: {str(e)}")
                    return [html.Div(f"Error al procesar el archivo: {str(e)}", className="error-msg")], dash.no_update
            else:
                # Si no existe el Parquet, el nuevo dataframe será el consolidado
                df_merge = df_new
            
            # Guardar el dataframe consolidado en formato Parquet
            df_merge.write_parquet(merge_path)
            
            # Registra la acción en el historial
            add_history_entry("upload", filename)
            
            # Actualiza el componente de historial
            updated_history = generate_history_component()
            
        except Exception as e:
            # Restaurar desde la copia de seguridad si existe
            if os.path.exists(backup_path) and os.path.getsize(backup_path) > 0:
                try:
                    import shutil
                    shutil.copy2(backup_path, merge_path)
                    print(f"Se ha restaurado el archivo {merge_path} desde la copia de seguridad después de un error.")
                except Exception as backup_error:
                    print(f"Error al restaurar desde la copia de seguridad: {str(backup_error)}")
            return [html.Div(f"Error al procesar el archivo: {str(e)}", className="error-msg")], dash.no_update
        
        return [html.Div(f"Archivo '{filename}' procesado y datos añadidos al dataframe.", className="success-msg")], updated_history
    
    

    # ========================================================================
    # Callback para mostrar/ocultar el modal de edición
    # ========================================================================
    @app.callback(
        [Output('edit-modal', 'style'),
         Output('modal-content', 'children')],
        [Input('edit-files-btn', 'n_clicks'),
         Input('close-modal-btn', 'n_clicks'),
         Input('cancel-edit-btn', 'n_clicks')],
        prevent_initial_call=True
    )
    def toggle_edit_modal(edit_clicks, close_clicks, cancel_clicks):
        """
        PROPÓSITO:
        Controla la visibilidad del modal de edición de archivos y genera dinámicamente
        su contenido con la lista de archivos disponibles para eliminación.
        
        FUNCIONALIDAD:
        - Detecta qué botón fue presionado usando callback_context para determinar la acción
        - Muestra u oculta el modal según el botón activado (abrir, cerrar, cancelar)
        - Carga dinámicamente la lista de archivos desde el dataset consolidado Parquet
        - Genera una checklist interactiva con todos los archivos disponibles
        - Valida la existencia y integridad del dataset antes de mostrar opciones
        - Proporciona mensajes informativos cuando no hay datos disponibles
        - Maneja errores de lectura de datos con retroalimentación específica
        
        IMPLEMENTACIÓN:
        - Utiliza callback_context para identificar el elemento que disparó el callback
        - Emplea polars para lectura eficiente del dataset consolidado
        - Genera componentes Dash dinámicamente según el estado de los datos
        - Implementa validación de archivos y manejo de casos edge
        - Retorna tuplas con estilos CSS y contenido HTML para actualizar la interfaz
        - Usa try-catch para manejo robusto de errores de lectura de datos
        
        """
        ctx = callback_context
        if not ctx.triggered:
            return {"display": "none"}, []
        
        button_id = ctx.triggered[0]['prop_id'].split('.')[0]
        
        # Si se presionó cerrar o cancelar, ocultar el modal
        if button_id in ['close-modal-btn', 'cancel-edit-btn']:
            return {"display": "none"}, []
        
        # Si se presionó el botón de editar, mostrar el modal con contenido
        if button_id == 'edit-files-btn':
            # Obtiene la lista de archivos desde el dataframe consolidado
            gps_folder = os.path.join(os.path.dirname(__file__), '..', 'data', 'gps')
            merge_path = os.path.join(gps_folder, 'df_gps.parquet')
            
            # Verificar si el archivo parquet existe
            if not os.path.exists(merge_path):
                modal_content = html.Div([
                    html.P('No hay archivos cargados aún.', style={'margin-bottom': '10px'}),
                    html.P('Por favor, cargue primero algunos archivos usando el botón "Upload".')
                ], className="error-msg")
                return {"display": "flex"}, modal_content
            
            try:
                # Leer el dataframe consolidado
                df = pl.read_parquet(merge_path)
                if df.height == 0:
                    modal_content = html.Div('No hay datos para editar.', className="error-msg")
                    return {"display": "flex"}, modal_content
                
                # Obtener la lista única de nombres de archivos
                files = df['File Name'].unique().to_list()
                
                if not files:
                    modal_content = html.Div([
                        html.P('No hay archivos para editar.', style={'margin-bottom': '10px'}),
                        html.P('No se encontraron nombres de archivos en los datos.')
                    ], className="info-msg")
                    return {"display": "flex"}, modal_content
                
                # Crear el contenido del modal con la checklist
                modal_content = html.Div([
                    html.Label('Seleccione los archivos que desea eliminar:', className="modal-label"),
                    dcc.Checklist(
                        id='files-checklist',
                        options=[{'label': f, 'value': f} for f in files],
                        value=[],  # Lista vacía para que ningún archivo esté seleccionado por defecto
                        inputStyle={'marginRight': '8px'},
                        className="modal-checklist"
                    )
                ])
                
                return {"display": "flex"}, modal_content
                
            except Exception as e:
                print(f"Error al leer el dataframe: {str(e)}")
                modal_content = html.Div(f'Error al cargar los datos: {str(e)}', className="error-msg")
                return {"display": "flex"}, modal_content
        
        return {"display": "none"}, []

    # ========================================================================
    # Callback para confirmar cambios en la edición de archivos
    # ========================================================================
    @app.callback(
        [
            Output('status-messages', 'children', allow_duplicate=True),
            Output('file-history', 'children', allow_duplicate=True),
            Output('edit-modal', 'style', allow_duplicate=True)
        ],
        Input('confirm-edit-btn', 'n_clicks'),
        State('files-checklist', 'value'),
        prevent_initial_call=True
    )
    def confirm_edit(confirm_clicks, selected_files):
        """
        PROPÓSITO:
        Procesa la confirmación de eliminación de archivos seleccionados del dataset
        consolidado, actualizando el dataframe Parquet y el historial de archivos.
        
        FUNCIONALIDAD:
        - Valida que se haya hecho clic en el botón de confirmación
        - Crea una copia de seguridad automática del dataset antes de modificaciones
        - Filtra el dataframe consolidado para eliminar los archivos seleccionados
        - Actualiza el archivo Parquet con los datos filtrados
        - Registra cada eliminación en el historial de operaciones
        - Maneja el caso especial cuando se eliminan todos los archivos
        - Limpia archivos procesados cuando el dataset queda vacío
        - Proporciona retroalimentación detallada sobre las operaciones realizadas
        - Implementa recuperación automática desde backup en caso de errores
        
        IMPLEMENTACIÓN:
        - Utiliza polars para operaciones eficientes de filtrado de datos
        - Crea backup automático usando shutil.copy2 antes de modificaciones
        - Emplea filtros de polars (~pl.col().is_in()) para eliminar archivos
        - Actualiza el historial usando add_history_entry para cada eliminación
        - Elimina completamente el Parquet si no quedan datos
        - Llama a clear_processed_files() cuando se vacía el dataset
        - Implementa try-catch con restauración automática desde backup
        - Actualiza la interfaz cerrando el modal y mostrando mensajes de estado
        
       
        
        PARÁMETROS:
        - confirm_clicks (int): Número de clics en el botón de confirmar eliminación
        - selected_files (list): Lista de nombres de archivos seleccionados para eliminar
        
        RETORNA:
        - tuple: (mensaje_estado, historial_actualizado, estilo_modal) donde:
          * mensaje_estado: Componente HTML con el resultado de la operación
          * historial_actualizado: Componente actualizado del historial de archivos
          * estilo_modal: Dict para ocultar el modal después de la operación
        
        """
        if not confirm_clicks:
            return dash.no_update, dash.no_update, dash.no_update
            
        # Define la ruta del archivo Parquet consolidado
        gps_folder = os.path.join(os.path.dirname(__file__), '..', 'data', 'gps')
        merge_path = os.path.join(gps_folder, 'df_gps.parquet')
        backup_path = os.path.join(gps_folder, 'df_gps_backup.parquet')
        removed = []
        
        # Crear una copia de seguridad del archivo Parquet actual si existe y no está vacío
        if os.path.exists(merge_path) and os.path.getsize(merge_path) > 0:
            try:
                # Leer el archivo Parquet actual para verificar que sea válido
                df_actual = pl.read_parquet(merge_path)
                if df_actual.height > 0:
                    # Crear copia de seguridad
                    import shutil
                    shutil.copy2(merge_path, backup_path)
                    print(f"Se ha creado una copia de seguridad del archivo")
                    
                    # Filtrar el dataframe para eliminar los archivos seleccionados
                    if selected_files:
                        # Filtrar el dataframe para mantener solo los archivos que no están seleccionados
                        df_filtrado = df_actual.filter(~pl.col('File Name').is_in(selected_files))
                        
                        # Identificar archivos eliminados para el mensaje
                        for f in selected_files:
                            removed.append(f)
                            # Registra la eliminación en el historial
                            add_history_entry("remove", f)
                        
                        # Verificar si el dataframe filtrado está vacío (todos los archivos fueron eliminados)
                        if df_filtrado.height == 0:
                            # Si no hay más datos, eliminar el archivo Parquet
                            if os.path.exists(merge_path):
                                os.remove(merge_path)
                                print("Todos los archivos fueron eliminados. Archivo df_gps.parquet eliminado.")
                            # También eliminar el backup si existe
                            if os.path.exists(backup_path):
                                os.remove(backup_path)
                                print("Archivo de backup eliminado.")
                            # Limpiar también la carpeta processed cuando todos los archivos son eliminados
                            clear_processed_files()
                            print("Carpeta processed limpiada después de eliminar todos los archivos.")
                        else:
                            # Guardar el dataframe filtrado en formato Parquet
                            df_filtrado.write_parquet(merge_path)
                            print("Dataframe filtrado guardado correctamente.")
                        
                        # Asignar df_filtrado a df_merge para evitar el error
                        df_merge = df_filtrado
                        
            except Exception as e:
                print(f"Error al procesar el dataframe: {str(e)}")
                # Restaurar desde la copia de seguridad si existe
                if os.path.exists(backup_path) and os.path.getsize(backup_path) > 0:
                    try:
                        import shutil
                        shutil.copy2(backup_path, merge_path)
                        print(f"Se ha restaurado el archivo {merge_path} desde la copia de seguridad después de un error.")
                    except Exception as backup_error:
                        print(f"Error al restaurar desde la copia de seguridad: {str(backup_error)}")
                return [html.Div(f"Error al actualizar el dataframe: {str(e)}", className="error-msg")], dash.no_update
        
        # Preparar mensaje de confirmación
        if removed:
            # Verificar si el archivo Parquet aún existe después de la eliminación
            if not os.path.exists(merge_path):
                msg = f"Todos los archivos fueron eliminados ({', '.join(removed)}). El dataframe ha sido completamente eliminado."
            else:
                msg = f"Archivos eliminados: {', '.join(removed)}."
        else:
            msg = "No se eliminó ningún archivo."
        
        if not selected_files:
            msg = "No se seleccionó ningún archivo para eliminar."
        
        # Actualiza el componente de historial
        updated_history = generate_history_component()
        
        # Oculta el modal y muestra el mensaje de confirmación
        return [html.Div(msg, className="success-msg")], updated_history, {"display": "none"}
    
    # ========================================================================
    # Callback para actualizar referencias y crear dataset principal
    # ========================================================================
    @app.callback(
        Output('status-messages', 'children', allow_duplicate=True),
        Input('update-references-btn', 'n_clicks'),
        prevent_initial_call=True
    )
    def handle_update_references(n_clicks):
        """
        PROPÓSITO:
        Maneja el proceso de actualización de referencias y creación del dataset principal
        del proyecto, ejecutando todas las funciones de cálculo estadístico secuencialmente.
        
        FUNCIONALIDAD:
        - Valida que se haya hecho clic en el botón de actualización
        - Proporciona retroalimentación inmediata al usuario sobre el inicio del proceso
        - Ejecuta la función principal update_references() que coordina todos los cálculos
        - Procesa el resultado de la operación (éxito o fallo)
        - Maneja errores inesperados durante la ejecución
        - Proporciona mensajes de estado detallados sobre el resultado
        - Actualiza la interfaz con el estado final de la operación
        
        IMPLEMENTACIÓN:
        - Utiliza dash.no_update para evitar actualizaciones innecesarias
        - Llama directamente a update_references() que contiene toda la lógica de procesamiento
        - Implementa try-catch para capturar errores inesperados del sistema
        - Retorna mensajes HTML con clases CSS apropiadas para el estado
        - Distingue entre errores controlados (de update_references) y errores inesperados
        - Proporciona retroalimentación visual mediante clases de estilo (success-msg, error-msg, info-msg)
        
       
        PARÁMETROS:
        - n_clicks (int): Número de clics en el botón de actualizar referencias
        
        RETORNA:
        - html.Div: Componente HTML con mensaje de estado de la operación, que puede ser:
          * Mensaje de éxito con clase "success-msg" si la operación fue exitosa
          * Mensaje de error con clase "error-msg" si hubo fallo controlado
          * Mensaje de error inesperado con clase "error-msg" si hubo excepción no controlada
          * dash.no_update si no se hizo clic en el botón
        
        """
        if not n_clicks:
            return dash.no_update
        
        # Mostrar mensaje de inicio
        initial_msg = html.Div("Iniciando creación del dataset y actualización de referencias... Esto puede tomar varios minutos.", className="info-msg")
        
        try:
            # Ejecutar la función de actualización de referencias
            success, message = update_references()
            
            if success:
                return html.Div(message, className="success-msg")
            else:
                return html.Div(f"Error: {message}", className="error-msg")
                
        except Exception as e:
            error_msg = f"Error inesperado durante la actualización: {str(e)}"
            return html.Div(error_msg, className="error-msg")
    
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

# Importación de la función calcular_estadisticas desde utils
from utils.utils import calcular_estadisticas


# ============================================================================
# FUNCIONES DE UTILIDAD PARA EL HISTORIAL DE ARCHIVOS
# ============================================================================

# Función para obtener la ruta del archivo de historial
def get_history_file_path():
    """Obtiene la ruta del archivo JSON que almacena el historial"""
    # Define la carpeta donde se guardará el historial
    data_folder = os.path.join(os.path.dirname(__file__), '..', 'data')
    os.makedirs(data_folder, exist_ok=True)
    return os.path.join(data_folder, 'file_history.json')

# Función para cargar el historial de archivos
def load_file_history():
    """Carga el historial de archivos desde el archivo JSON"""
    history_path = get_history_file_path()
    if os.path.exists(history_path):
        try:
            with open(history_path, 'r') as f:
                return json.load(f)
        except Exception:
            return []
    return []

# Función para guardar el historial de archivos
def save_file_history(history):
    """Guarda el historial de archivos en el archivo JSON"""
    history_path = get_history_file_path()
    with open(history_path, 'w') as f:
        json.dump(history, f)

# Función para añadir una entrada al historial
def add_history_entry(action, filename):
    """Añade una nueva entrada al historial con la acción, nombre de archivo y timestamp"""
    history = load_file_history()
    entry = {
        'action': action,
        'filename': filename,
        'timestamp': datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    }
    history.append(entry)
    save_file_history(history)

# ============================================================================
# FUNCIONES DE INTERFAZ
# ============================================================================

# Función para generar el componente de historial de archivos
def generate_history_component(history=None):
    """Genera el componente HTML que muestra el historial de archivos"""
    if history is None:
        history = load_file_history()
    
    if not history:
        return html.Div("Nenhum histórico de arquivos disponível.", className="no-files-msg")
    
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

# ============================================================================
# LAYOUT DE LA PÁGINA
# ============================================================================

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
                # Botón para descargar archivos
                html.Button('DOWNLOAD', id='btn-download', className='btn-download')
            ],  className="buttons-right"),
        ], className="cargar-datos-header"),
        
        # Contenedor de mensajes de estado
        html.Div(id='status-messages', children=[html.Div("Sube o ajusta el número de ficheros almacenados.", className="info-msg")]),
        
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
    
    # Intervalo para actualizar el historial periódicamente (cada 30 segundos)
    dcc.Interval(
        id='interval-component',
        interval=30*1000,  # en milisegundos
        n_intervals=0
    )
])

# ============================================================================
# CALLBACKS
# ============================================================================

def register_callbacks(app):
    """Registra todos los callbacks de la página"""
    
    # ========================================================================
    # Callback para subir archivos y actualizar el CSV consolidado
    # ========================================================================
    @app.callback(
        [Output('status-messages', 'children'),
         Output('file-history', 'children', allow_duplicate=True)],
        Input('upload-data', 'contents'),
        State('upload-data', 'filename'),
        prevent_initial_call=True
    )
    def save_file(contents, filename):
        """Procesa la subida de archivos, actualiza el CSV consolidado y el historial"""
        # Si no hay archivo subido, no hacer nada
        if contents is None or filename is None:
            return [], dash.no_update
        
        # Define la carpeta donde se guardarán los archivos
        gps_folder = os.path.join(os.path.dirname(__file__), '..', 'data', 'gps')
        os.makedirs(gps_folder, exist_ok=True)
        file_path = os.path.join(gps_folder, filename)
        
        # Verifica si el archivo ya existe
        if os.path.exists(file_path):
            return [html.Div(f"Archivo '{filename}' ya fue cargado.", className="error-msg")], dash.no_update
        
        # Decodifica el archivo subido
        content_type, content_string = contents.split(',')
        decoded = base64.b64decode(content_string)
        with open(file_path, 'wb') as f:
            f.write(decoded)
        
        try:           
            # Lee el archivo XLSX y añade la columna 'File Name'
            df_new = pl.read_excel(file_path)
            df_new = df_new.with_columns(pl.lit(filename).alias('File Name'))
        except Exception as e:
            return [html.Div(f"Error al leer el archivo: {str(e)}", className="error-msg")], dash.no_update
        
        # Ruta del archivo CSV consolidado
        merge_path = os.path.join(gps_folder, 'df_gps.csv')
        print("Archivo salvado")
        
        try:
            # Obtiene todos los archivos XLSX en la carpeta (incluido el recién subido)
            all_files = [f for f in os.listdir(gps_folder) if f.endswith('.xlsx')]
            
            # Lee y concatena todos los archivos XLSX, añadiendo la columna 'File Name'
            dfs = [pl.read_excel(os.path.join(gps_folder, f)).with_columns(pl.lit(f).alias('File Name')) for f in all_files]
            df_merge = pl.concat(dfs, how='vertical_relaxed')
            
            # Guarda el DataFrame consolidado en CSV
            df_merge.write_csv(merge_path)
            
            # Registra la acción en el historial
            add_history_entry("upload", filename)
            
            # Ejecuta la función calcular_estadisticas para actualizar las estadísticas
            try:
                calcular_estadisticas()
                print("Estadísticas calculadas correctamente después de añadir archivo.")
            except Exception as e:
                print(f"Error al calcular estadísticas: {str(e)}")
            
            # Actualiza el componente de historial
            updated_history = generate_history_component()
            
        except Exception as e:
            return [html.Div(f"Error al guardar el merge: {str(e)}", className="error-msg")], dash.no_update
        
        return [html.Div(f"Archivo '{filename}' guardado y concatenado.", className="success-msg")], updated_history

    # ========================================================================
    # Callback para mostrar lista de archivos XLSX y permitir su edición
    # ========================================================================
    @app.callback(
        Output('file-history', 'children', allow_duplicate=True),
        Input('edit-files-btn', 'n_clicks'),
        prevent_initial_call=True
    )
    def show_edit_files(n_clicks):
        """Muestra la interfaz de edición con la lista de archivos disponibles"""
        # Obtiene la lista de archivos XLSX en la carpeta gps
        gps_folder = os.path.join(os.path.dirname(__file__), '..', 'data', 'gps')
        files = [f for f in os.listdir(gps_folder) if f.endswith('.xlsx')]
        if not files:
            return html.Div('No hay archivos para editar.')
        
        # Muestra los archivos como checklist para seleccionar cuáles mantener
        return html.Div([
            html.Label('Seleccione los archivos que desea mantener:'),
            dcc.Checklist(
                id='files-checklist',
                options=[{'label': f, 'value': f} for f in files],
                value=files,
                inputStyle={'marginRight': '8px'}
            ),
            html.Button('Confirmar Alterações', id='confirm-edit-btn', className='btn-confirm'),
            html.Button('Cancelar Alterações', id='cancel-edit-btn', className='btn-cancel')
        ], className="edit-section")

    # ========================================================================
    # Callback para confirmar cambios en la edición de archivos
    # ========================================================================
    @app.callback(
        [
            Output('status-messages', 'children', allow_duplicate=True),
            Output('file-history', 'children', allow_duplicate=True)
        ],
        [
            Input('confirm-edit-btn', 'n_clicks'),
            Input('cancel-edit-btn', 'n_clicks')
        ],
        State('files-checklist', 'value'),
        prevent_initial_call=True
    )
    def confirm_edit(confirm_clicks, cancel_clicks, selected_files):
        """Procesa la confirmación o cancelación de la edición de archivos"""
        # Identifica qué botón fue presionado
        ctx = callback_context
        if not ctx.triggered:
            return [], dash.no_update
        
        button_id = ctx.triggered[0]['prop_id'].split('.')[0]
        
        # Si se presionó el botón de cancelar, vuelve a la vista del historial
        if button_id == 'cancel-edit-btn':
            msg = 'Alterações canceladas.'
            return [html.Div(msg, className="success-msg")], generate_history_component()
            
        # Procesa la confirmación de cambios
        gps_folder = os.path.join(os.path.dirname(__file__), '..', 'data', 'gps')
        all_files = [f for f in os.listdir(gps_folder) if f.endswith('.xlsx')]
        removed = []
       
        # Elimina los archivos que no están seleccionados
        for f in all_files:
            if f not in selected_files:
                try:
                    os.remove(os.path.join(gps_folder, f))
                    removed.append(f)
                    # Registra la eliminación en el historial
                    add_history_entry("remove", f)
                except Exception as e:
                    return [html.Div(f"Error al eliminar '{f}': {str(e)}", className="error-msg")], dash.no_update
       
        # Actualiza el CSV consolidado solo con los archivos seleccionados
        merge_path = os.path.join(gps_folder, 'df_gps.csv')
        print("Archivo editado")
        if selected_files:
            try:    
                # Recrea el df_gps.csv solo con los archivos seleccionados
                # Lee y concatena los archivos seleccionados, añadiendo la columna 'File Name'
                dfs = [pl.read_excel(os.path.join(gps_folder, f)).with_columns(pl.lit(f).alias('File Name')) for f in selected_files]
                df_merge = pl.concat(dfs, how='vertical_relaxed')
                df_merge.write_csv(merge_path)
            except Exception as e:
                return [html.Div(f"Error al actualizar el merge: {str(e)}", className="error-msg")], dash.no_update
            msg = f"Archivos eliminados: {', '.join(removed)}."
        
        else:
            # Si no hay archivos seleccionados, elimina el CSV consolidado
            if os.path.exists(merge_path):
                os.remove(merge_path)
            msg = "Todos los archivos fueron eliminados."
        
        # Ejecuta la función calcular_estadisticas para actualizar las estadísticas
        if selected_files:  # Solo si hay archivos seleccionados
            try:
                calcular_estadisticas()
                print("Estadísticas calculadas correctamente después de editar archivos.")
            except Exception as e:
                print(f"Error al calcular estadísticas: {str(e)}")
        
        # Actualiza el componente de historial
        updated_history = generate_history_component()
        
        return [html.Div(msg, className="success-msg")], updated_history
    
    # ========================================================================
    # Callback para actualizar el historial periódicamente
    # ========================================================================
    @app.callback(
        Output('file-history', 'children', allow_duplicate=True),
        Input('interval-component', 'n_intervals'),
        prevent_initial_call=True
    )
    def update_history_periodically(n):
        """Actualiza el historial de archivos periódicamente"""
        return generate_history_component()
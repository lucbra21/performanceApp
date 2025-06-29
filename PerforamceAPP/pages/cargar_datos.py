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

# Importación de funciones desde utils
from utils.utils import calcular_estadisticas


# ============================================================================
# FUNCIONES DE UTILIDAD PARA EL HISTORIAL DE ARCHIVOS
# ============================================================================

# Función para cargar el historial de archivos
def load_file_history():
    """Carga el historial de archivos desde el archivo JSON"""
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
    """Añade una nueva entrada al historial con la acción, nombre de archivo y timestamp"""
    history = load_file_history()
    entry = {
        'action': action,
        'filename': filename,
        'timestamp': datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    }
    history.append(entry)
    
    # Define la carpeta y guarda el historial actualizado
    data_folder = os.path.join(os.path.dirname(__file__), '..', 'data')
    history_path = os.path.join(data_folder, 'file_history.json')
    with open(history_path, 'w') as f:
        json.dump(history, f)
        
# ============================================================================
# FUNCIONES DE INTERFAZ
# ============================================================================

# Función para generar el componente de historial de archivos
def generate_history_component(history=None):
    """Genera el componente HTML que muestra el historial de archivos"""
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
    """Registra todos los callbacks de la página"""
    
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
        """Procesa la subida de archivos, actualiza el Parquet consolidado y el historial sin guardar archivos Excel"""
        
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
            
            # Ejecuta la función calcular_estadisticas para actualizar las estadísticas
            try:
                resultado = calcular_estadisticas()
                if resultado is not None and len(resultado) > 0 and resultado[0] is not None:
                    print("Estadísticas calculadas correctamente después de añadir archivo.")
                else:
                    print("No hay suficientes datos para calcular estadísticas.")
            except Exception as e:
                print(f"Error al calcular estadísticas: {str(e)}")
            
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
        """Controla la visibilidad del modal de edición y carga el contenido"""
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
        """Procesa la confirmación de la edición de archivos en el dataframe consolidado"""
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
        
        #Ejecuta la función calcular_estadisticas para actualizar las estadísticas
        try:
            # Verificar si aún hay datos en el dataframe después de eliminar archivos
            if os.path.exists(merge_path) and os.path.getsize(merge_path) > 0:
                df_check = pl.read_parquet(merge_path)
                if df_check.height > 0:
                    resultado = calcular_estadisticas()
                    if resultado is not None and len(resultado) > 0 and resultado[0] is not None:
                        print("Estadísticas calculadas correctamente después de editar archivos.")
                    else:
                        print("No hay suficientes datos para calcular estadísticas.")
                else:
                    print("No hay datos en el dataframe para calcular estadísticas.")
            else:
                print("No existe el archivo de datos para calcular estadísticas.")
        except Exception as e:
            print(f"Error al calcular estadísticas: {str(e)}")
        
        # Actualiza el componente de historial
        updated_history = generate_history_component()
        
        # Oculta el modal y muestra el mensaje de confirmación
        return [html.Div(msg, className="success-msg")], updated_history, {"display": "none"}
    
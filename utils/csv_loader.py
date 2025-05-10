# utils/csv_loader.py
import base64
import io
import pandas as pd
from dash import html

def parse_contents(contents, filename):
    """Analiza el contenido de un archivo CSV cargado y devuelve un DataFrame y una tabla de vista previa."""
    content_type, content_string = contents.split(',')
    decoded = base64.b64decode(content_string)
    try:
        if 'csv' in filename:
            # Assume Csv
            df = pd.read_csv(io.StringIO(decoded.decode('utf-8')))
        elif 'xls' in filename:
            # Assume Excel
            df = pd.read_excel(io.BytesIO(decoded))
        else:
            return html.Div([
                'Hubo un error al procesar este archivo. Por favor, sube un archivo CSV o Excel.'
            ]), None
    except Exception as e:
        # Log detailed error to server console
        print(f"Error processing file {filename}: {e}")
        # Create a more informative error message for the UI
        error_message_div = html.Div([
            f'Error al procesar el archivo: {filename}.',
            html.Br(),
            'Por favor, verifique que el archivo no esté corrupto y que el formato (CSV o Excel) sea correcto.',
            html.Br(),
            'Asegúrese también de que el archivo utiliza la codificación UTF-8 si es un CSV.'
        ])
        return error_message_div, None

    # Generate a preview table
    preview_table = html.Table([
        html.Thead(
            html.Tr([html.Th(col) for col in df.columns[:5]]) # Show first 5 columns
        ),
        html.Tbody([
            html.Tr([
                html.Td(df.iloc[i][col]) for col in df.columns[:5]
            ]) for i in range(min(len(df), 5)) # Show first 5 rows
        ])
    ])
    return preview_table, df
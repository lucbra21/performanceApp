# utils/csv_loader.py
import base64
import io
import pandas as pd
from dash import html

def parse_contents(contents, filename):
    """Parses the content of an uploaded CSV or Excel file and returns a DataFrame and a preview table or an error message."""
    content_type, content_string = contents.split(',')
    decoded = base64.b64decode(content_string)
    try:
        # Make filename extension check case-insensitive
        if filename.lower().endswith('.csv'):
            # Try decoding with utf-8, then latin-1 as a fallback
            try:
                df = pd.read_csv(io.StringIO(decoded.decode('utf-8')))
            except UnicodeDecodeError:
                df = pd.read_csv(io.StringIO(decoded.decode('latin-1')))
        elif filename.lower().endswith('.xls') or filename.lower().endswith('.xlsx'):
            df = pd.read_excel(io.BytesIO(decoded))
        else:
            return html.Div([
                f'Error: El archivo "{filename}" no es un CSV o Excel válido. Por favor, sube un archivo con extensión .csv, .xls o .xlsx.'
            ]), None
    except Exception as e:
        print(f"Error processing file {filename}: {e}")
        return html.Div([
            f'Hubo un error al procesar el archivo "{filename}": {str(e)}'
        ]), None

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
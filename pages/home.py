# pages/home.py
import pandas as pd
from dash import html, dcc
from dash.dependencies import Input, Output, State

# Placeholder for csv_loader functions - to be defined in utils/csv_loader.py
# from utils.csv_loader import parse_contents

# Import components
from components.header import Header
from components.sidebar import Sidebar

def layout():
    """Crea el diseño para la página de inicio."""
    return html.Div([
        Header(),
        Sidebar(),
        html.Div([
            html.H2('Página de Inicio - Carga de Datos'),
            
            # GPS Data Upload
            html.H3('Datos GPS'),
            dcc.Upload(
                id='upload-gps-data',
                children=html.Div([
                    'Arrastra y Suelta o ',
                    html.A('Selecciona Archivos GPS')
                ]),
                style={
                    'width': '100%',
                    'height': '60px',
                    'lineHeight': '60px',
                    'borderWidth': '1px',
                    'borderStyle': 'dashed',
                    'borderRadius': '5px',
                    'textAlign': 'center',
                    'margin': '10px'
                },
                multiple=True # Allow multiple files to be uploaded
            ),
            html.Div(id='output-gps-upload'),
            html.Div(id='last-upload-gps'),
            html.Div(id='preview-gps-data'),

            # Wellness Data Upload
            html.H3('Datos Wellness'),
            dcc.Upload(
                id='upload-wellness-data',
                children=html.Div([
                    'Arrastra y Suelta o ',
                    html.A('Selecciona Archivos Wellness')
                ]),
                style={
                    'width': '100%',
                    'height': '60px',
                    'lineHeight': '60px',
                    'borderWidth': '1px',
                    'borderStyle': 'dashed',
                    'borderRadius': '5px',
                    'textAlign': 'center',
                    'margin': '10px'
                },
                multiple=True
            ),
            html.Div(id='output-wellness-upload'),
            html.Div(id='last-upload-wellness'),
            html.Div(id='preview-wellness-data'),

            # Training Data Upload
            html.H3('Datos de Entrenamiento'),
            dcc.Upload(
                id='upload-training-data',
                children=html.Div([
                    'Arrastra y Suelta o ',
                    html.A('Selecciona Archivos de Entrenamiento')
                ]),
                style={
                    'width': '100%',
                    'height': '60px',
                    'lineHeight': '60px',
                    'borderWidth': '1px',
                    'borderStyle': 'dashed',
                    'borderRadius': '5px',
                    'textAlign': 'center',
                    'margin': '10px'
                },
                multiple=True
            ),
            html.Div(id='output-training-upload'),
            html.Div(id='last-upload-training'),
            html.Div(id='preview-training-data'),
            
            # dcc.Store components to store uploaded data
            dcc.Store(id='store-gps-data'),
            dcc.Store(id='store-wellness-data'),
            dcc.Store(id='store-training-data'),

        ], className='content')
    ])

import datetime
from app import app # Import the app instance
from utils.csv_loader import parse_contents # Import the parsing function

# Callback for GPS data upload
@app.callback(
    [Output('output-gps-upload', 'children'),
     Output('last-upload-gps', 'children'),
     Output('preview-gps-data', 'children'),
     Output('store-gps-data', 'data')],
    [Input('upload-gps-data', 'contents')],
    [State('upload-gps-data', 'filename'),
     State('upload-gps-data', 'last_modified')],
    prevent_initial_call=True
)
def update_output_gps(list_of_contents, list_of_names, list_of_dates):
    if list_of_contents is not None:
        children = []
        df_combined = pd.DataFrame() # Initialize an empty DataFrame to combine data if multiple files
        for i, c in enumerate(list_of_contents):
            filename = list_of_names[i]
            # date = list_of_dates[i] # Not directly used here, but available
            preview_table, df = parse_contents(c, filename)
            if df is not None:
                df_combined = pd.concat([df_combined, df], ignore_index=True)
                children.append(html.Div([f'Archivo cargado: {filename}']))
            else:
                children.append(preview_table) # This will be an error message div
        
        last_upload_time_str = f"Última carga: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
        
        # Generate a combined preview if data was loaded
        if not df_combined.empty:
            combined_preview_table = html.Table([
                html.Thead(html.Tr([html.Th(col) for col in df_combined.columns[:5]])),
                html.Tbody([
                    html.Tr([html.Td(df_combined.iloc[j][col]) for col in df_combined.columns[:5]])
                    for j in range(min(len(df_combined), 5))
                ])
            ])
            return children, last_upload_time_str, combined_preview_table, df_combined.to_dict('records')
        else:
            return children, "Ningún archivo válido cargado.", html.Div("Nenhum dado válido para pré-visualização."), None

    return html.Div(), "Ningún archivo cargado.", html.Div("Nenhum ficheiro carregado para pré-visualização."), None

# Callback for Wellness data upload
@app.callback(
    [Output('output-wellness-upload', 'children'),
     Output('last-upload-wellness', 'children'),
     Output('preview-wellness-data', 'children'),
     Output('store-wellness-data', 'data')],
    [Input('upload-wellness-data', 'contents')],
    [State('upload-wellness-data', 'filename'),
     State('upload-wellness-data', 'last_modified')],
    prevent_initial_call=True
)
def update_output_wellness(list_of_contents, list_of_names, list_of_dates):
    if list_of_contents is not None:
        children = []
        df_combined = pd.DataFrame()
        for i, c in enumerate(list_of_contents):
            filename = list_of_names[i]
            preview_table, df = parse_contents(c, filename)
            if df is not None:
                df_combined = pd.concat([df_combined, df], ignore_index=True)
                children.append(html.Div([f'Archivo cargado: {filename}']))
            else:
                children.append(preview_table)
        
        last_upload_time_str = f"Última carga: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
        
        if not df_combined.empty:
            combined_preview_table = html.Table([
                html.Thead(html.Tr([html.Th(col) for col in df_combined.columns[:5]])),
                html.Tbody([
                    html.Tr([html.Td(df_combined.iloc[j][col]) for col in df_combined.columns[:5]])
                    for j in range(min(len(df_combined), 5))
                ])
            ])
            return children, last_upload_time_str, combined_preview_table, df_combined.to_dict('records')
        else:
            return children, "Ningún archivo válido cargado.", html.Div("Nenhum dado válido para pré-visualização."), None

    return html.Div(), "Ningún archivo cargado.", html.Div("Nenhum ficheiro carregado para pré-visualização."), None

# Callback for Training data upload
@app.callback(
    [Output('output-training-upload', 'children'),
     Output('last-upload-training', 'children'),
     Output('preview-training-data', 'children'),
     Output('store-training-data', 'data')],
    [Input('upload-training-data', 'contents')],
    [State('upload-training-data', 'filename'),
     State('upload-training-data', 'last_modified')],
    prevent_initial_call=True
)
def update_output_training(list_of_contents, list_of_names, list_of_dates):
    if list_of_contents is not None:
        children = []
        df_combined = pd.DataFrame()
        for i, c in enumerate(list_of_contents):
            filename = list_of_names[i]
            preview_table, df = parse_contents(c, filename)
            if df is not None:
                df_combined = pd.concat([df_combined, df], ignore_index=True)
                children.append(html.Div([f'Archivo cargado: {filename}']))
            else:
                children.append(preview_table)
        
        last_upload_time_str = f"Última carga: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
        
        if not df_combined.empty:
            combined_preview_table = html.Table([
                html.Thead(html.Tr([html.Th(col) for col in df_combined.columns[:5]])),
                html.Tbody([
                    html.Tr([html.Td(df_combined.iloc[j][col]) for col in df_combined.columns[:5]])
                    for j in range(min(len(df_combined), 5))
                ])
            ])
            return children, last_upload_time_str, combined_preview_table, df_combined.to_dict('records')
        else:
            return children, "Ningún archivo válido cargado.", html.Div("Nenhum dado válido para pré-visualização."), None

    return html.Div(), "Ningún archivo cargado.", html.Div("Nenhum ficheiro carregado para pré-visualização."), None
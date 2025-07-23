# ============================================================================
# IMPORTACIONES
# ============================================================================

# Importaciones de Dash
from dash import html, dcc, Output, Input, State, callback_context, dash_table
import dash

# Importaciones del sistema y utilidades
import os
from datetime import datetime
import polars as pl
import pandas as pd
import numpy as np
from utils.utils import *

# Importaciones del Plotly para gráficos
import plotly.graph_objects as go
import plotly.express as px

# ============================================================================
# ESTILOS PARA DATATABLES - CENTRALIZADOS PARA MEJOR ORGANIZACIÓN
# ============================================================================

# Estilos para tabla de jugadores
PLAYERS_TABLE_STYLES = {
    'style_table': {
        'overflowX': 'auto',
        'maxHeight': '600px',
        'overflowY': 'auto'
    },
    'style_cell': {
        'textAlign': 'left',
        'padding': '10px',
        'fontFamily': 'Arial, sans-serif',
        'fontSize': '14px',
        'border': '1px solid #ddd'
    },
    'style_header': {
        'backgroundColor': '#f8f9fa',
        'fontWeight': 'bold',
        'textAlign': 'center',
        'border': '1px solid #ddd'
    },
    'style_data': {
        'backgroundColor': 'white',
        'border': '1px solid #ddd'
    },
    'style_data_conditional': [
        {
            'if': {'row_index': 'odd'},
            'backgroundColor': '#f8f9fa'
        }
    ]
}

# Estilos para tabla combinada de estadísticas
COMBINED_TABLE_STYLES = {
    'style_table': {
        'overflowX': 'auto',
        'maxHeight': '600px',
        'overflowY': 'auto'
    },
    'style_cell': {
        'textAlign': 'left',
        'padding': '8px',
        'fontFamily': 'Arial, sans-serif',
        'fontSize': '13px',
        'border': '1px solid #ddd'
    },
    'style_header': {
        'backgroundColor': '#e8f4fd',
        'fontWeight': 'bold',
        'textAlign': 'center',
        'border': '1px solid #ddd'
    },
    'style_data': {
        'backgroundColor': 'white',
        'border': '1px solid #ddd'
    },
    'style_data_conditional': [
        {
            'if': {'row_index': 'odd'},
            'backgroundColor': '#f8f9fa'
        },
        {
            'if': {'filter_query': '{_tipo_interno} = JugadorIndividual'},
            'backgroundColor': '#e8f5e8'
        },
        {
            'if': {'filter_query': '{_tipo_interno} = Jugador'},
            'backgroundColor': '#f0f8ff'
        },
        {
            'if': {'filter_query': '{_tipo_interno} = Equipo'},
            'backgroundColor': '#e8f4fd'
        },
        {
            'if': {'filter_query': '{_tipo_interno} = Posición'},
            'backgroundColor': '#fff3cd'
        }
    ]
}



# ============================================================================
# FUNCIONES AUXILIARES
# ============================================================================

# Función auxiliar para formatear valores con absoluto y relativo
def format_value_with_relative(df_data, match_day_filter):
    """
    Formatea valores de métricas mostrando valor absoluto y diferencia porcentual relativa.
    
    Esta función procesa los datos de un DataFrame específico para mostrar tanto el valor
    absoluto de cada métrica como su diferencia porcentual respecto al promedio histórico,
    presentándolos en formato "valor_absoluto (diferencia_relativa%)".
    
    Args:
        df_data (pl.DataFrame): DataFrame con datos de métricas que incluye tanto valores
                               absolutos como diferencias porcentuales por Match Day
        match_day_filter (str): Identificador del Match Day específico para filtrar los datos
                               de la fecha seleccionada
    
    Returns:
        list: Lista de diccionarios donde cada diccionario representa una fila formateada
              con valores en formato "absoluto (relativo%)" para visualización en tabla
    """
    formatted_data = []
    
    # Filtrar solo los datos del Match Day específico de la fecha seleccionada
    df_filtered = df_data.filter(pl.col('Match Day') == match_day_filter)
    
    if df_filtered.height == 0:
        return []
    
    df_pandas = df_filtered.to_pandas()
    
    # Obtener datos de diferencia para calcular valores relativos
    df_diff = df_data.filter(pl.col('Match Day') == 'diferencia')
    df_diff_pandas = df_diff.to_pandas() if df_diff.height > 0 else None
    
    for _, row in df_pandas.iterrows():
        formatted_row = {}
        
        # Copiar columnas de identificación
        for col in ['Player', 'Position', 'Team', 'Match Day', 'Estadistica']:
            if col in row:
                formatted_row[col] = row[col]
        
        # Formatear columnas numéricas con valor absoluto y relativo
        for col in df_pandas.columns:
            if col not in ['Player', 'Position', 'Team', 'Match Day', 'Estadistica']:
                abs_value = row[col]
                
                # Buscar valor relativo correspondiente en df_diff
                rel_value = None
                if df_diff_pandas is not None:
                    # Buscar la fila correspondiente en diferencias
                    mask = True
                    for id_col in ['Player', 'Position', 'Team', 'Estadistica']:
                        if id_col in row and id_col in df_diff_pandas.columns:
                            mask = mask & (df_diff_pandas[id_col] == row[id_col])
                    
                    matching_rows = df_diff_pandas[mask]
                    if len(matching_rows) > 0 and col in matching_rows.columns:
                        rel_value = matching_rows[col].iloc[0]
                
                # Formatear el valor
                if abs_value is not None and not pd.isna(abs_value):
                    if rel_value is not None and not pd.isna(rel_value):
                        formatted_row[col] = f"{abs_value:.1f}        ({rel_value:.1f}%)"
                    else:
                        formatted_row[col] = f"{abs_value:.1f}"
                else:
                    formatted_row[col] = "N/A"
        
        formatted_data.append(formatted_row)
    
    return formatted_data


def extract_zscore_from_formatted_value(value_str):
    """
    Extrae el valor de z-score de una cadena de texto formateada.
    
    Esta función utiliza expresiones regulares para buscar y extraer valores de z-score
    que están embebidos en cadenas de texto con formato específico. Es especialmente útil
    para procesar datos que han sido formateados previamente con información estadística.
    
    Args:
        value_str (str): Cadena de texto que contiene un valor formateado con patrón
                        "valor (relativo%) [z:zscore]" donde se busca extraer el z-score
    
    Returns:
        float: Valor numérico del z-score extraído de la cadena, o None si no se encuentra
               un patrón válido o si ocurre un error en la conversión
    
    Example:
        >>> extract_zscore_from_formatted_value("15.3 (12.5%) [z:1.25]")
        1.25
        >>> extract_zscore_from_formatted_value("10.0")
        None
    """
    if not isinstance(value_str, str):
        return None
    
    import re
    # Buscar patrón [z:número]
    match = re.search(r'\[z:([-+]?\d*\.?\d+)\]', value_str)
    if match:
        try:
            return float(match.group(1))
        except ValueError:
            return None
    return None


def create_zscore_heatmap_styles(data, columns_order):
    """
    Genera estilos condicionales para crear un mapa de calor basado en valores de z-score.
    
    Esta función crea un sistema de coloración gradual para tablas donde cada celda se colorea
    según su valor de z-score. Los valores positivos se muestran en gradiente naranja y los
    negativos en gradiente azul, con intensidad proporcional al valor absoluto del z-score.
    
    Args:
        data (list): Lista de diccionarios donde cada diccionario representa una fila de datos
                    con valores formateados que contienen z-scores embebidos
        columns_order (list): Lista ordenada de nombres de columnas que determina qué columnas
                             serán procesadas para el mapa de calor
    
    Returns:
        list: Lista de estilos condicionales de Dash DataTable que incluye:
              - Estilos base para diferentes tipos de entidades (jugador, equipo, posición)
              - Estilos de mapa de calor con gradientes de color basados en z-scores
              - Configuraciones de peso de fuente según intensidad del z-score
    
    Note:
        - Z-scores positivos: gradiente blanco → naranja oscuro
        - Z-scores negativos: gradiente blanco → azul oscuro  
        - Z-scores cercanos a 0: color blanco (sin resaltado)
        - Valores extremos (>70% del máximo): texto en negrita
    """
    styles = []
    
    # Mantener estilos existentes para tipos de entidad (solo para la columna Player/Team/Position)
    base_styles = [
        {
            'if': {'row_index': 'odd', 'column_id': 'Player/Team/Position'},
            'backgroundColor': '#f8f9fa'
        },
        {
            'if': {'filter_query': '{_tipo_interno} = JugadorIndividual', 'column_id': 'Player/Team/Position'},
            'backgroundColor': '#e8f5e8'
        },
        {
            'if': {'filter_query': '{_tipo_interno} = Jugador', 'column_id': 'Player/Team/Position'},
            'backgroundColor': '#f0f8ff'
        },
        {
            'if': {'filter_query': '{_tipo_interno} = Equipo', 'column_id': 'Player/Team/Position'},
            'backgroundColor': '#e8f4fd'
        },
        {
            'if': {'filter_query': '{_tipo_interno} = Posición', 'column_id': 'Player/Team/Position'},
            'backgroundColor': '#fff3cd'
        }
    ]
    
    # Recopilar todos los z-scores para normalización
    all_zscores = []
    for record in data:
        for col in columns_order:
            if col != 'Player/Team/Position':  # Excluir columna de identificación
                value = record.get(col, '')
                zscore = extract_zscore_from_formatted_value(value)
                if zscore is not None:
                    all_zscores.append(abs(zscore))
    
    if not all_zscores:
        return base_styles
    
    # Calcular el valor máximo de z-score para normalización
    max_zscore = max(all_zscores)
    
    # Definir umbral mínimo para considerar "próximo a cero"
    min_threshold = 0.1
    
    def get_gradient_color(zscore_value, max_value):
        """
        Calcula el color del gradiente basado en el valor del z-score
        
        Args:
            zscore_value: Valor del z-score
            max_value: Valor máximo de z-score para normalización
        
        Returns:
            str: Color en formato rgba
        """
        abs_zscore = abs(zscore_value)
        
        # Si el z-score está muy cerca de 0, usar blanco
        if abs_zscore <= min_threshold:
            return 'rgba(255, 255, 255, 1.0)'  # Blanco
        
        # Normalizar el valor absoluto del z-score (0 a 1)
        if max_value > min_threshold:
            intensity = min(1.0, (abs_zscore - min_threshold) / (max_value - min_threshold))
        else:
            intensity = 0
        
        # Aplicar función de suavizado para gradiente más natural
        intensity = intensity ** 0.7  # Raíz para suavizar la transición
        
        if zscore_value > 0:
            # Z-score positivo: gradiente de blanco a naranja oscuro
            # RGB del naranja oscuro: (255, 140, 0)
            red = 255
            green = int(255 - (255 - 140) * intensity)
            blue = int(255 - 255 * intensity)
            alpha = 0.3 + 0.7 * intensity  # De transparente a opaco
        else:
            # Z-score negativo: gradiente de blanco a azul oscuro
            # RGB del azul oscuro: (0, 100, 200)
            red = int(255 - 255 * intensity)
            green = int(255 - (255 - 100) * intensity)
            blue = int(255 - (255 - 200) * intensity)
            alpha = 0.3 + 0.7 * intensity  # De transparente a opaco
        
        return f'rgba({red}, {green}, {blue}, {alpha})'
    
    # Crear estilos para cada celda con z-score
    for row_idx, record in enumerate(data):
        for col in columns_order:
            if col != 'Player/Team/Position':  # Excluir columna de identificación
                value = record.get(col, '')
                zscore = extract_zscore_from_formatted_value(value)
                
                if zscore is not None:
                    color = get_gradient_color(zscore, max_zscore)
                    abs_zscore = abs(zscore)
                    
                    # Determinar peso de la fuente basado en la intensidad
                    font_weight = 'bold' if abs_zscore > max_zscore * 0.7 else 'normal'
                    
                    # Añadir estilo para esta celda específica
                    styles.append({
                        'if': {
                            'row_index': row_idx,
                            'column_id': col
                        },
                        'backgroundColor': color,
                        'color': 'black',
                        'fontWeight': font_weight
                    })
    
    # Combinar estilos base con estilos de mapa de calor
    return base_styles + styles


def format_value_with_zscore_only(df_data, match_day_filter, zscore_data, estadistica_seleccionada):
    """
    Formatea valores de métricas mostrando únicamente valor absoluto y z-score.
    
    Esta función procesa datos de rendimiento para presentar información estadística
    en formato simplificado, mostrando solo el valor absoluto de cada métrica junto
    con su correspondiente z-score, omitiendo las diferencias porcentuales relativas.
    
    Args:
        df_data (pl.DataFrame): DataFrame principal con datos de métricas organizados por Match Day
        match_day_filter (str): Identificador del Match Day específico para filtrar los datos
                               de la fecha seleccionada
        zscore_data (dict): Diccionario con datos de z-score calculados previamente, organizado
                           por entidad (jugador/equipo/posición) con estructura:
                           {entidad: {'z_scores': {métrica: valor_zscore}}}
        estadistica_seleccionada (str): Tipo de estadística de referencia utilizada para
                                      los cálculos ('mean', 'median', etc.)
    
    Returns:
        list: Lista de diccionarios formateados donde cada diccionario representa una fila
              con valores en formato "valor_absoluto [z:zscore]" para visualización en tabla
    
    Note:
        Esta función es ideal cuando se quiere mostrar información estadística sin
        sobrecargar la visualización con demasiados números, manteniendo solo los
        datos más relevantes para análisis de rendimiento.
    """
    formatted_data = []
    
    # Filtrar solo los datos del Match Day específico de la fecha seleccionada
    df_filtered = df_data.filter(pl.col('Match Day') == match_day_filter)
    
    if df_filtered.height == 0:
        return []
    
    df_pandas = df_filtered.to_pandas()
    
    for _, row in df_pandas.iterrows():
        formatted_row = {}
        
        # Copiar columnas de identificación
        for col in ['Player', 'Position', 'Team', 'Match Day', 'Estadistica']:
            if col in row:
                formatted_row[col] = row[col]
        
        # Formatear columnas numéricas con valor absoluto y z-score
        for col in df_pandas.columns:
            if col not in ['Player', 'Position', 'Team', 'Match Day', 'Estadistica']:
                abs_value = row[col]
                
                # Buscar valor de z-score correspondiente
                zscore_value = None
                if zscore_data:
                    # Identificar la entidad (jugador, equipo o posición)
                    entity_key = None
                    if 'Player' in row and row['Player'] in zscore_data:
                        entity_key = row['Player']
                    elif 'Team' in row and row['Team'] in zscore_data:
                        entity_key = row['Team']
                    elif 'Position' in row and row['Position'] in zscore_data:
                        entity_key = row['Position']
                    
                    if entity_key and 'z_scores' in zscore_data[entity_key]:
                        zscore_value = zscore_data[entity_key]['z_scores'].get(col)
                
                # Formatear el valor con absoluto y z-score
                if abs_value is not None and not pd.isna(abs_value):
                    value_str = f"{abs_value:.1f}"
                    
                    # Añadir z-score si existe
                    if zscore_value is not None and not pd.isna(zscore_value):
                        value_str += f" [z:{zscore_value:.2f}]"
                    
                    formatted_row[col] = value_str
                else:
                    formatted_row[col] = "N/A"
        
        formatted_data.append(formatted_row)
    
    return formatted_data


def format_value_with_relative_and_zscore(df_data, match_day_filter, zscore_data, estadistica_seleccionada):
    """
    Formatea valores de métricas mostrando información estadística completa.
    
    Esta función procesa datos de rendimiento para presentar la información más completa
    posible, combinando valor absoluto, diferencia porcentual relativa y z-score en un
    formato integrado que facilita el análisis comparativo detallado.
    
    Args:
        df_data (pl.DataFrame): DataFrame principal con datos de métricas que incluye valores
                               absolutos y diferencias porcentuales organizados por Match Day
        match_day_filter (str): Identificador del Match Day específico para filtrar los datos
                               de la fecha seleccionada
        zscore_data (dict): Diccionario con datos de z-score calculados previamente, organizado
                           por entidad (jugador/equipo/posición) con estructura:
                           {entidad: {'z_scores': {métrica: valor_zscore}}}
        estadistica_seleccionada (str): Tipo de estadística de referencia utilizada para
                                      los cálculos ('mean', 'median', etc.)
    
    Returns:
        list: Lista de diccionarios formateados donde cada diccionario representa una fila
              con valores en formato "valor_absoluto (diferencia_relativa%) [z:zscore]"
              para visualización completa en tabla
    
    Note:
        Esta función proporciona la vista más detallada de los datos, ideal para análisis
        profundos donde se requiere toda la información estadística disponible para
        tomar decisiones informadas sobre el rendimiento.
    """
    formatted_data = []
    
    # Filtrar solo los datos del Match Day específico de la fecha seleccionada
    df_filtered = df_data.filter(pl.col('Match Day') == match_day_filter)
    
    if df_filtered.height == 0:
        return []
    
    df_pandas = df_filtered.to_pandas()
    
    # Obtener datos de diferencia para calcular valores relativos
    df_diff = df_data.filter(pl.col('Match Day') == 'diferencia')
    df_diff_pandas = df_diff.to_pandas() if df_diff.height > 0 else None
    
    for _, row in df_pandas.iterrows():
        formatted_row = {}
        
        # Copiar columnas de identificación
        for col in ['Player', 'Position', 'Team', 'Match Day', 'Estadistica']:
            if col in row:
                formatted_row[col] = row[col]
        
        # Formatear columnas numéricas con valor absoluto, relativo y z-score
        for col in df_pandas.columns:
            if col not in ['Player', 'Position', 'Team', 'Match Day', 'Estadistica']:
                abs_value = row[col]
                
                # Buscar valor relativo correspondiente en df_diff
                rel_value = None
                if df_diff_pandas is not None:
                    # Buscar la fila correspondiente en diferencias
                    mask = True
                    for id_col in ['Player', 'Position', 'Team', 'Estadistica']:
                        if id_col in row and id_col in df_diff_pandas.columns:
                            mask = mask & (df_diff_pandas[id_col] == row[id_col])
                    
                    matching_rows = df_diff_pandas[mask]
                    if len(matching_rows) > 0 and col in matching_rows.columns:
                        rel_value = matching_rows[col].iloc[0]
                
                # Buscar valor de z-score correspondiente
                zscore_value = None
                if zscore_data:
                    # Identificar la entidad (jugador, equipo o posición)
                    entity_key = None
                    if 'Player' in row and row['Player'] in zscore_data:
                        entity_key = row['Player']
                    elif 'Team' in row and row['Team'] in zscore_data:
                        entity_key = row['Team']
                    elif 'Position' in row and row['Position'] in zscore_data:
                        entity_key = row['Position']
                    
                    if entity_key and 'z_scores' in zscore_data[entity_key]:
                        zscore_value = zscore_data[entity_key]['z_scores'].get(col)
                
                # Formatear el valor con absoluto, relativo y z-score
                if abs_value is not None and not pd.isna(abs_value):
                    value_str = f"{abs_value:.1f}"
                    
                    # Añadir valor relativo si existe
                    if rel_value is not None and not pd.isna(rel_value):
                        value_str += f" ({rel_value:.1f}%)"
                    
                    # Añadir z-score si existe
                    if zscore_value is not None and not pd.isna(zscore_value):
                        value_str += f" [z:{zscore_value:.2f}]"
                    
                    formatted_row[col] = value_str
                else:
                    formatted_row[col] = "N/A"
        
        formatted_data.append(formatted_row)
    
    return formatted_data
            
# Las funciones get_sorted_dates y get_latest_date_for_picker ahora están en utils.py
# y se importan automáticamente con 'from utils.utils import *'
    


# ============================================================================
# LAYOUT DE LA PÁGINA
# ============================================================================

layout = html.Div([
    # Store para manter o estado do botão de alternância
    dcc.Store(id='toggle-values-store', data={'show_zscore': True}),
    
    # Título de la página
    html.H2('Session Report', className="page-title"),
    html.Hr(),
    
    # Contenedor principal
    html.Div([
        # Container para selección de fecha y estadística
        html.Div([
            html.H4('Seleccionar Parámetros', className="section-title"),
            html.Div([
                # Input para fecha con botones de navegación
                html.Div([
                    html.Label('Fecha:', className="input-label"),
                    html.Div([
                        html.Button(
                            '-',
                            id='date-minus-btn',
                            className='date-nav-btn date-minus',
                            title='Día anterior'
                        ),
                        dcc.DatePickerSingle(
                            id='date-selector',
                            date=get_latest_date_for_picker(),
                            placeholder='Selecciona una fecha...',
                            display_format='DD/MM/YYYY',
                            className="date-picker"
                        ),
                        html.Button(
                            '+',
                            id='date-plus-btn',
                            className='date-nav-btn date-plus',
                            title='Día siguiente'
                        )
                    ], className="date-input-container")
                ], className="input-item"),
                
                # Input para estadística
                html.Div([
                    html.Label('Estadística:', className="input-label"),
                    dcc.Dropdown(
                        id='statistic-selector',
                        options=[
                            {'label': 'Media', 'value': 'mean'},
                            {'label': 'Mediana', 'value': 'median'},
                            {'label': 'Máximo', 'value': 'max'},
                            {'label': 'Mínimo', 'value': 'min'},
                            {'label': 'Percentil 75', 'value': 'p75'},
                            {'label': 'Percentil 90', 'value': 'p90'},
                            {'label': 'Percentil 95', 'value': 'p95'}
                        ],
                        value='median',
                        placeholder='Selecciona una estadística...',
                        className="statistic-dropdown"
                    )
                ], className="input-item")
            ], className="inputs-row")
        ], className="date-selection-container"),
        
        # Container unificado para información de sesión, tarjetas y tabla de jugadores
        html.Div([
            html.Div(id='session-info-output'),
            # Controles para tarjetas
            html.Div([
                # Dropdown para seleccionar vista de tarjetas
                html.Div([
                    html.Label('Vista de tarjetas:', className="input-label"),
                    dcc.Dropdown(
                        id='cards-view-selector',
                        placeholder='Selecciona vista...',
                        value='Equipo',
                        className="statistic-dropdown",
                        style={'width': '300px'}
                    )
                ], className="input-item", style={'display': 'inline-block', 'margin-right': '100px'}),
                
                # Selector de columnas diff
                html.Div([
                    html.Label('Columnas a mostrar:', className="input-label"),
                    dcc.Dropdown(
                        id='diff-columns-selector',
                        placeholder='Selecciona columnas...',
                        multi=True,
                        className="statistic-dropdown",
                        style={'width': '400px'}
                    )
                ], className="input-item", style={'display': 'inline-block'})
            ], style={'margin-bottom': '10px', 'margin-top': '20px'}),
            html.Div(id='team-diff-cards-output'),
            
            # Botón para alternar entre z-scores y valores relativos
            html.Div([
                html.Button(
                    'Mostrar Valores Relativos (%)',
                    id='toggle-values-btn',
                    className='toggle-btn',
                    style={
                        'backgroundColor': '#007bff',
                        'color': 'white',
                        'border': 'none',
                        'padding': '10px 20px',
                        'borderRadius': '5px',
                        'cursor': 'pointer',
                        'fontSize': '14px',
                        'fontWeight': 'bold',
                        'marginBottom': '15px',
                        'marginTop': '20px'
                    }
                )
            ], style={'textAlign': 'center'}),
            
            html.Div(id='players-table-output'),
            
            # Seção de gráficos
            html.Div([
                html.H4('Gráficos de Análisis', className="section-title", style={'margin-top': '30px'}),
                
                # Primera fila de gráficos - Distance e HSR
                html.Div([
                    html.Div([
                        html.Div([
                            dcc.Graph(id='grafico-distance')
                        ], className="graph-box")
                    ], style={'width': '48%', 'display': 'inline-block', 'margin-right': '2%'}),
                    
                    html.Div([
                        html.Div([
                            dcc.Graph(id='grafico-hsr')
                        ], className="graph-box")
                    ], style={'width': '48%', 'display': 'inline-block'})
                ], style={'margin-bottom': '20px'}),
                
                # Segunda fila de gráficos - ACC, DCC e Velocidad
                html.Div([
                    html.Div([
                        html.Div([
                            dcc.Graph(id='grafico-acc')
                        ], className="graph-box")
                    ], style={'width': '32%', 'display': 'inline-block', 'margin-right': '2%'}),
                    
                    html.Div([
                        html.Div([
                            dcc.Graph(id='grafico-dcc')
                        ], className="graph-box")
                    ], style={'width': '32%', 'display': 'inline-block', 'margin-right': '2%'}),
                    
                    html.Div([
                        html.Div([
                            dcc.Graph(id='grafico-velocidad')
                        ], className="graph-box")
                    ], style={'width': '32%', 'display': 'inline-block'})
                ], style={'margin-bottom': '20px'}),
                
                # Tercera fila de gráficos - Posiciones
                html.Div([
                    html.Div([
                        html.Div([
                            dcc.Graph(id='grafico-posiciones')
                        ], className="graph-box")
                    ], style={'width': '100%'})
                ], style={'margin-bottom': '20px'})
            ], id='graficos-section')
        ], className="session-and-players-container")
    ])
])

# ============================================================================
# CALLBACKS
# ============================================================================

def register_callbacks(app):
    """
    Registra todos los callbacks de la página Session Report.
    
    Esta función central configura toda la interactividad de la página mediante callbacks
    de Dash que manejan la navegación de fechas, actualización de datos, formateo de tablas,
    generación de gráficos y alternancia entre diferentes vistas de datos.
    
    Args:
        app (dash.Dash): Instancia de la aplicación Dash donde se registrarán los callbacks
    
    Callbacks incluidos:
        - Navegación de fechas con botones y configuración del calendario
        - Actualización de información de sesión
        - Alternancia entre visualización de z-scores y valores relativos
        - Actualización de tabla de jugadores con formateo condicional
        - Generación de múltiples gráficos de análisis de rendimiento
        - Gestión de estados de visualización mediante dcc.Store
    
    Note:
        Esta función debe ser llamada durante la inicialización de la aplicación
        para activar toda la funcionalidad interactiva de la página.
    """
    
    # ============================================================================
    # CALLBACKS - Input fecha y metricas
    # ============================================================================

    # Callback unificado para navegación de fechas y configuración del calendario
    @app.callback(
        [Output('date-selector', 'date'),
         Output('date-selector', 'min_date_allowed'),
         Output('date-selector', 'max_date_allowed'),
         Output('date-selector', 'disabled_days')],
        [Input('date-minus-btn', 'n_clicks'),
         Input('date-plus-btn', 'n_clicks'),
         Input('date-selector', 'id')],
        State('date-selector', 'date')
    )
    def manage_date_navigation_and_config(minus_clicks, plus_clicks, selector_id, current_date):
        """
        Gestiona la navegación de fechas y configuración del calendario de forma unificada.
        
        Esta función callback maneja tanto la navegación mediante botones como la configuración
        inicial del calendario, asegurando que solo se muestren fechas con datos disponibles
        y permitiendo navegación secuencial entre fechas válidas.
        
        Args:
            minus_clicks (int): Número de clics en el botón de fecha anterior
            plus_clicks (int): Número de clics en el botón de fecha siguiente  
            selector_id (str): ID del selector de fecha (usado para inicialización)
            current_date (str): Fecha actualmente seleccionada en formato YYYY-MM-DD
        
        Returns:
            tuple: (nueva_fecha, fecha_mínima, fecha_máxima, días_deshabilitados)
                  - nueva_fecha: Fecha seleccionada en formato YYYY-MM-DD
                  - fecha_mínima/máxima: Límites del calendario
                  - días_deshabilitados: Lista de fechas sin datos (vacía en este caso)
        """
        ctx = dash.callback_context
        
        # Obtener fechas ordenadas cronológicamente en formato dd/mm/aaaa
        fechas_ordenadas = get_sorted_dates()
        
        if not fechas_ordenadas:
            return dash.no_update, None, None, []
        
        # Configurar límites del calendario (convertir a datetime.date para el DatePickerSingle)
        try:
            min_date = datetime.strptime(fechas_ordenadas[0], '%d/%m/%Y').date()
            max_date = datetime.strptime(fechas_ordenadas[-1], '%d/%m/%Y').date()
        except:
            min_date = max_date = None
        
        # Si no hay trigger (inicialización de la página), retornar la fecha más reciente
        if not ctx.triggered:
            # Convertir la fecha más reciente (última en la lista) a formato YYYY-MM-DD
            try:
                latest_date_dt = datetime.strptime(fechas_ordenadas[-1], '%d/%m/%Y')
                latest_date_formatted = latest_date_dt.strftime('%Y-%m-%d')
            except:
                latest_date_formatted = fechas_ordenadas[-1]
            return latest_date_formatted, min_date, max_date, []
        
        # Si es solo inicialización del selector, solo configurar calendario
        if ctx.triggered[0]['prop_id'] == 'date-selector.id':
            return dash.no_update, min_date, max_date, []
        
        # Manejar navegación con botones
        if not current_date:
            # Si no hay fecha actual, retornar la primera fecha disponible
            return fechas_ordenadas[0], min_date, max_date, []
        
        # Convertir current_date (formato aaaa-mm-dd del DatePickerSingle) a dd/mm/aaaa
        try:
            if isinstance(current_date, str):
                if '-' in current_date:
                    current_dt = datetime.strptime(current_date, '%Y-%m-%d')
                    current_date_formatted = current_dt.strftime('%d/%m/%Y')
                else:
                    current_date_formatted = current_date
            else:
                current_date_formatted = current_date.strftime('%d/%m/%Y')
        except:
            current_date_formatted = fechas_ordenadas[0]
        
        # Encontrar índice de la fecha actual en la lista ordenada
        try:
            current_index = fechas_ordenadas.index(current_date_formatted)
        except ValueError:
            # Si la fecha actual no está en la lista, buscar la más cercana
            try:
                current_dt = datetime.strptime(current_date_formatted, '%d/%m/%Y')
                fechas_dt = [datetime.strptime(f, '%d/%m/%Y') for f in fechas_ordenadas]
                closest_index = min(range(len(fechas_dt)), key=lambda i: abs((fechas_dt[i] - current_dt).days))
                current_index = closest_index
            except:
                current_index = 0
        
        # Determinar qué botón fue presionado y navegar
        button_id = ctx.triggered[0]['prop_id'].split('.')[0]
        
        if button_id == 'date-minus-btn' and current_index > 0:
            # Navegar a la fecha anterior
            new_date = fechas_ordenadas[current_index - 1]
        elif button_id == 'date-plus-btn' and current_index < len(fechas_ordenadas) - 1:
            # Navegar a la fecha siguiente
            new_date = fechas_ordenadas[current_index + 1]
        else:
            # No se puede navegar más o no hay cambio
            new_date = fechas_ordenadas[current_index]
        
        # Convertir la nueva fecha de dd/mm/aaaa a aaaa-mm-dd para el DatePickerSingle
        try:
            new_date_dt = datetime.strptime(new_date, '%d/%m/%Y')
            new_date_formatted = new_date_dt.strftime('%Y-%m-%d')
        except:
            new_date_formatted = new_date
        
        return new_date_formatted, min_date, max_date, []
    
    
    # ============================================================================
    # CALLBACKS - Información de Sesión
    # ============================================================================
    
    @app.callback(
        Output('session-info-output', 'children'),
        [Input('date-selector', 'date'),
         Input('statistic-selector', 'value')]
    )
    def update_session_info(selected_date, selected_statistic):
        """
        Actualiza la información de la sesión basada en la fecha y estadística seleccionadas.
        
        Esta función callback genera un resumen de la sesión de entrenamiento incluyendo
        número de jugadores, duración total, match days disponibles y la estadística
        actualmente seleccionada para análisis.
        
        Args:
            selected_date (str): Fecha seleccionada en formato YYYY-MM-DD
            selected_statistic (str): Código de la estadística seleccionada 
                                    ('mean', 'median', 'max', 'min', 'p75', 'p90', 'p95')
        
        Returns:
            html.Div: Componente HTML con la información de la sesión formateada,
                     incluyendo título, detalles de la sesión y estadística seleccionada
        
        Note:
            Maneja errores de datos faltantes y muestra mensajes informativos apropiados
        """
        if not selected_date:
            return html.Div("Selecciona una fecha para ver la información de la sesión.", 
                          className="info-message")
        
        try:
            path_to_parquet = os.path.join(DATA_GPS_PATH, 'df_gps.parquet')
            if not os.path.exists(path_to_parquet):
                return html.Div("No se encontró el archivo de datos.", 
                              className="error-message")
            
            df = pl.read_parquet(path_to_parquet)
            
            result = format_and_filter_date(selected_date)
            if result is None or result[0] is None:
                return html.Div(f"No se encontraron datos para la fecha {selected_date}.", 
                              className="warning-message")
            
            df_fecha, formatted_date = result
            
            if df_fecha.height == 0:
                return html.Div(f"No se encontraron datos para la fecha {selected_date}.", 
                              className="warning-message")
            
            
            # Obtener información básica de la sesión
            num_jugadores = df_fecha.select('Player').n_unique()
            
            # Calcular duración total de drills si la columna existe
            if 'Drills Duration' in df_fecha.columns:
                duration = df_fecha.select('Drills Duration').row(0)[0]
            
            # Filtrar Match Days excluyendo 'Rehab'
            match_days_filtered = df_fecha.filter(pl.col('Match Day') != 'Rehab').select('Match Day').unique().to_series().to_list()
            
            # Crear información base
            session_info = [
                html.H5(f"Información de la Sesión - {formatted_date}", className="session-title"),
                html.P(f"Número de jugadores: {num_jugadores}", className="session-detail"),
                html.P(f"Duración total: {duration}", className="session-detail"),
                html.P(f"Match Days: {', '.join(match_days_filtered)}", className="session-detail")
            ]
            
            # Agregar información de estadística si está seleccionada
            if selected_statistic:
                statistic_labels = {
                    'mean': 'Media',
                    'median': 'Mediana', 
                    'max': 'Máximo',
                    'min': 'Mínimo',
                    'p75': 'Percentil 75',
                    'p90': 'Percentil 90',
                    'p95': 'Percentil 95'
                }
                statistic_name = statistic_labels.get(selected_statistic, selected_statistic)
                session_info.append(
                    html.P(f"Estadística seleccionada: {statistic_name}", 
                          className="session-detail statistic-selected")
                )
            
            return html.Div(session_info, className="session-info-card")
            
        except Exception as e:
            return html.Div(f"Error al cargar información de la sesión: {str(e)}", 
                          className="error-message")
            
    # ============================================================================
    # CALLBACKS - Tabla
    # ============================================================================

    # Callback para gerenciar o estado do botão de alternância
    @app.callback(
        [Output('toggle-values-store', 'data'),
         Output('toggle-values-btn', 'children'),
         Output('toggle-values-btn', 'style')],
        [Input('toggle-values-btn', 'n_clicks')],
        [State('toggle-values-store', 'data')]
    )
    def toggle_values_display(n_clicks, current_state):
        """
        Alterna entre mostrar z-scores y valores relativos en la tabla.
        
        Esta función callback gestiona el estado del botón de alternancia que permite
        cambiar entre la visualización de z-scores (valores estandarizados) y valores
        relativos (porcentajes) en la tabla de datos de rendimiento.
        
        Args:
            n_clicks (int): Número de clics en el botón de alternancia
            current_state (dict): Estado actual del botón con clave 'show_zscore'
        
        Returns:
            tuple: (nuevo_estado, texto_botón, estilo_botón)
                  - nuevo_estado: Diccionario con 'show_zscore' (bool)
                  - texto_botón: Texto a mostrar en el botón
                  - estilo_botón: Diccionario con estilos CSS del botón
        
        Note:
            El estado inicial muestra z-scores por defecto
        """
        if n_clicks is None:
            # Estado inicial
            return {'show_zscore': True}, 'Mostrar Valores Relativos (%)', {
                'backgroundColor': '#007bff',
                'color': 'white',
                'border': 'none',
                'padding': '10px 20px',
                'borderRadius': '5px',
                'cursor': 'pointer',
                'fontSize': '14px',
                'fontWeight': 'bold',
                'marginBottom': '15px',
                'marginTop': '20px'
            }
        
        # Alternar estado
        new_show_zscore = not current_state.get('show_zscore', True)
        
        if new_show_zscore:
            # Mostrando z-scores, botão para alternar para relativos
            button_text = 'Mostrar Valores Relativos (%)'
            button_color = '#007bff'
        else:
            # Mostrando relativos, botão para alternar para z-scores
            button_text = 'Mostrar Z-Scores'
            button_color = '#28a745'
        
        button_style = {
            'backgroundColor': button_color,
            'color': 'white',
            'border': 'none',
            'padding': '10px 20px',
            'borderRadius': '5px',
            'cursor': 'pointer',
            'fontSize': '14px',
            'fontWeight': 'bold',
            'marginBottom': '15px',
            'marginTop': '20px'
        }
        
        return {'show_zscore': new_show_zscore}, button_text, button_style

    
    @app.callback(
        Output('players-table-output', 'children'),
        [Input('date-selector', 'date'),
         Input('statistic-selector', 'value'),
         Input('toggle-values-store', 'data')]
    )
    def update_players_table(selected_date, selected_statistic, toggle_state):
        """
        Actualiza la tabla de jugadores con datos de jugadores, equipos y posiciones.
        
        Esta función callback genera una tabla unificada que combina datos de rendimiento
        de jugadores individuales, equipos y posiciones, aplicando la formatación apropiada
        según el estado del botón de alternancia (z-scores o valores relativos).
        
        Args:
            selected_date (str): Fecha seleccionada en formato YYYY-MM-DD
            selected_statistic (str): Estadística seleccionada para el análisis
            toggle_state (dict): Estado del botón de alternancia con clave 'show_zscore'
        
        Returns:
            html.Div: Componente HTML con la tabla de datos formateada, incluyendo:
                     - Datos de jugadores individuales
                     - Datos agregados por equipo
                     - Datos agregados por posición
                     - Mapa de calor basado en z-scores
                     - Información de resumen de la tabla
        
        Note:
            La tabla siempre usa z-scores para el mapa de calor, independientemente
            del modo de visualización seleccionado
        """
        
        if not selected_date:
            return html.Div("Selecciona una fecha para ver los datos de los jugadores.", 
                          className="info-message")
        
        if not selected_statistic:
            return html.Div("Selecciona una estadística para ver los datos.", 
                          className="info-message")
        
        # Determinar qué tipo de formatação usar baseado no estado do botão
        show_zscore = toggle_state.get('show_zscore', True) if toggle_state else True
        
        try:
            # Convertir fecha al formato correcto para calcular_estadisticas
            result = format_and_filter_date(selected_date)
            if result is None or result[0] is None:
                return html.Div(f"No se encontraron datos para la fecha {selected_date}.", 
                              className="warning-message")
            
            df_fecha, formatted_date = result

            # Obtener columnas de interés y calcular métricas por minuto
            session_minutes = df_fecha.select('Drills Duration').row(0)[0]
            df_inutil, columnas_interes = add_per_minute_metrics(df_fecha, session_minutes)
            
            # Obtener datos individuales de jugadores para la fecha seleccionada
            df_individual_players = filter_and_get_players_data(formatted_date)
            print(df_individual_players)
            
            # Calcular estadísticas para la fecha y estadística seleccionadas
            df_players, df_position, df_team = calcular_estadisticas( fecha=formatted_date, estadistica=selected_statistic)
            
            # Obtener el Match Day específico de la fecha seleccionada
            match_day_especifico = df_fecha['Match Day'][0] if df_fecha.height > 0 else None
            
            # Calcular z-scores usando la nueva función
            zscore_data_players = None
            zscore_data_team = None
            zscore_data_position = None
            
            try:
                # Calcular z-scores para jugadores
                if df_players is not None and df_players.height > 0:
                    zscore_data_players = calcular_comparacion_fecha_md(df_players, selected_statistic, columnas_interes)
                
                # Calcular z-scores para equipos
                if df_team is not None and df_team.height > 0:
                    zscore_data_team = calcular_comparacion_fecha_md(df_team, selected_statistic, columnas_interes)
                
                # Calcular z-scores para posiciones
                if df_position is not None and df_position.height > 0:
                    zscore_data_position = calcular_comparacion_fecha_md(df_position, selected_statistic, columnas_interes)
                    
            except Exception as e:
                print(f"Error al calcular z-scores: {e}")
            
            # Crear dataframe combinado con todos los datos
            combined_data_all = []
            combined_info = []
            
            # Agregar datos individuales de jugadores (solo del Match Day específico)
            if df_players is not None and df_players.height > 0 and match_day_especifico:
                if show_zscore:
                    # Formatear solo con valores absolutos y z-scores (sin valores relativos)
                    formatted_players_data = format_value_with_zscore_only(
                        df_players, match_day_especifico, zscore_data_players, selected_statistic
                    )
                else:
                    # Formatear solo con valores relativos
                    formatted_players_data = format_value_with_relative(
                        df_players, match_day_especifico
                    )
                
                for record in formatted_players_data:
                    # Renombrar columna Player a Player/Team/Position
                    if 'Player' in record:
                        record['Player/Team/Position'] = record.pop('Player')
                    
                    # Filtrar solo columnas de interés y Player/Team/Position
                    available_columns = ['Player/Team/Position'] + [col for col in columnas_interes if col in record]
                    filtered_record = {col: record.get(col, '') for col in available_columns}
                    
                    # Agregar identificador interno para estilos
                    filtered_record['_tipo_interno'] = 'JugadorIndividual'
                    
                    combined_data_all.append(filtered_record)
                
                combined_info.append(f"Jugadores: {len(formatted_players_data)} registros")
            
            # Agregar datos de equipos (solo del Match Day específico)
            if df_team is not None and df_team.height > 0 and match_day_especifico:
                if show_zscore:
                    # Formatear solo con valores absolutos y z-scores (sin valores relativos)
                    formatted_team_data = format_value_with_zscore_only(
                        df_team, match_day_especifico, zscore_data_team, selected_statistic
                    )
                else:
                    # Formatear solo con valores relativos
                    formatted_team_data = format_value_with_relative(
                        df_team, match_day_especifico
                    )
                
                for record in formatted_team_data:
                    # Renombrar columna Team a Player/Team/Position
                    if 'Team' in record:
                        record['Player/Team/Position'] = record.pop('Team')
                    
                    # Filtrar solo columnas de interés y Player/Team/Position
                    available_columns = ['Player/Team/Position'] + [col for col in columnas_interes if col in record]
                    filtered_record = {col: record.get(col, '') for col in available_columns}
                    
                    # Agregar identificador interno para estilos
                    filtered_record['_tipo_interno'] = 'Equipo'
                    
                    combined_data_all.append(filtered_record)
                
                combined_info.append(f"Equipos: {len(formatted_team_data)} registros")
            
            # Agregar datos de posiciones (solo del Match Day específico)
            if df_position is not None and df_position.height > 0 and match_day_especifico:
                if show_zscore:
                    # Formatear solo con valores absolutos y z-scores (sin valores relativos)
                    formatted_position_data = format_value_with_zscore_only(
                        df_position, match_day_especifico, zscore_data_position, selected_statistic
                    )
                else:
                    # Formatear solo con valores relativos
                    formatted_position_data = format_value_with_relative(
                        df_position, match_day_especifico
                    )
                
                for record in formatted_position_data:
                    # Renombrar columna Position a Player/Team/Position
                    if 'Position' in record:
                        record['Player/Team/Position'] = record.pop('Position')
                    
                    # Filtrar solo columnas de interés y Player/Team/Position
                    available_columns = ['Player/Team/Position'] + [col for col in columnas_interes if col in record]
                    filtered_record = {col: record.get(col, '') for col in available_columns}
                    
                    # Agregar identificador interno para estilos
                    filtered_record['_tipo_interno'] = 'Posición'
                    
                    combined_data_all.append(filtered_record)
                
                combined_info.append(f"Posiciones: {len(formatted_position_data)} registros")
            
            # Crear tabla única con todos los dados
            if combined_data_all:
                # Ordenar los datos alfabéticamente por tipo y nombre
                def get_sort_key(record):
                    """
                    Función para ordenar los registros por tipo y luego alfabéticamente
                    Orden: Equipo, Posiciones (alfabético), Jugadores (alfabético)
                    """
                    tipo = record.get('_tipo_interno', '')
                    nombre = record.get('Player/Team/Position', '')
                    
                    # Asignar prioridad por tipo
                    if tipo == 'Equipo':
                        return (1, nombre)  # Equipos primero
                    elif tipo == 'Posición':
                        return (2, nombre)  # Posiciones segundo
                    elif tipo == 'JugadorIndividual':
                        return (3, nombre)  # Jugadores último
                    else:
                        return (4, nombre)  # Otros al final
                
                # Ordenar los datos combinados
                combined_data_all.sort(key=get_sort_key)
                # Obtener todas las columnas únicas
                all_columns = set()
                for record in combined_data_all:
                    all_columns.update(record.keys())
                
                # Reorganizar columnas para que 'Player/Team/Position' esté al principio, excluyendo el identificador interno
                columns_order = ['Player/Team/Position'] + [col for col in sorted(all_columns) if col not in ['Player/Team/Position', '_tipo_interno']]
                
                # Completar registros faltantes con valores vacíos
                for record in combined_data_all:
                    for col in columns_order:
                        if col not in record:
                            record[col] = 'NULL'
                
                # Todas las columnas serán tratadas como texto ya que contienen formato "valor (porcentaje%)"
                numeric_columns_combined = []
                
                # Filtrar datos para mostrar solo las columnas visibles
                filtered_data_all = []
                for record in combined_data_all:
                    filtered_record = {col: record.get(col, '') for col in columns_order}
                    
                    # Mantener _tipo_interno para estilos pero no mostrarlo
                    filtered_record['_tipo_interno'] = record.get('_tipo_interno', '')
                    filtered_data_all.append(filtered_record)
                
                # Sempre aplicar o mapa de calor baseado em z-scores
                # Para isso, precisamos criar dados temporários com z-scores para calcular os estilos
                temp_data_for_heatmap = []
                
                # Recriar dados com z-scores para o mapa de calor
                for record in combined_data_all:
                    temp_record = record.copy()
                    
                    # Se não estamos mostrando z-scores, precisamos adicioná-los temporariamente para o mapa de calor
                    if not show_zscore:
                        entity_type = record.get('_tipo_interno', '')
                        entity_name = record.get('Player/Team/Position', '')
                        
                        # Determinar qual zscore_data usar
                        zscore_data_to_use = None
                        if entity_type == 'JugadorIndividual' and zscore_data_players:
                            zscore_data_to_use = zscore_data_players
                        elif entity_type == 'Equipo' and zscore_data_team:
                            zscore_data_to_use = zscore_data_team
                        elif entity_type == 'Posición' and zscore_data_position:
                            zscore_data_to_use = zscore_data_position
                        
                        # Adicionar z-scores temporariamente para o cálculo do mapa de calor
                        if zscore_data_to_use and entity_name in zscore_data_to_use:
                            z_scores = zscore_data_to_use[entity_name].get('z_scores', {})
                            for col in columns_order:
                                if col != 'Player/Team/Position' and col in temp_record:
                                    current_value = temp_record[col]
                                    if col in z_scores and z_scores[col] is not None:
                                        # Adicionar z-score temporariamente para o mapa de calor
                                        temp_record[col] = f"{current_value} [z:{z_scores[col]:.2f}]"
                    
                    temp_data_for_heatmap.append(temp_record)
                
                # Criar estilos de mapa de calor sempre baseados em z-scores
                heatmap_styles = create_zscore_heatmap_styles(temp_data_for_heatmap, columns_order)
                
                # Crear estilos de tabla con mapa de calor
                table_styles_with_heatmap = {
                    'style_table': {
                        'overflowX': 'auto',
                        'maxHeight': '600px',
                        'overflowY': 'auto'
                    },
                    'style_cell': {
                        'textAlign': 'left',
                        'padding': '8px',
                        'fontFamily': 'Arial, sans-serif',
                        'fontSize': '13px',
                        'border': '1px solid #ddd'
                    },
                    'style_header': {
                        'backgroundColor': '#e8f4fd',
                        'fontWeight': 'bold',
                        'textAlign': 'center',
                        'border': '1px solid #ddd'
                    },
                    'style_data': {
                        'backgroundColor': 'white',
                        'border': '1px solid #ddd'
                    },
                    'style_data_conditional': heatmap_styles
                }
                
                combined_table = dash_table.DataTable(
                    id='combined-all-stats-table',
                    data=filtered_data_all,
                    columns=[
                        {"name": col, "id": col, "type": "numeric" if col in numeric_columns_combined else "text"}
                        for col in columns_order
                    ],
                    **table_styles_with_heatmap,
                    sort_action="native",
                    filter_action="native",
                    page_action="none",
                    export_format="xlsx",
                    export_headers="display"
                )
                
                # Crear información de resumen
                combined_info_text = ' | '.join(combined_info)
                
                return html.Div([
                    html.H5('Datos Combinados - Jugadores, Equipos y Posiciones', className="section-subtitle"),
                    html.P(f"Mostrando {combined_info_text}", className="table-info"),
                    combined_table
                ], className="combined-stats-table-container")
            else:
                return html.Div("No se encontraron datos para la fecha y estadística seleccionadas.", 
                              className="warning-message")
            
        except Exception as e:
            return html.Div(f"Error al cargar tabla de jugadores: {str(e)}", 
                          className="error-message")
    
    # ============================================================================
    # CALLBACKS - Tarjetas
    # ============================================================================
    

    # Callback para popular el dropdown de vista de tarjetas
    @app.callback(
        Output('cards-view-selector', 'options'),
        [Input('date-selector', 'date')]
    )
    def update_cards_view_options(selected_date):
        """
        Actualiza las opciones del dropdown de vista de tarjetas basado en la fecha seleccionada.
        
        Esta función callback genera dinámicamente las opciones disponibles para el selector
        de vista de tarjetas, incluyendo equipos, posiciones y jugadores individuales
        basándose en los datos disponibles para la fecha seleccionada.
        
        Args:
            selected_date (str): Fecha seleccionada en formato YYYY-MM-DD
        
        Returns:
            list: Lista de diccionarios con opciones del dropdown, cada uno con:
                 - 'label': Texto a mostrar en el dropdown
                 - 'value': Valor interno para identificar la selección
                 Incluye 'Equipo', posiciones (Position_X) y jugadores (Player_X)
        
        Note:
            Siempre incluye la opción 'Equipo' como predeterminada, incluso si no hay datos
        """
        if not selected_date:
            return [{'label': 'Equipo', 'value': 'Equipo'}]
        
        try:
            result = format_and_filter_date(selected_date)
            if result is None or result[0] is None:
                return [{'label': 'Equipo', 'value': 'Equipo'}]
            
            df_fecha, formatted_date = result
            
            if df_fecha.height == 0:
                return [{'label': 'Equipo', 'value': 'Equipo'}]
            
            # Obtener valores únicos de jugadores y posiciones
            players = sorted(df_fecha.select('Player').unique().to_series().to_list())
            positions = sorted(df_fecha.select('Position').unique().to_series().to_list())
            
            # Crear opciones del dropdown
            options = [{'label': 'Equipo', 'value': 'Equipo'}]
            
            # Añadir posiciones
            for position in positions:
                options.append({'label': f'{position}', 'value': f'Position_{position}'})
            
            # Añadir jugadores
            for player in players:
                options.append({'label': f'{player}', 'value': f'Player_{player}'})
            
            return options
            
        except Exception as e:
            print(f"Error al cargar opciones del dropdown: {e}")
            return [{'label': 'Equipo', 'value': 'Equipo'}]
    
    # Callback para popular el dropdown de columnas diff
    @app.callback(
        Output('diff-columns-selector', 'options'),
        [Input('date-selector', 'date'),
         Input('statistic-selector', 'value'),
         Input('cards-view-selector', 'value')]
    )
    def update_diff_columns_options(selected_date, selected_statistic, selected_view):
        """
        Actualiza las opciones del dropdown de columnas diff basado en la vista seleccionada.
        
        Esta función callback genera las opciones disponibles para el selector de columnas
        de diferencias, basándose en las métricas disponibles en los datos de diferencias
        porcentuales para la vista seleccionada (equipo, posición o jugador).
        
        Args:
            selected_date (str): Fecha seleccionada en formato YYYY-MM-DD
            selected_statistic (str): Estadística seleccionada para el análisis
            selected_view (str): Vista seleccionada ('Equipo', 'Position_X', 'Player_X')
        
        Returns:
            list: Lista de diccionarios con opciones del dropdown, cada uno con:
                 - 'label': Nombre de la métrica
                 - 'value': Nombre de la columna para uso interno
                 Solo incluye métricas de rendimiento, excluyendo columnas de identificación
        
        Note:
            Filtra automáticamente las filas con Match Day = 'diferencia' para obtener
            solo las métricas de comparación porcentual
        """
        if not selected_date or not selected_statistic or not selected_view:
            return []
        
        try:
            # Convertir fecha al formato correcto
            result = format_and_filter_date(selected_date)
            if result is None or result[0] is None:
                return []
            
            df_fecha, formatted_date = result

            
            # Calcular estadísticas para obtener las columnas diff
            df_players, df_position, df_team = calcular_estadisticas(
                fecha=formatted_date,  
                estadistica=selected_statistic
            )
            
            # Determinar qué dataframe usar basado en selected_view
            if selected_view == 'Equipo':
                df_to_use = df_team
            elif selected_view and selected_view.startswith('Position_'):
                df_to_use = df_position
            elif selected_view and selected_view.startswith('Player_'):
                df_to_use = df_players
            else:
                df_to_use = df_team
            
            if df_to_use is None or df_to_use.height == 0:
                return []
            
            # Filtrar filas con Match Day = 'diferencia' para obtener las métricas disponibles
            df_diferencias = df_to_use.filter(pl.col('Match Day') == 'diferencia')
            
            if df_diferencias.height == 0:
                return []
            
            df_pandas = df_diferencias.to_pandas()
            
            # Obtener columnas de métricas (excluyendo columnas de identificación)
            exclude_cols = ['Player', 'Position', 'Team', 'Match Day', 'Estadistica']
            metric_columns = [col for col in df_pandas.columns if col not in exclude_cols]
            
            # Crear opciones del dropdown
            options = []
            for col in sorted(metric_columns):
                options.append({'label': col, 'value': col})
            
            return options
            
        except Exception as e:
            print(f"Error al cargar opciones de columnas diff: {e}")
            return []
    
    
    # Callback para actualizar las tarjetas de diferencias
    @app.callback(
        Output('team-diff-cards-output', 'children'),
        [Input('date-selector', 'date'),
         Input('statistic-selector', 'value'),
         Input('cards-view-selector', 'value'),
         Input('diff-columns-selector', 'value')]
    )
    def update_team_diff_cards(selected_date, selected_statistic, selected_view, selected_columns):
        """
        Crea tarjetas mostrando las diferencias porcentuales para la vista seleccionada.
        
        Esta función callback genera tarjetas visuales que muestran las diferencias porcentuales
        entre el rendimiento actual y el histórico para equipos, posiciones o jugadores
        individuales, basándose en las columnas de métricas seleccionadas.
        
        Args:
            selected_date (str): Fecha seleccionada en formato YYYY-MM-DD
            selected_statistic (str): Estadística seleccionada para el análisis
            selected_view (str): Vista seleccionada ('Equipo', 'Position_X', 'Player_X')
            selected_columns (list): Lista de columnas de métricas a mostrar en las tarjetas
        
        Returns:
            html.Div: Componente HTML con tarjetas de diferencias que incluyen:
                     - Título descriptivo de la vista
                     - Tarjetas individuales para cada métrica seleccionada
                     - Valores de diferencia porcentual con codificación de colores
                     - Información contextual sobre el rendimiento
        
        Note:
            Las tarjetas muestran diferencias porcentuales con colores indicativos:
            verde para mejoras, rojo para decrementos, según la métrica
        """
        
        if not selected_date or not selected_statistic:
            return html.Div()
        
        try:
            # Convertir fecha al formato correcto para calcular_estadisticas
            result = format_and_filter_date(selected_date)
            if result is None or result[0] is None:
                return html.Div()
            
            df_fecha, formatted_date = result

            # Obtener columnas de interés y calcular métricas por minuto
            session_minutes = df_fecha.select('Drills Duration').row(0)[0]
            df_inutil, columnas_interes = add_per_minute_metrics(df_fecha, session_minutes)
            
             # Cargar datos originales del archivo parquet
            try:
                
                        match_day_value = df_fecha['Match Day'].unique().to_list()[0]
                        #print(match_day_value)
                        
                        # Ahora cargar y filtrar df_team_estadisticas
                        df_team_estadisticas_path = os.path.join(DATA_GPS_PATH, '../processed/df_team_estadisticas.parquet')
                        if os.path.exists(df_team_estadisticas_path):
                            df_team_estadisticas = pl.read_parquet(df_team_estadisticas_path)
                            # Filtrar datos originales por el Match Day correcto y Estadística
                            df_team_estadisticas_filtered = df_team_estadisticas.filter(
                                (pl.col('Match Day') == match_day_value) & 
                                (pl.col('Estadistica') == selected_statistic)
                            )          
                        else:
                            df_team_estadisticas_filtered = None

            except Exception as e:
                print(f"Error cargando archivo original: {e}")
                df_team_estadisticas_filtered = None
            
            # print(df_team_estadisticas_filtered)
            # print("df_team_estadisticas_filtered.height = ", df_team_estadisticas_filtered.height )
            
            # Calcular estadísticas para la fecha y estadística seleccionadas
            df_players, df_position, df_team = calcular_estadisticas(
                fecha=formatted_date, 
                columnas_interes=columnas_interes, 
                estadistica=selected_statistic
            )
            
            # Determinar qué dataframe usar basado en selected_view
            if selected_view == 'Equipo':
                df_to_use = df_team
                title_prefix = 'Diferencias Porcentuales del Equipo'
            elif selected_view and selected_view.startswith('Position_'):
                # Extraer la posición del valor selected_view
                position = selected_view.replace('Position_', '')
                if df_position is None or df_position.height == 0:
                    return html.Div()
                # Filtrar por posición
                df_to_use = df_position.filter(pl.col('Position') == position)
                title_prefix = f'Diferencias Porcentuales - Posición: {position}'
            elif selected_view and selected_view.startswith('Player_'):
                # Extraer el nombre del jugador del valor selected_view
                player = selected_view.replace('Player_', '')
                if df_players is None or df_players.height == 0:
                    return html.Div()
                # Filtrar por jugador
                df_to_use = df_players.filter(pl.col('Player') == player)
                title_prefix = f'Diferencias Porcentuales - Jugador: {player}'
            else:
                # Fallback a equipo
                df_to_use = df_team
                title_prefix = 'Diferencias Porcentuales del Equipo'
            
            if df_to_use is None or df_to_use.height == 0:
                return html.Div()
            
            # Filtrar filas con Match Day = 'diferencia'
            df_diferencias = df_to_use.filter(pl.col('Match Day') == 'diferencia')
            
            if df_diferencias.height == 0:
                return html.Div("No hay datos de diferencias disponibles para esta selección.", 
                              className="info-message")
            
            # Convertir a pandas para facilitar el manejo
            df_pandas = df_diferencias.to_pandas()
            
            # Obtener columnas de métricas (excluyendo columnas de identificación)
            exclude_cols = ['Player', 'Position', 'Team', 'Match Day', 'Estadistica']
            all_metric_columns = [col for col in df_pandas.columns if col not in exclude_cols]
            
            # Filtrar por las columnas seleccionadas por el usuario
            if selected_columns:
                # Si hay columnas seleccionadas, usar solo esas
                metric_columns = [col for col in selected_columns if col in all_metric_columns]
            else:
                # Si no hay columnas seleccionadas, mostrar todas
                metric_columns = all_metric_columns
            
            if not metric_columns:
                return html.Div("Selecciona al menos una métrica para mostrar.", 
                              className="info-message")
            
            # Crear tarjetas para cada columna diff
            cards = []
            
            # Título de la sección
            cards.append(
                html.H5(title_prefix, 
                        className="section-subtitle", 
                        style={'margin-top': '10px', 'margin-bottom': '20px'})
            )
            
            # Contenedor para las tarjetas
            cards_container = []
            
            for col in metric_columns:
                # Obtener el valor de la diferencia porcentual
                if len(df_pandas) > 0:
                    diff_value = df_pandas[col].iloc[0]
                    
                    # Formatear valor de la diferencia porcentual
                    if pd.isna(diff_value):
                        formatted_diff_value = 'N/A'
                    else:
                        formatted_diff_value = f'{diff_value:.2f}%'
                     
                    # Crear tarjeta usando estilos CSS definidos
                    card_content = [
                         # Título da métrica usando classe card-title
                         html.H6(col, className="card-title"),
                         
                         # Container de valores usando classe card-values-container
                         html.Div([
                             html.Span("Diferencia con respecto al partido", className="session-label"),
                             html.Span(formatted_diff_value, className="stat-value")
                         ], className="card-values-container")
                     ]
                     
                    card = html.Div(card_content, className="metric-card")
                    
                    cards_container.append(card)
            
            # Contenedor usando estilos CSS definidos
            if cards_container:
                 cards.append(
                     html.Div(cards_container, className="metrics-cards-container")
                 )
            
            return html.Div(cards)
            
        except Exception as e:
            return html.Div(f"Error al cargar cartões de diferencias: {str(e)}", 
                          className="error-message")
            
            
    # ============================================================================
    # CALLBACKS - Graficos
    # ============================================================================
    @app.callback(
        [Output('grafico-distance', 'figure'),
         Output('grafico-hsr', 'figure'),
         Output('grafico-acc', 'figure'),
         Output('grafico-dcc', 'figure'),
         Output('grafico-velocidad', 'figure'),
         Output('grafico-posiciones', 'figure')],
        [Input('date-selector', 'date'),
         Input('statistic-selector', 'value')]
    )
    def update_graficos(selected_date, selected_statistic):
        """
        Actualiza todos los gráficos basados en la fecha y estadística seleccionadas.
        
        Esta función callback genera seis gráficos de rendimiento deportivo que visualizan
        diferentes métricas de los jugadores para la fecha seleccionada, incluyendo
        distancias por zonas de velocidad, HSR, aceleraciones, desaceleraciones,
        velocidad máxima y comparaciones por posición.
        
        Args:
            selected_date (str): Fecha seleccionada en formato YYYY-MM-DD
            selected_statistic (str): Estadística seleccionada para el análisis
        
        Returns:
            tuple: Seis figuras de Plotly para los gráficos:
                  - fig_distance: Gráfico de barras apiladas de distancia por zonas de velocidad
                  - fig_hsr: Gráfico de barras horizontales de distancia en alta velocidad
                  - fig_acc: Gráfico de barras de aceleraciones por zona de intensidad
                  - fig_dcc: Gráfico de barras de desaceleraciones por zona de intensidad
                  - fig_velocidad: Gráfico de barras de velocidad máxima por jugador
                  - fig_posiciones: Gráfico de barras agrupadas de métricas por posición
        
        Note:
            Todos los gráficos incluyen líneas de promedio donde es relevante y
            utilizan esquemas de colores consistentes para facilitar la interpretación
        """
        
        # Figura vacía para casos de error
        empty_fig = {
            'data': [],
            'layout': {
                'title': 'Sin datos disponibles',
                'plot_bgcolor': '#f8f9fa',
                'paper_bgcolor': '#f8f9fa',
                'font': {'color': '#000000'},
                'margin': dict(t=40, b=40, l=40, r=40)
            }
        }
        
        try:
            if not selected_date:
                return [empty_fig] * 6
            
            # Obtener datos filtrados usando la función auxiliar existente
            path_to_parquet = os.path.join(DATA_GPS_PATH, 'df_gps.parquet')
            if not os.path.exists(path_to_parquet):
                print(f"Archivo parquet no existe: {path_to_parquet}")
                return None
        
            df = pl.read_parquet(path_to_parquet)
            
            result = format_and_filter_date(selected_date)
            if result is None or result[0] is None:
                return [empty_fig] * 6
            
            df_fecha, formatted_date = result
            
            if df_fecha.height == 0:
                return [empty_fig] * 6
            
            # Convertir a pandas para facilitar manipulación
            df_pandas = df_fecha.to_pandas()

            # Verificar si tenemos las columnas necesarias
            required_columns = [
                'Player',
                'Speed Zones (m) [0.0, 45.0]% (m)',
                'Speed Zones (m) [45.0, 65.0]% (m)',
                'Speed Zones (m) [65.0, 75.0]% (m)',
                'Speed Zones (m) [75.0, 85.0]% (m)',
                'Speed Zones (m) [85.0, 95.0]% (m)',
                'Speed Zones (m) [95.0, 100.0]% (m)',
                'Abs HSR(m)',
                'Acceleration Zones  [0, 50]% Cnt',
                'Acceleration Zones  [50, 60]% Cnt',
                'Acceleration Zones  [-50, 0]% Cnt',
                'Acceleration Zones  [-60, -50]% Cnt',
                'MAX Speed(km/h)'
            ]
            
            missing_columns = [col for col in required_columns if col not in df_pandas.columns]
            if missing_columns:
                print(f"Columnas faltantes: {missing_columns}")
                return [empty_fig] * 6
            
            # ============================================================================
            # GRÁFICO 1: DISTANCE
            # ============================================================================
            
            # Filtrar solo jugadores (no TEAM)
            df_players = df_pandas[df_pandas['Player'] != 'TEAM'].copy()
            
            if len(df_players) == 0:
                fig_distance = empty_fig
            else:
                # Convertir a formato largo
                df_melted = df_players.melt(
                    id_vars=['Player'],
                    value_vars=[
                        "Speed Zones (m) [0.0, 45.0]% (m)",
                        "Speed Zones (m) [45.0, 65.0]% (m)",
                        "Speed Zones (m) [65.0, 75.0]% (m)",
                        "Speed Zones (m) [75.0, 85.0]% (m)",
                        "Speed Zones (m) [85.0, 95.0]% (m)",
                        "Speed Zones (m) [95.0, 100.0]% (m)"
                    ],
                    var_name='Zona',
                    value_name='Distancia'
                )
                
                # Renombrar zonas para la leyenda
                df_melted['Zona'] = df_melted['Zona'].replace({
                    "Speed Zones (m) [0.0, 45.0]% (m)": "Z1",
                    "Speed Zones (m) [45.0, 65.0]% (m)": "Z2",
                    "Speed Zones (m) [65.0, 75.0]% (m)": "Z3",
                    "Speed Zones (m) [75.0, 85.0]% (m)": "Z4",
                    "Speed Zones (m) [85.0, 95.0]% (m)": "Z5",
                    "Speed Zones (m) [95.0, 100.0]% (m)": "Z6"
                })
                
                fig_distance = px.bar(
                    df_melted,
                    y="Player",
                    x="Distancia",
                    color="Zona",
                    orientation="h",
                    title="<b>Distancia por Zonas de Velocidad</b>",
                    color_discrete_map={
                        "Z1": "#e4dcc6",
                        "Z2": "#d9cdb2",
                        "Z3": "#cfc09e",
                        "Z4": "#c3b89a",
                        "Z5": "#FF0000",
                        "Z6": "#CF0000"
                    },
                    labels={
                        "Distancia": "Distancia (m)",
                        "Player": "Jugador",
                        "Zona": "Zona de Velocidad"
                    }
                )
                
                fig_distance.update_layout(
                    plot_bgcolor="white",
                    paper_bgcolor="white",
                    font=dict(family="Arial, sans-serif", size=12, color="#2c3e50"),
                    title=dict(x=0.5, font=dict(size=16, color="#2c3e50")),
                    margin=dict(t=80, b=80, l=120, r=50),
                    barmode="stack",
                    height=550,
                    legend=dict(
                        orientation="h",
                        yanchor="top",
                        y=-0.15,
                        xanchor="center",
                        x=0.5
                    )
                )
                
                fig_distance.update_xaxes(
                    title_font=dict(size=14, color="#2c3e50"),
                    tickfont=dict(size=11, color="#2c3e50"),
                    gridcolor="#ecf0f1",
                    showgrid=True
                )
                
                fig_distance.update_yaxes(
                    title_font=dict(size=14, color="#2c3e50"),
                    tickfont=dict(size=11, color="#2c3e50"),
                    gridcolor="#ecf0f1",
                    showgrid=True
                )
            
            # ============================================================================
            # GRÁFICO 2: HSR
            # ============================================================================
            
            if len(df_players) == 0:
                fig_hsr = empty_fig
            else:
                # Eliminar NaNs en la métrica
                df_hsr = df_players.dropna(subset=["Abs HSR(m)"])
                
                if len(df_hsr) == 0:
                    fig_hsr = empty_fig
                else:
                    # Agrupar por jugador y sumar la métrica
                    df_agrupado = df_hsr.groupby("Player", as_index=False)["Abs HSR(m)"].sum()
                    
                    # Calcular promedio
                    promedio_hsr = df_agrupado["Abs HSR(m)"].mean()
                    
                    fig_hsr = px.bar(
                        df_agrupado,
                        y="Player",
                        x="Abs HSR(m)",
                        orientation="h",
                        title="<b>Distancia en Alta Velocidad (HSR)</b>",
                        color_discrete_sequence=["#525252"],
                        labels={
                            "Abs HSR(m)": "HSR (m)",
                            "Player": "Jugador"
                        }
                    )
                    
                    # Añadir línea del promedio
                    fig_hsr.add_shape(
                        type="line",
                        x0=promedio_hsr,
                        x1=promedio_hsr,
                        y0=0,
                        y1=1,
                        line=dict(color="#e74c3c", width=3, dash="dash"),
                        xref="x",
                        yref="paper"
                    )
                    
                    fig_hsr.add_annotation(
                        x=promedio_hsr,
                        y=1.05,
                        xref="x",
                        yref="paper",
                        text=f"<b>Promedio: {promedio_hsr:.1f} m</b>",
                        showarrow=False,
                        font=dict(color="#e74c3c", size=12),
                        bgcolor="white",
                        bordercolor="#e74c3c",
                        borderwidth=1
                    )
                    
                    fig_hsr.update_layout(
                        plot_bgcolor="white",
                        paper_bgcolor="white",
                        font=dict(family="Arial, sans-serif", size=12, color="#2c3e50"),
                        title=dict(x=0.5, font=dict(size=16, color="#2c3e50")),
                        margin=dict(t=70, b=50, l=120, r=50),
                        height=500
                    )
                    
                    fig_hsr.update_xaxes(
                        title_font=dict(size=14, color="#2c3e50"),
                        tickfont=dict(size=11, color="#2c3e50"),
                        gridcolor="#ecf0f1",
                        showgrid=True
                    )
                    
                    fig_hsr.update_yaxes(
                        title_font=dict(size=14, color="#2c3e50"),
                        tickfont=dict(size=11, color="#2c3e50"),
                        gridcolor="#ecf0f1",
                        showgrid=True
                    )
            
            # ============================================================================
            # GRÁFICO 3: ACC
            # ============================================================================
            
            if len(df_players) == 0:
                fig_acc = empty_fig
            else:
                # Convertir a formato largo
                df_melted_acc = df_players.melt(
                    id_vars=['Player'],
                    value_vars=[
                        "Acceleration Zones  [0, 50]% Cnt",
                        "Acceleration Zones  [50, 60]% Cnt"
                    ],
                    var_name='Zona',
                    value_name='Cuenta'
                )
                
                # Renombrar zonas
                df_melted_acc['Zona'] = df_melted_acc['Zona'].replace({
                    "Acceleration Zones  [0, 50]% Cnt": "[Z1]",
                    "Acceleration Zones  [50, 60]% Cnt": "[Z2]"
                })
                
                fig_acc = px.bar(
                    df_melted_acc,
                    y="Player",
                    x="Cuenta",
                    color="Zona",
                    orientation="h",
                    title="<b>Aceleraciones por Zona de Intensidad</b>",
                    color_discrete_map={
                        "[Z1]": "#38a838",
                        "[Z2]": "#01fa16"
                    },
                    labels={
                        "Cuenta": "Número de Aceleraciones",
                        "Player": "Jugador",
                        "Zona": "Zona de Aceleración"
                    }
                )
                
                fig_acc.update_layout(
                    plot_bgcolor="white",
                    paper_bgcolor="white",
                    font=dict(family="Arial, sans-serif", size=12, color="#2c3e50"),
                    title=dict(x=0.5, font=dict(size=16, color="#2c3e50")),
                    margin=dict(t=80, b=80, l=120, r=50),
                    height=550,
                    legend=dict(
                        orientation="h",
                        yanchor="top",
                        y=-0.15,
                        xanchor="center",
                        x=0.5
                    )
                )
                
                fig_acc.update_xaxes(
                    title_font=dict(size=14, color="#2c3e50"),
                    tickfont=dict(size=11, color="#2c3e50"),
                    gridcolor="#ecf0f1",
                    showgrid=True
                )
                
                fig_acc.update_yaxes(
                    title_font=dict(size=14, color="#2c3e50"),
                    tickfont=dict(size=11, color="#2c3e50"),
                    gridcolor="#ecf0f1",
                    showgrid=True
                )
            
            # ============================================================================
            # GRÁFICO 4: DCC
            # ============================================================================
            
            if len(df_players) == 0:
                fig_dcc = empty_fig
            else:
                # Convertir a formato largo
                df_melted_dcc = df_players.melt(
                    id_vars=['Player'],
                    value_vars=[
                        "Acceleration Zones  [-50, 0]% Cnt",
                        "Acceleration Zones  [-60, -50]% Cnt"
                    ],
                    var_name='Zona',
                    value_name='Cuenta'
                )
                
                # Renombrar zonas
                df_melted_dcc['Zona'] = df_melted_dcc['Zona'].replace({
                    "Acceleration Zones  [-50, 0]% Cnt": "[Z1]",
                    "Acceleration Zones  [-60, -50]% Cnt": "[Z2]"
                })
                
                fig_dcc = px.bar(
                    df_melted_dcc,
                    y="Player",
                    x="Cuenta",
                    color="Zona",
                    orientation="h",
                    title="<b>Desaceleraciones por Zona de Intensidad</b>",
                    color_discrete_map={
                        "[Z1]": "#a83838",
                        "[Z2]": "#fa0101"
                    },
                    labels={
                        "Cuenta": "Número de Desaceleraciones",
                        "Player": "Jugador",
                        "Zona": "Zona de Desaceleración"
                    }
                )
                
                fig_dcc.update_layout(
                    plot_bgcolor="white",
                    paper_bgcolor="white",
                    font=dict(family="Arial, sans-serif", size=12, color="#2c3e50"),
                    title=dict(x=0.5, font=dict(size=16, color="#2c3e50")),
                    margin=dict(t=80, b=80, l=120, r=50),
                    height=550,
                    legend=dict(
                        orientation="h",
                        yanchor="top",
                        y=-0.15,
                        xanchor="center",
                        x=0.5
                    )
                )
                
                fig_dcc.update_xaxes(
                    title_font=dict(size=14, color="#2c3e50"),
                    tickfont=dict(size=11, color="#2c3e50"),
                    gridcolor="#ecf0f1",
                    showgrid=True
                )
                
                fig_dcc.update_yaxes(
                    title_font=dict(size=14, color="#2c3e50"),
                    tickfont=dict(size=11, color="#2c3e50"),
                    gridcolor="#ecf0f1",
                    showgrid=True
                )
            
            # ============================================================================
            # GRÁFICO 5: VELOCIDAD
            # ============================================================================
            
            if len(df_players) == 0:
                fig_velocidad = empty_fig
            else:
                # Eliminar NaNs en la métrica
                df_velocidad = df_players.dropna(subset=["MAX Speed(km/h)"])
                
                if len(df_velocidad) == 0:
                    fig_velocidad = empty_fig
                else:
                    # Agrupar por jugador y calcular máximo
                    df_agrupado_vel = df_velocidad.groupby("Player", as_index=False)["MAX Speed(km/h)"].max()
                    
                    # Calcular promedio
                    promedio_velocidad = df_agrupado_vel["MAX Speed(km/h)"].mean()
                    
                    fig_velocidad = px.bar(
                        df_agrupado_vel,
                        y="Player",
                        x="MAX Speed(km/h)",
                        orientation="h",
                        title="<b>Velocidad Máxima por Jugador</b>",
                        color_discrete_sequence=["#12A7C2"],
                        labels={
                            "MAX Speed(km/h)": "Velocidad Máxima (km/h)",
                            "Player": "Jugador"
                        }
                    )
                    
                    # Añadir línea del promedio
                    fig_velocidad.add_shape(
                        type="line",
                        x0=promedio_velocidad,
                        x1=promedio_velocidad,
                        y0=0,
                        y1=1,
                        line=dict(color="#e74c3c", width=3, dash="dash"),
                        xref="x",
                        yref="paper"
                    )
                    
                    fig_velocidad.add_annotation(
                        x=promedio_velocidad,
                        y=1.05,
                        xref="x",
                        yref="paper",
                        text=f"<b>Promedio: {promedio_velocidad:.1f} km/h</b>",
                        showarrow=False,
                        font=dict(color="#e74c3c", size=12),
                        bgcolor="white",
                        bordercolor="#e74c3c",
                        borderwidth=1
                    )
                    
                    fig_velocidad.update_layout(
                        plot_bgcolor="white",
                        paper_bgcolor="white",
                        font=dict(family="Arial, sans-serif", size=12, color="#2c3e50"),
                        title=dict(x=0.5, font=dict(size=16, color="#2c3e50")),
                        margin=dict(t=70, b=50, l=120, r=50),
                        height=500
                    )
                    
                    fig_velocidad.update_xaxes(
                        title_font=dict(size=14, color="#2c3e50"),
                        tickfont=dict(size=11, color="#2c3e50"),
                        gridcolor="#ecf0f1",
                        showgrid=True
                    )
                    
                    fig_velocidad.update_yaxes(
                        title_font=dict(size=14, color="#2c3e50"),
                        tickfont=dict(size=11, color="#2c3e50"),
                        gridcolor="#ecf0f1",
                        showgrid=True
                    )
            
            # ============================================================================
            # GRÁFICO 6: POSICIONES
            # ============================================================================
            
            # Verificar si tenemos columna Position
            if 'Position' not in df_pandas.columns:
                fig_posiciones = empty_fig
            else:
                # Filtrar jugadores válidos con posición
                df_pos = df_players.dropna(subset=['Position'])
                
                if len(df_pos) == 0:
                    fig_posiciones = empty_fig
                else:
                    # Agrupar por posición y calcular promedio
                    df_agrupado_pos = df_pos.groupby("Position", as_index=False).agg({
                        "Speed Zones (m) [0.0, 45.0]% (m)": "mean",
                        "Speed Zones (m) [45.0, 65.0]% (m)": "mean",
                        "Speed Zones (m) [65.0, 75.0]% (m)": "mean",
                        "Speed Zones (m) [75.0, 85.0]% (m)": "mean",
                        "Speed Zones (m) [85.0, 95.0]% (m)": "mean",
                        "Speed Zones (m) [95.0, 100.0]% (m)": "mean",
                        "Abs HSR(m)": "mean",
                        "Acceleration Zones  [0, 50]% Cnt": "mean",
                        "Acceleration Zones  [50, 60]% Cnt": "mean",
                        "Acceleration Zones  [-50, 0]% Cnt": "mean",
                        "Acceleration Zones  [-60, -50]% Cnt": "mean",
                        "MAX Speed(km/h)": "mean"
                    })
                    
                    # Crear métricas finales
                    df_agrupado_pos["Distance"] = (
                        df_agrupado_pos["Speed Zones (m) [0.0, 45.0]% (m)"] +
                        df_agrupado_pos["Speed Zones (m) [45.0, 65.0]% (m)"] +
                        df_agrupado_pos["Speed Zones (m) [65.0, 75.0]% (m)"] +
                        df_agrupado_pos["Speed Zones (m) [75.0, 85.0]% (m)"] +
                        df_agrupado_pos["Speed Zones (m) [85.0, 95.0]% (m)"] +
                        df_agrupado_pos["Speed Zones (m) [95.0, 100.0]% (m)"]
                    )
                    
                    df_agrupado_pos["Acc"] = (
                        df_agrupado_pos["Acceleration Zones  [0, 50]% Cnt"] +
                        df_agrupado_pos["Acceleration Zones  [50, 60]% Cnt"]
                    )
                    
                    df_agrupado_pos["Decc"] = (
                        df_agrupado_pos["Acceleration Zones  [-50, 0]% Cnt"] +
                        df_agrupado_pos["Acceleration Zones  [-60, -50]% Cnt"]
                    )
                    
                    df_agrupado_pos.rename(columns={"Abs HSR(m)": "HSR", "MAX Speed(km/h)": "Max Speed"}, inplace=True)
                    
                    # Seleccionar columnas finales
                    df_final_pos = df_agrupado_pos[["Position", "Distance", "HSR", "Acc", "Decc", "Max Speed"]]
                    
                    # Transformar a formato largo
                    df_melted_pos = df_final_pos.melt(
                        id_vars="Position",
                        var_name="Métrica",
                        value_name="Valor"
                    )
                    
                    fig_posiciones = px.bar(
                        df_melted_pos,
                        x="Position",
                        y="Valor",
                        color="Métrica",
                        barmode="group",
                        title="<b>Análisis por Posición - Métricas Promedio</b>",
                        color_discrete_map={
                            "Distance": "#6e6e6e",
                            "HSR": "#000000",
                            "Acc": "#33b300",
                            "Decc": "#bb0404",
                            "Max Speed": "#0092cc"
                        },
                        text="Valor",
                        labels={
                            "Position": "Posición",
                            "Valor": "Valor Promedio",
                            "Métrica": "Métrica"
                        }
                    )
                    
                    fig_posiciones.update_traces(
                        texttemplate='%{text:.0f}',
                        textposition="outside",
                        textangle=0,
                        textfont=dict(size=10, color="#2c3e50")
                    )
                    
                    fig_posiciones.update_layout(
                        plot_bgcolor="white",
                        paper_bgcolor="white",
                        font=dict(family="Arial, sans-serif", size=12, color="#2c3e50"),
                        title=dict(x=0.5, font=dict(size=16, color="#2c3e50")),
                        margin=dict(t=80, b=80, l=50, r=50),
                        height=550,
                        legend=dict(
                            orientation="h",
                            yanchor="top",
                            y=-0.15,
                            xanchor="center",
                            x=0.5
                        )
                    )
                    
                    fig_posiciones.update_xaxes(
                        title_font=dict(size=14, color="#2c3e50"),
                        tickfont=dict(size=11, color="#2c3e50"),
                        gridcolor="#ecf0f1",
                        showgrid=True
                    )
                    
                    fig_posiciones.update_yaxes(
                        title_font=dict(size=14, color="#2c3e50"),
                        tickfont=dict(size=11, color="#2c3e50"),
                        gridcolor="#ecf0f1",
                        showgrid=True
                    )
            
            return [fig_distance, fig_hsr, fig_acc, fig_dcc, fig_velocidad, fig_posiciones]
            
        except Exception as e:
            print(f"Error al generar gráficos: {e}")
            import traceback
            traceback.print_exc()
            return [empty_fig] * 6

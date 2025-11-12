# ============================================================================
# IMPORTACIONES
# ============================================================================
from dash import html, dcc, Output, Input, State, callback_context, dash_table
import dash
import os
from datetime import datetime
import polars as pl
import pandas as pd
import numpy as np
from utils.utils import *
import plotly.graph_objects as go
import plotly.express as px

# ============================================================================
# CONSTANTES Y CONFIGURACIÓN
# ============================================================================
ESTADISTICAS_MATCHDAY_PATH = os.path.join('data', 'processed', 'references', 'estadisticas_matchday.parquet')

# ============================================================================
# FUNCIONES AUXILIARES
# ============================================================================
def load_estadisticas_matchday():
    try:
        if not os.path.exists(ESTADISTICAS_MATCHDAY_PATH):
            print(f"No se encontró el archivo: {ESTADISTICAS_MATCHDAY_PATH}")
            return None
        
        df = pl.read_parquet(ESTADISTICAS_MATCHDAY_PATH)
        
        df_processed = df.with_columns([
            pl.when(pl.col('Position').is_null() | (pl.col('Position') == '0'))
            .then(pl.lit('Team'))
            .otherwise(pl.col('Position'))
            .alias('Position')
        ])
        
        return df_processed
        
    except Exception as e:
        print(f"Error al cargar estadísticas de match day: {e}")
        return None


def get_unique_values_from_estadisticas():
    df = load_estadisticas_matchday()
    if df is None:
        return {
            'match_days': [], 'estadisticas': [], 'players': [], 'positions': [], 'tipos': []
        }
    
    try:
        return {
            'match_days': df.filter(pl.col('Match Day').is_not_null())['Match Day'].unique().sort().to_list(),
            'estadisticas': df.filter(pl.col('Estadistica').is_not_null())['Estadistica'].unique().sort().to_list(),
            'players': df.filter(pl.col('Player').is_not_null())['Player'].unique().sort().to_list(),
            'positions': df.filter(pl.col('Position').is_not_null())['Position'].unique().sort().to_list(),
            'tipos': df.filter(pl.col('Tipo').is_not_null())['Tipo'].unique().sort().to_list()
        }
    except Exception as e:
        print(f"Error al obtener valores únicos: {e}")
        return {
            'match_days': [], 'estadisticas': [], 'players': [], 'positions': [], 'tipos': []
        }


def get_available_dates_from_estadisticas():
    try:
        return get_sorted_dates()
    except Exception as e:
        print(f"Error al obtener fechas disponibles: {e}")
        return []

# ============================================================================
# LAYOUT DE LA PÁGINA
# ============================================================================
layout = html.Div([
    html.H2('Referencias - Estadísticas Match Day', className="page-title"),
    html.Hr(),
    
    html.Div([
        html.Div([
            html.H4('Seleccionar Parámetros', className="section-title"),
            html.Div([
                html.Div([
                    html.Label('Rango de Fechas:', className="input-label"),
                    dcc.DatePickerRange(
                        id='ref-date-range-selector',
                        start_date=get_first_date_for_picker(),
                        end_date=get_latest_date_for_picker(),
                        display_format='DD/MM/YYYY',
                        start_date_placeholder_text='Fecha inicio',
                        end_date_placeholder_text='Fecha fin',
                        className="date-picker-range"
                    )
                ], className="input-item"),

                html.Div([
                    html.Label('Estadística:', className="input-label"),
                    dcc.Dropdown(
                        id='ref-estadistica-selector',
                        placeholder='Selecciona una o más estadísticas...',
                        className="statistic-dropdown",
                        multi=True
                    )
                ], className="input-item"),

                html.Div([
                    html.Label('Match Day:', className="input-label"),
                    dcc.Dropdown(
                        id='ref-matchday-selector',
                        placeholder='Selecciona uno o más Match Days...',
                        className="statistic-dropdown",
                        multi=True
                    )
                ], className="input-item"),

                html.Div([
                    html.Label('Jugador:', className="input-label"),
                    dcc.Dropdown(
                        id='ref-player-selector',
                        placeholder='Selecciona uno o más jugadores...',
                        className="statistic-dropdown",
                        multi=True
                    )
                ], className="input-item"),

                html.Div([
                    html.Label('Posicion:', className="input-label"),
                    dcc.Dropdown(
                        id='ref-position-selector',
                        placeholder='Selecciona una o más posiciones...',
                        className="statistic-dropdown",
                        multi=True
                    )
                ], className="input-item"),

                html.Div([
                    html.Label('Tipo:', className="input-label"),
                    dcc.Dropdown(
                        id='ref-tipo-selector',
                        placeholder='Selecciona uno o más tipos...',
                        className="statistic-dropdown",
                        multi=True
                    )
                ], className="input-item"),

                html.Div([
                    html.Label('Team:', className="input-label"),
                    dcc.Dropdown(
                        id='ref-team-selector',
                        placeholder='Selecciona un equipo...',
                        className="statistic-dropdown",
                        multi=False  # o True si querés varios equipos
                                )
                            ], className="input-item"),

            ], className="inputs-row", style={'display': 'flex', 'flex-wrap': 'wrap', 'gap': '20px'})
        ], className="date-selection-container"),
        
        html.Div([
            html.H4('Datos de Referencia', className="section-title", style={'margin-top': '30px'}),
            html.Div(id='ref-data-output')
        ], className="references-data-container")
    ])
])

# ============================================================================
# CALLBACKS
# ============================================================================
def register_callbacks(app):
    @app.callback(
        [Output('ref-date-range-selector', 'min_date_allowed'),
         Output('ref-date-range-selector', 'max_date_allowed')],
        [Input('ref-date-range-selector', 'id')]
    )
    def configure_date_range_limits(selector_id):
        try:
            fechas_ordenadas = get_available_dates_from_estadisticas()
            if not fechas_ordenadas:
                return None, None
            min_date = datetime.strptime(fechas_ordenadas[0], '%d/%m/%Y').date()
            max_date = datetime.strptime(fechas_ordenadas[-1], '%d/%m/%Y').date()
            return min_date, max_date
        except Exception as e:
            print(f"Error al configurar límites de fechas: {e}")
            return None, None

    @app.callback(
        [Output('ref-estadistica-selector', 'options'),
        Output('ref-matchday-selector', 'options'),
        Output('ref-player-selector', 'options'),
        Output('ref-position-selector', 'options'),
        Output('ref-tipo-selector', 'options'),
        Output('ref-team-selector', 'options')],  # 👈 nuevo output
        [Input('ref-date-range-selector', 'start_date'),
        Input('ref-date-range-selector', 'end_date')]
    )
    def update_dropdown_options(start_date, end_date):
        try:
            unique_values = get_unique_values_from_estadisticas()
            
            estadistica_options = [
                {'label': 'Media', 'value': 'mean'},
                {'label': 'Mediana', 'value': 'median'},
                {'label': 'Desv Estándar', 'value': 'std'},
                {'label': 'Percentil 75', 'value': 'p75'},
                {'label': 'Percentil 25', 'value': 'p25'},
                {'label': 'Percentil 99', 'value': 'p99'},
                {'label': 'Percentil 1', 'value': 'p1'}
            ]
            
            matchday_options = [{'label': md, 'value': md} for md in unique_values['match_days']]
            player_options = [{'label': p, 'value': p} for p in unique_values['players']]
            position_options = [{'label': pos, 'value': pos} for pos in unique_values['positions']]
            tipo_options = [{'label': t, 'value': t} for t in unique_values['tipos']]
            
            df = load_estadisticas_matchday()
            team_options = []
            if df is not None and "Team" in df.columns:
                team_options = [
                    {'label': t, 'value': t}
                    for t in df["Team"].drop_nulls().unique().to_list()
                    if t not in [None, "", "null"]
                ]

            return (
                estadistica_options,
                matchday_options,
                player_options,
                position_options,
                tipo_options,
                team_options
            )
        except Exception as e:
            print(f"Error al actualizar opciones de dropdowns: {e}")
            return [], [], [], [], [], []

    @app.callback(
    Output('ref-data-output', 'children'),
    [
        Input('ref-date-range-selector', 'start_date'),
        Input('ref-date-range-selector', 'end_date'),
        Input('ref-estadistica-selector', 'value'),
        Input('ref-matchday-selector', 'value'),
        Input('ref-player-selector', 'value'),
        Input('ref-position-selector', 'value'),
        Input('ref-tipo-selector', 'value'),
        Input('ref-team-selector', 'value')  # 👈 nuevo input
    ]
)
    def update_reference_data(start_date, end_date, selected_estadistica, selected_matchday, 
                          selected_player, selected_position, selected_tipo, selected_team):
        try:
            df = load_estadisticas_matchday()
            if df is None:
                return html.Div("No se pudieron cargar los datos de referencia.", className="error-message")
            
            df_filtered = df

            # 🔹 Filtrar por equipo
            if selected_team:
                df_filtered = df_filtered.filter(pl.col("Team") == selected_team)

            # 🔹 Filtrar por fechas
            if start_date and end_date:
                start_dt = datetime.strptime(start_date, '%Y-%m-%d')
                end_dt = datetime.strptime(end_date, '%Y-%m-%d')
                df_filtered = df_filtered.filter(pl.col('Date').is_between(start_dt.date(), end_dt.date()))
            
            # 🔹 Filtrar por estadística
            if selected_estadistica:
                df_filtered = df_filtered.filter(pl.col('Estadistica').is_in(selected_estadistica))
            
            # 🔹 Filtrar por matchday
            if selected_matchday:
                df_filtered = df_filtered.filter(pl.col('Match Day').is_in(selected_matchday))
            
            # 🔹 Filtrar por tipo
            if selected_tipo:
                df_filtered = df_filtered.filter(pl.col('Tipo').is_in(selected_tipo))

            # 🔹 Filtrar por posición
            if selected_position:
                df_filtered = df_filtered.filter(pl.col('Position').is_in(selected_position))

            # ==============================================================
            # Armado del DataFrame final (jugadores, posición, equipo)
            # ==============================================================
            frames = []

            # 1️⃣ Si hay jugadores seleccionados
            if selected_player:
                df_players = df_filtered.filter(pl.col('Player').is_in(selected_player))
                frames.append(df_players)

                # 2️⃣ Agregar referencias por posición
                for jugador in selected_player:
                    pos = df_filtered.filter(pl.col('Player') == jugador)['Position'].unique().to_list()
                    if pos:
                        posicion = pos[0]
                        df_pos_ref = df_filtered.filter(
                            (pl.col('Position') == posicion) & (pl.col('Tipo') == 'Posicion')
                        )
                        if df_pos_ref.height > 0:
                            frames.append(df_pos_ref)

                # 3️⃣ Agregar referencia del equipo completo
                df_team = df_filtered.filter(pl.col('Tipo') == 'Equipo')
                if df_team.height > 0:
                    frames.append(df_team)
            else:
                frames.append(df_filtered)

            # Concatenar todo
            df_final = pl.concat(frames).unique()

            if df_final.height == 0:
                return html.Div("No se encontraron datos con los filtros seleccionados.", className="warning-message")
            
            df_pandas = df_final.to_pandas()

            if "Date" in df_pandas.columns:
                df_pandas["Date"] = pd.to_datetime(df_pandas["Date"], errors="coerce").dt.strftime("%d/%m/%Y")

            return html.Div([
                html.P(f"Mostrando {len(df_pandas)} registros", className="info-text"),
                dash_table.DataTable(
                    data=df_pandas.to_dict('records'),
                    columns=[{"name": col, "id": col} for col in df_pandas.columns],
                    style_table={'overflowX': 'auto'},
                    style_cell={'textAlign': 'left', 'padding': '10px', 'fontFamily': 'Arial'},
                    style_header={'backgroundColor': 'rgb(230, 230, 230)', 'fontWeight': 'bold'},
                    page_size=20,
                    sort_action="native",
                    filter_action="native"
                )
            ])
        
        except Exception as e:
            print(f"Error al actualizar datos de referencia: {e}")
            return html.Div(f"Error al procesar los datos: {str(e)}", className="error-message")

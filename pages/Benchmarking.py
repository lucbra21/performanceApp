import dash
from dash import dcc, html, Output, Input
import dash_bootstrap_components as dbc
import polars as pl
import os
from datetime import datetime
import pandas as pd
from dash import dash_table


ESTADISTICAS_MATCHDAY_PATH = os.path.join('data', 'processed', 'references', 'estadisticas_matchday.parquet')

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
# Obtener fechas mínimas y máximas desde el parquet
df = load_estadisticas_matchday()
if df is not None and "Date" in df.columns:
    # Nos aseguramos de que Date sea tipo date
    if df["Date"].dtype != pl.Date:
        df = df.with_columns(pl.col("Date").str.strptime(pl.Date, format="%d/%m/%Y", strict=False))
    
    min_date = df["Date"].min()
    max_date = df["Date"].max()
else:
    min_date = "2023-01-01"
    max_date = "2023-12-31"
# ====================================================================
# LAYOUT
# ====================================================================

layout = html.Div([
    html.H2("Benchmarking Match Day", className="page-title"),
    html.Hr(),

    # Selección de parámetros
    html.Div([
        html.Div([
            html.Label("Tipo:", className="input-label"),
            dcc.Dropdown(
                id="benchmark-tipo",
                options=[
                    {"label": "Jugador", "value": "Jugador"},
                    {"label": "Posicion", "value": "Posicion"},
                    {"label": "Equipo", "value": "Equipo"},
                ],
                value="Jugador",
                className="statistic-dropdown"
            )
        ], className="input-item"),

        html.Div([
            html.Label("Entidad:", className="input-label"),
            dcc.Dropdown(
                id="benchmark-entidad",
                placeholder="Selecciona entidad...",
                className="statistic-dropdown",
                multi=True
            )
        ], className="input-item"),
        
        html.Div([
            html.Label("Match Day:", className="input-label"),
            dcc.Dropdown(
                id="benchmark-matchday",
                placeholder="Selecciona uno o más Match Days...",
                className="statistic-dropdown",
                multi=True
            )
        ], className="input-item"),

        # Periodo 1
        html.Div([
            html.Label("Rango de Fechas (Periodo 1):", className="input-label"),
            dcc.DatePickerRange(
                id="benchmark-date-range-1",
                display_format="DD/MM/YYYY",
                start_date_placeholder_text="Fecha inicio",
                end_date_placeholder_text="Fecha fin",
                min_date_allowed=min_date,
                max_date_allowed=max_date,
                start_date=min_date,
                end_date=max_date
            )
        ], className="input-item"),

        # Periodo 2
        html.Div([
            html.Label("Rango de Fechas (Periodo 2):", className="input-label"),
            dcc.DatePickerRange(
                id="benchmark-date-range-2",
                display_format="DD/MM/YYYY",
                start_date_placeholder_text="Fecha inicio",
                end_date_placeholder_text="Fecha fin",
                min_date_allowed=min_date,
                max_date_allowed=max_date,
                start_date=min_date,
                end_date=max_date
            )
        ], className="input-item"),

        html.Div([
            html.Label("Métrica:", className="input-label"),
            dcc.Dropdown(
                id="benchmark-metrica",
                placeholder="Selecciona métrica...",
                className="statistic-dropdown"
            )
        ], className="input-item"),

        html.Div([
            html.Label("Estadística:", className="input-label"),
            dcc.Dropdown(
                id="benchmark-estadistica",
                placeholder="Selecciona estadística...",
                className="statistic-dropdown"
            )
        ], className="input-item"),

    ], className="inputs-row", style={'display': 'flex', 'flex-wrap': 'wrap', 'gap': '20px'}),

    html.Hr(),

    # Contenedor para mostrar resultados
    html.Div(id="benchmark-output", className="references-data-container")
])

# ====================================================================
# CALLBACKS
# ====================================================================



def register_callbacks(app):
    # =====================================================
    # Callback: actualizar entidades según tipo
    # =====================================================
    @app.callback(
        Output("benchmark-entidad", "options"),
        Input("benchmark-tipo", "value")
    )
    def update_entidad_options(tipo):
        df = load_estadisticas_matchday()
        if df is None or tipo is None:
            return []

        tipo_col_map = {
            "Jugador": "Player",
            "Posicion": "Position",
            "Equipo": "Team"
        }
        columna = tipo_col_map.get(tipo)
        if not columna or columna not in df.columns:
            return []

        entidades = df[columna].unique().to_list()
        return [{"label": e, "value": e} for e in entidades if e is not None]

    # =====================================================
    # Callback: actualizar métricas disponibles
    # =====================================================
    @app.callback(
        Output("benchmark-metrica", "options"),
        Input("benchmark-tipo", "value")
    )
    def update_metricas_options(tipo):
        df = load_estadisticas_matchday()
        if df is None:
            return []
        exclude = {"Player", "Team", "Position", "Match Day", "Date", "Estadistica", "Tipo"}
        metricas = [
            c for c, dt in zip(df.columns, df.dtypes)
            if c not in exclude and dt in (pl.Int64, pl.Float64)
        ]
        return [{"label": m, "value": m} for m in metricas]

    # =====================================================
    # Callback: actualizar estadísticas disponibles
    # =====================================================
    @app.callback(
        Output("benchmark-estadistica", "options"),
        Input("benchmark-metrica", "value")
    )
    def update_estadisticas_options(metrica):
        df = load_estadisticas_matchday()
        if df is None or not metrica:
            return []
        if "Estadistica" not in df.columns:
            return []
        estadisticas = df["Estadistica"].unique().to_list()
        return [{"label": e, "value": e} for e in estadisticas if e is not None]
    
    @app.callback(
        Output("benchmark-matchday", "options"),
        Input("benchmark-tipo", "value")
    )
    def update_matchdays(_):
        df = load_estadisticas_matchday()
        if df is None:
            return []
        
        if "Match Day" not in df.columns:
            print("No existe columna 'Match Day' en parquet:", df.columns)
            return []

        matchdays = df["Match Day"].unique().to_list()
        return [{"label": f"MD {m}", "value": m} for m in matchdays if m is not None]

    # =====================================================
    # Callback principal: comparación de periodos
    # =====================================================
    @app.callback(
    Output("benchmark-output", "children"),
    [
        Input("benchmark-tipo", "value"),
        Input("benchmark-entidad", "value"),
        Input("benchmark-date-range-1", "start_date"),
        Input("benchmark-date-range-1", "end_date"),
        Input("benchmark-date-range-2", "start_date"),
        Input("benchmark-date-range-2", "end_date"),
        Input("benchmark-metrica", "value"),
        Input("benchmark-estadistica", "value"),
        Input("benchmark-matchday", "value"),
    ],
    )
    def update_benchmark_output(tipo, entidades, start1, end1, start2, end2, metrica, estadistica, matchdays):
        df = load_estadisticas_matchday()
        if df is None or not entidades or not metrica or not estadistica:
            return html.Div("Selecciona todos los filtros para ver resultados.")

        # Parseo de fechas
        def parse_date(d):
            if d is None:
                return None
            return datetime.strptime(d, "%Y-%m-%d").date()

        start1_dt, end1_dt = parse_date(start1), parse_date(end1)
        start2_dt, end2_dt = parse_date(start2), parse_date(end2)

        # Mapeo de columnas
        tipo_col_map = {
            "Jugador": "Player",
            "Posicion": "Position",
            "Equipo": "Team",
            "Matchday": "Match Day"
        }
        columna = tipo_col_map.get(tipo, "Player")

        # Filtro base
        df = df.filter(
            (pl.col(columna).is_in(entidades)) &
            (pl.col("Estadistica") == estadistica)
        )

        if matchdays:
            df = df.filter(pl.col("Match Day").is_in(matchdays))

        # Filtrar periodos
        df_periodo1 = df.filter(
            (pl.col("Date") >= start1_dt) & (pl.col("Date") <= end1_dt)
        ) if start1_dt and end1_dt else pl.DataFrame([])

        df_periodo2 = df.filter(
            (pl.col("Date") >= start2_dt) & (pl.col("Date") <= end2_dt)
        ) if start2_dt and end2_dt else pl.DataFrame([])

        if df_periodo1.is_empty() and df_periodo2.is_empty():
            return html.Div("No se encontraron datos para los periodos seleccionados.")

        # Función de agregación: devolver el valor total (o podrías usar mean(), sum(), etc.)
        def valor_periodo(periodo, nombre):
            if periodo.is_empty():
                return {"Periodo": nombre, "Valor": None}
            vals = periodo[metrica]
            return {"Periodo": nombre, "Valor": float(vals.mean())}  # o .sum() si prefieres acumulado

        resumen1 = valor_periodo(df_periodo1, "Periodo 1")
        resumen2 = valor_periodo(df_periodo2, "Periodo 2")

        # Convertir a pandas para mostrar en dash_table
        resumen_df = pd.DataFrame([resumen1, resumen2])

        table = dash_table.DataTable(
            data=resumen_df.to_dict("records"),
            columns=[{"name": c, "id": c} for c in resumen_df.columns],
            style_table={"overflowX": "auto"},
            style_cell={"textAlign": "center"},
            style_header={"fontWeight": "bold"},
        )

        return html.Div([
            html.H4(f"Benchmarking - {metrica} ({estadistica})"),
            table
        ])

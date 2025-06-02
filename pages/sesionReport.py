from dash import html, dcc, callback
import dash_bootstrap_components as dbc
import pandas as pd
from dash.dependencies import Input, Output
from datetime import date
from dash import dash_table
import plotly.graph_objects as go





# Cargar el archivo Excel con las sesiones
df = pd.read_excel('excel-0.xlsx')  # ← Cambia esta ruta
df['Date'] = pd.to_datetime(df['Date'], dayfirst=True, errors='coerce')  # Asegúrate de convertir a datetime

layout = html.Div([

    # Encabezado con logo y título
    html.Div([
        html.Div(
            dcc.Link(
                html.Img(src='/assets/logo.png', style={'height': '100px', 'cursor': 'pointer'}),
                href='/home'
            ),
            className="p-2 me-3"
        ),

        dbc.Col(
            html.H2("Sesion Report", style={'fontWeight': 'bold', 'height': '50px'}, className="text-center"),
            md=4
        ),
    ], className="d-flex align-items-center"),
    html.Hr(),
    # Filtros
    html.Div([
        html.Div([
            html.Label("Sesión:", className="mb-1"),
            dcc.Dropdown(
                id='dropdown-sesion',
                options=[],
                placeholder="Seleccionar sesión",
                style={'fontWeight': 'bold'}
            )
        ], style={'width': '250px', 'marginRight': '20px'}),

        html.Div([
            html.Label("Rango de fechas:", style={'fontWeight': 'bold'}),
            dcc.DatePickerRange(
                id='rango-fechas',
                min_date_allowed=df['Date'].min(),
                max_date_allowed=df['Date'].max(),
                start_date=df['Date'].min(),
                end_date=df['Date'].max(),
                minimum_nights=0,
                display_format='DD/MM/YYYY',
                style={'width': '100%'}
            )
        ], style={'width': '300px'})
    ], className="d-flex flex-row align-items-end mb-4"),
    html.Hr(),
    # Tabla debajo de los filtros
    html.Div([
        dash_table.DataTable(
            id='tabla-metricas-sesion',
            columns=[],  # definidas en callback
            data=[],
            style_table={'overflowX': 'auto'},
            style_cell={'textAlign': 'center'},
            style_header={'fontWeight': 'bold'},
        )
        ], className='tabla-container'),
    html.Div(
        id='indicadores-kpi',
        className="d-flex flex-wrap justify-content-center gap-3 mt-4"
        ),
    html.Hr(),
        dbc.Row([
            dbc.Col([
                dcc.Graph(id='grafico-distance')
            ], width=6),
            dbc.Col([
                dcc.Graph(id='grafico-hibd')
            ], width=6),
        ], className="mb-4"),
    dbc.Row([
            dbc.Col([
                dcc.Graph(id='grafico-barras-acc')
            ], width=4),
            dbc.Col([
                dcc.Graph(id='grafico-barras-decc')
            ], width=4),
            dbc.Col([
                dcc.Graph(id='grafico-barras-max-sprint')
            ], width=4),
        ], className="mb-4")
])


@callback(
    Output('tabla-metricas-sesion', 'data'),
    Output('tabla-metricas-sesion', 'columns'),
    Output('indicadores-kpi', 'children'),
    Input('rango-fechas', 'start_date'),
    Input('rango-fechas', 'end_date')
)
def actualizar_tabla_sesion(start_date, end_date):
    if start_date is None or end_date is None:
        return [], [], []

    df['Date'] = pd.to_datetime(df['Date'])

    df_filtrado = df[
        (df['Date'] >= start_date) &
        (df['Date'] <= end_date) &
        (df['Selection'] == 'Session')
    ]

    df_filtrado = df_filtrado[df_filtrado['Player'] != 'TEAM']

    columnas_metricas = [
        'Distance (m)',
        'High Intensity Dec Abs (count)',
        'Sprint Max Distance (m)',
        'High Intensity Acc Abs (count)',
        'MAX Speed(km/h)'
    ]

    columnas_presentes = [col for col in columnas_metricas if col in df_filtrado.columns]

    df_agrupado = df_filtrado.groupby('Player', as_index=False)[columnas_presentes].sum()
    columnas_extra = df_filtrado.groupby('Player', as_index=False)[['Nick Name', 'Team ', 'Position']].first()
    df_final = pd.merge(df_agrupado, columnas_extra, on='Player', how='left')

    # Ordenar columnas si están disponibles
    columnas_finales = ['Player', 'Nick Name', 'Team ', 'Position'] + columnas_presentes
    columnas_finales = [col for col in columnas_finales if col in df_final.columns]
    df_final = df_final[columnas_finales]

    # Detectar columnas numéricas
    columnas_numericas = df_final.select_dtypes(include='number').columns

    # 👉 Promedios por posición
    filas_posiciones = []
    posiciones = df_final['Position'].dropna().unique()

    for pos in posiciones:
        df_pos = df_final[df_final['Position'] == pos]
        fila_pos = {'Player': f'PROMEDIO {pos}', 'Position': pos}
        for col in df_final.columns:
            if col in columnas_numericas:
                fila_pos[col] = df_pos[col].mean()
            elif col not in ['Player', 'Position']:
                fila_pos[col] = ''
        filas_posiciones.append(fila_pos)

    # 👉 Filas resumen generales
    fila_prom = {'Player': 'PROMEDIO'}
    fila_max = {'Player': 'MÁXIMO'}
    fila_min = {'Player': 'MÍNIMO'}

    for col in df_final.columns:
        if col in columnas_numericas:
            fila_prom[col] = df_final[col].mean()
            fila_max[col] = df_final[col].max()
            fila_min[col] = df_final[col].min()
        elif col != 'Player':
            fila_prom[col] = ''
            fila_max[col] = ''
            fila_min[col] = ''

    # 👉 Agregar todo al final
    df_final = pd.concat([df_final, pd.DataFrame(filas_posiciones + [fila_prom, fila_max, fila_min])], ignore_index=True)

    data = df_final.to_dict('records')
    columns = [{"name": col, "id": col} for col in df_final.columns]

    # 👉 KPIs
    metrics_targets = {
        'Distance (m)': 8500,
        'High Intensity Acc Abs (count)': 40,
        'High Intensity Dec Abs (count)': 30
    }

    kpis = []
    for metric, target in metrics_targets.items():
        if metric in df_final.columns:
            promedio_fila = df_final[df_final['Player'] == 'PROMEDIO']
            if not promedio_fila.empty:
                avg_value = promedio_fila[metric].values[0]
                difference = avg_value - target
                percent = (difference / target) * 100
                color = "success" if difference >= 0 else "danger"
                icon = "✅" if difference >= 0 else "❌"

                card = dbc.Card([
                    dbc.CardBody([
                        html.H5(metric, className="card-title"),
                        html.H6(f"{avg_value:.1f} {icon}", className=f"text-{color}"),
                        html.P(f"Objetivo: {target} ({percent:+.1f}%)", className="card-text"),
                    ])
                ], color="black", style={"width": "250px", "border": f"2px solid {'green' if difference >= 0 else 'red'}"})

                kpis.append(card)

    return data, columns, kpis





@callback(
    Output('grafico-distance', 'figure'),
    Input('rango-fechas', 'start_date'),
    Input('rango-fechas', 'end_date')
)
def actualizar_grafico_distance(start_date, end_date):
    if start_date is None or end_date is None:
        return go.Figure()

    df['Date'] = pd.to_datetime(df['Date'])

    df_filtrado = df[
        (df['Date'] >= start_date) &
        (df['Date'] <= end_date) &
        (df['Selection'] == 'Session') &
        (df['Player'] != 'TEAM')
    ]

    df_grouped = df_filtrado.groupby('Player', as_index=False)['Distance (m)'].sum()
    df_grouped = df_grouped.sort_values(by='Distance (m)', ascending=True)

    prom_distance = df_grouped['Distance (m)'].mean()

    # Calcula la altura: 40px por jugador (ajusta si quieres más/menos espacio)
    height = max(300, 20 * len(df_grouped))

    fig = go.Figure()
    fig.add_trace(go.Bar(
        y=df_grouped['Player'],
        x=df_grouped['Distance (m)'],
        orientation='h',
        marker_color='steelblue'
    ))

    fig.add_shape(
        type="line",
        x0=prom_distance, x1=prom_distance,
        y0=-0.5, y1=len(df_grouped)-0.5,
        line=dict(color="red", width=2, dash="dash")
    )

    fig.update_layout(
        title='Distance (m) por jugador',
        xaxis_title='Distance (m)',
        yaxis_title='Jugador',
        showlegend=False,
        height=height,  # <-- agrega esto
        margin=dict(l=100)  # <-- más espacio a la izquierda para los nombres
    )

    return fig


@callback(
    Output('grafico-hibd', 'figure'),
    Input('rango-fechas', 'start_date'),
    Input('rango-fechas', 'end_date')
)
def actualizar_grafico_hibd(start_date, end_date):
    if start_date is None or end_date is None:
        return go.Figure()

    df['Date'] = pd.to_datetime(df['Date'])

    df_filtrado = df[
        (df['Date'] >= start_date) &
        (df['Date'] <= end_date) &
        (df['Selection'] == 'Session') &
        (df['Player'] != 'TEAM')
    ]

    df_grouped = df_filtrado.groupby('Player', as_index=False)['HIBD (m)'].sum()
    df_grouped = df_grouped.sort_values(by='HIBD (m)', ascending=True)

    prom_hibd = df_grouped['HIBD (m)'].mean()

    # Calcula la altura: 40px por jugador (ajusta si quieres más/menos espacio)
    height = max(300, 20 * len(df_grouped))


    fig = go.Figure()
    fig.add_trace(go.Bar(
        y=df_grouped['Player'],
        x=df_grouped['HIBD (m)'],
        orientation='h',
        marker_color='steelblue'
    ))

    fig.add_shape(
        type="line",
        x0=prom_hibd, x1=prom_hibd,
        y0=-0.5, y1=len(df_grouped)-0.5,
        line=dict(color="red", width=2, dash="dash")
    )

    fig.update_layout(
        title='HIBD (m) por jugador',
        xaxis_title='HIBD (m)',
        yaxis_title='Jugador',
        showlegend=False,
        height=height,  # <-- agrega esto
        margin=dict(l=100)  # <-- más espacio a la izquierda para los nombres
    )

    return fig


@callback(
    Output('grafico-barras-acc', 'figure'),
    Input('rango-fechas', 'start_date'),
    Input('rango-fechas', 'end_date')
)
def actualizar_grafico_hibd(start_date, end_date):
    if start_date is None or end_date is None:
        return go.Figure()

    df['Date'] = pd.to_datetime(df['Date'])

    df_filtrado = df[
        (df['Date'] >= start_date) &
        (df['Date'] <= end_date) &
        (df['Selection'] == 'Session') &
        (df['Player'] != 'TEAM')
    ]

    df_grouped = df_filtrado.groupby('Player', as_index=False)['High Intensity Acc Abs (count)'].sum()
    df_grouped = df_grouped.sort_values(by='High Intensity Acc Abs (count)', ascending=True)

    prom_hibd = df_grouped['High Intensity Acc Abs (count)'].mean()

    # Calcula la altura: 40px por jugador (ajusta si quieres más/menos espacio)
    height = max(200, 20 * len(df_grouped))


    fig = go.Figure()
    fig.add_trace(go.Bar(
        y=df_grouped['Player'],
        x=df_grouped['High Intensity Acc Abs (count)'],
        orientation='h',
        marker_color='green'
    ))

    fig.add_shape(
        type="line",
        x0=prom_hibd, x1=prom_hibd,
        y0=-0.5, y1=len(df_grouped)-0.5,
        line=dict(color="red", width=2, dash="dash")
    )

    fig.update_layout(
        title='ACC por jugador',
        xaxis_title='High Intensity Acc Abs (count)',
        yaxis_title='Jugador',
        showlegend=False,
        height=height,  # <-- agrega esto
        margin=dict(l=100)  # <-- más espacio a la izquierda para los nombres
    )

    return fig


@callback(
    Output('grafico-barras-decc', 'figure'),
    Input('rango-fechas', 'start_date'),
    Input('rango-fechas', 'end_date')
)
def actualizar_grafico_hibd(start_date, end_date):
    if start_date is None or end_date is None:
        return go.Figure()

    df['Date'] = pd.to_datetime(df['Date'])

    df_filtrado = df[
        (df['Date'] >= start_date) &
        (df['Date'] <= end_date) &
        (df['Selection'] == 'Session') &
        (df['Player'] != 'TEAM')
    ]

    df_grouped = df_filtrado.groupby('Player', as_index=False)['High Intensity Dec Abs (count)'].sum()
    df_grouped = df_grouped.sort_values(by='High Intensity Dec Abs (count)', ascending=True)

    prom_hibd = df_grouped['High Intensity Dec Abs (count)'].mean()

    # Calcula la altura: 40px por jugador (ajusta si quieres más/menos espacio)
    height = max(200, 20 * len(df_grouped))


    fig = go.Figure()
    fig.add_trace(go.Bar(
        y=df_grouped['Player'],
        x=df_grouped['High Intensity Dec Abs (count)'],
        orientation='h',
        marker_color='Red'
    ))

    fig.add_shape(
        type="line",
        x0=prom_hibd, x1=prom_hibd,
        y0=-0.5, y1=len(df_grouped)-0.5,
        line=dict(color="black", width=2, dash="dash")
    )

    fig.update_layout(
        title='DECC por jugador',
        xaxis_title='High Intensity Dec Abs (count)',
        yaxis_title='Jugador',
        showlegend=False,
        height=height,  # <-- agrega esto
        margin=dict(l=100)  # <-- más espacio a la izquierda para los nombres
    )

    return fig


@callback(
    Output('grafico-barras-max-sprint', 'figure'),
    Input('rango-fechas', 'start_date'),
    Input('rango-fechas', 'end_date')
)
def actualizar_grafico_hibd(start_date, end_date):
    if start_date is None or end_date is None:
        return go.Figure()

    df['Date'] = pd.to_datetime(df['Date'])

    df_filtrado = df[
        (df['Date'] >= start_date) &
        (df['Date'] <= end_date) &
        (df['Selection'] == 'Session') &
        (df['Player'] != 'TEAM')
    ]

    df_grouped = df_filtrado.groupby('Player', as_index=False)['MAX Speed(km/h)'].sum()
    df_grouped = df_grouped.sort_values(by='MAX Speed(km/h)', ascending=True)

    prom_hibd = df_grouped['MAX Speed(km/h)'].mean()

    # Calcula la altura: 40px por jugador (ajusta si quieres más/menos espacio)
    height = max(200, 20 * len(df_grouped))


    fig = go.Figure()
    fig.add_trace(go.Bar(
        y=df_grouped['Player'],
        x=df_grouped['MAX Speed(km/h)'],
        orientation='h',
        marker_color='gray'
    ))

    fig.add_shape(
        type="line",
        x0=prom_hibd, x1=prom_hibd,
        y0=-0.5, y1=len(df_grouped)-0.5,
        line=dict(color="black", width=2, dash="dash")
    )

    fig.update_layout(
        title='Max Speed por jugador',
        xaxis_title='MAX Speed(km/h)',
        yaxis_title='Jugador',
        showlegend=False,
        height=height,  # <-- agrega esto
        margin=dict(l=100)  # <-- más espacio a la izquierda para los nombres
    )

    return fig
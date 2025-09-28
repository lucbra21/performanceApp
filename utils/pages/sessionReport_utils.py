"""
=============================================================================
MÓDULO: SESSION REPORT UTILITIES (sessionReport_utils.py)
=============================================================================

Este módulo contiene funciones especializadas para generar reportes de sesiones
de entrenamiento, incluyendo procesamiento de datos, análisis comparativos,
generación de tablas con múltiples tipos de métricas y esquemas de coloración
basados en Z-scores.

=============================================================================
FUNCIONES DISPONIBLES
=============================================================================

PROCESAMIENTO DE DATOS BÁSICOS:
--------------------------------
• get_table_data(selected_date, selected_statistic)
  - Procesa datos de jugadores, equipos y posiciones para una fecha específica
  - Combina datos absolutos con estadísticas agrupadas según la métrica seleccionada
  - Utiliza: get_players_data(), calcular_metricas() de utils.metrics

ANÁLISIS COMPARATIVO AVANZADO:
------------------------------
• generate_comparative_table(selected_date, selected_statistic, num_games, reference_statistic)
  - Genera tabla comparativa con valores absolutos, Z-scores y diferencias porcentuales
  - Organiza datos en estructura de tres filas por jugador/entidad
  - Utiliza: get_table_data(), calcular_zscore_fecha_vs_matchday_acumulado(), 
    calculate_percentage_difference_vs_reference() de utils.metrics

FORMATEO Y VISUALIZACIÓN:
-------------------------
• format_data_for_display_mode(comparative_data, display_mode)
  - Formatea datos según modo de visualización ('absolutos', 'zscore', 'diferencia')
  - Combina valores absolutos con Z-scores o diferencias porcentuales
  - Utiliza funciones auxiliares: _combine_absolute_with_zscore_format(), 
    _combine_absolute_with_percentage_format()

• _combine_absolute_with_zscore_format(absolutos_data, zscore_data, metric_columns)
  - Función auxiliar: combina valores absolutos con Z-scores en formato texto
  - Formato de salida: "valor_absoluto (Z = zscore)"

• _combine_absolute_with_percentage_format(absolutos_data, diferencia_data, metric_columns)
  - Función auxiliar: combina valores absolutos con diferencias porcentuales
  - Formato de salida: "valor_absoluto (+/-diferencia%)"

ESQUEMAS DE COLORACIÓN:
-----------------------
• generate_zscore_color_scheme(zscore_data, metric_columns, display_data)
  - Genera esquema de colores basado en Z-scores con gradientes azul-blanco-rojo
  - Aplica coloración condicional para tablas Dash DataTable
  - Lógica: azul (Z < -1), blanco (-1 ≤ Z ≤ 1), rojo (Z > 1)


=============================================================================
FLUJO DE TRABAJO TÍPICO
=============================================================================
1. get_table_data() → Obtener datos básicos para una fecha
2. generate_comparative_table() → Generar análisis comparativo completo
3. format_data_for_display_mode() → Formatear según modo de visualización
4. generate_zscore_color_scheme() → Aplicar esquema de colores basado en Z-scores

"""

import os
import polars as pl
import numpy as np
from dash import html
from ..config import BASE_PATH, REFERENCES_PATH
from ..data_access import *
from ..metrics import *


def get_table_data(selected_date, selected_statistic):
    """
    =============================================================================
    FUNCIÓN: get_table_data
    =============================================================================
    
    PROPÓSITO:
    ----------
    Procesa y combina datos de entrenamiento para una fecha específica, generando
    una tabla unificada que incluye datos individuales de jugadores, estadísticas
    agregadas del equipo y estadísticas agrupadas por posición.
    
    QUÉ HACE EXACTAMENTE:
    --------------------
    1. Obtiene datos individuales de todos los jugadores para la fecha seleccionada
    2. Calcula estadísticas agregadas del equipo completo usando la métrica especificada
    3. Agrupa jugadores por posición y calcula estadísticas para cada posición
    4. Combina todos los datos en un DataFrame unificado con estructura consistente
    5. Ordena y organiza los datos para presentación en tabla
    
    CÓMO LO HACE (IMPLEMENTACIÓN):
    -----------------------------
    • Utiliza get_players_data() para obtener datos base de jugadores
    • Identifica columnas numéricas excluyendo 'Player' y 'Position'
    • Aplica calcular_metricas() para generar estadísticas agregadas
    • Itera sobre posiciones únicas para calcular estadísticas por posición
    • Usa pl.concat() para combinar DataFrames manteniendo estructura consistente
    • Reordena columnas para asegurar compatibilidad entre todos los DataFrames
    
    ARCHIVOS UTILIZADOS:
    -------------------
    • utils.data_access.get_players_data() - Obtiene datos de jugadores
    • utils.metrics.calcular_metricas() - Calcula estadísticas agregadas
    
    ESTRUCTURA DE DATOS DE SALIDA:
    -----------------------------
    DataFrame con columnas:
    - Player: Identificador (nombre jugador, "TEAM", "POS_[posición]")
    - [Métricas]: Columnas numéricas con valores de métricas de rendimiento
    
    
    ORDEN DE DATOS EN SALIDA:
    ------------------------
    1. Jugadores individuales (ordenados alfabéticamente)
    2. Estadísticas del equipo (fila "TEAM")
    3. Estadísticas por posición (filas "POS_[posición]")
    """
    
    # Obtener datos de jugadores para la fecha seleccionada
    # Esta función viene del módulo utils.data_access
    df_players = get_players_data(selected_date)
    
    # Verificar si se obtuvieron datos válidos para la fecha
    if df_players is None:
        return None
    
    # PROCESAMIENTO DE ESTADÍSTICAS DEL EQUIPO
    # ========================================
    
    # Identificar columnas numéricas (métricas) excluyendo identificadores
    # Estas son las columnas sobre las cuales se calcularán las estadísticas
    columnas_numericas = [col for col in df_players.columns if col not in ['Player', 'Position']]
    
    # Calcular estadísticas del equipo usando la función calcular_metricas del módulo utils.metrics
    # La estadística seleccionada (mean, median, etc.) determina cómo se agregan los datos
    team_stats = calcular_metricas(df_players, columnas_numericas, selected_statistic)
    
    # Agregar identificador para distinguir la fila del equipo
    team_stats['Player'] = "TEAM"
    
    # Convertir el diccionario de estadísticas en DataFrame de Polars
    df_team = pl.DataFrame([team_stats])
    
    
    # PROCESAMIENTO DE ESTADÍSTICAS POR POSICIÓN
    # ==========================================
    
    # Obtener lista única de posiciones presentes en los datos
    positions = df_players.select('Position').unique()['Position'].to_list()
    
    # Lista para almacenar estadísticas de cada posición
    position_data = []
    
    # Iterar sobre cada posición para calcular sus estadísticas
    for pos in positions:
        # Filtrar jugadores de la posición actual
        df_pos = df_players.filter(pl.col('Position') == pos)
        
        # Calcular estadísticas para esta posición usando la misma métrica que el equipo
        pos_stats = calcular_metricas(df_pos, columnas_numericas, selected_statistic)
        
        # Agregar identificador con prefijo "POS_" para distinguir filas de posición
        pos_stats['Player'] = f"POS_{pos}"
        
        # Agregar estadísticas de esta posición a la lista
        position_data.append(pos_stats)
    
    # Crear DataFrame de posiciones si hay datos disponibles
    df_position = pl.DataFrame(position_data) if position_data else None
    
    
    # COMBINACIÓN Y ESTRUCTURACIÓN FINAL
    # ==================================
    
    # Preparar DataFrame de jugadores removiendo la columna 'Position' para compatibilidad
    # y ordenando alfabéticamente por nombre de jugador
    df_players_clean = df_players.drop('Position').sort('Player')
    
    # Obtener el orden de columnas del DataFrame de jugadores (sin Position)
    # Este orden se usará para asegurar consistencia en todos los DataFrames
    column_order = df_players_clean.columns
    
    # Inicializar lista de DataFrames a concatenar con los jugadores
    dfs_to_concat = [df_players_clean]
    
    # Agregar DataFrame del equipo si existe, reordenando columnas para consistencia
    if df_team is not None:
        df_team = df_team.select(column_order)
        dfs_to_concat.append(df_team)
        
    # Agregar DataFrame de posiciones si existe, reordenando columnas para consistencia
    if df_position is not None:
        df_position = df_position.select(column_order)
        dfs_to_concat.append(df_position)
    
    # Concatenar todos los DataFrames en el orden deseado:
    # 1. Jugadores individuales (ordenados alfabéticamente)
    # 2. Estadísticas del equipo
    # 3. Estadísticas por posición
    result_df = pl.concat(dfs_to_concat)
    
    return result_df



def generate_comparative_table(selected_date, selected_statistic='median', num_games=None, reference_statistic='median'):
    """
    =============================================================================
    FUNCIÓN: generate_comparative_table
    =============================================================================
    
    PROPÓSITO:
    ----------
    Genera una tabla comparativa avanzada que integra tres tipos de análisis para
    cada jugador/posición/equipo en una fecha específica: valores absolutos,
    Z-scores (comparación con histórico) y diferencias porcentuales (comparación
    con referencia estadística).
    
    QUÉ HACE EXACTAMENTE:
    --------------------
    1. Obtiene valores absolutos usando get_table_data()
    2. Calcula Z-scores históricos usando calcular_zscore_fecha_vs_matchday_acumulado()
    3. Calcula diferencias porcentuales usando calculate_percentage_difference_vs_reference()
    4. Organiza los datos en estructura de tres filas por entidad (jugador/posición/equipo)
    5. Combina y alinea todas las métricas en un DataFrame unificado
    6. Aplica redondeo y formateo final para presentación
    
    CÓMO LO HACE (IMPLEMENTACIÓN):
    -----------------------------
    • Manejo robusto de errores con try-catch y validaciones de datos
    • Mapeo inteligente de columnas entre diferentes fuentes de datos
    • Identificación automática de métricas comunes entre todos los análisis
    • Organización sistemática por jugador con tres tipos de valores por entidad
    • Redondeo automático a 2 decimales para todas las columnas numéricas
    • Estructura de salida consistente con columnas ['Player', 'Tipo', métricas...]
    
    ARCHIVOS UTILIZADOS:
    -------------------
    • get_table_data() - Valores absolutos (función local)
    • utils.metrics.calcular_zscore_fecha_vs_matchday_acumulado() - Z-scores históricos
    • utils.metrics.calculate_percentage_difference_vs_reference() - Diferencias porcentuales
    
    ESTRUCTURA DE DATOS DE SALIDA:
    -----------------------------
    DataFrame organizado con:
    - Player: Identificador de jugador/posición/equipo
    - Tipo: Tipo de análisis ('Absoluto', 'Z-Score', 'Diferencia %')
    - [Métricas]: Columnas numéricas con valores correspondientes al tipo de análisis
    
    LÓGICA DE ORGANIZACIÓN:
    ----------------------
    Para cada jugador/entidad se crean 3 filas consecutivas:
    1. Fila 'Absoluto': Valores reales de métricas
    2. Fila 'Z-Score': Desviaciones estándar respecto al histórico
    3. Fila 'Diferencia %': Porcentaje de cambio respecto a referencia
    

    
    PARÁMETROS DE CONFIGURACIÓN:
    ---------------------------
    • selected_date: Fecha específica para análisis
    • selected_statistic: Estadística para agregaciones ('mean', 'median', etc.)
    • num_games: Número de juegos para referencia de diferencias porcentuales
    • reference_statistic: Estadística de referencia para diferencias porcentuales
    """
    try:
        # PASO 1: OBTENER VALORES ABSOLUTOS
        # =================================
        
        # Obtener datos absolutos usando la función local get_table_data
        # Esta función ya maneja la combinación de jugadores, equipo y posiciones
        df_absolutos = get_table_data(selected_date, selected_statistic)
        if df_absolutos is None:
            print(f"No se pudieron obtener datos absolutos para la fecha: {selected_date}")
            return None
            
        # PASO 2: OBTENER Z-SCORES HISTÓRICOS
        # ===================================
        
        # Calcular Z-scores comparando la fecha actual con el histórico acumulado
        # Esta función viene del módulo utils.metrics
        df_zscores = calcular_zscore_fecha_vs_matchday_acumulado(selected_date)
        if df_zscores is None:
            print(f"No se pudieron calcular z-scores para la fecha: {selected_date}")
            return None
            
        # PASO 3: OBTENER DIFERENCIAS PORCENTUALES
        # ========================================
        
        # Calcular diferencias porcentuales respecto a estadísticas de referencia
        # Esta función también viene del módulo utils.metrics
        df_diff_pct = calculate_percentage_difference_vs_reference(
            selected_date, 
            num_games=num_games, 
            reference_statistic=reference_statistic
        )
        if df_diff_pct is None or df_diff_pct.is_empty():
            print(f"No se pudieron calcular diferencias porcentuales para la fecha: {selected_date}")
            return None
            
        # PASO 4: PREPARAR DATAFRAMES PARA COMBINACIÓN
        # ============================================
        
        # Identificar columnas de métricas (excluyendo 'Player' que es el identificador)
        metric_columns = [col for col in df_absolutos.columns if col != 'Player']
        
        # Preparar DataFrame de valores absolutos agregando columna 'Tipo'
        df_abs_formatted = df_absolutos.with_columns(
            pl.lit('Absoluto').alias('Tipo')  # Literal 'Absoluto' para identificar tipo
        ).select(['Player', 'Tipo'] + metric_columns)
        
        # Preparar DataFrame de z-scores
        # Verificar que las columnas de métricas coincidan entre fuentes
        zscore_metric_columns = [col for col in df_zscores.columns if col != 'Player']
        common_metrics = [col for col in metric_columns if col in zscore_metric_columns]
        
        df_zscore_formatted = df_zscores.with_columns(
            pl.lit('Z-Score').alias('Tipo')  # Literal 'Z-Score' para identificar tipo
        ).select(['Player', 'Tipo'] + common_metrics)
        
        # PASO 5: MAPEAR COLUMNAS DE DIFERENCIAS PORCENTUALES
        # ==================================================
        
        # Las columnas de diferencias porcentuales tienen sufijo '_diff_pct'
        # Necesitamos mapearlas a las columnas originales para consistencia
        diff_pct_mapping = {}
        for col in metric_columns:
            diff_col = f"{col}_diff_pct"  # Buscar columna con sufijo
            if diff_col in df_diff_pct.columns:
                diff_pct_mapping[diff_col] = col  # Mapear a nombre original
                
        # Renombrar columnas de diferencias porcentuales para que coincidan con originales
        df_diff_pct_renamed = df_diff_pct.rename(diff_pct_mapping)
        
        # Seleccionar solo las columnas que existen en común entre todas las fuentes
        available_diff_metrics = [col for col in common_metrics if col in df_diff_pct_renamed.columns]
        
        df_diff_formatted = df_diff_pct_renamed.with_columns(
            pl.lit('Diferencia %').alias('Tipo')  # Literal 'Diferencia %' para identificar tipo
        ).select(['Player', 'Tipo'] + available_diff_metrics)
        
        # PASO 6: ASEGURAR CONSISTENCIA DE COLUMNAS
        # =========================================
        
        # Usar las métricas disponibles en todos los DataFrames para evitar errores
        final_metrics = available_diff_metrics
        
        # Reseleccionar todos los DataFrames con las columnas finales consistentes
        df_abs_final = df_abs_formatted.select(['Player', 'Tipo'] + final_metrics)
        df_zscore_final = df_zscore_formatted.select(['Player', 'Tipo'] + final_metrics)
        df_diff_final = df_diff_formatted.select(['Player', 'Tipo'] + final_metrics)
        
        # PASO 7: CREAR ESTRUCTURA FINAL ORGANIZADA
        # =========================================
        
        # Obtener lista única de jugadores/entidades para organizar datos
        players_list = df_abs_final['Player'].unique().to_list()
        
        # Lista para almacenar filas organizadas en el orden correcto
        organized_rows = []
        
        # Para cada jugador/entidad, agregar sus tres tipos de análisis consecutivamente
        for player in players_list:
            # Obtener datos para cada tipo de valor para este jugador
            abs_row = df_abs_final.filter(pl.col('Player') == player)
            zscore_row = df_zscore_final.filter(pl.col('Player') == player)
            diff_row = df_diff_final.filter(pl.col('Player') == player)
            
            # Agregar filas en orden específico: Absoluto, Z-Score, Diferencia %
            if abs_row.height > 0:
                organized_rows.append(abs_row)
            if zscore_row.height > 0:
                organized_rows.append(zscore_row)
            if diff_row.height > 0:
                organized_rows.append(diff_row)
        
        # PASO 8: CONCATENAR Y FORMATEAR RESULTADO FINAL
        # ==============================================
        
        if organized_rows:
            # Concatenar todas las filas organizadas en un solo DataFrame
            result_df = pl.concat(organized_rows)
            
            # Reordenar columnas en orden lógico: Player, Tipo, luego todas las métricas
            final_columns = ['Player', 'Tipo'] + final_metrics
            result_df = result_df.select(final_columns)
            
            # APLICAR REDONDEO A VALORES NUMÉRICOS
            # ====================================
            
            # Identificar columnas numéricas (excluyendo identificadores de texto)
            numeric_columns = [col for col in result_df.columns if col not in ['Player', 'Tipo']]
            
            # Aplicar redondeo a 2 decimales para todas las columnas numéricas
            for col in numeric_columns:
                # Verificar que la columna sea de tipo numérico antes de redondear
                if result_df[col].dtype in [pl.Float64, pl.Float32, pl.Int64, pl.Int32]:
                    result_df = result_df.with_columns(
                        pl.col(col).round(2).alias(col)  # Redondear a 2 decimales
                    )
            
            return result_df
        else:
            print("No se pudieron organizar los datos")
            return None
            
    except Exception as e:
        # Manejo robusto de errores con información detallada
        print(f"Error al generar tabla comparativa: {str(e)}")
        return None





def format_data_for_display_mode(comparative_data, display_mode):
    """
    =============================================================================
    FUNCIÓN: format_data_for_display_mode
    =============================================================================
    
    PROPÓSITO:
    ----------
    Transforma los datos de la tabla comparativa según el modo de visualización
    seleccionado por el usuario, combinando valores absolutos con información
    contextual (Z-scores o diferencias porcentuales) en formato legible.
    
    QUÉ HACE EXACTAMENTE:
    --------------------
    1. Filtra datos para mostrar solo jugadores (excluye TEAM y POS_)
    2. Separa los tres tipos de datos: Absoluto, Z-Score, Diferencia %
    3. Aplica formateo específico según el modo de visualización seleccionado
    4. Combina valores absolutos con información contextual en formato texto
    5. Ordena los datos alfabéticamente por jugador para presentación consistente
    6. Retorna tanto los datos formateados como los Z-scores para coloración
    
    CÓMO LO HACE (IMPLEMENTACIÓN):
    -----------------------------
    • Usa filtros de Polars para separar tipos de datos y excluir agregaciones
    • Aplica funciones auxiliares especializadas para cada modo de formateo
    • Mantiene consistencia en el orden de jugadores entre datos y colores
    • Maneja casos de error con validaciones robustas y retorno de None
    • Preserva Z-scores originales para aplicación de esquema de colores
    
    ARCHIVOS UTILIZADOS:
    -------------------
    • _combine_absolute_with_zscore_format() - Función auxiliar local
    • _combine_absolute_with_percentage_format() - Función auxiliar local
    
    MODOS DE VISUALIZACIÓN SOPORTADOS:
    ---------------------------------
    • 'absolutos': Solo valores absolutos sin información adicional
    • 'zscore': Valores absolutos + Z-scores en formato "valor (Z = zscore)"
    • 'diferencia': Valores absolutos + diferencias % en formato "valor (+/-diferencia%)"
    
    ESTRUCTURA DE DATOS DE ENTRADA:
    ------------------------------
    DataFrame con columnas:
    - Player: Identificador (jugadores, "TEAM", "POS_[posición]")
    - Tipo: Tipo de análisis ('Absoluto', 'Z-Score', 'Diferencia %')
    - [Métricas]: Columnas numéricas con valores correspondientes
    
    ESTRUCTURA DE DATOS DE SALIDA:
    -----------------------------
    Tupla (display_data, zscore_data_for_colors) donde:
    - display_data: DataFrame formateado para mostrar en tabla con jugadores únicamente
    - zscore_data_for_colors: DataFrame con Z-scores para generar esquema de colores
    
    LÓGICA DE FILTRADO:
    ------------------
    • Excluye filas que empiecen con 'TEAM' (estadísticas del equipo)
    • Excluye filas que empiecen con 'POS_' (estadísticas por posición)
    • Mantiene solo datos de jugadores individuales para visualización

    """
    try:
        # VALIDACIÓN INICIAL DE DATOS
        # ===========================
        
        if comparative_data is None or comparative_data.height == 0:
            return None, None
            
        # FILTRADO DE DATOS DE JUGADORES
        # =============================
        
        # Filtrar para mostrar solo datos de jugadores individuales
        # Excluir agregaciones de equipo (TEAM) y posiciones (POS_)
        players_data = comparative_data.filter(
            ~pl.col('Player').str.starts_with('TEAM') & 
            ~pl.col('Player').str.starts_with('POS_')
        )
        
        # Verificar que existan datos de jugadores después del filtrado
        if players_data.height == 0:
            return None, None
            
        # SEPARACIÓN DE TIPOS DE DATOS
        # ============================
        
        # Separar los tres tipos de análisis en DataFrames independientes
        absolutos_data = players_data.filter(pl.col('Tipo') == 'Absoluto')
        zscore_data = players_data.filter(pl.col('Tipo') == 'Z-Score')
        diferencia_data = players_data.filter(pl.col('Tipo') == 'Diferencia %')
        
        # Identificar columnas de métricas (excluyendo identificadores)
        metric_columns = [col for col in players_data.columns if col not in ['Player', 'Tipo']]
        
        # APLICACIÓN DE FORMATEO SEGÚN MODO DE VISUALIZACIÓN
        # =================================================
        
        if display_mode == 'absolutos':
            # MODO ABSOLUTOS: Solo valores absolutos sin información adicional
            display_data = absolutos_data.select(['Player'] + metric_columns)
            
        elif display_mode == 'zscore':
            # MODO Z-SCORE: Combinar valores absolutos con Z-scores
            # Formato: "valor_absoluto (Z = zscore)"
            display_data = _combine_absolute_with_zscore_format(
                absolutos_data, zscore_data, metric_columns
            )
            
        elif display_mode == 'diferencia':
            # MODO DIFERENCIA: Combinar valores absolutos con diferencias porcentuales
            # Formato: "valor_absoluto (+/-diferencia%)"
            display_data = _combine_absolute_with_percentage_format(
                absolutos_data, diferencia_data, metric_columns
            )
        else:
            # Modo de visualización no válido
            return None, None
            
        # ORDENAMIENTO Y PREPARACIÓN FINAL
        # ================================
        
        # Ordenar datos de visualización alfabéticamente por jugador para consistencia
        if display_data is not None:
            display_data = display_data.sort('Player')
            
        # Preparar Z-scores para esquema de colores (también ordenados para alineación)
        # Los Z-scores se usan siempre para coloración, independientemente del modo de visualización
        zscore_for_colors = zscore_data.select(['Player'] + metric_columns).sort('Player')
        
        return display_data, zscore_for_colors
        
    except Exception as e:
        # Manejo robusto de errores con información detallada
        print(f"Error al formatear datos para visualización: {str(e)}")
        return None, None


def _combine_absolute_with_zscore_format(absolutos_data, zscore_data, metric_columns):
    """
    =============================================================================
    FUNCIÓN AUXILIAR: _combine_absolute_with_zscore_format
    =============================================================================
    
    PROPÓSITO:
    ----------
    Combina valores absolutos con sus correspondientes Z-scores en un formato
    legible para visualización, creando texto que muestra tanto el valor real
    como su significancia estadística relativa.
    
    QUÉ HACE EXACTAMENTE:
    --------------------
    1. Alinea datos absolutos y Z-scores por jugador usando joins
    2. Itera sobre cada métrica para crear formato combinado
    3. Genera texto en formato "valor_absoluto (Z = zscore_redondeado)"
    4. Maneja casos donde faltan datos de Z-score mostrando solo valor absoluto
    5. Redondea Z-scores a 2 decimales para legibilidad
    6. Retorna DataFrame con formato híbrido para visualización
    
    CÓMO LO HACE (IMPLEMENTACIÓN):
    -----------------------------
    • Usa join de Polars para alinear datos por jugador
    • Aplica with_columns para transformar cada métrica individualmente
    • Utiliza when().then().otherwise() para manejo condicional de valores nulos
    • Aplica round(2) para formateo consistente de Z-scores
    • Construye strings con concatenación de expresiones Polars
    
    FORMATO DE SALIDA:
    -----------------
    • Con Z-score disponible: "15.2 (Z = 1.34)"
    • Sin Z-score disponible: "15.2"
    • Valores nulos: se mantienen como null
    
    """
    try:
        # VALIDACIÓN DE DATOS DE ENTRADA
        # ==============================
        
        if absolutos_data.height == 0 or zscore_data.height == 0:
            return None
            
        # ALINEACIÓN DE DATOS POR JUGADOR
        # ==============================
        
        # Realizar join para alinear valores absolutos con Z-scores por jugador
        # Usar left join para mantener todos los jugadores con valores absolutos
        combined = absolutos_data.select(['Player'] + metric_columns).join(
            zscore_data.select(['Player'] + metric_columns), 
            on='Player', 
            how='left',
            suffix='_zscore'  # Sufijo para distinguir columnas de Z-score
        )
        
        # CONSTRUCCIÓN DEL FORMATO COMBINADO
        # =================================
        
        # Iterar sobre cada métrica para crear el formato híbrido
        for metric in metric_columns:
            zscore_col = f"{metric}_zscore"  # Nombre de columna de Z-score correspondiente
            
            # Crear expresión condicional para formateo
            # Si hay Z-score: "valor (Z = zscore)"
            # Si no hay Z-score: "valor"
            # Si no hay valor: null
            combined = combined.with_columns(
                pl.when(pl.col(metric).is_not_null())
                .then(
                    pl.when(pl.col(zscore_col).is_not_null())
                    .then(
                        # Formato completo: valor absoluto + Z-score redondeado
                        pl.col(metric).cast(pl.Utf8) + 
                        " (Z = " + 
                        pl.col(zscore_col).round(2).cast(pl.Utf8) + 
                        ")"
                    )
                    .otherwise(
                        # Solo valor absoluto si no hay Z-score
                        pl.col(metric).cast(pl.Utf8)
                    )
                )
                .otherwise(pl.lit(None))  # Mantener null si no hay valor absoluto
                .alias(metric)  # Mantener nombre original de la métrica
            )
            
        # LIMPIEZA Y PREPARACIÓN FINAL
        # ============================
        
        # Seleccionar solo las columnas necesarias (Player + métricas formateadas)
        # Excluir columnas auxiliares de Z-score con sufijo
        final_columns = ['Player'] + metric_columns
        result = combined.select(final_columns)
        
        return result
        
    except Exception as e:
        # Manejo de errores con información detallada para debugging
        print(f"Error al combinar valores absolutos con Z-scores: {str(e)}")
        return None


def _combine_absolute_with_percentage_format(absolutos_data, diferencia_data, metric_columns):
    """
    =============================================================================
    FUNCIÓN AUXILIAR: _combine_absolute_with_percentage_format
    =============================================================================
    
    PROPÓSITO:
    ----------
    Combina valores absolutos con sus correspondientes diferencias porcentuales
    en un formato legible para visualización, mostrando tanto el valor actual
    como su variación relativa respecto a un valor de referencia.
    
    QUÉ HACE EXACTAMENTE:
    --------------------
    1. Alinea datos absolutos y diferencias porcentuales por jugador usando joins
    2. Itera sobre cada métrica para crear formato combinado
    3. Genera texto en formato "valor_absoluto (+/-diferencia%)"
    4. Maneja casos donde faltan datos de diferencia mostrando solo valor absoluto
    5. Redondea diferencias porcentuales a 2 decimales para legibilidad
    6. Aplica signos apropiados (+/-) según el valor de la diferencia
    7. Retorna DataFrame con formato híbrido para visualización
    
    CÓMO LO HACE (IMPLEMENTACIÓN):
    -----------------------------
    • Usa join de Polars para alinear datos por jugador
    • Aplica with_columns para transformar cada métrica individualmente
    • Utiliza when().then().otherwise() para manejo condicional de valores nulos
    • Aplica round(2) para formateo consistente de diferencias porcentuales
    • Construye strings con concatenación de expresiones Polars
    • Maneja automáticamente el signo de las diferencias (positivas/negativas)
    
    FORMATO DE SALIDA:
    -----------------
    • Con diferencia positiva: "15.2 (+12.34%)"
    • Con diferencia negativa: "15.2 (-8.76%)"
    • Con diferencia cero: "15.2 (0.00%)"
    • Sin diferencia disponible: "15.2"
    • Valores nulos: se mantienen como null
    
    CASOS ESPECIALES:
    ----------------
    • Si no hay datos de diferencia para un jugador, muestra solo valor absoluto
    • Si el valor absoluto es null, el resultado final es null
    • Si la diferencia es null pero hay valor absoluto, muestra solo el valor
    • Maneja automáticamente diferencias en jugadores entre datasets
    • El signo se incluye automáticamente en el formateo (+ para positivos, - para negativos)
    
    """
    try:
        # VALIDACIÓN DE DATOS DE ENTRADA
        # ==============================
        
        if absolutos_data.height == 0 or diferencia_data.height == 0:
            return None
            
        # ALINEACIÓN DE DATOS POR JUGADOR
        # ==============================
        
        # Realizar join para alinear valores absolutos con diferencias porcentuales por jugador
        # Usar left join para mantener todos los jugadores con valores absolutos
        combined = absolutos_data.select(['Player'] + metric_columns).join(
            diferencia_data.select(['Player'] + metric_columns), 
            on='Player', 
            how='left',
            suffix='_diff'  # Sufijo para distinguir columnas de diferencia porcentual
        )
        
        # CONSTRUCCIÓN DEL FORMATO COMBINADO
        # =================================
        
        # Iterar sobre cada métrica para crear el formato híbrido
        for metric in metric_columns:
            diff_col = f"{metric}_diff"  # Nombre de columna de diferencia correspondiente
            
            # Crear expresión condicional para formateo
            # Si hay diferencia: "valor (+/-diferencia%)"
            # Si no hay diferencia: "valor"
            # Si no hay valor: null
            combined = combined.with_columns(
                pl.when(pl.col(metric).is_not_null())
                .then(
                    pl.when(pl.col(diff_col).is_not_null())
                    .then(
                        # Formato completo: valor absoluto + diferencia porcentual con signo
                        pl.col(metric).cast(pl.Utf8) + 
                        " (" + 
                        pl.when(pl.col(diff_col) >= 0)
                        .then("+" + pl.col(diff_col).round(2).cast(pl.Utf8) + "%")
                        .otherwise(pl.col(diff_col).round(2).cast(pl.Utf8) + "%") +
                        ")"
                    )
                    .otherwise(
                        # Solo valor absoluto si no hay diferencia porcentual
                        pl.col(metric).cast(pl.Utf8)
                    )
                )
                .otherwise(pl.lit(None))  # Mantener null si no hay valor absoluto
                .alias(metric)  # Mantener nombre original de la métrica
            )
            
        # LIMPIEZA Y PREPARACIÓN FINAL
        # ============================
        
        # Seleccionar solo las columnas necesarias (Player + métricas formateadas)
        # Excluir columnas auxiliares de diferencia con sufijo
        final_columns = ['Player'] + metric_columns
        result = combined.select(final_columns)
        
        return result
        
    except Exception as e:
        # Manejo de errores con información detallada para debugging
        print(f"Error al combinar valores absolutos con diferencias porcentuales: {str(e)}")
        return None


def generate_zscore_color_scheme(zscore_data, metric_columns, display_data=None):
    """
    =============================================================================
    FUNCIÓN: generate_zscore_color_scheme
    =============================================================================
    
    PROPÓSITO:
    ----------
    Genera un esquema de colores basado en Z-scores para visualización de datos
    en tablas, aplicando gradientes azul-blanco-rojo que reflejan la significancia
    estadística de cada valor respecto a su distribución histórica.
    
    QUÉ HACE EXACTAMENTE:
    --------------------
    1. Procesa DataFrame con Z-scores de jugadores y métricas
    2. Aplica gradientes de color basados en rangos de desviación estándar
    3. Genera estilos CSS condicionales para cada celda de la tabla
    4. Alinea índices de filas con datos de visualización cuando se proporciona
    5. Aplica coloración especial para columna de identificación de jugadores
    6. Maneja casos especiales como valores nulos y extremos
    7. Retorna estructura compatible con Dash DataTable para renderizado
    
    CÓMO LO HACE (IMPLEMENTACIÓN):
    -----------------------------
    • Utiliza función auxiliar get_color_for_zscore() para mapeo de colores
    • Alinea row_index con display_data cuando está disponible para consistencia
    • Itera sobre jugadores y métricas aplicando colores basados en Z-scores
    • Valida tipos de datos y maneja valores no numéricos graciosamente
    • Aplica estilo especial (fondo amarillo) para columna 'Player'
    • Construye lista de diccionarios con formato específico de Dash DataTable
    
    ARCHIVOS UTILIZADOS:
    -------------------
    • get_color_for_zscore() - Función auxiliar local para mapeo de colores
    • numpy para validación de valores NaN
    

    
    ESTRUCTURA DE DATOS DE ENTRADA:
    ------------------------------
    • zscore_data: DataFrame con columnas ['Player', métricas...] y Z-scores
    • metric_columns: Lista de nombres de columnas de métricas a colorear
    • display_data: DataFrame opcional para alineación de índices de fila
    
    ESTRUCTURA DE DATOS DE SALIDA:
    -----------------------------
    Lista de diccionarios con formato:
    [
        {
            'if': {'row_index': índice, 'column_id': 'métrica'},
            'backgroundColor': 'color_hex',
            'color': 'color_texto',
            'border': '1px solid #ddd'
        },
        ...
    ]
    
    
    CONSIDERACIONES DE ALINEACIÓN:
    -----------------------------
    • Cuando display_data se proporciona, los row_index se alinean con su ordenación
    • Esto asegura que los colores se apliquen a las filas correctas en la tabla
    • Sin display_data, se usa la ordenación natural de zscore_data
    """
    try:
        if zscore_data is None or zscore_data.height == 0:
            return []
            
        color_styles = []  # Lista para almacenar estilos de color de cada celda
        
        # FUNCIÓN AUXILIAR PARA MAPEO DE COLORES
        # =====================================
        
        def get_color_for_zscore(zscore_value):
            """
            Función auxiliar que mapea valores Z-score a colores específicos.
            
            LÓGICA DE COLORACIÓN:
            --------------------
            • Z-score < -1: Gradiente azul (más intenso cuanto más negativo)
            • Z-score entre -1 y 1: Blanco (zona neutra)
            • Z-score > 1: Gradiente rojo (más intenso cuanto más positivo)
            
            PARÁMETROS:
            ----------
            zscore_value (float): Valor Z-score a mapear
            
            RETORNA:
            -------
            tuple: (background_color, text_color) en formato hexadecimal
            """
            if zscore_value is None:
                return '#ffffff', '#000000'  # Blanco con texto negro para valores nulos
            
            # ZONA NEUTRA: valores entre -1 y 1 permanecen blancos
            if -1.0 <= zscore_value <= 1.0:
                return '#ffffff', '#000000'
            
            # Z-SCORES NEGATIVOS < -1: Gradiente azul (rendimiento bajo)
            elif zscore_value < -1.0:
                if zscore_value <= -2.5:
                    return '#1f4e79', '#ffffff'  # Azul oscuro profundo
                elif zscore_value <= -2.0:
                    return '#2e5984', '#ffffff'  # Azul oscuro medio-profundo
                elif zscore_value <= -1.5:
                    return '#4a90e2', '#ffffff'  # Azul medio
                else:  # Entre -1.5 y -1.0
                    return '#7bb3f0', '#000000'  # Azul claro
            
            # Z-SCORES POSITIVOS > 1: Gradiente rojo (rendimiento alto)
            elif zscore_value > 1.0:
                if zscore_value >= 2.5:
                    return '#8b0000', '#ffffff'  # Rojo oscuro profundo
                elif zscore_value >= 2.0:
                    return '#a52a2a', '#ffffff'  # Rojo oscuro medio-profundo
                elif zscore_value >= 1.5:
                    return '#dc143c', '#ffffff'  # Rojo medio
                else:  # Entre 1.0 y 1.5
                    return '#f5a3a3', '#000000'  # Rojo claro
            
            # Fallback para casos no contemplados
            return '#ffffff', '#000000'
        
        # PROCESAMIENTO CON ALINEACIÓN DE DATOS DE VISUALIZACIÓN
        # =====================================================
        
        if display_data is not None:
            # MODO CON ALINEACIÓN: Usar ordenación de display_data para row_index
            display_players = display_data['Player'].to_list()
            
            # Iterar sobre cada jugador en la tabla de visualización
            for display_row_idx, display_player in enumerate(display_players):
                
                # BÚSQUEDA DE DATOS Z-SCORE CORRESPONDIENTES
                # =========================================
                
                # Buscar el jugador en los datos de Z-score
                player_zscore_data = zscore_data.filter(pl.col('Player') == display_player)
                
                if player_zscore_data.height > 0:
                    # APLICACIÓN DE COLORES POR MÉTRICA
                    # ================================
                    
                    # Para cada métrica, aplicar color basado en el Z-score
                    for col in metric_columns:
                        if col in player_zscore_data.columns:
                            zscore_value = player_zscore_data[col].to_list()[0]
                            
                            # VALIDACIÓN DE VALOR NUMÉRICO
                            # ============================
                            
                            # Verificar que el valor sea válido (no None, no NaN, y numérico)
                            if (zscore_value is not None and 
                                isinstance(zscore_value, (int, float)) and 
                                not np.isnan(zscore_value)):
                                
                                # Obtener colores basados en Z-score
                                background_color, text_color = get_color_for_zscore(zscore_value)
                                
                                # CONSTRUCCIÓN DEL ESTILO DE CELDA
                                # ===============================
                                
                                # Crear estilo para la celda específica
                                style = {
                                    'if': {
                                        'row_index': display_row_idx,  # Índice alineado con display_data
                                        'column_id': col               # Identificador de métrica
                                    },
                                    'backgroundColor': background_color,
                                    'color': text_color,
                                    'border': '1px solid #ddd'  # Borde consistente
                                }
                                color_styles.append(style)
                            # NOTA: Si el valor no es válido, se usa estilo por defecto (blanco)
                
                # ESTILO ESPECIAL PARA COLUMNA DE JUGADORES
                # ========================================
                
                # Aplicar fondo amarillo distintivo para la columna 'Player'
                player_style = {
                    'if': {
                        'row_index': display_row_idx,
                        'column_id': 'Player'
                    },
                    'backgroundColor': '#fff9c4',  # Fondo amarillo suave
                    'color': '#987b22',            # Texto dorado oscuro
                    'fontWeight': 'bold',          # Texto en negrita
                    'border': '1px solid #ddd'     # Borde consistente
                }
                color_styles.append(player_style)
        else:
            # MODO SIN ALINEACIÓN: Usar ordenación original de zscore_data
            # ===========================================================
            
            for row_idx, player in enumerate(zscore_data['Player'].to_list()):
                
                # APLICACIÓN DE COLORES POR MÉTRICA (MODO FALLBACK)
                # ================================================
                
                for col in metric_columns:
                    if col in zscore_data.columns:
                        zscore_value = zscore_data[col].to_list()[row_idx]
                        
                        # VALIDACIÓN DE VALOR NUMÉRICO (MODO FALLBACK)
                        # ============================================
                        
                        if (zscore_value is not None and 
                            isinstance(zscore_value, (int, float)) and 
                            not np.isnan(zscore_value)):
                            
                            background_color, text_color = get_color_for_zscore(zscore_value)
                            
                            # CONSTRUCCIÓN DEL ESTILO DE CELDA (MODO FALLBACK)
                            # ===============================================
                            
                            style = {
                                'if': {
                                    'row_index': row_idx,  # Índice original de zscore_data
                                    'column_id': col
                                },
                                'backgroundColor': background_color,
                                'color': text_color,
                                'border': '1px solid #ddd'
                            }
                            color_styles.append(style)
                
                # ESTILO ESPECIAL PARA COLUMNA DE JUGADORES (MODO FALLBACK)
                # ========================================================
                
                player_style = {
                    'if': {
                        'row_index': row_idx,
                        'column_id': 'Player'
                    },
                    'backgroundColor': '#fff9c4',  # Fondo amarillo suave
                    'color': '#987b22',            # Texto dorado oscuro
                    'fontWeight': 'bold',          # Texto en negrita
                    'border': '1px solid #ddd'     # Borde consistente
                }
                color_styles.append(player_style)
        
        return color_styles
        
    except Exception as e:
        # MANEJO DE ERRORES ROBUSTO
        # ========================
        
        # En caso de error, registrar información básica para debugging
        print(f"Error al generar esquema de colores: {str(e)}")
        
        # RETORNO SEGURO EN CASO DE ERROR
        # ==============================
        
        # Retornar lista vacía para evitar fallos en la interfaz
        # Esto permite que la tabla se muestre sin colores en lugar de fallar completamente
        return []






def format_and_filter_date_graphs(selected_date):
    """
    FUNCIÓN DEPRECADA - MANTENIDA PARA COMPATIBILIDAD CON GRÁFICOS
    =============================================================
    
    ESTADO: Esta función está desactualizada y pertenece a la versión anterior del proyecto.
    Ya hay una nueva versión de esta función, pero es necesario realizar modificaciones 
    en la parte de los gráficos para que la nueva función funcione.
    Por lo tanto, se mantiene esta función para que los gráficos sigan funcionando.
    
    PROPÓSITO:
    ---------
    Formatea una fecha seleccionada y filtra los datos GPS para esa fecha específica.
    Convierte la fecha del formato de entrada (YYYY-MM-DD) al formato interno (dd/mm/aaaa)
    y filtra el DataFrame para obtener solo los datos de esa fecha.
    
    QUÉ HACE EXACTAMENTE:
    --------------------
    1. Recibe una fecha en formato YYYY-MM-DD o dd/mm/aaaa
    2. Convierte la fecha al formato interno dd/mm/aaaa si es necesario
    3. Carga los datos GPS completos desde el archivo CSV
    4. Filtra los datos para obtener solo las filas de la fecha especificada
    5. Retorna el DataFrame filtrado o None si no hay datos
    
    CÓMO LO HACE:
    ------------
    • Utiliza `format_date()` de utils.date para conversión de formato
    • Carga datos con `get_gps_data()` de utils.data_access
    • Aplica filtro Polars con `pl.col('Date') == formatted_date`
    • Maneja casos donde no existen datos para la fecha
    
    ARCHIVOS UTILIZADOS:
    ------------------
    • utils.date.format_date - Conversión de formatos de fecha
    • utils.data_access.get_gps_data - Carga de datos GPS desde CSV
    
    PARÁMETROS:
    ----------
    selected_date (str): Fecha en formato YYYY-MM-DD o dd/mm/aaaa
    
    RETORNA:
    -------
    polars.DataFrame o None: DataFrame filtrado con datos de la fecha específica,
                            o None si no hay datos para esa fecha
    

    """

    
    # CARGA INICIAL DE DATOS GPS
    # =========================
    
    # Cargar el conjunto completo de datos GPS desde el archivo CSV
    df = load_gps_data()  # Función de utils.data_access
    if df is None:
        # Si no se pueden cargar los datos, retornar valores nulos
        return None, None
        
    # APLICACIÓN DE FILTROS ESTÁNDAR
    # ==============================
    
    # Aplicar filtros de limpieza y validación estándar a los datos
    df_filtered = apply_standard_filters(df)  # Función de utils.data_access
    
    # CONVERSIÓN DE FORMATO DE FECHA
    # =============================
    
    # Convertir la fecha seleccionada al formato interno correcto (dd/mm/yyyy)
    if isinstance(selected_date, str):
        try:
            # INTENTO DE CONVERSIÓN DESDE FORMATO YYYY-MM-DD
            # ==============================================
            
            # Parsear fecha desde formato de entrada estándar
            selected_dt = datetime.strptime(selected_date, '%Y-%m-%d')
            # Convertir al formato interno dd/mm/yyyy
            formatted_date = selected_dt.strftime('%d/%m/%Y')
        except:
            # FALLBACK: ASUMIR QUE YA ESTÁ EN FORMATO CORRECTO
            # ===============================================
            
            # Si falla la conversión, asumir que ya está en formato dd/mm/yyyy
            formatted_date = selected_date
    else:
        # MANEJO DE TIPOS NO STRING
        # ========================
        
        # Si no es string, usar el valor tal como está
        formatted_date = selected_date
        
    # FILTRADO POR FECHA ESPECÍFICA
    # ============================
    
    # Filtrar el DataFrame para obtener solo los datos de la fecha especificada
    df_fecha = df_filtered.filter(pl.col('Date') == formatted_date)
    
    # VALIDACIÓN DE RESULTADOS
    # =======================
    
    # Verificar si se encontraron datos para la fecha especificada
    if df_fecha is None or df_fecha.height == 0:
        # LOGGING PARA DEBUGGING
        # =====================
        
        print("No se encontraron datos para ningún formato de fecha")
        
        # Mostrar muestra de fechas disponibles para facilitar debugging
        available_dates = df.select('Date').unique().limit(5)['Date'].to_list()
        # print(f"Fechas disponibles (muestra): {available_dates}")  # Comentado para producción
        
        # RETORNO PARA CASO SIN DATOS
        # ==========================
        
        return None, None
        
    # RETORNO EXITOSO
    # ==============
    
    # Retornar tanto los datos filtrados como la fecha formateada
    return df_fecha, formatted_date
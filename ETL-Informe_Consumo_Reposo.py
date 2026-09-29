# =============================================================================
# ETL + INFORME EXACTO (diseño idéntico al notebook 3_Informe_final)
# =============================================================================
import os
import sys
import re
import glob
import time
import logging
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta


import polars as pl
import pandas as pd
import numpy as np
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import seaborn as sns

import os
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication


from dotenv import load_dotenv

# Carga variables desde .env en la carpeta del proyecto
load_dotenv(os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env"))

from reportlab.lib.pagesizes import letter
from reportlab.lib import colors
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.platypus import (
    SimpleDocTemplate, Paragraph, Spacer, Image, HRFlowable, PageBreak, Table, TableStyle
)

# =============================================================================
# CONFIGURACIÓN
# =============================================================================
CARPETA_PROYECTO     = os.path.dirname(os.path.abspath(__file__))
RUTA_PARQUETS_ORIGEN = r"G:\Mi unidad\Green_2026\P60"
CARPETA_DATOS_ORIGEN = r"G:\Mi unidad\Green_2026\ISO50001\P_60_HORAS"
ARCHIVO_KML          = os.path.join(CARPETA_DATOS_ORIGEN, "Geocercas_Completas.kml")
LOGO_PATH            = os.path.join(CARPETA_DATOS_ORIGEN, "reporte_elementos", "logo.png")

ARCHIVO_CONSOLIDADO  = os.path.join(CARPETA_PROYECTO, "datos_finales_2026_quietos_PIR.parquet")
ARCHIVO_RESUMEN_HORA = os.path.join(CARPETA_PROYECTO, "resumen_horario_consumo_PIR.parquet")
CARPETA_RECURSOS     = os.path.join(CARPETA_PROYECTO, "reporte_elementos")
ARCHIVO_PDF_FINAL    = os.path.join(CARPETA_PROYECTO, "Informe_Gestion_Energetica_Consumo_Reposo_v3.pdf")

UMBRAL_ENERGIA_MIN   = 0.0
UMBRAL_ENERGIA_MAX   = 10.0
SALTO_SESION_MIN     = 2

os.makedirs(CARPETA_RECURSOS, exist_ok=True)
# Copiar logo al proyecto si existe en origen
if os.path.exists(LOGO_PATH):
    import shutil
    dest_logo = os.path.join(CARPETA_RECURSOS, "logo.png")
    if not os.path.exists(dest_logo):
        shutil.copy2(LOGO_PATH, dest_logo)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    handlers=[
        logging.FileHandler(os.path.join(CARPETA_PROYECTO, "etl_informe.log"), encoding="utf-8"),
        logging.StreamHandler(sys.stdout)
    ]
)
log = logging.getLogger("etl")

# =============================================================================
# 1. GEOCERCAS
# =============================================================================
def extraer_poligonos_kml(ruta_kml):
    with open(ruta_kml, "r", encoding="utf-8") as f:
        root = ET.fromstring(f.read())
    ns = {"kml": "http://www.opengis.net/kml/2.2"}
    poligonos = []
    for pm in root.findall(".//kml:Placemark", ns):
        name_node = pm.find("kml:name", ns)
        nombre = name_node.text.strip() if (name_node is not None and name_node.text) else "Zona Sin Nombre"
        coord_node = pm.find(".//kml:coordinates", ns)
        if coord_node is not None and coord_node.text:
            coords = [(float(c.split(",")[0]), float(c.split(",")[1]))
                      for c in coord_node.text.strip().split()]
            if len(coords) >= 3:
                poligonos.append({"nombre": nombre, "coords": coords})
    return poligonos

def area_aprox(coords):
    lats = [c[1] for c in coords]
    lons = [c[0] for c in coords]
    return (max(lats) - min(lats)) * (max(lons) - min(lons))

def punto_en_poligono_expr(lat_col, lon_col, coords):
    lons = [c[0] for c in coords]
    lats = [c[1] for c in coords]
    min_lat, max_lat = min(lats), max(lats)
    min_lon, max_lon = min(lons), max(lons)
    en_caja = (
        (pl.col(lat_col) >= min_lat) & (pl.col(lat_col) <= max_lat) &
        (pl.col(lon_col) >= min_lon) & (pl.col(lon_col) <= max_lon)
    )
    expr = pl.lit(False)
    for i in range(len(coords) - 1):
        lon_i, lat_i = lons[i], lats[i]
        lon_j, lat_j = lons[i+1], lats[i+1]
        cond = (
            ((pl.col(lat_col) > lat_i) != (pl.col(lat_col) > lat_j)) &
            (pl.col(lon_col) < (lon_j - lon_i) * (pl.col(lat_col) - lat_i) / (lat_j - lat_i + 1e-10) + lon_i)
        )
        expr = expr ^ cond
    return en_caja & expr

def etl_filtro_geocercas():
    log.info("ETL 1/5 – Filtro velocidad 0 + geocercas")
    t0 = time.time()
    poligonos = extraer_poligonos_kml(ARCHIVO_KML)
    poligonos.sort(key=lambda x: area_aprox(x["coords"]), reverse=True)
    archivos = [f for f in glob.glob(os.path.join(RUTA_PARQUETS_ORIGEN, "*.parquet"))
                if re.search(r"2026\d{4}", os.path.basename(f))]
    if not archivos:
        raise FileNotFoundError("No hay archivos 2026")
    df_m = pl.read_parquet(archivos[0], n_rows=1)
    campos = df_m["localizacionVehiculo"].struct.fields
    lat_f, lon_f = campos[0], campos[1]
    lf = pl.concat([pl.scan_parquet(f) for f in archivos], how="vertical_relaxed")
    lf = lf.filter(pl.col("velocidadVehiculo") == 0.0)
    lf = lf.with_columns([
        pl.col("localizacionVehiculo").struct.field(lat_f).cast(pl.Float64).alias("lat_bus"),
        pl.col("localizacionVehiculo").struct.field(lon_f).cast(pl.Float64).alias("lon_bus"),
    ])
    lf = lf.with_columns(nombre_pir=pl.lit(None).cast(pl.Utf8))
    for p in poligonos:
        mask = punto_en_poligono_expr("lat_bus", "lon_bus", p["coords"])
        lf = lf.with_columns(
            nombre_pir=pl.when(mask).then(pl.lit(p["nombre"])).otherwise(pl.col("nombre_pir"))
        )
    df = lf.filter(pl.col("nombre_pir").is_not_null()).collect()
    df.write_parquet(ARCHIVO_CONSOLIDADO)
    log.info(f"Consolidado: {df.height:,} | {round((time.time()-t0)/60,2)} min")

def etl_resumen_horario():
    log.info("ETL 2/5 – Resumen horario")
    t0 = time.time()
    lf = pl.scan_parquet(ARCHIVO_CONSOLIDADO)
    lf = lf.with_columns(
        pl.col("fechaHoraLecturaDato").str.to_datetime(format="%d/%m/%Y %H:%M:%S%.f", strict=False)
    ).sort(["idVehiculo", "nombre_pir", "fechaHoraLecturaDato"]).with_columns(
        dia_semana_num=pl.col("fechaHoraLecturaDato").dt.weekday()
    )
    lf = lf.with_columns(
        diferencia_tiempo=(
            (pl.col("fechaHoraLecturaDato") - pl.col("fechaHoraLecturaDato").shift(1))
            .dt.total_minutes()
        ).over(["idVehiculo", "nombre_pir"])
    ).with_columns(
        es_nuevo_bloque=(pl.col("diferencia_tiempo").is_null()) | (pl.col("diferencia_tiempo") > SALTO_SESION_MIN)
    ).with_columns(
        sub_sesion_id=pl.col("es_nuevo_bloque").cum_sum().over(["idVehiculo", "nombre_pir"])
    )
    lf = lf.with_columns(
        delta_energia=(pl.col("consumoEnergia") - pl.col("consumoEnergia").shift(1))
                      .over(["idVehiculo", "nombre_pir", "sub_sesion_id"]),
        delta_t_minutos=pl.col("diferencia_tiempo")
    ).with_columns(
        delta_energia=pl.when(pl.col("delta_energia") < 0).then(0.0)
                        .otherwise(pl.col("delta_energia")).fill_null(0.0),
        delta_t_minutos=pl.col("delta_t_minutos").fill_null(0.0)
    )
    df = (
        lf.with_columns(
            hora_grupo=pl.col("fechaHoraLecturaDato").dt.truncate("1h"),
            dia_semana_nombre=pl.col("fechaHoraLecturaDato").dt.strftime("%A").replace({
                "Monday": "Lunes", "Tuesday": "Martes", "Wednesday": "Miércoles",
                "Thursday": "Jueves", "Friday": "Viernes", "Saturday": "Sábado", "Sunday": "Domingo"
            })
        )
        .group_by(["hora_grupo", "dia_semana_num", "dia_semana_nombre", "idVehiculo", "nombre_pir"])
        .agg([
            pl.col("delta_energia").sum().alias("energia_consumida_kWh"),
            pl.col("delta_t_minutos").sum().alias("tiempo_estancia_minutos"),
            pl.len().alias("cantidad_registros_minuto")
        ])
        .sort(["hora_grupo", "nombre_pir", "idVehiculo"])
        .collect()
    )
    df.write_parquet(ARCHIVO_RESUMEN_HORA)
    log.info(f"Resumen horario: {df.height:,} | {round((time.time()-t0)/60,2)} min")

# =============================================================================
# 3. MATRICES (lógica EXACTA del notebook original – left joins)
# =============================================================================
def preparar_matrices():
    log.info("ETL 3/5 – Matrices de desempeño (lógica original)")
    lf_limpio = pl.scan_parquet(ARCHIVO_RESUMEN_HORA).filter(
        (pl.col("energia_consumida_kWh") > UMBRAL_ENERGIA_MIN) &
        (pl.col("energia_consumida_kWh") <= UMBRAL_ENERGIA_MAX)
    )
    fecha_maxima = lf_limpio.select(pl.col("hora_grupo").dt.date().max()).collect().item()

    dias_al_ultimo_domingo = (fecha_maxima.weekday() + 1) % 7
    fin_semana_actual = fecha_maxima - timedelta(days=dias_al_ultimo_domingo) if dias_al_ultimo_domingo != 0 else fecha_maxima
    inicio_semana_actual = fin_semana_actual - timedelta(days=6)
    fin_semana_anterior = inicio_semana_actual - timedelta(days=1)
    inicio_semana_anterior = fin_semana_anterior - timedelta(days=6)

    if fecha_maxima.day < 28:
        mes_3_num = 12 if fecha_maxima.month == 1 else fecha_maxima.month - 1
        año_3 = fecha_maxima.year - 1 if fecha_maxima.month == 1 else fecha_maxima.year
    else:
        mes_3_num = fecha_maxima.month
        año_3 = fecha_maxima.year
    mes_2_num = 12 if mes_3_num == 1 else mes_3_num - 1
    año_2 = año_3 - 1 if mes_3_num == 1 else año_3
    mes_1_num = 12 if mes_2_num == 1 else mes_2_num - 1
    año_1 = año_2 - 1 if mes_2_num == 1 else año_2

    meses_es = {1: "ENERO", 2: "FEBRERO", 3: "MARZO", 4: "ABRIL", 5: "MAYO", 6: "JUNIO",
                7: "JULIO", 8: "AGOSTO", 9: "SEPTIEMBRE", 10: "OCTUBRE", 11: "NOVIEMBRE", 12: "DICIEMBRE"}
    etiqueta_mes_3 = f"{meses_es[mes_3_num]} {año_3}"
    etiqueta_mes_2 = f"{meses_es[mes_2_num]} {año_2}"
    etiqueta_mes_1 = f"{meses_es[mes_1_num]} {año_1}"
    etiqueta_rango_semana = f"{inicio_semana_actual.strftime('%d-%b')} al {fin_semana_actual.strftime('%d-%b')}"

    lf_dimensionado = lf_limpio.with_columns(
        fecha=pl.col("hora_grupo").dt.date(),
        año=pl.col("hora_grupo").dt.year(),
        mes=pl.col("hora_grupo").dt.month()
    )
    lf_bus_hora = lf_dimensionado.group_by(["nombre_pir", "fecha", "año", "mes", "idVehiculo"]).agg(
        pl.col("energia_consumida_kWh").sum().alias("energia_bus_hora_real")
    )

    df_mes_3 = lf_bus_hora.filter((pl.col("año") == año_3) & (pl.col("mes") == mes_3_num)).group_by("nombre_pir").agg(pl.col("energia_bus_hora_real").sum().alias("Mes_3_kWh"))
    df_mes_2 = lf_bus_hora.filter((pl.col("año") == año_2) & (pl.col("mes") == mes_2_num)).group_by("nombre_pir").agg(pl.col("energia_bus_hora_real").sum().alias("Mes_2_kWh"))
    df_mes_1 = lf_bus_hora.filter((pl.col("año") == año_1) & (pl.col("mes") == mes_1_num)).group_by("nombre_pir").agg(pl.col("energia_bus_hora_real").sum().alias("Mes_1_kWh"))
    df_sem_act = lf_bus_hora.filter(pl.col("fecha").is_between(inicio_semana_actual, fin_semana_actual)).group_by("nombre_pir").agg(pl.col("energia_bus_hora_real").sum().alias("Semana_Actual_kWh"))
    df_sem_ant = lf_bus_hora.filter(pl.col("fecha").is_between(inicio_semana_anterior, fin_semana_anterior)).group_by("nombre_pir").agg(pl.col("energia_bus_hora_real").sum().alias("Semana_Anterior_kWh"))

    df_pids_totales = lf_bus_hora.select(pl.col("nombre_pir").unique())
    df_cuadro = (df_pids_totales
                 .join(df_mes_1, on="nombre_pir", how="left")
                 .join(df_mes_2, on="nombre_pir", how="left")
                 .join(df_mes_3, on="nombre_pir", how="left")
                 .join(df_sem_ant, on="nombre_pir", how="left")
                 .join(df_sem_act, on="nombre_pir", how="left")
                 .with_columns(pl.all().exclude("nombre_pir").fill_null(0.0)))
    df_base = df_cuadro.collect().to_pandas()

    def calcular_desviacion(actual, anterior):
        if anterior == 0.0:
            return 0.0 if actual == 0.0 else 100.0
        return ((actual - anterior) / anterior) * 100.0

    df_base["raw_desv_1"] = df_base.apply(lambda r: calcular_desviacion(r["Mes_2_kWh"], r["Mes_1_kWh"]), axis=1)
    df_base["raw_desv_2"] = df_base.apply(lambda r: calcular_desviacion(r["Mes_3_kWh"], r["Mes_2_kWh"]), axis=1)

    col_m1 = f"CONSUMO {etiqueta_mes_1} (kWh)"
    col_m2 = f"CONSUMO {etiqueta_mes_2} (kWh)"
    col_m3 = f"CONSUMO {etiqueta_mes_3} (kWh)"
    col_desv_1 = f"Desv {meses_es[mes_1_num].title()} vs {meses_es[mes_2_num].title()}"
    col_desv_2 = f"Desv {meses_es[mes_2_num].title()} vs {meses_es[mes_3_num].title()}"

    df_final_mensual = pd.DataFrame()
    df_final_mensual["PIR"] = df_base["nombre_pir"].str.upper()
    df_final_mensual[col_m1] = df_base["Mes_1_kWh"].map("{:,.1f}".format)
    df_final_mensual[col_m2] = df_base["Mes_2_kWh"].map("{:,.1f}".format)
    df_final_mensual[col_m3] = df_base["Mes_3_kWh"].map("{:,.1f}".format)
    df_final_mensual[col_desv_1] = df_base["raw_desv_1"].map("{:+.1f}%".format)
    df_final_mensual[col_desv_2] = df_base["raw_desv_2"].map("{:+.1f}%".format)
    df_final_mensual = df_final_mensual.sort_values(
        by=col_m3, key=lambda x: x.str.replace(",", "").astype(float), ascending=False
    ).reset_index(drop=True)

    df_base["Δ Sem %"] = df_base.apply(lambda r: calcular_desviacion(r["Semana_Actual_kWh"], r["Semana_Anterior_kWh"]), axis=1)
    df_final_semanal = pd.DataFrame()
    df_final_semanal["INSTALACIÓN / PIR"] = df_base["nombre_pir"].str.upper()
    df_final_semanal["SEMANA ANTERIOR (kWh)"] = df_base["Semana_Anterior_kWh"].map("{:,.1f}".format)
    df_final_semanal["SEMANA ACTUAL (kWh)"] = df_base["Semana_Actual_kWh"].map("{:,.1f}".format)
    df_final_semanal["VARIACIÓN %"] = df_base["Δ Sem %"].map("{:+.1f}%".format)
    df_final_semanal = df_final_semanal.sort_values(
        by="SEMANA ACTUAL (kWh)", key=lambda x: x.str.replace(",", "").astype(float), ascending=False
    ).reset_index(drop=True)

    return {
        "df_final_mensual": df_final_mensual,
        "df_final_semanal": df_final_semanal,
        "etiqueta_mes_1": etiqueta_mes_1,
        "etiqueta_mes_2": etiqueta_mes_2,
        "etiqueta_mes_3": etiqueta_mes_3,
        "etiqueta_rango_semana": etiqueta_rango_semana,
        "inicio_semana_actual": inicio_semana_actual,
        "fin_semana_actual": fin_semana_actual,
    }

# =============================================================================
# 4. GRÁFICOS (EXACTOS del notebook – seaborn)
# =============================================================================

def limpiar_recursos(excepto_logo=True):
    """Borra PNG y TXT de reporte_elementos, deja solo logo.png."""
    if not os.path.isdir(CARPETA_RECURSOS):
        return
    for f in os.listdir(CARPETA_RECURSOS):
        if excepto_logo and f.lower() == "logo.png":
            continue
        if f.lower().endswith((".png", ".txt")):
            try:
                os.remove(os.path.join(CARPETA_RECURSOS, f))
            except OSError:
                pass




def generar_graficos(ctx):
    limpiar_recursos()
    log.info("ETL 4/5 – Curvas y heatmaps (seaborn original)")
    sns.set_theme(style="whitegrid")
    lf_limpio = pl.scan_parquet(ARCHIVO_RESUMEN_HORA).filter(
        (pl.col("energia_consumida_kWh") > 0.0) & (pl.col("energia_consumida_kWh") <= 10.0)
    )
    ini, fin = ctx["inicio_semana_actual"], ctx["fin_semana_actual"]
    etq = ctx["etiqueta_rango_semana"]
    lista_horas_str = [f"{h:02d}:00" for h in range(24)]
    orden_dias = ["Lunes", "Martes", "Miércoles", "Jueves", "Viernes", "Sábado", "Domingo"]
    dias_semana_es = {1: "Lunes", 2: "Martes", 3: "Miércoles", 4: "Jueves", 5: "Viernes", 6: "Sábado", 7: "Domingo"}

    lf_dim = lf_limpio.with_columns(
        fecha=pl.col("hora_grupo").dt.date(),
        hora_dia=pl.col("hora_grupo").dt.hour(),
        dia_num=pl.col("hora_grupo").dt.weekday()
    ).filter(pl.col("fecha").is_between(ini, fin))

    # --- Curvas: promedio diario por hora (lógica original) ---
    lf_dia = lf_dim.group_by(["nombre_pir", "fecha", "hora_dia"]).agg(
        pl.col("energia_consumida_kWh").sum().alias("energia_total_punto_dia")
    )
    df_curvas = (
        lf_dia.group_by(["nombre_pir", "hora_dia"])
        .agg(pl.col("energia_total_punto_dia").mean().alias("energia_bruta_punto"))
        .sort(["nombre_pir", "hora_dia"]).collect().to_pandas()
    )
    df_curvas["nombre_pir"] = df_curvas["nombre_pir"].str.upper()

    puntos_criticos = ["CENTRO LOGISTICO GREEN", "MANTENIMIENTO"]
    df_gigantes = df_curvas[df_curvas["nombre_pir"].isin(puntos_criticos)].copy()
    df_demas = df_curvas[~df_curvas["nombre_pir"].isin(puntos_criticos)].copy()
    orden_gigantes = df_gigantes.groupby("nombre_pir")["energia_bruta_punto"].mean().sort_values(ascending=False).index.tolist()

    # Curva patios
    fig1, ax1 = plt.subplots(figsize=(15, 6.5))
    sns.lineplot(data=df_gigantes, x="hora_dia", y="energia_bruta_punto", hue="nombre_pir",
                 hue_order=orden_gigantes, marker="o", markersize=8, linewidth=3.5,
                 palette=["#1F4E79", "#ED7D31"], ax=ax1)
    ax1.set_title(f"Perfil de Carga Energética Bruta: Patios Principales (Velocidad 0)\nConsumo Acumulado del Punto • Período: {etq}",
                  fontsize=14, fontweight="bold", pad=15, color="#1F4E79")
    ax1.set_xlabel("Hora del Día", fontsize=11, fontweight="bold", labelpad=10)
    ax1.set_ylabel("Energía Total Consumida en el Punto (kWh / Hora)", fontsize=11, fontweight="bold", labelpad=10)
    ax1.set_xlim(-0.5, 23.5)
    ax1.set_xticks(range(24))
    ax1.set_xticklabels(lista_horas_str, rotation=45, ha="right")
    ax1.legend(title="Instalaciones Bajo Análisis", bbox_to_anchor=(1.02, 1), loc="upper left", fontsize=10)
    plt.savefig(os.path.join(CARPETA_RECURSOS, "03_curva_bruta_patios_principales.png"), dpi=300, bbox_inches="tight")
    plt.close()

    # Curva vías Top 5 + resto
    top_5 = df_demas.groupby("nombre_pir")["energia_bruta_punto"].mean().nlargest(5).index.tolist()
    df_agr = df_demas.copy()
    df_agr["categoria_pir"] = df_agr["nombre_pir"].apply(lambda x: x if x in top_5 else "OTRAS GEOCERCAS (CONSOLIDADO)")
    df_opt = df_agr.groupby(["categoria_pir", "hora_dia"])["energia_bruta_punto"].mean().reset_index()
    orden_cat = top_5 + ["OTRAS GEOCERCAS (CONSOLIDADO)"]
    paleta = ["#1F4E79", "#D9534F", "#ED7D31", "#2E75B6", "#70AD47", "#7F7F7F"]

    fig2, ax2 = plt.subplots(figsize=(15, 7.0))
    sns.lineplot(data=df_opt, x="hora_dia", y="energia_bruta_punto", hue="categoria_pir",
                 hue_order=orden_cat, marker="s", markersize=6, linewidth=2.5, palette=paleta, ax=ax2)
    for line in ax2.get_lines():
        if line.get_label() == "OTRAS GEOCERCAS (CONSOLIDADO)":
            line.set_linestyle("--")
            line.set_linewidth(1.8)
            line.set_alpha(0.8)
    ax2.set_title(f"Perfil de Carga Energética Bruta: Puntos Operativos y Vías (Top Impacto vs Resto)\nConsumo Acumulado por Punto • Período: {etq}",
                  fontsize=14, fontweight="bold", pad=15, color="#1F4E79")
    ax2.set_xlabel("Hora del Día", fontsize=11, fontweight="bold", labelpad=10)
    ax2.set_ylabel("Energía Total Consumida en el Punto (kWh / Hora)", fontsize=11, fontweight="bold", labelpad=10)
    ax2.set_xlim(-0.5, 23.5)
    ax2.set_xticks(range(24))
    ax2.set_xticklabels(lista_horas_str, rotation=45, ha="right")
    ax2.legend(title="Geocercas PIR (Top 5 + Resto)", bbox_to_anchor=(1.02, 1), loc="upper left", fontsize=10)
    plt.savefig(os.path.join(CARPETA_RECURSOS, "04_curva_bruta_puntos_operativos.png"), dpi=300, bbox_inches="tight")
    plt.close()

    # Heatmaps patios
    sns.set_theme(style="white")
    lf_hm = lf_dim.filter(pl.col("nombre_pir").str.to_uppercase().is_in(puntos_criticos))
    df_hm = (
        lf_hm.group_by(["nombre_pir", "dia_num", "hora_dia"])
        .agg(pl.col("energia_consumida_kWh").sum().alias("energia_total_hora"))
        .collect().to_pandas()
    )
    df_hm["Dia_Nombre"] = df_hm["dia_num"].map(dias_semana_es)
    df_hm["nombre_pir"] = df_hm["nombre_pir"].str.upper()

    def heatmap_punto(df_punto, nombre_punto, prefijo):
        pivot = df_punto.pivot(index="Dia_Nombre", columns="hora_dia", values="energia_total_hora")
        pivot = pivot.reindex(orden_dias).reindex(columns=range(24)).fillna(0.0)
        pivot.columns = lista_horas_str
        fig, ax = plt.subplots(figsize=(18, 6))
        sns.heatmap(pivot, annot=True, fmt=".1f", cmap="YlOrRd", linewidths=0.4,
                    cbar_kws={"label": "Energía Bruta Consumida (kWh)"}, ax=ax)
        ax.set_title(f"Distribución de Energía Bruta Consumida \nInstalación: {nombre_punto} (Velocidad 0) • Período: {etq}",
                     fontsize=13, fontweight="bold", pad=15, color="#1F4E79")
        ax.set_xlabel("Hora del Día", fontsize=11, fontweight="bold", labelpad=10)
        ax.set_ylabel("Día de la Semana", fontsize=11, fontweight="bold", labelpad=10)
        plt.xticks(rotation=45, ha="right")
        file_enc = nombre_punto.lower().replace(" ", "_")
        plt.savefig(os.path.join(CARPETA_RECURSOS, f"{prefijo}_heatmap_energia_bruta_{file_enc}.png"), dpi=300, bbox_inches="tight")
        plt.close()

    heatmap_punto(df_hm[df_hm["nombre_pir"] == "CENTRO LOGISTICO GREEN"], "CENTRO LOGISTICO GREEN", "05")
    heatmap_punto(df_hm[df_hm["nombre_pir"] == "MANTENIMIENTO"], "MANTENIMIENTO", "05")

    # Heatmaps top 3 vías
    lf_vias = lf_dim.filter(~pl.col("nombre_pir").str.to_uppercase().is_in(puntos_criticos))
    top_3 = (
        lf_vias.group_by("nombre_pir")
        .agg(pl.col("energia_consumida_kWh").sum().alias("total_semanal"))
        .sort("total_semanal", descending=True).limit(3).collect()["nombre_pir"].to_list()
    )
    log.info(f"Top 3 PIR vía: {top_3}")
    df_vias = (
        lf_vias.filter(pl.col("nombre_pir").is_in(top_3))
        .group_by(["nombre_pir", "dia_num", "hora_dia"])
        .agg(pl.col("energia_consumida_kWh").sum().alias("energia_total_hora"))
        .collect().to_pandas()
    )
    df_vias["Dia_Nombre"] = df_vias["dia_num"].map(dias_semana_es)
    df_vias["nombre_pir"] = df_vias["nombre_pir"].str.upper()

    for idx, pir_name in enumerate(top_3):
        nombre = pir_name.upper()
        df_p = df_vias[df_vias["nombre_pir"] == nombre]
        pivot = df_p.pivot(index="Dia_Nombre", columns="hora_dia", values="energia_total_hora")
        pivot = pivot.reindex(orden_dias).reindex(columns=range(24)).fillna(0.0)
        pivot.columns = lista_horas_str
        fig, ax = plt.subplots(figsize=(18, 5.8))
        sns.heatmap(pivot, annot=True, fmt=".1f", cmap="YlOrRd", linewidths=0.4,
                    cbar_kws={"label": "Energía Bruta Consumida (kWh)"}, ax=ax)
        ax.set_title(f"Distribución de Energía Bruta Consumida - Puntos de Operación en Vía\nInstalación: {nombre} (Velocidad 0) • Período: {etq}",
                     fontsize=13, fontweight="bold", pad=15, color="#1F4E79")
        ax.set_xlabel("Hora del Día", fontsize=11, fontweight="bold", labelpad=10)
        ax.set_ylabel("Día de la Semana", fontsize=11, fontweight="bold", labelpad=10)
        plt.xticks(rotation=45, ha="right")
        file_enc = nombre.lower().replace(" ", "_")
        plt.savefig(os.path.join(CARPETA_RECURSOS, f"{7+idx:02d}_heatmap_vias_{file_enc}.png"), dpi=300, bbox_inches="tight")
        plt.close()

    # Textos de conclusión (fallback = mismos del PDF adjunto)
    with open(os.path.join(CARPETA_RECURSOS, "10_1_conclusion_curvas_patios.txt"), "w", encoding="utf-8") as f:
        f.write("El Centro Logístico presenta una demanda horaria altamente fluctuante con un pico de consumo muy elevado de 68 kWh a las 04:00 y otro pico notable de 34 kWh a las 22:00, lo que sugiere actividades intensivas de alistamiento nocturno.")
    with open(os.path.join(CARPETA_RECURSOS, "10_2_conclusion_heatmaps_patios.txt"), "w", encoding="utf-8") as f:
        f.write("La mayor concentración de densidad térmica de consumo en ambas instalaciones ocurre de lunes a jueves entre las 03:00 y 05:00, siendo el Centro Logístico el que presenta los picos más críticos, especialmente el miércoles de 03:00 a 04:00.")
    with open(os.path.join(CARPETA_RECURSOS, "11_1_conclusion_curvas_vias.txt"), "w", encoding="utf-8") as f:
        f.write("La demanda de energía bruta en los puntos de operación en vía fluctúa con picos notables en la madrugada (04:00-07:00) y al mediodía (12:00-15:00) durante los días hábiles.")
    with open(os.path.join(CARPETA_RECURSOS, "11_2_conclusion_heatmaps_vias.txt"), "w", encoding="utf-8") as f:
        f.write("PIR Puente Grande y CocaCola son las instalaciones más críticas por su elevado consumo en reposo, con franjas complejas de mayor demanda en madrugada (04:00-07:00) y mediodía (12:00-15:00), destacando 17.0 kWh en Puente Grande el jueves a las 13:00.")
    generar_conclusiones_ia(ctx)
    log.info("Gráficos y conclusiones generados")

# =============================================================================
# 5. PDF (EXACTO del notebook – ReportLab letter + estilos originales)
# =============================================================================

def generar_conclusiones_ia(ctx):
    """
    Analiza las gráficas con Gemini (API key desde .env).
    Si falla o no hay clave, usa el contenido actual de los .txt de respaldo.
    Imprime si funcionó o no.
    """
    import base64
    import requests

    nombres = [
        "10_1_conclusion_curvas_patios.txt",
        "10_2_conclusion_heatmaps_patios.txt",
        "11_1_conclusion_curvas_vias.txt",
        "11_2_conclusion_heatmaps_vias.txt",
    ]
    defaults = {
        nombres[0]: "Análisis de curvas de patio no disponible.",
        nombres[1]: "Análisis de mapas de calor de patio no disponible.",
        nombres[2]: "Análisis de curvas en vía no disponible.",
        nombres[3]: "Análisis de mapas de calor de los 3 PIR no disponible.",
    }

    def ruta(n):
        return os.path.join(CARPETA_RECURSOS, n)

    def leer_respaldo(n):
        r = ruta(n)
        if os.path.exists(r):
            with open(r, "r", encoding="utf-8") as f:
                t = f.read().strip()
                if t:
                    return t
        return defaults[n]

    def escribir(n, texto):
        with open(ruta(n), "w", encoding="utf-8") as f:
            f.write(texto.strip())

    respaldo = {n: leer_respaldo(n) for n in nombres}

    API_KEY = os.environ.get("GEMINI_API_KEY", "").strip()
    if not API_KEY:
        log.warning("⚠️ GEMINI_API_KEY no definida en .env – textos de respaldo")
        for n, t in respaldo.items():
            escribir(n, t)
        print("⚠️ Análisis IA (Gemini): NO FUNCIONÓ – sin API key")
        return

    url = (
        "https://generativelanguage.googleapis.com/v1beta/models/"
        f"gemini-2.5-flash:generateContent?key={API_KEY}"
    )

    def encodear(p):
        with open(p, "rb") as f:
            return base64.b64encode(f.read()).decode("utf-8")

  

    def pedir(parts, intentos=3):
            # Deshabilitar advertencias de SSL no verificado en la consola
            import urllib3
            urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

            headers = {
                "Content-Type": "application/json",
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
            }

            for i in range(intentos):
                try:
                    r = requests.post(
                        url,
                        json={"contents": [{"parts": parts}]},
                        headers=headers,
                        timeout=90,
                        verify=False  # Salta el bloqueo del proxy/firewall SSL corporativo
                    )
                    if r.status_code == 200:
                        return r.json()["candidates"][0]["content"]["parts"][0]["text"]
                    
                    if r.status_code in [503, 429]:
                        log.warning(f"Gemini {r.status_code} – reintento {i + 1}/{intentos}")
                        time.sleep(5)
                        continue
                    
                    log.warning(f"Gemini HTTP {r.status_code}: {r.text[:200]}")
                    break
                except Exception as e:
                    log.warning(f"Gemini error de red (intento {i + 1}/{intentos}): {e}")
                    time.sleep(3) # Espera 3 segundos antes del reintento
            return None





    ia_ok = True
    try:
        archivos = os.listdir(CARPETA_RECURSOS) if os.path.isdir(CARPETA_RECURSOS) else []

        # ----- PATIOS -----
        parts_p = [{
            "text": (
                "Actúa como un Auditor Senior de Eficiencia Energética bajo la norma ISO 50001. "
                "Analiza las gráficas adjuntas del CENTRO LOGÍSTICO y MANTENIMIENTO (Velocidad 0). "
                "Entrega exactamente DOS (2) conclusiones cortas, separadas por un salto de línea, "
                "sin viñetas, números ni Markdown:\n"
                "Línea 1: comportamiento de la demanda horaria y picos en la curva de carga.\n"
                "Línea 2: concentración térmica en los mapas de calor (días u horas críticos).\n"
                "Sin recomendaciones, introducciones ni saludos."
            )
        }]
        for n in [
            "03_curva_bruta_patios_principales.png",
            "05_heatmap_energia_bruta_centro_logistico_green.png",
            "05_heatmap_energia_bruta_mantenimiento.png",
        ]:
            p = ruta(n)
            if os.path.exists(p):
                parts_p.append({"inlineData": {"mimeType": "image/png", "data": encodear(p)}})

        txt_p = pedir(parts_p)
        lineas_p = [x.strip() for x in (txt_p or "").split("\n") if x.strip()]
        if len(lineas_p) < 2:
            ia_ok = False
        escribir(nombres[0], lineas_p[0] if len(lineas_p) > 0 else respaldo[nombres[0]])
        escribir(nombres[1], lineas_p[1] if len(lineas_p) > 1 else respaldo[nombres[1]])

        # ----- VÍAS -----
        parts_v = [{
            "text": (
                "Actúa como un Auditor Senior de Eficiencia Energética bajo la norma ISO 50001. "
                "Analiza las gráficas de puntos en vía (Velocidad 0). "
                "Entrega exactamente DOS (2) conclusiones cortas, separadas por un salto de línea, "
                "sin viñetas, números ni Markdown:\n"
                "Línea 1: fluctuación y picos de la curva de vías.\n"
                "Línea 2: instalaciones más críticas y franjas horarias en los mapas de calor.\n"
                "Sin recomendaciones, introducciones ni saludos."
            )
        }]
        for f in archivos:
            if f.endswith(".png") and ("via" in f.lower() or "puntos_operativos" in f.lower()):
                parts_v.append({
                    "inlineData": {
                        "mimeType": "image/png",
                        "data": encodear(ruta(f)),
                    }
                })

        txt_v = pedir(parts_v)
        lineas_v = [x.strip() for x in (txt_v or "").split("\n") if x.strip()]
        if len(lineas_v) < 2:
            ia_ok = False
        escribir(nombres[2], lineas_v[0] if len(lineas_v) > 0 else respaldo[nombres[2]])
        escribir(nombres[3], lineas_v[1] if len(lineas_v) > 1 else respaldo[nombres[3]])

    except Exception as e:
        ia_ok = False
        log.warning(f"Gemini excepción: {e}")
        for n, t in respaldo.items():
            escribir(n, t)

    if ia_ok:
        log.info(" Análisis IA (Gemini): FUNCIONÓ – conclusiones desde las gráficas")
        print(" Análisis IA (Gemini): FUNCIONÓ")
    else:
        log.warning(" Análisis IA (Gemini): NO FUNCIONÓ – textos desde .txt de respaldo")
        print(" Análisis IA (Gemini): NO FUNCIONÓ – textos desde .txt de respaldo")

        
def generar_pdf(ctx):
    log.info("ETL 5/5 – PDF corporativo (diseño original)")
    archivos_en_carpeta = os.listdir(CARPETA_RECURSOS)

    def buscar_archivo_por_patron(patron):
        for archivo in archivos_en_carpeta:
            if re.search(patron, archivo, re.IGNORECASE):
                return os.path.join(CARPETA_RECURSOS, archivo)
        return None

    def leer_txt(patron, default=""):
        r = buscar_archivo_por_patron(patron)
        return open(r, "r", encoding="utf-8").read() if r else default

    c1_texto = leer_txt(r"10_1_conclusion_curvas_patios")
    c2_texto = leer_txt(r"10_2_conclusion_heatmaps_patios")
    c3_texto = leer_txt(r"11_1_conclusion_curvas_vias")
    c4_texto = leer_txt(r"11_2_conclusion_heatmaps_vias")
    ruta_logo = buscar_archivo_por_patron(r"logo\.png") or LOGO_PATH

    etiqueta_mes_1 = ctx["etiqueta_mes_1"]
    etiqueta_mes_2 = ctx["etiqueta_mes_2"]
    etiqueta_mes_3 = ctx["etiqueta_mes_3"]
    etiqueta_rango_semana = ctx["etiqueta_rango_semana"]
    df_final_mensual = ctx["df_final_mensual"]
    df_final_semanal = ctx["df_final_semanal"]

    doc = SimpleDocTemplate(ARCHIVO_PDF_FINAL, pagesize=letter,
                            rightMargin=40, leftMargin=40, topMargin=50, bottomMargin=35)
    estilos = getSampleStyleSheet()
    COLOR_PRIMARIO = colors.HexColor("#1F4E79")
    COLOR_SECUNDARIO = colors.HexColor("#2E75B6")
    COLOR_TEXTO = colors.HexColor("#262626")
    COLOR_MUTED = colors.HexColor("#595959")
    HEX_AHORRO_BG = colors.HexColor("#E2EFDA")
    HEX_AHORRO_FG = colors.HexColor("#375623")
    HEX_EXCESO_BG = colors.HexColor("#FCE4D6")
    HEX_EXCESO_FG = colors.HexColor("#C00000")
    HEX_ESTABLE_BG = colors.HexColor("#FFF2CC")
    HEX_ESTABLE_FG = colors.HexColor("#7F6000")

    estilo_titulo = ParagraphStyle("DocTitulo", parent=estilos["Heading1"], fontName="Helvetica-Bold",
                                   fontSize=18, leading=22, textColor=COLOR_PRIMARIO, spaceAfter=2)
    estilo_subtitulo = ParagraphStyle("DocSubTitulo", parent=estilos["Normal"], fontName="Helvetica",
                                      fontSize=9, leading=12, textColor=COLOR_MUTED, spaceAfter=6)
    estilo_h1 = ParagraphStyle("SeccionH1", parent=estilos["Heading2"], fontName="Helvetica-Bold",
                               fontSize=12.5, leading=16, textColor=COLOR_PRIMARIO, spaceBefore=4, spaceAfter=3, keepWithNext=True)
    estilo_h2 = ParagraphStyle("SeccionH2", parent=estilos["Heading3"], fontName="Helvetica-Bold",
                               fontSize=10.5, leading=14, textColor=COLOR_SECUNDARIO, spaceBefore=4, spaceAfter=2, keepWithNext=True)
    estilo_parrafo = ParagraphStyle("TextoCuerpo", parent=estilos["BodyText"], fontName="Helvetica",
                                    fontSize=9, leading=12.5, textColor=COLOR_TEXTO, alignment=4, spaceAfter=5)
    estilo_nota_grafica = ParagraphStyle("NotaGrafica", parent=estilos["Normal"], fontName="Helvetica-Oblique",
                                         fontSize=8, leading=10, textColor=COLOR_MUTED, spaceBefore=1, spaceAfter=3, alignment=4)
    estilo_th = ParagraphStyle("CeldaTH", fontName="Helvetica-Bold", fontSize=8, leading=10, textColor=colors.white, alignment=1)

    def incrustar_logo_y_footer(canvas, doc_obj):
        canvas.saveState()
        if ruta_logo and os.path.exists(ruta_logo):
            canvas.drawImage(ruta_logo, 572 - 65, 792 - 20 - 26, width=65, height=26, mask="auto")
        canvas.setFont("Helvetica", 8)
        canvas.setFillColor(colors.HexColor("#595959"))
        canvas.drawString(40, 18, "greenmóvil — Sistema de Gestión de Eficiencia Energética ISO 50001")
        canvas.drawRightString(572, 18, f"Página {doc_obj.page}")
        canvas.restoreState()

    def generar_tabla_desde_df(df, es_mensual=False):
        df_temp = df.copy()
        if es_mensual:
            nuevas_cols = ["PIR"]
            for c in df_temp.columns[1:4]:
                mes_año = c.replace("CONSUMO ", "").replace(" (kWh)", "").strip()
                nuevas_cols.append(f"{mes_año} (kWh)")
            nuevas_cols += ["Δ% (M1 vs M2)", "Δ% (M2 vs M3)"]
            df_temp.columns = nuevas_cols
        else:
            df_temp.columns = ["INSTALACIÓN / PIR", "SEM. ANTERIOR (kWh)", "SEM. ACTUAL (kWh)", "VARIACIÓN %"]

        matriz = [[Paragraph(str(col), estilo_th) for col in df_temp.columns]]
        estilo_celdas = [
            ("BACKGROUND", (0, 0), (-1, 0), COLOR_PRIMARIO),
            ("ALIGN", (0, 0), (-1, -1), "CENTER"),
            ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
            ("TOPPADDING", (0, 0), (-1, -1), 3),
            ("BOTTOMPADDING", (0, 0), (-1, -1), 3),
            ("GRID", (0, 0), (-1, -1), 0.5, colors.HexColor("#D6D6D6")),
        ]
        for i_fila, fila in enumerate(df_temp.values.tolist()):
            num_fila_pdf = i_fila + 1
            fila_limpia = []
            for num_col, celda in enumerate(fila):
                texto_celda = str(celda).strip()
                fila_limpia.append(texto_celda)
                es_col_desvio = (es_mensual and num_col >= 4) or (not es_mensual and num_col == len(fila) - 1)
                if es_col_desvio:
                    if "-" in texto_celda and texto_celda not in ["-0.0%", "-0.1%", "-0.2%"]:
                        estilo_celdas += [("BACKGROUND", (num_col, num_fila_pdf), (num_col, num_fila_pdf), HEX_AHORRO_BG),
                                         ("TEXTCOLOR", (num_col, num_fila_pdf), (num_col, num_fila_pdf), HEX_AHORRO_FG)]
                    elif "+" in texto_celda and texto_celda not in ["+0.0%", "+0.1%", "+0.2%"]:
                        estilo_celdas += [("BACKGROUND", (num_col, num_fila_pdf), (num_col, num_fila_pdf), HEX_EXCESO_BG),
                                         ("TEXTCOLOR", (num_col, num_fila_pdf), (num_col, num_fila_pdf), HEX_EXCESO_FG)]
                    else:
                        estilo_celdas += [("BACKGROUND", (num_col, num_fila_pdf), (num_col, num_fila_pdf), HEX_ESTABLE_BG),
                                         ("TEXTCOLOR", (num_col, num_fila_pdf), (num_col, num_fila_pdf), HEX_ESTABLE_FG)]
                else:
                    estilo_celdas += [("TEXTCOLOR", (num_col, num_fila_pdf), (num_col, num_fila_pdf), COLOR_TEXTO),
                                      ("FONTNAME", (num_col, num_fila_pdf), (num_col, num_fila_pdf), "Helvetica"),
                                      ("FONTSIZE", (num_col, num_fila_pdf), (num_col, num_fila_pdf), 8)]
            matriz.append(fila_limpia)
        anchos = [142.0, 75.0, 75.0, 75.0, 82.0, 82.0] if es_mensual else [162.0, 120.0, 120.0, 130.0]
        t = Table(matriz, colWidths=anchos, repeatRows=1)
        t.setStyle(TableStyle(estilo_celdas))
        return t

    story = []
    story.append(Paragraph("INFORME DE GESTIÓN OPERATIVA Y EFICIENCIA ENERGÉTICA", estilo_titulo))
    story.append(Paragraph("Diagnóstico de Desperdicio en Reposo (Velocidad 0) en Geocercas PIR", estilo_subtitulo))
    story.append(HRFlowable(width="100%", thickness=1.5, color=COLOR_PRIMARIO, spaceBefore=0, spaceAfter=6))

    story.append(Paragraph("1. Diagnóstico de Desperdicio en Reposo (Velocidad 0) en Geocercas PIR", estilo_h1))
    story.append(Paragraph(
        f"Para establecer la línea base de la operación durante el periodo de {etiqueta_mes_3}, se evalúa el comportamiento "
        f"macro del consumo energético en reposo dentro de los PIR. A continuación, se presenta el "
        f"consolidado evolutivo de la flota para los ciclos de {etiqueta_mes_1}, {etiqueta_mes_2} y {etiqueta_mes_3}, "
        f"permitiendo identificar desvíos acumulados y tendencias críticas en el desempeño energético general.",
        estilo_parrafo))
    story.append(Paragraph("Matriz de Desempeño Energético Mensual (Evolutivo)", estilo_h2))
    story.append(generar_tabla_desde_df(df_final_mensual, es_mensual=True))
    story.append(Paragraph("Tabla 1.1: Consolidado evolutivo de energía bruta consumida durante los últimos tres meses de control.", estilo_nota_grafica))
    story.append(Spacer(1, 6))
    story.append(Paragraph(
        f"Con el objetivo de profundizar en la operación y realizar un seguimiento táctico, se detalla a continuación el "
        f"desglose operativo del consumo en reposo correspondiente a la última semana de control ({etiqueta_rango_semana}). "
        f"Esta matriz evalúa el comportamiento específico por geocerca (PIR), facilitando la identificación oportuna "
        f"de excesos de energía y desvíos puntuales de manera semanal.", estilo_parrafo))
    story.append(Paragraph(f"Matriz de Desempeño Energético Semanal — {etiqueta_rango_semana}", estilo_h2))
    story.append(generar_tabla_desde_df(df_final_semanal, es_mensual=False))
    story.append(Paragraph("Tabla 1.2: Registro continuo del consumo acumulado en la última semana de control por instalación.", estilo_nota_grafica))

    story.append(PageBreak())
    story.append(Paragraph("2. Análisis de Perfiles de Carga en Centro Logístico", estilo_h1))
    story.append(Paragraph(
        f"Esta sección analiza la demanda energética en reposo dentro del centro logístico durante el periodo {etiqueta_rango_semana}. "
        f"A través de las curvas de carga horarias se identifican las horas pico de consumo ineficiente durante los "
        f"turnos de alistamiento, mientras que los mapas de calor exponen de forma gráfica las concentraciones térmicas "
        f"de consumo por día y hora, permitiendo focalizar los esfuerzos en las franjas horarias más críticas.", estilo_parrafo))

    if r := buscar_archivo_por_patron(r"curva.*patios"):
        story.append(Paragraph("Comportamiento Horario Acumulado - Centro Logístico", estilo_h2))
        story.append(Image(r, width=532, height=210))
        story.append(Paragraph("Figura 2.1: Curva de carga horaria donde se contrastan los perfiles de consumo energético.", estilo_nota_grafica))
        story.append(Paragraph(c1_texto, estilo_parrafo))
        story.append(Spacer(1, 8))
    if r := buscar_archivo_por_patron(r"heatmap.*green"):
        story.append(Paragraph("Distribución Térmica de Consumo - Centro Logístico Green", estilo_h2))
        story.append(Image(r, width=532, height=180))
        story.append(Paragraph("Figura 2.2: Matriz horaria de calor para el Centro Logístico Green.", estilo_nota_grafica))
    if r := buscar_archivo_por_patron(r"heatmap.*mantenimiento"):
        story.append(Paragraph("Distribución Térmica de Consumo - Patio Mantenimiento", estilo_h2))
        story.append(Image(r, width=532, height=180))
        story.append(Paragraph("Figura 2.3: Mapa de densidad energética horaria en el área de Mantenimiento.", estilo_nota_grafica))
        story.append(Paragraph(c2_texto, estilo_parrafo))

    story.append(PageBreak())
    story.append(Paragraph("3. Distribución Energética en Puntos de Operación en Vía (PIR)", estilo_h1))
    story.append(Paragraph(
        f"Esta sección evalúa el comportamiento del consumo en reposo enfocado en los PIR en vías durante el periodo {etiqueta_rango_semana}. "
        f"A través de la gráfica de fluctuación horaria se visibilizan los picos de demanda energética acumulados en vía pública, "
        f"mientras que los mapas de calor detallan la severidad operativa en los puntos más críticos, aislando las franjas horarias "
        f"de mayor impacto e ineficiencia para la organización.", estilo_parrafo))

    if r := buscar_archivo_por_patron(r"curva.*puntos.*operativos"):
        story.append(Paragraph("Fluctuación Horaria de Energía en Puntos Operativos", estilo_h2))
        story.append(Image(r, width=532, height=210))
        story.append(Paragraph("Figura 3.1: Análisis comparativo de las curvas de tránsito energético horario.", estilo_nota_grafica))
        story.append(Paragraph(c3_texto, estilo_parrafo))
        story.append(Spacer(1, 8))

    imagenes_vias = sorted([os.path.join(CARPETA_RECURSOS, f) for f in archivos_en_carpeta
                            if "via" in f.lower() and f.endswith(".png") and "curva" not in f.lower()])
    if imagenes_vias:
        story.append(Paragraph("Mapas de Calor de Puntos de Control Críticos en Vía", estilo_h2))
        for i, r_img in enumerate(imagenes_vias):
            nombre_base = os.path.basename(r_img)
            n_via = re.sub(r"^\d+\s*Heatmap\s*Vias?\s*", "", nombre_base.replace(".png", "").replace("_", " ").title())
            story.append(Image(r_img, width=532, height=170))
            story.append(Paragraph(f"Figura 3.2.{i+1}: Distribución de consumo en reposo horario para el punto operativo: {n_via}.", estilo_nota_grafica))
            if "recodo" in nombre_base.lower():
                story.append(Paragraph(c4_texto, estilo_parrafo))
                story.append(Spacer(1, 4))

    doc.build(story, onFirstPage=incrustar_logo_y_footer, onLaterPages=incrustar_logo_y_footer)
    log.info(f"PDF: {ARCHIVO_PDF_FINAL}")
    return ARCHIVO_PDF_FINAL



def limpiar_parquets_temporales():
    """Elimina los parquet intermedios de la ETL (ya no se necesitan tras el informe)."""
    for ruta in (ARCHIVO_CONSOLIDADO, ARCHIVO_RESUMEN_HORA):
        if os.path.exists(ruta):
            try:
                os.remove(ruta)
                log.info(f"Parquet temporal borrado: {os.path.basename(ruta)}")
            except OSError as e:
                log.warning(f"No se pudo borrar {ruta}: {e}")



# =============================================================================
# 6. Envio del correo
# =============================================================================

def enviar_correo_informe(ruta_pdf):
    """Envia el informe PDF generado por correo SMTP con credenciales desde .env."""
    if not os.path.exists(ruta_pdf):
        log.error(f"No se encontro el PDF para enviar: {ruta_pdf}")
        return False

    ruta_destinatarios = "destinatarios.txt"
    if not os.path.exists(ruta_destinatarios):
        log.error(f"No se encontro el archivo de destinatarios: {ruta_destinatarios}")
        return False

    with open(ruta_destinatarios, "r", encoding="utf-8") as f:
        receptores = [line.strip() for line in f if line.strip() and not line.startswith("#")]

    if not receptores:
        log.warning("Lista de destinatarios vacia. Se cancela el envio.")
        return False

    server_host = os.getenv("SMTP_SERVER", "smtp.gmail.com")
    server_port = int(os.getenv("SMTP_PORT", 587))
    emisor = os.getenv("CORREO_EMISOR")
    password = os.getenv("CONTRASENA_EMISOR")

    msg = MIMEMultipart()
    msg['From'] = emisor
    msg['To'] = ", ".join(receptores)
    msg['Subject'] = "Informe de Gestion Operativa y Eficiencia Energetica (Velocidad 0) -- Flota Greenmovil"

    cuerpo = (
        "Estimado equipo,\n\n"
        "Compartimos el Informe de Gestion Operativa y Eficiencia Energetica de la flota "
        "en condicion de reposo (Velocidad 0), estructurado bajo los lineamientos de la norma ISO 50001.\n\n"
        "Este documento consolida el diagnostico macro de consumo mensual, el desglose de desviaciones "
        "energeticas acumuladas por semana y la identificacion automatizada de los Puntos de Interes en Ruta (PIR) "
        "con mayor impacto en consumo en reposo.\n\n"
        "El informe completo se encuentra adjunto en formato PDF para su revision.\n\n"
        "Atentamente,\n"
        "Area de BI\n"
        "Greenmovil S.A.S."
    )
    msg.attach(MIMEText(cuerpo, 'plain'))

    with open(ruta_pdf, "rb") as f:
        adjunto = MIMEApplication(f.read(), Name=os.path.basename(ruta_pdf))
        adjunto['Content-Disposition'] = f'attachment; filename="{os.path.basename(ruta_pdf)}"'
        msg.attach(adjunto)

    try:
        log.info(f"Conectando a {server_host}:{server_port} para envio de correo...")
        with smtplib.SMTP(server_host, server_port) as server:
            server.starttls()
            server.login(emisor, password)
            server.sendmail(emisor, receptores, msg.as_string())
        log.info("Correo enviado exitosamente a todos los destinatarios.")
        return True
    except Exception as e:
        log.exception(f"Error al enviar el correo SMTP: {e}")
        return False


# =============================================================================
# MAIN
# =============================================================================
def main():
    t0 = time.time()
    log.info("=" * 70)
    log.info("INICIO ETL + INFORME EXACTO")
    try:
        etl_filtro_geocercas()
        etl_resumen_horario()
        ctx = preparar_matrices()
        generar_graficos(ctx)
        pdf = generar_pdf(ctx)
        enviar_correo_informe(pdf)
        limpiar_parquets_temporales()
        log.info("=" * 70)
        log.info(f"ÉXITO | PDF: {pdf}")
        log.info(f"Tiempo: {round((time.time()-t0)/60, 2)} min")
        log.info("=" * 70)
        return 0
    except Exception as e:
        log.exception(f"Error crítico: {e}")
        return 1

if __name__ == "__main__":
    sys.exit(main())
#!/usr/bin/env python3
"""
Gerador de Relatório Diário de Geração de Energia — EnergoPro
=============================================================
Busca dados da API, gera HTML com tabelas + gráficos e abre rascunho no Outlook.

Uso:
    python report_generator.py                        # Mês/ano atual
    python report_generator.py --year 2026 --month 4 # Mês/ano específico
    python report_generator.py --no-email             # Sem abrir e-mail
"""

import os
import io
import base64
import logging
import argparse
import calendar
import smtplib
from datetime import datetime, date, timedelta
from typing import Optional
from html import escape
from urllib.parse import urlparse, parse_qs
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email.mime.image import MIMEImage
from email import encoders

import requests
import pandas as pd
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker

from reportlab.lib.pagesizes import A4, landscape
from reportlab.lib import colors
from reportlab.lib.units import cm, mm
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.enums import TA_CENTER, TA_LEFT, TA_RIGHT
from reportlab.platypus import (
    SimpleDocTemplate, Table, TableStyle, Paragraph,
    Spacer, Image, KeepTogether, HRFlowable,
)


# ─────────────────────────────────────────────
# LOGGING
# ─────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)


# ═════════════════════════════════════════════
# 1.  CONFIGURAÇÕES  (edite aqui)
# ═════════════════════════════════════════════

# --- API -----------------------------------------------------------
API_BASE_URL = "https://api.tempoenergia.com/gestao/ccee_medicao/"   # TODO: URL da sua API
API_HEADERS  = {
    # "Authorization": "Bearer SEU_TOKEN",       # TODO: descomente e preencha
    "Content-Type": "application/json",
}


def get_api_date_range(reference_date: Optional[date] = None) -> tuple[date, date]:
    """
    Retorna o intervalo usado na API:
      - data: primeiro dia do mês atual
      - data_fim: ontem

    No primeiro dia do mês, usa o próprio primeiro dia para evitar um
    intervalo inválido.
    """
    reference_date = reference_date or date.today()
    start_date = reference_date.replace(day=1)
    end_date = reference_date - timedelta(days=1)
    if end_date < start_date:
        end_date = start_date
    return start_date, end_date


# Parâmetros extras que a API precisa (ajuste conforme necessário)
def build_api_params(meter_id: str, year: int, month: int) -> dict:
    start_date, end_date = get_api_date_range()
    return {
        "data":    start_date.isoformat(),
        "data_fim": end_date.isoformat(),
        "medidor": meter_id,
        "ano":     year,
        "mes":     month,
        # Adicione outros parâmetros necessários aqui
    }


def infer_period_from_api_url(api_url: str) -> Optional[tuple[int, int]]:
    """
    Infere ano e mês a partir dos parâmetros `data` e `data_fim` da URL.
    Retorna (year, month) usando a data inicial do período.
    """
    parsed = urlparse(api_url)
    query = parse_qs(parsed.query)
    start_raw = query.get("data", [None])[0]
    end_raw = query.get("data_fim", [None])[0]

    if not start_raw:
        start_date, _ = get_api_date_range()
        return start_date.year, start_date.month

    try:
        start_date = datetime.strptime(start_raw, "%Y-%m-%d").date()
    except ValueError:
        log.warning("Não foi possível interpretar a data inicial da API_BASE_URL: %s", start_raw)
        return None

    if end_raw:
        try:
            end_date = datetime.strptime(end_raw, "%Y-%m-%d").date()
            if (start_date.year, start_date.month) != (end_date.year, end_date.month):
                log.warning(
                    "API_BASE_URL cobre mais de um mês (%s até %s). O relatório usará %02d/%d com base em `data`.",
                    start_raw,
                    end_raw,
                    start_date.month,
                    start_date.year,
                )
        except ValueError:
            log.warning("Não foi possível interpretar a data final da API_BASE_URL: %s", end_raw)

    return start_date.year, start_date.month

def resolve_report_period(cli_year: Optional[int], cli_month: Optional[int]) -> tuple[int, int]:
    """Define o período do relatório priorizando CLI e depois a API_BASE_URL."""
    if cli_year is not None and cli_month is not None:
        return cli_year, cli_month

    inferred = infer_period_from_api_url(API_BASE_URL)
    if inferred is not None:
        inferred_year, inferred_month = inferred
        return cli_year or inferred_year, cli_month or inferred_month

    today = date.today()
    return cli_year or today.year, cli_month or today.month

# --- E-MAIL --------------------------------------------------------
EMAIL_SENDER    = "matheus.arruda@tempoenergia.com.br"
EMAIL_PASSWORD  = "Ma!%(#%&"
EMAIL_TO        = ["cilinaldo.silva@tempoenergia.com.br, matheus.arruda@tempoenergia.com.br"]   # lista de destinatários
EMAIL_SMTP_HOST = "smtp.office365.com"
EMAIL_SMTP_PORT = 587

# --- HORAS ESPERADAS POR DIA ---------------------------------------
EXPECTED_HOURS_PER_DAY = 24   # ajuste se a API retornar 25 slots (0-24)


# ═════════════════════════════════════════════
# 2.  MAPEAMENTO MEDIDOR → USINA
# ═════════════════════════════════════════════
# Chave: medidor (exatamente como vem na API)
# Valor: nome da usina para exibição no relatório
METER_TO_PLANT: dict[str, str] = {
    "RJCAJUUSINA01P": "PCH CAJU I5",
    "RJBJARUSTOA01P": "PCH SANTO ANTONIO I5",
    "RJSSALUSINA01P": "PCH SAO SEBASTIAO DO ALTO I5",
    "MGIB23UZTN-01P": "PCH ZE TUNIN PIE",
    "MGCRISUSINA01P": "SPE CRISTINA PIE",
    "MSVER4UVER401P": "VERDE 4",
    "MSVER4UVE4A02P": "VERDE 4A",
    "RJCTRAGTOT-01P": "SE COLETORA",
    "MSMIM-VQTRA03P": "SE MIMOSO",
    "PRUHBITR1--04P": "CEU AZUL",
    "PRUHBITR2--05P": "CEU AZUL",
    "PRUHBITR3--06P": "CEU AZUL",
    "PRUHBIUG1--01B": "CEU AZUL",
    "PRUHBIUG2--02B": "CEU AZUL",
    "PRUHBIUG3--03B": "CEU AZUL",
    "TONJD-UDOI-03P": "PCH DOIDO",
    "TONJD-UDCP-01P": "PCH DOIDO",
    "MTY51RUMAMI01P": "PCH MANTOVILIS",
    "TONJD-UPCUM02P": "PCH PIARUCUM",
    "GOSRNPUTSRN01P": "SERRANOPOLIS",
    # ── Pontos a adicionar futuramente ──────────────────────────────
    # "SPURN-USINA01P": "SCGE",
    # "PEGCIPUS---01P": "USINA IPOJUCA",
    # "MSELDBTR1--01P": "ELDORADO BR",
    # "MSELDBTR2--02P": "ELDORADO BR",
    "TODIA-URDCO02P": "CRIO"
}


# ═════════════════════════════════════════════
# 3.  CATEGORIAS (seções do relatório)
# ═════════════════════════════════════════════
# Cada entrada: (medidor_api, codigo_ponto_display)
# O codigo_ponto_display é o que aparece na coluna "Ponto/Grupo"
CATEGORIES: dict[str, list[tuple[str, str]]] = {
    "PCH's Madalena": [
        ("RJCAJUUSINA01P", "RJCAJUUSINA01"),
        ("RJBJARUSTOA01P", "RJBJARUSTOA01"),
        ("RJSSALUSINA01P", "RJSSALUSINA01"),
        ("MGIB23UZTN-01P", "MGIB23UZTN-01"),
        ("MGCRISUSINA01P", "MGCRISUSINA01"),
        ("MSVER4UVER401P", "MSVER4UVER401"),
        ("MSVER4UVE4A02P", "MSVER4UVE4A02"),
    ],
    "Subestações": [
        ("RJCTRAGTOT-01P", "RJCTRAGTOT-01"),
        ("MSMIM-VQTRA03P", "MSMIM-VQTRA03"),
    ],
    "UHE - Baixo Iguaçu": [
        ("PRUHBITR1--04P", "PRUHBITR1--04"),
        ("PRUHBITR2--05P", "PRUHBITR2--05"),
        ("PRUHBITR3--06P", "PRUHBITR3--06"),
        ("PRUHBIUG1--01B", "PRUHBIUG1--01"),
        ("PRUHBIUG2--02B", "PRUHBIUG2--02"),
        ("PRUHBIUG3--03B", "PRUHBIUG3--03"),
    ],
    "Usinas de Gestão": [
        ("TONJD-UDOI-03P", "TONJD-UDOI-03"),
        ("TONJD-UDCP-01P", "TONJD-UDCP-01"),
        ("MTY51RUMAMI01P", "MTY51RUMAMI01"),
        ("TONJD-UPCUM02P", "TONJD-UPCUM02"),
        ("GOSRNPUTSRN01P", "GOSRNPUTSRN01"),
        # ── Pontos a adicionar futuramente ──────────────────────────
        # ("SPURN-USINA01P", "SPURN-USINA01"),
        # ("PEGCIPUS---01P", "PEGCIPUS---01"),
        # ("MSELDBTR1--01P", "MSELDBTR1--01"),
        # ("MSELDBTR2--02P", "MSELDBTR2--02"),
        ("TODIA-URDCO02P", "TODIA-URDCO02")
    ]
}

# Gráficos: quais categorias agrupar em cada chart
CHART_GROUPS: list[dict] = [
    {"title": "Geração PCH's - Madalena",         "category": "PCH's Madalena"},
    {"title": "Geração UHE's Baixo Iguaçu",       "category": "UHE - Baixo Iguaçu"},
    {"title": "Usinas Gestão",                     "category": "Usinas de Gestão"},
    {"title": "Subestações",                       "category": "Subestações"},
]


# ═════════════════════════════════════════════
# 4.  PALETA DE CORES DO RELATÓRIO
# ═════════════════════════════════════════════
C_HEADER_BG     = colors.HexColor("#1B3A6B")   # azul escuro — cabeçalho
C_HEADER_TEXT   = colors.white
C_SECTION_BG    = colors.HexColor("#2E6B2E")   # verde escuro — seção
C_SECTION_TEXT  = colors.white
C_COL_HEADER_BG = colors.HexColor("#D9E1F2")   # azul claro — header de coluna
C_ROW_EVEN      = colors.HexColor("#FFFFFF")
C_ROW_ODD       = colors.HexColor("#F0F4FF")
C_GRID          = colors.HexColor("#CCCCCC")

# Células de qualidade por hora
C_OK            = colors.HexColor("#4CAF50")   # verde  — 0 horas faltantes
C_WARN          = colors.HexColor("#FF9800")   # laranja — 1-23 horas faltantes
C_FAIL          = colors.HexColor("#F44336")   # vermelho — 24 horas faltantes
C_NO_DATA       = colors.HexColor("#9E9E9E")   # cinza — sem dados da API


# ═════════════════════════════════════════════
# 5.  BUSCA DE DADOS NA API
# ═════════════════════════════════════════════

def fetch_meter_data(meter_id: str, year: int, month: int) -> Optional[pd.DataFrame]:
    """
    Busca dados de um medidor na API.
    Retorna DataFrame com colunas: data, hora, medidor, Qualidade,
    ea_geracao_kwh, ea_consumo_kwh, er_geracao_kvarh, er_consumo_kvarh
    Retorna None se a API não responder ou não tiver dados.
    """
    try:
        params = build_api_params(meter_id, year, month)
        resp = requests.get(
            API_BASE_URL,
            params=params,
            headers=API_HEADERS,
            timeout=500,
        )
        resp.raise_for_status()
        json_data = resp.json()

        # A API pode retornar {"dados": [...]} ou diretamente [...]
        records = json_data.get("dados", json_data) if isinstance(json_data, dict) else json_data

        if not records:
            log.warning("  [%s] API retornou lista vazia.", meter_id)
            return None

        df = pd.DataFrame(records)
        df["data"] = pd.to_datetime(df["data"]).dt.date
        df["hora"] = pd.to_numeric(df["hora"], errors="coerce")
        df["ea_geracao_kwh"] = pd.to_numeric(df.get("ea_geracao_kwh", 0), errors="coerce").fillna(0)
        return df

    except requests.exceptions.RequestException as exc:
        log.warning("  [%s] Erro ao buscar dados: %s", meter_id, exc)
        return None
    except Exception as exc:
        log.error("  [%s] Erro inesperado: %s", meter_id, exc)
        return None


def fetch_all_data(year: int, month: int) -> dict[str, Optional[pd.DataFrame]]:
    """Busca dados de todos os medidores configurados."""
    all_meters = {
        meter_id
        for meters in CATEGORIES.values()
        for meter_id, _ in meters
    }
    results: dict[str, Optional[pd.DataFrame]] = {}
    for meter_id in sorted(all_meters):
        log.info("Buscando medidor: %s", meter_id)
        results[meter_id] = fetch_meter_data(meter_id, year, month)
    return results


# ═════════════════════════════════════════════
# 6.  PROCESSAMENTO DOS DADOS
# ═════════════════════════════════════════════

def compute_generation_mwh(df: Optional[pd.DataFrame]) -> float:
    """Soma total de geração do mês em MWh."""
    if df is None or df.empty:
        return 0.0
    return df["ea_geracao_kwh"].sum() / 1000.0


def compute_daily_missing(
    df: Optional[pd.DataFrame],
    year: int,
    month: int,
    days_in_month: int,
    reference_date: Optional[date] = None,
) -> dict[int, Optional[int]]:
    """
    Para cada dia do mês retorna:
      - None  → sem dados da API (cinza)
      - int   → número de horas com Qualidade = "Faltante" (0 = verde)
    """
    result: dict[int, Optional[int]] = {}
    reference_date = reference_date or date.today()

    if df is None or df.empty:
        # Não veio nada da API para este medidor
        for d in range(1, days_in_month + 1):
            result[d] = None
        return result

    # Agrupa por dia
    for d in range(1, days_in_month + 1):
        day_date = date(year, month, d)
        day_df = df[df["data"] == day_date]

        if day_df.empty:
            # No mês atual, dias de hoje em diante ainda podem não ter sido
            # disponibilizados pela API e devem aparecer como "Sem dados".
            if day_date >= reference_date:
                result[d] = None
            else:
                result[d] = None
        else:
            quality = day_df["Qualidade"].fillna("").astype(str).str.strip().str.casefold()
            missing = int((quality == "faltante").sum())
            result[d] = missing

    return result


def compute_daily_generation(
    df: Optional[pd.DataFrame],
    year: int,
    month: int,
    days_in_month: int,
) -> dict[int, float]:
    """Retorna geração diária em kWh por dia."""
    result = {d: 0.0 for d in range(1, days_in_month + 1)}
    if df is None or df.empty:
        return result
    for d in range(1, days_in_month + 1):
        day_date = date(year, month, d)
        day_df = df[df["data"] == day_date]
        result[d] = day_df["ea_geracao_kwh"].sum()
    return result


def missing_to_color(missing: Optional[int]) -> colors.Color:
    """Mapeia contagem de horas faltantes → cor da célula."""
    if missing is None:
        return C_NO_DATA
    if missing == 0:
        return C_OK
    if missing < EXPECTED_HOURS_PER_DAY:
        return C_WARN
    return C_FAIL


def missing_to_text(missing: Optional[int]) -> str:
    """Texto exibido na célula de qualidade."""
    if missing is None:
        return "-"
    if missing == 0:
        return "0"
    return str(missing)


def color_to_css(color) -> str:
    """Converte cor do ReportLab em hexadecimal CSS."""
    if isinstance(color, str):
        return color
    red = int(round(color.red * 255))
    green = int(round(color.green * 255))
    blue = int(round(color.blue * 255))
    return f"#{red:02X}{green:02X}{blue:02X}"


def missing_to_css_color(missing: Optional[int]) -> str:
    return color_to_css(missing_to_color(missing))


def missing_to_text_color(missing: Optional[int]) -> str:
    """Escolhe cor do texto para contraste com a cor de fundo."""
    return "#1B5E20" if missing == 0 else "#FFFFFF"


# ═════════════════════════════════════════════
# 7.  GERAÇÃO DE GRÁFICOS (matplotlib → PNG em memória)
# ═════════════════════════════════════════════

CHART_COLORS = [
    "#1f77b4", "#ff7f0e", "#2ca02c", "#d62728",
    "#9467bd", "#8c564b", "#e377c2", "#7f7f7f",
    "#bcbd22", "#17becf",
]


def make_bar_chart(
    title: str,
    category: str,
    meter_data: dict[str, Optional[pd.DataFrame]],
    year: int,
    month: int,
    days_in_month: int,
    width_px: int = 1400,
    height_px: int = 620,
    dpi: int = 140,
) -> io.BytesIO:
    """
    Gera gráfico de linhas de geração diária para uma categoria.
    Retorna buffer PNG.
    """
    meters = CATEGORIES.get(category, [])
    labels = [str(d) for d in range(1, days_in_month + 1)]
    x = list(range(1, days_in_month + 1))

    fig, ax = plt.subplots(figsize=(width_px / dpi, height_px / dpi), dpi=dpi)

    plotted_labels: list[str] = []
    plotted_any = False

    for idx, (meter_id, _) in enumerate(meters):
        df = meter_data.get(meter_id)
        daily = compute_daily_generation(df, year, month, days_in_month)
        values = [daily[d] for d in range(1, days_in_month + 1)]

        if any(v > 0 for v in values):
            plant_name = METER_TO_PLANT.get(meter_id, meter_id)
            point_code = next(
                (code for m, code in CATEGORIES[category] if m == meter_id), meter_id
            )
            label = f"{plant_name}\n({point_code})"
            color = CHART_COLORS[idx % len(CHART_COLORS)]
            ax.plot(
                x, values, label=label,
                color=color, linewidth=1.6,
                marker="o", markersize=2.8,
            )
            plotted_labels.append(label)
            plotted_any = True

    ax.set_title(title, fontsize=10, fontweight="bold", pad=6)
    ax.set_xlabel("Dia", fontsize=8)
    ax.set_ylabel("Geração (kWh)", fontsize=8)
    ax.set_xticks(x)
    ax.set_xticklabels(labels, fontsize=6)
    ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda v, _: f"{v:,.0f}"))
    ax.tick_params(axis="y", labelsize=7)
    ax.grid(axis="y", linestyle="--", alpha=0.4)
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)

    legend_rows = 1

    if plotted_any:
        legend_cols = min(4, max(1, len(plotted_labels)))
        legend_rows = (len(plotted_labels) + legend_cols - 1) // legend_cols
        legend_anchor_y = -0.18 - (0.08 * (legend_rows - 1))
        ax.legend(
            fontsize=5.2,
            loc="upper center",
            bbox_to_anchor=(0.5, legend_anchor_y),
            framealpha=0.9,
            ncol=legend_cols,
            borderpad=0.35,
            labelspacing=0.3,
            handlelength=1.6,
            columnspacing=0.8,
        )
    else:
        ax.text(
            0.5, 0.5, "Sem dados disponíveis",
            ha="center", va="center", transform=ax.transAxes,
            fontsize=10, color="gray",
        )

    bottom_margin = 0.12 + (0.08 * max(0, legend_rows - 1))
    plt.tight_layout(pad=0.8, rect=(0, bottom_margin, 1, 1))

    buf = io.BytesIO()
    plt.savefig(buf, format="png", dpi=dpi, bbox_inches="tight")
    plt.close(fig)
    buf.seek(0)
    return buf


# ═════════════════════════════════════════════
# 8.  CONSTRUÇÃO DAS TABELAS (ReportLab)
# ═════════════════════════════════════════════

def get_suffix(meter_id: str) -> str:
    return "(B)" if meter_id.endswith("B") else "(L)"


def build_section_table(
    category_name: str,
    meters: list[tuple[str, str]],
    meter_data: dict[str, Optional[pd.DataFrame]],
    year: int,
    month: int,
    days_in_month: int,
    styles: dict,
) -> list:
    """
    Constrói a tabela de uma seção do relatório.
    Retorna lista de flowables (KeepTogether).
    """
    # ── Cabeçalho de seção ─────────────────────────────────────────
    section_title = Paragraph(
        f'<font color="white"><b>{category_name}</b></font>',
        styles["section_title"],
    )

    # ── Larguras das colunas ───────────────────────────────────────
    # Página A4 landscape ≈ 277mm utilizável
    PAGE_W = 277 * mm
    COL_NAME    = 44 * mm
    COL_POINT   = 33 * mm
    COL_GEN     = 18 * mm
    day_total_w = PAGE_W - COL_NAME - COL_POINT - COL_GEN
    COL_DAY     = day_total_w / days_in_month

    col_widths = [COL_NAME, COL_POINT, COL_GEN] + [COL_DAY] * days_in_month

    # ── Linha de cabeçalho das colunas ─────────────────────────────
    header_row = [
        Paragraph("<b>Nome Usina</b>",       styles["col_header"]),
        Paragraph("<b>Ponto/Grupo</b>",      styles["col_header"]),
        Paragraph("<b>Geração<br/>(MWh)</b>",styles["col_header"]),
    ] + [
        Paragraph(f"<b>{d}</b>", styles["day_header"])
        for d in range(1, days_in_month + 1)
    ]

    table_data = [header_row]
    table_style_cmds = [
        # Cabeçalho
        ("BACKGROUND",    (0, 0), (-1, 0), C_COL_HEADER_BG),
        ("TEXTCOLOR",     (0, 0), (-1, 0), colors.HexColor("#1B3A6B")),
        ("FONTNAME",      (0, 0), (-1, 0), "Helvetica-Bold"),
        ("FONTSIZE",      (0, 0), (-1, 0), 6.5),
        ("ALIGN",         (0, 0), (-1, 0), "CENTER"),
        ("VALIGN",        (0, 0), (-1, 0), "MIDDLE"),
        ("ROWBACKGROUNDS",(0, 1), (-1, -1), [C_ROW_EVEN, C_ROW_ODD]),
        ("GRID",          (0, 0), (-1, -1), 0.3, C_GRID),
        ("LEFTPADDING",   (0, 0), (-1, -1), 2),
        ("RIGHTPADDING",  (0, 0), (-1, -1), 2),
        ("TOPPADDING",    (0, 0), (-1, -1), 2),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 2),
        ("VALIGN",        (0, 1), (-1, -1), "MIDDLE"),
    ]

    # ── Linhas de dados ────────────────────────────────────────────
    for row_idx, (meter_id, point_code) in enumerate(meters, start=1):
        df         = meter_data.get(meter_id)
        gen_mwh    = compute_generation_mwh(df)
        daily_miss = compute_daily_missing(df, year, month, days_in_month)
        suffix     = get_suffix(meter_id)
        plant_name = METER_TO_PLANT.get(meter_id, meter_id)

        row = [
            Paragraph(plant_name,                         styles["cell_left"]),
            Paragraph(f"{point_code} {suffix}",           styles["cell_center"]),
            Paragraph(f"{gen_mwh:,.2f}",                  styles["cell_right"]),
        ]

        for d in range(1, days_in_month + 1):
            missing = daily_miss[d]
            cell_color = missing_to_color(missing)
            text       = missing_to_text(missing)
            # Escolhe cor do texto para contraste
            txt_color  = "#FFFFFF" if missing != 0 else "#1B5E20"
            row.append(
                Paragraph(
                    f'<font color="{txt_color}"><b>{text}</b></font>',
                    styles["day_cell"],
                )
            )
            # Aplica cor de fundo na célula do dia
            col = 3 + (d - 1)
            table_style_cmds.append(
                ("BACKGROUND", (col, row_idx), (col, row_idx), cell_color)
            )

        table_data.append(row)

    tbl = Table(table_data, colWidths=col_widths, repeatRows=1)
    tbl.setStyle(TableStyle(table_style_cmds))

    section_bg_row = Table(
        [[section_title]],
        colWidths=[PAGE_W],
    )
    section_bg_row.setStyle(TableStyle([
        ("BACKGROUND",    (0, 0), (-1, -1), C_SECTION_BG),
        ("LEFTPADDING",   (0, 0), (-1, -1), 6),
        ("RIGHTPADDING",  (0, 0), (-1, -1), 6),
        ("TOPPADDING",    (0, 0), (-1, -1), 4),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 4),
    ]))

    return [section_bg_row, tbl, Spacer(1, 6 * mm)]


# ═════════════════════════════════════════════
# 9.  MONTAGEM DO PDF COMPLETO
# ═════════════════════════════════════════════

def build_styles() -> dict:
    base = getSampleStyleSheet()
    return {
        "title": ParagraphStyle(
            "title",
            parent=base["Normal"],
            fontSize=14, fontName="Helvetica-Bold",
            textColor=C_HEADER_TEXT,
            alignment=TA_CENTER, spaceAfter=2,
        ),
        "subtitle": ParagraphStyle(
            "subtitle",
            parent=base["Normal"],
            fontSize=9, fontName="Helvetica",
            textColor=C_HEADER_TEXT,
            alignment=TA_CENTER,
        ),
        "section_title": ParagraphStyle(
            "section_title",
            parent=base["Normal"],
            fontSize=8, fontName="Helvetica-Bold",
            textColor=C_SECTION_TEXT,
            alignment=TA_LEFT,
        ),
        "col_header": ParagraphStyle(
            "col_header",
            parent=base["Normal"],
            fontSize=6.5, fontName="Helvetica-Bold",
            alignment=TA_CENTER,
        ),
        "day_header": ParagraphStyle(
            "day_header",
            parent=base["Normal"],
            fontSize=5.5, fontName="Helvetica-Bold",
            alignment=TA_CENTER,
        ),
        "cell_left": ParagraphStyle(
            "cell_left",
            parent=base["Normal"],
            fontSize=6, fontName="Helvetica",
            alignment=TA_LEFT,
        ),
        "cell_center": ParagraphStyle(
            "cell_center",
            parent=base["Normal"],
            fontSize=6, fontName="Helvetica",
            alignment=TA_CENTER,
        ),
        "cell_right": ParagraphStyle(
            "cell_right",
            parent=base["Normal"],
            fontSize=6, fontName="Helvetica",
            alignment=TA_RIGHT,
        ),
        "day_cell": ParagraphStyle(
            "day_cell",
            parent=base["Normal"],
            fontSize=5.5, fontName="Helvetica-Bold",
            alignment=TA_CENTER,
        ),
        "legend_title": ParagraphStyle(
            "legend_title",
            parent=base["Normal"],
            fontSize=7, fontName="Helvetica-Bold",
            alignment=TA_LEFT,
        ),
        "legend_item": ParagraphStyle(
            "legend_item",
            parent=base["Normal"],
            fontSize=6.5, fontName="Helvetica",
            alignment=TA_LEFT,
        ),
        "chart_title": ParagraphStyle(
            "chart_title",
            parent=base["Normal"],
            fontSize=9, fontName="Helvetica-Bold",
            alignment=TA_CENTER, spaceAfter=4,
        ),
    }


def build_header(month: int, year: int, styles: dict, page_width: float) -> list:
    """Bloco de cabeçalho do relatório."""
    month_name_pt = [
        "", "Janeiro", "Fevereiro", "Março", "Abril", "Maio", "Junho",
        "Julho", "Agosto", "Setembro", "Outubro", "Novembro", "Dezembro",
    ][month]

    header_tbl = Table(
        [[
            Paragraph("Análise dos Pontos de Medição", styles["title"]),
            Paragraph(f"{month_name_pt}/{year}", styles["subtitle"]),
        ]],
        colWidths=[page_width * 0.75, page_width * 0.25],
    )
    header_tbl.setStyle(TableStyle([
        ("BACKGROUND",    (0, 0), (-1, -1), C_HEADER_BG),
        ("VALIGN",        (0, 0), (-1, -1), "MIDDLE"),
        ("LEFTPADDING",   (0, 0), (-1, -1), 8),
        ("RIGHTPADDING",  (0, 0), (-1, -1), 8),
        ("TOPPADDING",    (0, 0), (-1, -1), 8),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 8),
    ]))

    return [header_tbl, Spacer(1, 5 * mm)]


def build_legend(styles: dict, page_width: float) -> list:
    """Legenda de cores de qualidade."""
    def color_cell(c: colors.Color, text: str) -> Table:
        tbl = Table(
            [[Paragraph(f'<font color="white"><b>{text}</b></font>', styles["day_cell"])]],
            colWidths=[10 * mm], rowHeights=[6 * mm],
        )
        tbl.setStyle(TableStyle([
            ("BACKGROUND", (0, 0), (0, 0), c),
            ("ALIGN", (0, 0), (0, 0), "CENTER"),
            ("VALIGN", (0, 0), (0, 0), "MIDDLE"),
        ]))
        return tbl

    legend_data = [[
        Paragraph("<b>Legenda Qualidade:</b>", styles["legend_title"]),
        color_cell(C_OK,   "0"),
        Paragraph("0 horas faltantes",        styles["legend_item"]),
        color_cell(C_WARN, "12"),
        Paragraph("1–23 horas faltantes",     styles["legend_item"]),
        color_cell(C_FAIL, "24"),
        Paragraph("24 horas faltantes",       styles["legend_item"]),
        color_cell(C_NO_DATA, "-"),
        Paragraph("Sem dados da API",         styles["legend_item"]),
    ]]

    legend_tbl = Table(
        legend_data,
        colWidths=[35*mm, 10*mm, 38*mm, 10*mm, 38*mm, 10*mm, 38*mm, 10*mm, 38*mm],
    )
    legend_tbl.setStyle(TableStyle([
        ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
        ("ALIGN",  (0, 0), (-1, -1), "LEFT"),
        ("LEFTPADDING",  (0, 0), (-1, -1), 3),
        ("RIGHTPADDING", (0, 0), (-1, -1), 3),
    ]))
    return [legend_tbl, Spacer(1, 4 * mm)]


def generate_pdf(
    year: int,
    month: int,
    meter_data: dict[str, Optional[pd.DataFrame]],
) -> bytes:
    """Gera o PDF completo em memória e retorna os bytes."""
    log.info("Gerando PDF em memória")

    days_in_month = calendar.monthrange(year, month)[1]

    pdf_buf = io.BytesIO()
    doc = SimpleDocTemplate(
        pdf_buf,
        pagesize=landscape(A4),
        leftMargin=10 * mm,
        rightMargin=10 * mm,
        topMargin=10 * mm,
        bottomMargin=10 * mm,
    )

    PAGE_W = landscape(A4)[0] - 20 * mm   # largura útil

    styles = build_styles()
    story  = []

    # Cabeçalho
    story += build_header(month, year, styles, PAGE_W)

    # Tabelas de cada seção
    for cat_name, meters in CATEGORIES.items():
        flowables = build_section_table(
            cat_name, meters, meter_data,
            year, month, days_in_month, styles,
        )
        story += flowables

    # Legenda
    story += build_legend(styles, PAGE_W)

    # Separador antes dos gráficos
    story.append(HRFlowable(width=PAGE_W, thickness=1, color=C_HEADER_BG))
    story.append(Spacer(1, 4 * mm))

    # Gráficos — dois por linha
    CHART_W = (PAGE_W - 6 * mm) / 2
    CHART_H = 55 * mm

    chart_row: list = []
    for group in CHART_GROUPS:
        buf = make_bar_chart(
            title=group["title"],
            category=group["category"],
            meter_data=meter_data,
            year=year, month=month,
            days_in_month=days_in_month,
        )
        img = Image(buf, width=CHART_W, height=CHART_H)
        chart_row.append(img)
        if len(chart_row) == 2:
            row_tbl = Table([chart_row], colWidths=[CHART_W, CHART_W])
            row_tbl.setStyle(TableStyle([
                ("VALIGN", (0, 0), (-1, -1), "TOP"),
                ("LEFTPADDING",  (0, 0), (-1, -1), 2),
                ("RIGHTPADDING", (0, 0), (-1, -1), 2),
            ]))
            story.append(row_tbl)
            story.append(Spacer(1, 4 * mm))
            chart_row = []

    # Gráfico restante (se ímpar)
    if chart_row:
        row_tbl = Table([chart_row + [""]], colWidths=[CHART_W, CHART_W])
        row_tbl.setStyle(TableStyle([("VALIGN", (0, 0), (-1, -1), "TOP")]))
        story.append(row_tbl)

    doc.build(story)
    log.info("PDF gerado: %d bytes", pdf_buf.tell())
    return pdf_buf.getvalue()


def build_section_table_html(
    category_name: str,
    meters: list[tuple[str, str]],
    meter_data: dict[str, Optional[pd.DataFrame]],
    year: int,
    month: int,
    days_in_month: int,
) -> str:
    """Constroi a tabela HTML de uma secao do relatorio."""
    header_cells = "".join(f"<th>{d}</th>" for d in range(1, days_in_month + 1))
    rows: list[str] = []

    for row_idx, (meter_id, point_code) in enumerate(meters, start=1):
        df = meter_data.get(meter_id)
        gen_mwh = compute_generation_mwh(df)
        daily_miss = compute_daily_missing(df, year, month, days_in_month)
        suffix = get_suffix(meter_id)
        plant_name = METER_TO_PLANT.get(meter_id, meter_id)
        row_class = "even" if row_idx % 2 else "odd"

        day_cells: list[str] = []
        for day in range(1, days_in_month + 1):
            missing = daily_miss[day]
            day_cells.append(
                (
                    f'<td class="day-cell" style="background:{missing_to_css_color(missing)};'
                    f'color:{missing_to_text_color(missing)};">'
                    f'{escape(missing_to_text(missing))}</td>'
                )
            )

        rows.append(
            "<tr>"
            f'<td class="left {row_class}">{escape(plant_name)}</td>'
            f'<td class="center {row_class}">{escape(f"{point_code} {suffix}")}</td>'
            f'<td class="right {row_class}">{gen_mwh:,.2f}</td>'
            f'{"".join(day_cells)}'
            "</tr>"
        )

    return f"""
    <section class="report-section">
      <div class="section-title">{escape(category_name)}</div>
      <div class="table-wrapper">
        <table class="report-table">
          <thead>
            <tr>
              <th>Nome Usina</th>
              <th>Ponto/Grupo</th>
              <th>Geracao (MWh)</th>
              {header_cells}
            </tr>
          </thead>
          <tbody>
            {"".join(rows)}
          </tbody>
        </table>
      </div>
    </section>
    """


def build_legend_html() -> str:
    """Monta a legenda de cores em HTML."""
    legend_items = [
        (C_OK, "0", "0 horas faltantes"),
        (C_WARN, "12", "1-23 horas faltantes"),
        (C_FAIL, "24", "24 horas faltantes"),
        (C_NO_DATA, "-", "Sem dados da API"),
    ]
    items: list[str] = []
    for color, text, label in legend_items:
        items.append(
            (
                '<div class="legend-item">'
                f'<span class="legend-swatch" style="background:{color_to_css(color)};'
                f'color:{missing_to_text_color(0 if text == "0" else 1)};">{escape(text)}</span>'
                f'<span>{escape(label)}</span>'
                '</div>'
            )
        )
    return '<section class="legend"><strong>Legenda Qualidade:</strong>' + "".join(items) + "</section>"


def make_chart_data_uri(
    title: str,
    category: str,
    meter_data: dict[str, Optional[pd.DataFrame]],
    year: int,
    month: int,
    days_in_month: int,
) -> str:
    """Converte o grafico em data URI para embutir no HTML."""
    buf = make_bar_chart(
        title=title,
        category=category,
        meter_data=meter_data,
        year=year,
        month=month,
        days_in_month=days_in_month,
    )
    payload = base64.b64encode(buf.getvalue()).decode("ascii")
    return f"data:image/png;base64,{payload}"


def generate_html(
    year: int,
    month: int,
    meter_data: dict[str, Optional[pd.DataFrame]],
) -> str:
    """Gera o HTML completo em memória e retorna a string."""
    log.info("Gerando HTML em memória")

    days_in_month = calendar.monthrange(year, month)[1]
    month_name_pt = [
        "", "Janeiro", "Fevereiro", "Marco", "Abril", "Maio", "Junho",
        "Julho", "Agosto", "Setembro", "Outubro", "Novembro", "Dezembro",
    ][month]

    sections_html = [
        build_section_table_html(cat_name, meters, meter_data, year, month, days_in_month)
        for cat_name, meters in CATEGORIES.items()
    ]

    chart_cards: list[str] = []
    for group in CHART_GROUPS:
        chart_cards.append(
            (
                '<article class="chart-card">'
                f'<h3>{escape(group["title"])}</h3>'
                f'<img src="{make_chart_data_uri(group["title"], group["category"], meter_data, year, month, days_in_month)}" '
                f'alt="{escape(group["title"])}" />'
                '</article>'
            )
        )

    html_content = f"""<!DOCTYPE html>
<html lang="pt-BR">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>Relatorio Diario de Geracao - {month_name_pt}/{year}</title>
  <style>
    :root {{
      --header-bg: {color_to_css(C_HEADER_BG)};
      --header-text: {color_to_css(C_HEADER_TEXT)};
      --section-bg: {color_to_css(C_SECTION_BG)};
      --section-text: {color_to_css(C_SECTION_TEXT)};
      --col-header-bg: {color_to_css(C_COL_HEADER_BG)};
      --row-even: {color_to_css(C_ROW_EVEN)};
      --row-odd: {color_to_css(C_ROW_ODD)};
      --grid: {color_to_css(C_GRID)};
      --text: #12304f;
      --shadow: 0 12px 32px rgba(17, 40, 73, 0.12);
    }}
    * {{ box-sizing: border-box; }}
    body {{
      margin: 0;
      font-family: "Segoe UI", Arial, sans-serif;
      background: linear-gradient(180deg, #eff4fb 0%, #e3edf8 100%);
      color: var(--text);
    }}
    .page {{
      max-width: 1800px;
      margin: 0 auto;
      padding: 24px;
    }}
    .header {{
      background: var(--header-bg);
      color: var(--header-text);
      border-radius: 16px;
      padding: 24px 28px;
      box-shadow: var(--shadow);
      margin-bottom: 20px;
    }}
    .header h1 {{ margin: 0 0 6px; font-size: 28px; }}
    .header p {{ margin: 0; font-size: 16px; opacity: 0.92; }}
    .report-section {{
      background: #ffffff;
      border-radius: 16px;
      overflow: hidden;
      box-shadow: var(--shadow);
      margin-bottom: 18px;
    }}
    .section-title {{
      background: var(--section-bg);
      color: var(--section-text);
      padding: 12px 16px;
      font-size: 16px;
      font-weight: 700;
    }}
    .table-wrapper {{ overflow-x: auto; padding-bottom: 8px; }}
    .report-table {{
      width: 100%;
      min-width: 1200px;
      border-collapse: collapse;
    }}
    .report-table th,
    .report-table td {{
      border: 1px solid var(--grid);
      padding: 6px 5px;
      text-align: center;
      font-size: 12px;
      white-space: nowrap;
    }}
    .report-table th {{
      background: var(--col-header-bg);
      color: var(--header-bg);
      font-weight: 700;
      position: sticky;
      top: 0;
      z-index: 1;
    }}
    .report-table td.left {{ text-align: left; min-width: 220px; }}
    .report-table td.center {{ min-width: 150px; }}
    .report-table td.right {{
      text-align: right;
      min-width: 110px;
      font-variant-numeric: tabular-nums;
    }}
    .report-table td.even {{ background: var(--row-even); }}
    .report-table td.odd {{ background: var(--row-odd); }}
    .report-table td.day-cell {{ font-weight: 700; min-width: 36px; }}
    .legend {{
      display: flex;
      flex-wrap: wrap;
      gap: 14px;
      align-items: center;
      background: #ffffff;
      border-radius: 16px;
      padding: 16px 18px;
      box-shadow: var(--shadow);
      margin: 18px 0 24px;
    }}
    .legend-item {{
      display: inline-flex;
      align-items: center;
      gap: 8px;
      font-size: 13px;
    }}
    .legend-swatch {{
      display: inline-flex;
      align-items: center;
      justify-content: center;
      width: 36px;
      height: 28px;
      border-radius: 8px;
      font-weight: 700;
    }}
    .charts {{
      display: grid;
      grid-template-columns: repeat(2, minmax(0, 1fr));
      gap: 22px;
      align-items: start;
    }}
    .chart-card {{
      background: #ffffff;
      border-radius: 16px;
      padding: 20px;
      box-shadow: var(--shadow);
    }}
    .chart-card h3 {{ margin: 0 0 14px; font-size: 18px; }}
    .chart-card img {{ width: 100%; height: auto; display: block; }}
    @media (max-width: 768px) {{
      .page {{ padding: 14px; }}
      .header h1 {{ font-size: 22px; }}
      .report-table th,
      .report-table td {{ font-size: 11px; padding: 5px 4px; }}
      .charts {{ grid-template-columns: 1fr; }}
    }}
  </style>
</head>
<body>
  <main class="page">
    <header class="header">
      <h1>Analise dos Pontos de Medicao</h1>
      <p>{month_name_pt}/{year}</p>
    </header>
    {"".join(sections_html)}
    {build_legend_html()}
    <section class="charts">
      {"".join(chart_cards)}
    </section>
  </main>
</body>
</html>
"""

    log.info("HTML gerado: %d chars", len(html_content))
    return html_content


# ═════════════════════════════════════════════
# 10.  ENVIO DE E-MAIL (SMTP / Office365)
# ═════════════════════════════════════════════

_SIG_DIR = os.path.join(
    os.environ.get("APPDATA", ""),
    "Microsoft", "Signatures",
    "MATHEUS (matheus.arruda@tempoenergia.com.br)_arquivos",
)


def _get_email_signature() -> str:
    img_path = os.path.join(_SIG_DIR, "image002.png")
    logo_tag = ""
    if os.path.exists(img_path):
        with open(img_path, "rb") as f:
            img_b64 = base64.b64encode(f.read()).decode("ascii")
        logo_tag = (
            f'<img src="data:image/png;base64,{img_b64}" '
            f'width="214" height="67" style="display:block;" />'
        )
    return (
        '<table border="0" cellspacing="0" cellpadding="0" style="border-collapse:collapse;">'
        "<tr>"
        '<td style="border-right:1px solid #6688C0;padding:0 12px 0 0;vertical-align:top;">'
        '<p style="margin:0;font-family:Aptos,Arial,sans-serif;font-size:12pt;color:#201F1E;">'
        "Atenciosamente,<br><br>"
        '<strong style="color:black;">Matheus Arruda</strong></p>'
        '<p style="margin:4px 0 0;font-family:Aptos,Arial,sans-serif;font-size:12pt;color:#201F1E;">'
        "Rua do Rocio, n&ordm; 84, 9&ordm; andar</p>"
        '<p style="margin:2px 0 0;font-family:Aptos,Arial,sans-serif;font-size:12pt;color:#201F1E;">'
        "Vila Ol&iacute;mpia, S&atilde;o Paulo - SP, 04.552-000</p>"
        '<p style="margin:2px 0 0;font-family:Aptos,Arial,sans-serif;font-size:12pt;color:#201F1E;">'
        "<strong>t.</strong>&nbsp;+55 11 4780-6788 | <strong>cel.</strong>&nbsp;+55 11&nbsp;97156-2284</p>"
        '<p style="margin:2px 0 0;font-family:Aptos,Arial,sans-serif;font-size:12pt;">'
        '<a href="mailto:matheus.arruda@tempoenergia.com.br" style="color:#0563C1;">matheus.arruda@tempoenergia.com.br</a></p>'
        '<p style="margin:2px 0 0;font-family:Aptos,Arial,sans-serif;font-size:12pt;">'
        '<a href="http://www.tempoenergia.com.br/" style="color:#0563C1;">www.tempoenergia.com.br</a></p>'
        "</td>"
        '<td style="padding:0 0 0 12px;vertical-align:top;">'
        f"{logo_tag}"
        "</td>"
        "</tr>"
        "</table>"
    )


def screenshot_html(html_content: str) -> Optional[bytes]:
    """Captura screenshot full-page do HTML em memória via Playwright. Retorna PNG bytes."""
    try:
        from playwright.sync_api import sync_playwright
        with sync_playwright() as pw:
            browser = pw.chromium.launch(headless=True)
            page = browser.new_page(viewport={"width": 1900, "height": 900})
            page.set_content(html_content, wait_until="load")
            png_bytes = page.screenshot(full_page=True)
            browser.close()
        log.info("Screenshot gerado: %d bytes", len(png_bytes))
        return png_bytes
    except Exception as exc:
        log.warning("Não foi possível gerar screenshot do HTML: %s", exc)
        return None


def send_email(pdf_bytes: bytes, year: int, month: int, html_content: Optional[str] = None) -> bool:
    """
    Envia o relatório por e-mail via SMTP (Office365).
    Corpo: screenshot do HTML (inline) + assinatura.
    Anexo: PDF do relatório (em memória, sem salvar em disco).
    """
    month_names = [
        "", "Janeiro", "Fevereiro", "Março", "Abril", "Maio", "Junho",
        "Julho", "Agosto", "Setembro", "Outubro", "Novembro", "Dezembro",
    ]
    month_name = month_names[month]
    subject = f"Análise dos Pontos de Medição — {month_name}/{year}"

    screenshot_bytes = screenshot_html(html_content) if html_content else None

    screenshot_tag = (
        '<p><img src="cid:relatorio_preview" style="max-width:100%;border:1px solid #ddd;'
        'border-radius:8px;" alt="Prévia do Relatório" /></p>'
        if screenshot_bytes else ""
    )

    body = (
        "<html><body style='font-family:Segoe UI,Arial,sans-serif;'>"
        "<p>Prezados,</p>"
        "<p>Segue a análise hora a hora dos pontos de medição da geração das usinas da "
        "Energo Pro e Gestão da Tempo Energia, considerando as informações do mês de "
        f"<strong>{month_name}</strong>.</p>"
        f"{screenshot_tag}"
        "<br>"
        + _get_email_signature()
        + "</body></html>"
    )

    try:
        for destinatario in EMAIL_TO:
            # Estrutura: mixed → related (html + imagem inline) + pdf anexo
            outer = MIMEMultipart("mixed")
            outer["From"]    = EMAIL_SENDER
            outer["To"]      = destinatario
            outer["Subject"] = subject

            related = MIMEMultipart("related")
            related.attach(MIMEText(body, "html"))

            if screenshot_bytes:
                img_part = MIMEImage(screenshot_bytes, "png")
                img_part.add_header("Content-ID", "<relatorio_preview>")
                img_part.add_header("Content-Disposition", "inline", filename="relatorio_preview.png")
                related.attach(img_part)

            outer.attach(related)

            ts = datetime.now().strftime("%Y%m%d")
            pdf_filename = f"Relatorio_Geracao_{year}{month:02d}_{ts}.pdf"
            attachment = MIMEBase("application", "octet-stream")
            attachment.set_payload(pdf_bytes)
            encoders.encode_base64(attachment)
            attachment.add_header(
                "Content-Disposition",
                f"attachment; filename={pdf_filename}",
            )
            outer.attach(attachment)

            with smtplib.SMTP(EMAIL_SMTP_HOST, EMAIL_SMTP_PORT) as server:
                server.starttls()
                server.login(EMAIL_SENDER, EMAIL_PASSWORD)
                server.sendmail(EMAIL_SENDER, destinatario, outer.as_bytes())

            log.info("E-mail enviado para: %s", destinatario)

        return True
    except Exception as exc:
        log.error("Falha ao enviar e-mail: %s", exc)
        return False


# ═════════════════════════════════════════════
# 11.  MODO DE DEMONSTRAÇÃO (dados simulados)
# ═════════════════════════════════════════════

def generate_demo_data(year: int, month: int) -> dict[str, Optional[pd.DataFrame]]:
    """
    Gera dados fictícios para testar o relatório sem conexão com a API.
    Remove esta função quando a API estiver disponível.
    """
    import numpy as np
    import random

    days_in_month = calendar.monthrange(year, month)[1]
    all_meters = {
        meter_id
        for meters in CATEGORIES.values()
        for meter_id, _ in meters
    }

    # Geração base por medidor (kWh/hora) — valores realistas
    base_gen = {
        "RJCAJUUSINA01P": 95,  "RJBJARUSTOA01P": 80,  "RJSSALUSINA01P": 115,
        "MGIB23UZTN-01P": 73,  "MGCRISUSINA01P": 30,  "MSVER4UVER401P": 140,
        "MSVER4UVE4A02P": 220, "RJCTRAGTOT-01P": 206, "MSMIM-VQTRA03P": 353,
        "PRUHBITR1--04P": 450, "PRUHBITR2--05P": 253, "PRUHBITR3--06P": 0,
        "PRUHBIUG1--01B": 457, "PRUHBIUG2--02B": 256, "PRUHBIUG3--03B": 0,
        "TONJD-UDOI-03P": 39,  "TONJD-UDCP-01P": 104, "MTY51RUMAMI01P": 64,
        "TONJD-UPCUM02P": 67,  "GOSRNPUTSRN01P": 0.2,
    }

    results: dict[str, Optional[pd.DataFrame]] = {}
    today = date.today()
    for meter_id in all_meters:
        bg = base_gen.get(meter_id, 50)
        records = []
        for d in range(1, days_in_month + 1):
            day_date = date(year, month, d)

            if day_date >= today:
                continue

            for h in range(1, EXPECTED_HOURS_PER_DAY + 1):
                if random.random() < 0.03:   # 3% chance de hora faltante
                    quality = "Faltante"
                    gen = 0.0
                else:
                    quality = "Completo"
                    gen = bg * random.gauss(1, 0.08) * max(0, random.uniform(0.8, 1.2))

                records.append({
                    "data":             str(day_date),
                    "hora":             h,
                    "medidor":          meter_id,
                    "Qualidade":        quality,
                    "ea_geracao_kwh":   max(0.0, gen),
                    "ea_consumo_kwh":   0.0,
                    "er_geracao_kvarh": gen * 0.01,
                    "er_consumo_kvarh": gen * 0.005,
                })

        df = pd.DataFrame(records)
        df["data"] = pd.to_datetime(df["data"]).dt.date
        results[meter_id] = df

    return results


# ═════════════════════════════════════════════
# 12.  PONTO DE ENTRADA
# ═════════════════════════════════════════════

def main():
    parser = argparse.ArgumentParser(
        description="Gerador de Relatório Diário de Geração — EnergoPro"
    )
    parser.add_argument("--year",     type=int, default=None,
                        help="Ano de referência (padrão: período da API_BASE_URL)")
    parser.add_argument("--month",    type=int, default=None,
                        help="Mês de referência (padrão: período da API_BASE_URL)")
    parser.add_argument("--no-email", action="store_true",
                        help="Não envia e-mail ao final")
    parser.add_argument("--demo",     action="store_true",
                        help="Usa dados simulados (sem conexão com a API)")
    parser.add_argument("--output",   type=str, default=None,
                        help="Se informado, salva HTML e PDF no caminho base indicado")
    args = parser.parse_args()

    year, month = resolve_report_period(args.year, args.month)

    month_names = [
        "", "Janeiro", "Fevereiro", "Março", "Abril", "Maio", "Junho",
        "Julho", "Agosto", "Setembro", "Outubro", "Novembro", "Dezembro",
    ]
    log.info("Iniciando relatório para %s/%d", month_names[month], year)

    # Coleta de dados
    if args.demo:
        log.info("Modo DEMO ativo — usando dados simulados")
        meter_data = generate_demo_data(year, month)
    else:
        meter_data = fetch_all_data(year, month)

    # Geração em memória (sem salvar em disco por padrão)
    html_content = generate_html(year, month, meter_data)
    pdf_bytes    = generate_pdf(year, month, meter_data)

    # Salva em disco somente se --output for fornecido
    if args.output:
        base_path = args.output.removesuffix(".html").removesuffix(".pdf")
        html_path = f"{base_path}.html"
        pdf_path  = f"{base_path}.pdf"
        with open(html_path, "w", encoding="utf-8") as f:
            f.write(html_content)
        with open(pdf_path, "wb") as f:
            f.write(pdf_bytes)
        log.info("Arquivos salvos: %s | %s", html_path, pdf_path)

    # Envia e-mail com screenshot inline + PDF em anexo
    if not args.no_email:
        send_email(pdf_bytes, year, month, html_content=html_content)
    else:
        log.info("Envio de e-mail desabilitado (--no-email).")

    log.info("Concluído.")


if __name__ == "__main__":
    main()

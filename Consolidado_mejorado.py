# -*- coding: utf-8 -*-
"""
CONSOLIDADO PROSEGUR – (estructura BRITIMP + SALDOS ANTERIORES + INV_ATM/INV_BCO)
-------------------------------------------------------------------------------
Ajustes clave:
- Estructura de carpetas (PENDIENTES=PROSEGUR/ASU|CDE|CNC|ENC|OVD,
  PROCESADO/AAAA-MM-DD/AGENCIA/, CONSOLIDADO/ se mantiene para usos futuros).
- Log con Loguru (archivo PROSEGUR_log.txt en PROSEGUR/).
- Extracción de SALDO ANTERIOR:
  * EC_ATM: fila con "Saldo Anterior" → [USD, PYG] → columnas SALDO_ANTERIOR_USD y
    SALDO_ANTERIOR_PYG (se repiten en todas las filas del archivo).
  * EC_BANCO: fila con "Saldo Anterior" por hoja/moneda → columna SALDO_ANTERIOR.
  * BULTOS_ATM: fila con "Saldo Anterior" → [cant_pyg, saldo_pyg, cant_usd, saldo_usd]
    → columnas SALDO_ANTERIOR_PYG y SALDO_ANTERIOR_USD.
  * BULTOS_BCO: fila con "Saldo Anterior" → [cant, saldo] → columna SALDO_ANTERIOR.
- Inventarios:
  * INV_ATM (saldo de inventario de billetes ATM)
  * INV_BCO (inventario de billetes banco)
- Normalización de agencias:
  * Carpetas físicas: ASU, CDE, CNC, ENC, OVD.
  * Si no se puede obtener la sucursal desde el documento, se intenta deducir
    desde el nombre de archivo con los códigos:
      1_10 → ASU
      2_10 → CDE
      5_10 → CNC (Concepción)
      3_10 → ENC
      4_10 → OVD
"""

import os
import re
import shutil
import unicodedata
from datetime import datetime
from pathlib import Path
from typing import Optional, Union, List, Dict, Tuple, Any

import numpy as np
import pandas as pd
import openpyxl  # asegura engine para to_excel
from loguru import logger
import sys


###############################
### DEFINICION DE VARIABLES ###
###############################

# ====== Estructura de carpetas estilo BRITIMP ======
AGENCIES = ['ASU', 'CDE', 'CNC', 'ENC', 'OVD']


def resolve_root_prosegur() -> Path:
    here = Path(__file__).resolve().parent if "__file__" in globals() else Path.cwd()
    if (here / 'PROSEGUR').exists():
        return here / 'PROSEGUR'
    return here


ROOT = resolve_root_prosegur()
PENDIENTES = ROOT  # PROSEGUR/ con subcarpetas de agencias
PROCESADO_DIR = ROOT / 'PROCESADO'
CONSOLIDADO_DIR = ROOT / 'CONSOLIDADO'

# Mantener nombres legacy por compatibilidad interna
FULL_PATH = str(PENDIENTES)
FULL_PATH_PROCESADO = str(PROCESADO_DIR)
FULL_PATH_CONSOLIDADO = str(CONSOLIDADO_DIR)

# Asegurar que existan las carpetas base
for d in [PENDIENTES, PROCESADO_DIR, CONSOLIDADO_DIR]:
    d.mkdir(parents=True, exist_ok=True)
for ag in AGENCIES:
    (PENDIENTES / ag).mkdir(parents=True, exist_ok=True)

# ====== Logger con Loguru ======
def setup_logger_prosegur() -> None:
    logger.remove()
    fmt = "{time:YYYY-MM-DD HH:mm:ss} - {level} - {message}"
    log_dir = ROOT
    log_file = log_dir / "PROSEGUR_log.txt"
    logger.add(sys.stdout, format=fmt, level="INFO")
    logger.add(str(log_file), format=fmt, level="INFO", encoding="utf-8")
    logger.info("==== Inicio ejecución PROSEGUR ====")
    logger.info(f"ROOT = {ROOT}")


#########################
### FUNCIONES HELPERS ###
#########################

def _strip_accents(s: str) -> str:
    return ''.join(c for c in unicodedata.normalize('NFD', s) if unicodedata.category(c) != 'Mn')


def _first_non_empty_after(row_vals: List[str], start_idx: int) -> Optional[int]:
    """Devuelve el índice de la primera celda no vacía a partir de start_idx."""
    if start_idx is None:
        return None
    for idx in range(start_idx + 1, len(row_vals)):
        if str(row_vals[idx]).strip() != '':
            return idx
    return None


def _get_cell(row_vals: List[str], idx: Optional[int], default: str = '') -> str:
    if idx is None or idx >= len(row_vals):
        return default
    v = str(row_vals[idx]).strip()
    return v if v != '' else default


def _only_digits(s: str) -> str:
    """Devuelve solo dígitos (para recibo), sin cortar ceros a la izquierda."""
    return ''.join(ch for ch in str(s) if ch.isdigit())


def _extraer_saldos_desde_fila(strip_cells: List[str], etiqueta: str = 'SALDO ANTERIOR') -> List[str]:
    """Dada una fila 'strippeada', devuelve todos los valores con dígitos a la derecha
    de la celda que contiene la etiqueta (por defecto 'Saldo Anterior')."""
    etiqueta_norm = _strip_accents(etiqueta).upper()
    for idx, celda in enumerate(strip_cells):
        celda_norm = _strip_accents(str(celda)).upper()
        if etiqueta_norm in celda_norm:
            valores: List[str] = []
            for j in range(idx + 1, len(strip_cells)):
                v = str(strip_cells[j]).strip()
                if not v:
                    continue
                if re.search(r'\d', v):
                    valores.append(v)
            return valores
    return []


def _ordenar_y_renombrar_columnas_ec(df: pd.DataFrame) -> pd.DataFrame:
    """Normaliza encabezados y asegura el orden final esperado para EC_ATM/EC_*."""
    df.columns = [str(c).replace(' ', '_').upper() for c in df.columns]
    rename_map = {'FECHA_OPER': 'FECHA', 'MOTIVO MOVIMIENTO': 'MOTIVO_MOVIMIENTO'}
    df = df.rename(columns=rename_map)
    orden_final = [
        'FECHA', 'SUCURSAL', 'RECIBO', 'BULTOS', 'GUARANIES', 'DOLARES',
        'ING_EGR', 'CLASIFICACION', 'FECHA_ARCHIVO', 'MOTIVO_MOVIMIENTO',
        'AGENCIA', 'SALDO_ANTERIOR_PYG', 'SALDO_ANTERIOR_USD'
    ]
    for col in orden_final:
        if col not in df.columns:
            df[col] = ''
    return df[orden_final]


def encontrar_fecha(texto: str) -> Optional[str]:
    """Encuentra dd/mm/yyyy en un texto; devuelve la primera coincidencia o None."""
    if not texto:
        return None
    m = re.search(r'(\d{1,2}/\d{1,2}/\d{4})', texto)
    return m.group(1) if m else None


def encontrar_fecha_en_columna(serie: pd.Series) -> Optional[str]:
    """Busca la primera fecha válida dd/mm/yyyy en una serie/columna libre."""
    for val in serie.astype(str).tolist():
        if not val:
            continue
        m = re.search(r'(\d{1,2}/\d{1,2}/\d{4})', val)
        if m:
            return m.group(1)
    return None


def get_agencia(linea_cabecera: str) -> str:
    """
    Extrae la agencia de una línea tipo 'PROSEGUR PARAGUAY S.A. (SUCURSAL: Ciudad del Este)'.
    Si no encuentra, devuelve ''.
    """
    if not linea_cabecera:
        return ''
    m = re.search(r'SUCURSAL:\s*([^)]+)\)', linea_cabecera, flags=re.IGNORECASE)
    return m.group(1).strip() if m else ''


def _normaliza_moneda(token: str) -> str:
    """Normaliza a: GUARANIES, DOLARES, EUROS, REALES, PESOS."""
    t = _strip_accents(str(token)).upper()
    if 'GUARANI' in t:
        return 'GUARANIES'
    if 'DOLAR' in t or 'DOLARE' in t or 'USD' in t:
        return 'DOLARES'
    if 'EURO' in t or 'EUR' in t:
        return 'EUROS'
    if 'REAL' in t or 'BRL' in t:
        return 'REALES'
    if 'PESO' in t or 'ARS' in t:
        return 'PESOS'
    return t


def _guess_currency_from_sheet_name(sheet_name: str) -> str:
    """Intenta inferir la moneda a partir del nombre de la hoja."""
    n = _strip_accents(str(sheet_name)).upper()
    if any(k in n for k in ['USD', 'DOLAR', 'DOLARES']):
        return 'DOLARES'
    if any(k in n for k in ['EURO', 'EUR']):
        return 'EUROS'
    if any(k in n for k in ['REAL', 'REALES', 'BRL']):
        return 'REALES'
    if any(k in n for k in ['PESO', 'PESOS', 'ARS']):
        return 'PESOS'
    return 'GUARANIES'


def _normaliza_moneda_iso(token: str) -> str:
    """Normaliza a ISO aproximado: PYG, USD, EUR, BRL, ARS."""
    t = _strip_accents(str(token)).upper()
    if 'USD' in t or 'DOLAR' in t or 'DOLARES' in t:
        return 'USD'
    if 'EUR' in t or 'EURO' in t:
        return 'EUR'
    if 'BRL' in t or 'REAL' in t or 'REALES' in t or 'R$' in token:
        return 'BRL'
    if 'ARS' in t or 'PESO' in t or 'PESOS' in t:
        return 'ARS'
    return 'PYG'


def _leer_hojas_excel(path_entrada: str, sheet_name=None) -> Dict[Union[int, str], pd.DataFrame]:
    """
    Devuelve un dict {nombre_hoja: DataFrame} según el parámetro.
    None -> todas las hojas; int/str -> una sola; lista -> solo esas.
    """
    if sheet_name is None:
        return pd.read_excel(path_entrada, sheet_name=None, header=None, dtype=str)
    if isinstance(sheet_name, (list, tuple)):
        return pd.read_excel(path_entrada, sheet_name=list(sheet_name), header=None, dtype=str)
    # un solo nombre/índice
    df = pd.read_excel(path_entrada, sheet_name=sheet_name, header=None, dtype=str)
    return {sheet_name: df}


def _ordenar_y_renombrar_columnas_ec_banco(df: pd.DataFrame) -> pd.DataFrame:
    """Normaliza encabezados y orden final para EC_BANCO (usa IMPORTE + MONEDA)."""
    df.columns = [str(c).replace(' ', '_').upper() for c in df.columns]
    df = df.rename(columns={
        'FECHA_OPER': 'FECHA',
        'MONTO': 'IMPORTE',
        'MOTIVO MOVIMIENTO': 'MOTIVO_MOVIMIENTO',
    })
    orden_final = [
        'FECHA', 'SUCURSAL', 'RECIBO', 'BULTOS', 'IMPORTE', 'MONEDA',
        'ING_EGR', 'CLASIFICACION', 'FECHA_ARCHIVO', 'MOTIVO_MOVIMIENTO',
        'AGENCIA', 'HOJA_ORIGEN', 'SALDO_ANTERIOR'
    ]
    for col in orden_final:
        if col not in df.columns:
            df[col] = ''
    return df[orden_final]


def _is_zero_like(s: str) -> bool:
    """True si el string representa 0 (con o sin separadores)."""
    t = str(s).replace(',', '').replace('.', '').strip()
    return t == '' or t == '0'


def _ordenar_y_renombrar_columnas_bultos(df: pd.DataFrame) -> pd.DataFrame:
    """
    Normaliza y ordena para BULTOS_* en formato LONG (ATM).

    Columnas finales:
      FECHA, SUCURSAL, RECIBO, BULTOS, MONEDA, IMPORTE,
      ING_EGR, CLASIFICACION, FECHA_ARCHIVO, MOTIVO_MOVIMIENTO, AGENCIA,
      SALDO_ANTERIOR_PYG, SALDO_ANTERIOR_USD
    """
    df.columns = [str(c).replace(' ', '_').upper() for c in df.columns]
    rename_map = {'FECHA_OPER': 'FECHA'}
    df = df.rename(columns=rename_map)
    orden_final = [
        'FECHA', 'SUCURSAL', 'RECIBO', 'BULTOS', 'MONEDA', 'IMPORTE',
        'ING_EGR', 'CLASIFICACION', 'FECHA_ARCHIVO', 'MOTIVO_MOVIMIENTO',
        'AGENCIA', 'SALDO_ANTERIOR_PYG', 'SALDO_ANTERIOR_USD'
    ]
    for col in orden_final:
        if col not in df.columns:
            df[col] = ''
    return df[orden_final]


def _ordenar_y_renombrar_columnas_bultos_bco(df: pd.DataFrame) -> pd.DataFrame:
    df.columns = [str(c).replace(' ', '_').upper() for c in df.columns]
    df = df.rename(columns={'FECHA_OPER': 'FECHA'})
    orden_final = [
        'FECHA', 'SUCURSAL', 'RECIBO', 'BULTOS', 'MONEDA', 'IMPORTE',
        'ING_EGR', 'CLASIFICACION', 'FECHA_ARCHIVO', 'MOTIVO_MOVIMIENTO',
        'AGENCIA', 'SALDO_ANTERIOR'
    ]
    for col in orden_final:
        if col not in df.columns:
            df[col] = ''
    return df[orden_final]


def _txt(x) -> str:
    return "" if (isinstance(x, (float, np.floating)) and pd.isna(x)) or x is None else str(x).strip()


def _upper(x) -> str:
    return re.sub(r"\s+", " ", _txt(x)).upper()


def _to_int(x) -> Optional[int]:
    """Convierte valores tipo '3.000', '3,000', '3000.0' a int. Devuelve None si no es número."""
    if x is None or (isinstance(x, float) and pd.isna(x)):
        return None
    if isinstance(x, (int, float)) and not isinstance(x, bool):
        try:
            return int(round(float(x)))
        except Exception:
            pass
    s = _txt(x).replace("\xa0", " ").strip()
    if s == "":
        return None
    if re.fullmatch(r"\d+\.\d+", s):
        return int(float(s))
    digits = re.sub(r"[^\d\-]", "", s)
    if digits in ("", "-", "--"):
        return None
    try:
        return int(digits)
    except Exception:
        return None


# ==== Helpers de agencia / sucursal ====
AGENCIA_FILE_DIGIT_MAP = {'1': 'ASU', '2': 'CDE', '5': 'CNC', '3': 'ENC', '4': 'OVD'}
AGENCIA_FILE_PATTERN = re.compile(r'(^|[^0-9])([1-5])_10([^0-9]|$)')


def infer_agencia_from_filename(path: Union[str, Path]) -> str:
    """Intenta deducir AGENCIA a partir del nombre de archivo usando códigos 1_10..5_10."""
    name = Path(path).name.upper()
    m = AGENCIA_FILE_PATTERN.search(name)
    if not m:
        return ''
    return AGENCIA_FILE_DIGIT_MAP.get(m.group(2), '')


def resolve_agencia_base(path_entrada: Union[str, Path], agencia_carpeta: Optional[str]) -> str:
    """Prioridad: nombre de archivo (1_10, 2_10, ...), luego carpeta (ASU/CDE/CNC/ENC/OVD)."""
    ag = infer_agencia_from_filename(path_entrada)
    if ag:
        return ag
    return (agencia_carpeta or '').strip()


def fill_agencia_column(df: pd.DataFrame, path_entrada: Union[str, Path], agencia_carpeta: Optional[str]) -> pd.DataFrame:
    """Rellena la columna AGENCIA cuando viene vacía.

    - Si el propio archivo ya tiene al menos un valor no vacío, se respeta y sólo se rellenan vacíos.
    - Si viene todo vacío, se usa:
        1) Código deducido del nombre del archivo (1_10→ASU, 2_10→CDE, 5_10→CNC, 3_10→ENC, 4_10→OVD)
        2) En su defecto, la carpeta de agencia (ASU/CDE/CNC/ENC/OVD)
    """
    if df is None or df.empty:
        return df
    if 'AGENCIA' not in df.columns:
        df['AGENCIA'] = ''
    non_empty = [str(v).strip() for v in df['AGENCIA'].unique() if str(v).strip()]
    if non_empty:
        base = non_empty[0]
    else:
        base = resolve_agencia_base(path_entrada, agencia_carpeta)
    if not base:
        return df
    df['AGENCIA'] = [base if not str(v).strip() else v for v in df['AGENCIA']]
    return df


################################################
### 1) EC_ATM: Estado de Cuenta de ATM (def) ###
################################################

def get_ec_atm(fecha_ejecucion: datetime,
               filename: str,
               dir_entrada: str,
               dir_consolidado: str,
               agencia_carpeta: Optional[str] = None,
               sheet_name: Union[int, str] = 0) -> Optional[str]:
    """
    Procesa un archivo EC_ATM*.* desde dir_entrada y genera un Excel procesado en dir_consolidado.
    Devuelve la ruta de salida o None si no se obtuvieron registros.
    """
    path_entrada = os.path.join(dir_entrada, filename)
    p = Path(path_entrada)
    stem_out = p.stem + '_PROCESADO'
    path_salida = os.path.join(dir_consolidado, f'{stem_out}.xlsx')

    df_raw = pd.read_excel(path_entrada, sheet_name=sheet_name, header=None, dtype=str).fillna('')

    rx_fecha_cell = re.compile(r'^\s*\d{1,2}/\d{1,2}/\d{4}\s*$')
    rx_totales = re.compile(r'\b(TOTAL|SUBTOTAL)\b', re.IGNORECASE)

    agencia = ''
    fecha_archivo = ''
    clasificacion = 'ATM'
    ing_egr = ''  # IN / OUT
    motivo_actual = ''

    registros: List[Dict[str, str]] = []

    saldo_ant_usd = ''
    saldo_ant_pyg = ''

    for _, row in df_raw.iterrows():
        cells = [str(x) if x is not None else '' for x in row.values]
        strip_cells = [c.strip() for c in cells]
        line_join = ' '.join([c for c in strip_cells if c])
        if not line_join:
            continue

        upper_join = _strip_accents(line_join).upper()

        # --- Captura de SALDO ANTERIOR (USD, PYG) ---
        if 'SALDO ANTERIOR' in upper_join and not (saldo_ant_usd or saldo_ant_pyg):
            saldos = _extraer_saldos_desde_fila(strip_cells)
            if len(saldos) >= 1:
                saldo_ant_usd = saldos[0]
            if len(saldos) >= 2:
                saldo_ant_pyg = saldos[1]
            continue

        if 'PROSEGUR PARAGUAY S.A.' in upper_join:
            ag = get_agencia(line_join)
            if ag:
                agencia = ag
            continue

        if 'ESTADO DE CUENTA DE' in upper_join:
            m_f = re.search(r'AL:\s*(\d{1,2}/\d{1,2}/\d{4})', line_join, flags=re.IGNORECASE)
            if m_f:
                fecha_archivo = m_f.group(1)
            continue

        if upper_join == 'INGRESOS':
            ing_egr = 'IN'
            motivo_actual = ''
            continue
        if upper_join == 'EGRESOS':
            ing_egr = 'OUT'
            motivo_actual = ''
            continue

        if 'INFORME DE PROCESOS' in upper_join:
            break
        if rx_totales.search(line_join):
            continue

        date_idx = next((i for i, c in enumerate(strip_cells) if rx_fecha_cell.match(c)), None)

        if ing_egr and date_idx is None:
            motivo_actual = line_join.strip()
            continue

        if ing_egr and motivo_actual and date_idx is not None:
            fecha_oper = strip_cells[date_idx]

            suc_idx = _first_non_empty_after(strip_cells, date_idx)
            sucursal = _get_cell(strip_cells, suc_idx, default='')

            rec_idx = _first_non_empty_after(strip_cells, suc_idx) if suc_idx is not None else None
            recibo_raw = _get_cell(strip_cells, rec_idx, default='')
            recibo_digits = _only_digits(recibo_raw)
            recibo = recibo_digits if recibo_digits != '' else recibo_raw

            bul_idx = _first_non_empty_after(strip_cells, rec_idx) if rec_idx is not None else None
            bultos = _get_cell(strip_cells, bul_idx, default='')

            gua_idx = _first_non_empty_after(strip_cells, bul_idx) if bul_idx is not None else None
            guaranies = _get_cell(strip_cells, gua_idx, default='0') or '0'

            usd_idx = _first_non_empty_after(strip_cells, gua_idx) if gua_idx is not None else None
            dolares = _get_cell(strip_cells, usd_idx, default='0') or '0'

            registros.append({
                'FECHA_OPER': fecha_oper,
                'SUCURSAL': sucursal,
                'RECIBO': recibo,
                'BULTOS': bultos,
                'GUARANIES': guaranies,
                'DOLARES': dolares,
                'ING_EGR': ing_egr,
                'CLASIFICACION': clasificacion,
                'FECHA_ARCHIVO': fecha_archivo,
                'MOTIVO_MOVIMIENTO': motivo_actual,
                'AGENCIA': agencia,
                'SALDO_ANTERIOR_PYG': saldo_ant_pyg,
                'SALDO_ANTERIOR_USD': saldo_ant_usd,
            })

    df_out = pd.DataFrame(registros)
    if df_out.empty:
        df_out = pd.DataFrame(columns=[
            'FECHA_OPER', 'SUCURSAL', 'RECIBO', 'BULTOS', 'GUARANIES', 'DOLARES',
            'ING_EGR', 'CLASIFICACION', 'FECHA_ARCHIVO', 'MOTIVO_MOVIMIENTO',
            'AGENCIA', 'SALDO_ANTERIOR_PYG', 'SALDO_ANTERIOR_USD'
        ])
        logger.info(f'[EC_ATM] {filename}: No se detectaron registros válidos.')

    df_out = _ordenar_y_renombrar_columnas_ec(df_out)
    df_out = fill_agencia_column(df_out, path_entrada, agencia_carpeta)
    df_out.to_excel(path_salida, index=False)
    logger.info(f'[EC_ATM] Guardado: {path_salida}')
    return path_salida


##################################################
### 2) EC_BCO: Estado de Cuenta de BANCO (def) ###
##################################################

def get_ec_banco(fecha_ejecucion: datetime,
                 filename: str,
                 dir_entrada: str,
                 dir_consolidado: str,
                 agencia_carpeta: Optional[str] = None,
                 sheet_name=None) -> Optional[str]:
    """
    Procesa un archivo EC_BANCO*.xlsx desde dir_entrada y genera un Excel procesado en dir_consolidado.
    Usa IMPORTE + MONEDA (una sola columna de monto con su moneda).
    Devuelve la ruta de salida o None si no se obtuvieron registros.
    """
    path_entrada = os.path.join(dir_entrada, filename)
    p = Path(path_entrada)
    stem_out = p.stem + '_PROCESADO'
    path_salida = os.path.join(dir_consolidado, f'{stem_out}.xlsx')

    hojas = _leer_hojas_excel(path_entrada, sheet_name=sheet_name)

    rx_fecha_linea = re.compile(r'^\s*(\d{1,2}/\d{1,2}/\d{4})\b')
    rx_totales = re.compile(r'\b(TOTAL|SUBTOTAL)\b', re.IGNORECASE)
    rx_moneda = re.compile(r'\b(GUARAN[IÍ]ES|D[ÓO]LARES|EUROS?|REALES?|PESOS?)\b', re.IGNORECASE)

    mapa_clasif = {
        'BANCO': 'BCO',
        'ATM': 'ATM',
        'BULTOS': 'BULTO BCO',
        'BULTO': 'BULTO BCO',
    }

    registros: List[Dict[str, str]] = []

    for nombre_hoja, df in hojas.items():
        df = df.fillna('')

        saldo_anterior_hoja = ''

        agencia = ''
        fecha_archivo = ''
        clasificacion = 'BCO'
        ing_egr = ''
        motivo_actual = ''

        moneda_por_hoja = _guess_currency_from_sheet_name(nombre_hoja)
        moneda_actual = moneda_por_hoja

        for _, row in df.iterrows():
            celdas = [str(x) if x is not None else '' for x in row.values]
            strip_cells = [c.strip() for c in celdas]
            linea = ' '.join([c for c in strip_cells if c])
            if not linea:
                continue

            linea_up = _strip_accents(linea).upper()

            if 'SALDO ANTERIOR' in linea_up and not saldo_anterior_hoja:
                valores = _extraer_saldos_desde_fila(strip_cells)
                if valores:
                    saldo_anterior_hoja = valores[0]
                continue

            if 'PROSEGUR PARAGUAY S.A.' in linea_up:
                ag = get_agencia(linea)
                if ag:
                    agencia = ag
                continue

            if 'ESTADO DE CUENTA DE' in linea_up:
                m_tipo = re.search(r'ESTADO DE CUENTA DE\s+(.*?)\s+AL[:\s]', linea, flags=re.IGNORECASE)
                if m_tipo:
                    texto = m_tipo.group(1).strip()
                    texto_norm = _strip_accents(texto).upper()
                    clasificacion = mapa_clasif.get(texto_norm, texto.strip()) or clasificacion
                m_f = re.search(r'AL[:\s]+(\d{1,2}/\d{1,2}/\d{4})', linea, flags=re.IGNORECASE)
                if m_f:
                    fecha_archivo = m_f.group(1)
                m_mon = rx_moneda.search(linea)
                if m_mon:
                    moneda_actual = _normaliza_moneda(m_mon.group(1))
                continue

            if linea_up == 'INGRESOS':
                ing_egr = 'IN'
                motivo_actual = ''
                continue
            if linea_up == 'EGRESOS':
                ing_egr = 'OUT'
                motivo_actual = ''
                continue

            if 'INFORME DE PROCESOS' in linea_up:
                break
            if rx_totales.search(linea):
                continue

            m_date = rx_fecha_linea.match(linea)
            if ing_egr and motivo_actual and m_date:
                parts = [str(x).strip() for x in row.values if str(x).strip()]
                if not parts:
                    continue

                fecha_oper = m_date.group(1)

                idx_rec = next(
                    (i for i, p in enumerate(parts[1:], 1) if re.fullmatch(r'\d{6,}', _strip_accents(p))),
                    None
                )
                if idx_rec is None:
                    continue

                sucursal = ' '.join(parts[1:idx_rec]).strip()
                recibo = parts[idx_rec]
                bultos = parts[idx_rec + 1] if idx_rec + 1 < len(parts) else ''
                importe = parts[idx_rec + 2] if idx_rec + 2 < len(parts) else ''

                registros.append({
                    'HOJA_ORIGEN': nombre_hoja,
                    'AGENCIA': agencia,
                    'FECHA_ARCHIVO': fecha_archivo,
                    'ING_EGR': ing_egr,
                    'CLASIFICACION': clasificacion,
                    'MOTIVO MOVIMIENTO': motivo_actual,
                    'FECHA_OPER': fecha_oper,
                    'SUCURSAL': sucursal,
                    'RECIBO': recibo,
                    'BULTOS': bultos,
                    'MONEDA': moneda_actual,
                    'MONTO': importe,
                    'SALDO_ANTERIOR': saldo_anterior_hoja,
                })

    df_out = pd.DataFrame(registros, columns=[
        'HOJA_ORIGEN', 'AGENCIA', 'FECHA_ARCHIVO', 'ING_EGR', 'CLASIFICACION',
        'MOTIVO MOVIMIENTO', 'FECHA_OPER', 'SUCURSAL',
        'RECIBO', 'BULTOS', 'MONEDA', 'MONTO', 'SALDO_ANTERIOR'
    ])

    if df_out.empty:
        df_out = pd.DataFrame(columns=[
            'HOJA_ORIGEN', 'AGENCIA', 'FECHA_ARCHIVO', 'ING_EGR', 'CLASIFICACION',
            'MOTIVO MOVIMIENTO', 'FECHA_OPER', 'SUCURSAL',
            'RECIBO', 'BULTOS', 'MONEDA', 'MONTO', 'SALDO_ANTERIOR'
        ])
        logger.info(f'[EC_BANCO] {filename}: No se detectaron registros válidos.')

    df_out = df_out.rename(columns={'MOTIVO MOVIMIENTO': 'MOTIVO_MOVIMIENTO'})
    df_out = _ordenar_y_renombrar_columnas_ec_banco(df_out)
    df_out = fill_agencia_column(df_out, path_entrada, agencia_carpeta)
    df_out.to_excel(path_salida, index=False)
    logger.info(f'[EC_BANCO] Guardado: {path_salida}')
    return path_salida


########################################################
### 3) EC_BULTOS_ATM: Estado Cuenta BULTOS ATM (def) ###
########################################################

def get_ec_bultos_atm(fecha_ejecucion: datetime,
                      filename: str,
                      dir_entrada: str,
                      dir_consolidado: str,
                      agencia_carpeta: Optional[str] = None,
                      sheet_name: Union[int, str] = 0,
                      descartar_usd_cero: bool = True) -> Optional[str]:
    """
    Procesa 'EC_BULTOS_ATM*.xlsx' en formato LONG (una fila por moneda: PYG, USD).
    - Genera salida en dir_consolidado con sufijo '_PROCESADO.xlsx'
    - Si descartar_usd_cero=True, no genera fila USD cuando IMPORTE USD==0
    Devuelve la ruta de salida o None si no hubo registros.
    """
    path_entrada = os.path.join(dir_entrada, filename)
    p = Path(path_entrada)
    stem_out = p.stem + '_PROCESADO'
    path_salida = os.path.join(dir_consolidado, f'{stem_out}.xlsx')

    df_x = pd.read_excel(path_entrada, sheet_name=sheet_name, header=None, dtype=str).fillna('')
    rx_fecha_cell = re.compile(r'^\s*\d{1,2}/\d{1,2}/\d{4}\s*$')
    rx_totales = re.compile(r'\b(TOTAL|SUBTOTAL)\b', re.IGNORECASE)

    agencia = ''
    fecha_archivo = ''
    clasificacion = 'ATM'
    ing_egr = ''
    motivo_actual = ''

    registros: List[Dict[str, str]] = []

    saldo_ant_pyg = ''
    saldo_ant_usd = ''

    for _, row in df_x.iterrows():
        cells = [str(x) if x is not None else '' for x in row.values]
        strip_cells = [c.strip() for c in cells]
        line_join = ' '.join([c for c in strip_cells if c])
        if not line_join:
            continue

        upper_join = _strip_accents(line_join).upper()

        if 'SALDO ANTERIOR' in upper_join and not (saldo_ant_pyg or saldo_ant_usd):
            saldos = _extraer_saldos_desde_fila(strip_cells)
            if len(saldos) >= 2:
                saldo_ant_pyg = saldos[1]
            if len(saldos) >= 4:
                saldo_ant_usd = saldos[3]
            continue

        if 'PROSEGUR PARAGUAY S.A.' in upper_join:
            agencia = get_agencia(line_join)
            continue

        if 'ESTADO DE CUENTA DE BULTOS DE ATM' in upper_join:
            m_f = re.search(r'AL:\s*(\d{1,2}/\d{1,2}/\d{4})', line_join, flags=re.IGNORECASE)
            if m_f:
                fecha_archivo = m_f.group(1)
            continue

        if upper_join == 'INGRESOS':
            ing_egr = 'IN'; motivo_actual = ''; continue
        if upper_join == 'EGRESOS':
            ing_egr = 'OUT'; motivo_actual = ''; continue

        if 'INFORME DE PROCESOS' in upper_join:
            break
        if rx_totales.search(line_join):
            continue

        date_idx = next((i for i, c in enumerate(strip_cells) if rx_fecha_cell.match(c)), None)

        if ing_egr and date_idx is None:
            motivo_actual = line_join.strip()
            continue

        if ing_egr and motivo_actual and date_idx is not None:
            fecha_oper = strip_cells[date_idx]

            suc_idx = _first_non_empty_after(strip_cells, date_idx)
            sucursal = _get_cell(strip_cells, suc_idx, default='')

            rec_idx = _first_non_empty_after(strip_cells, suc_idx) if suc_idx is not None else None
            recibo_raw = _get_cell(strip_cells, rec_idx, default='')
            recibo = _only_digits(recibo_raw) or recibo_raw

            bul_idx = _first_non_empty_after(strip_cells, rec_idx) if rec_idx is not None else None
            bultos_pyg = _get_cell(strip_cells, bul_idx, default='')
            gua_idx = _first_non_empty_after(strip_cells, bul_idx) if bul_idx is not None else None
            guaranies = _get_cell(strip_cells, gua_idx, default='0') or '0'

            usd_bul_idx = _first_non_empty_after(strip_cells, gua_idx) if gua_idx is not None else None
            bultos_usd = _get_cell(strip_cells, usd_bul_idx, default='')
            usd_imp_idx = _first_non_empty_after(strip_cells, usd_bul_idx) if usd_bul_idx is not None else None
            dolares = _get_cell(strip_cells, usd_imp_idx, default='0') or '0'

            registros.append({
                'FECHA_OPER': fecha_oper,
                'SUCURSAL': sucursal,
                'RECIBO': recibo,
                'BULTOS': bultos_pyg,
                'MONEDA': 'PYG',
                'IMPORTE': guaranies,
                'ING_EGR': ing_egr,
                'CLASIFICACION': clasificacion,
                'FECHA_ARCHIVO': fecha_archivo,
                'MOTIVO_MOVIMIENTO': motivo_actual,
                'AGENCIA': agencia,
                'SALDO_ANTERIOR_PYG': saldo_ant_pyg,
                'SALDO_ANTERIOR_USD': saldo_ant_usd,
            })

            if not (descartar_usd_cero and _is_zero_like(dolares)):
                registros.append({
                    'FECHA_OPER': fecha_oper,
                    'SUCURSAL': sucursal,
                    'RECIBO': recibo,
                    'BULTOS': bultos_usd,
                    'MONEDA': 'USD',
                    'IMPORTE': dolares,
                    'ING_EGR': ing_egr,
                    'CLASIFICACION': clasificacion,
                    'FECHA_ARCHIVO': fecha_archivo,
                    'MOTIVO_MOVIMIENTO': motivo_actual,
                    'AGENCIA': agencia,
                    'SALDO_ANTERIOR_PYG': saldo_ant_pyg,
                    'SALDO_ANTERIOR_USD': saldo_ant_usd,
                })

    df_out = pd.DataFrame(registros)
    if df_out.empty:
        df_out = pd.DataFrame(columns=[
            'FECHA_OPER','SUCURSAL','RECIBO','BULTOS','MONEDA','IMPORTE',
            'ING_EGR','CLASIFICACION','FECHA_ARCHIVO','MOTIVO_MOVIMIENTO',
            'AGENCIA','SALDO_ANTERIOR_PYG','SALDO_ANTERIOR_USD'
        ])
        logger.info(f'[EC_BULTOS_ATM] {filename}: No se detectaron registros válidos.')

    df_out = _ordenar_y_renombrar_columnas_bultos(df_out)
    df_out = fill_agencia_column(df_out, path_entrada, agencia_carpeta)
    df_out.to_excel(path_salida, index=False)
    logger.info(f'[EC_BULTOS_ATM] Guardado: {path_salida}')
    return path_salida


##########################################################
### 4) EC_BULTOS_BCO: Estado Cuenta BULTOS BANCO (def) ###
##########################################################

def get_ec_bultos_bco(fecha_ejecucion: datetime,
                      filename: str,
                      dir_entrada: str,
                      dir_consolidado: str,
                      agencia_carpeta: Optional[str] = None,
                      sheet_name=None) -> Optional[str]:
    """
    Procesa archivos 'EC_BULTOS_BCO*.xlsx' / 'EC_BULTOS_BANCO*.xlsx' en formato LONG:
      FECHA, SUCURSAL, RECIBO, BULTOS, MONEDA(ISO), IMPORTE, ING_EGR, CLASIFICACION,
      FECHA_ARCHIVO, MOTIVO_MOVIMIENTO, AGENCIA, SALDO_ANTERIOR
    """
    path_entrada = os.path.join(dir_entrada, filename)
    p = Path(path_entrada)
    stem_out = p.stem + '_PROCESADO'
    path_salida = os.path.join(dir_consolidado, f"{stem_out}.xlsx")

    if sheet_name is None:
        hojas = pd.read_excel(path_entrada, sheet_name=None, header=None, dtype=str)
    elif isinstance(sheet_name, (list, tuple)):
        hojas = pd.read_excel(path_entrada, sheet_name=list(sheet_name), header=None, dtype=str)
    else:
        df_one = pd.read_excel(path_entrada, sheet_name=sheet_name, header=None, dtype=str)
        hojas = {sheet_name: df_one}

    rx_fecha_linea = re.compile(r'^\s*(\d{1,2}/\d{1,2}/\d{4})\b')
    rx_totales = re.compile(r'\b(TOTAL|SUBTOTAL)\b', re.IGNORECASE)
    rx_moneda = re.compile(
        r'(GUARAN[IÍ]ES|D[ÓO]LARES|EUROS?|REALES?|PESOS?|PYG|GS|G\$|₲|USD|US\$|U\$S|R\$|BRL|EUR|ARS|€|\$)',
        re.IGNORECASE
    )

    mapa_clasif = {
        'BANCO': 'BCO',
        'ATM': 'ATM',
        'BULTOS': 'BULTO BCO',
        'BULTO': 'BULTO BCO',
    }

    registros: List[Dict[str, str]] = []

    for nombre_hoja, df in hojas.items():
        df = df.fillna('')

        saldo_anterior = ''

        agencia = ''
        fecha_archivo = ''
        clasificacion = 'BULTO BCO'
        ing_egr = ''
        motivo_actual = ''

        moneda_actual = _normaliza_moneda_iso(nombre_hoja)

        for _, row in df.iterrows():
            parts_all = [str(x).strip() for x in row.values if str(x).strip()]
            linea = ' '.join(parts_all)
            if not linea:
                continue

            linea_up = _strip_accents(linea).upper()

            if 'SALDO ANTERIOR' in linea_up and not saldo_anterior:
                valores = _extraer_saldos_desde_fila(parts_all)
                if len(valores) >= 2:
                    saldo_anterior = valores[1]
                elif len(valores) == 1:
                    saldo_anterior = valores[0]
                continue

            if 'PROSEGUR PARAGUAY S.A.' in linea_up:
                agencia = get_agencia(linea)
                continue

            if 'ESTADO DE CUENTA DE' in linea_up:
                m_tipo = re.search(r'ESTADO DE CUENTA DE\s+(.*?)\s+AL[:\s]', linea, flags=re.IGNORECASE)
                if m_tipo:
                    texto = m_tipo.group(1).strip()
                    texto_norm = _strip_accents(texto).upper()
                    clasificacion = mapa_clasif.get(texto_norm, texto.strip()) or clasificacion
                m_f = re.search(r'AL[:\s]+(\d{1,2}/\d{1,2}/\d{4})', linea, flags=re.IGNORECASE)
                if m_f:
                    fecha_archivo = m_f.group(1)
                m_mon = rx_moneda.search(linea)
                if m_mon:
                    moneda_actual = _normaliza_moneda_iso(m_mon.group(1))
                continue

            if linea_up == 'INGRESOS':
                ing_egr = 'IN'; motivo_actual = ''; continue
            if linea_up == 'EGRESOS':
                ing_egr = 'OUT'; motivo_actual = ''; continue

            if 'INFORME DE PROCESOS' in linea_up:
                break
            if rx_totales.search(linea):
                continue

            m_date = rx_fecha_linea.match(linea)
            if ing_egr and motivo_actual and m_date:
                parts = parts_all
                if not parts:
                    continue

                fecha_oper = parts[0]

                idx_rec = next(
                    (i for i, p in enumerate(parts[1:], 1) if re.fullmatch(r'\d{6,}', _strip_accents(p))),
                    None
                )
                if idx_rec is None:
                    continue

                sucursal = ' '.join(parts[1:idx_rec]).strip()
                recibo = parts[idx_rec]
                bultos = parts[idx_rec + 1] if idx_rec + 1 < len(parts) else ''
                importe = parts[idx_rec + 2] if idx_rec + 2 < len(parts) else ''

                m_mon_inline = rx_moneda.search(linea)
                if m_mon_inline:
                    moneda_actual = _normaliza_moneda_iso(m_mon_inline.group(1))

                registros.append({
                    'FECHA': fecha_oper,
                    'SUCURSAL': sucursal,
                    'RECIBO': recibo,
                    'BULTOS': bultos,
                    'MONEDA': moneda_actual or 'PYG',
                    'IMPORTE': importe,
                    'ING_EGR': ing_egr,
                    'CLASIFICACION': clasificacion,
                    'FECHA_ARCHIVO': fecha_archivo,
                    'MOTIVO_MOVIMIENTO': motivo_actual,
                    'AGENCIA': agencia,
                    'SALDO_ANTERIOR': saldo_anterior,
                })

    cols = [
        'FECHA','SUCURSAL','RECIBO','BULTOS','MONEDA','IMPORTE',
        'ING_EGR','CLASIFICACION','FECHA_ARCHIVO','MOTIVO_MOVIMIENTO','AGENCIA','SALDO_ANTERIOR'
    ]
    df_out = pd.DataFrame(registros, columns=cols)
    if df_out.empty:
        df_out = pd.DataFrame(columns=cols)
        logger.info(f'[EC_BULTOS_BCO] {filename}: No se detectaron registros válidos.')

    df_out = _ordenar_y_renombrar_columnas_bultos_bco(df_out)
    df_out = fill_agencia_column(df_out, path_entrada, agencia_carpeta)
    df_out.to_excel(path_salida, index=False)
    logger.info(f'[EC_BULTOS_BCO] Guardado: {path_salida}')
    return path_salida


###########################################
### 5) INV_ATM: INVENTARIO DE ATM (def) ###
###########################################

def get_inv_atm(fecha_ejecucion: datetime,
                filename: str,
                dir_entrada: str,
                dir_consolidado: str,
                agencia_carpeta: Optional[str] = None,
                include_zeros: bool = True) -> Optional[str]:
    """Procesa un archivo 'INV_BILLETES_ATM*.xls[x]' y genera un Excel procesado.

    Columnas de salida:
      FECHA_INVENTARIO, DIVISA, AGENCIA, AGRUPACION_EFECTIVO, TIPO_VALOR,
      DENOMINACION, DEPOSITO, CJE_DEP, CANJE, MONEDA, IMPORTE_TOTAL
    """
    AGRUP_TOKENS = ["TESORO ATM", "FAJOS ATM", "PICOS ATM"]
    TIPO_TOKENS = ["BILLETES (LADRILLOS)", "BILLETES"]
    FIN_MONEDA_UP = "TOTAL DE LA MONEDA"
    STOP_ROW_TOKENS = {
        "SUB TOTAL", "SUBTOTAL", "TOTAL DEL DEPÓSITO", "TOTAL DEL DEPOSITO",
        "TOTAL DEPÓSITO", "TOTAL DEPOSITO",
    }
    DATE_RE = re.compile(r"(\d{2}/\d{2}/\d{4})")
    AGENCIA_RE = re.compile(r"SUCURSAL:\s*([^)]+)\)", re.IGNORECASE)
    TITULO_RE = re.compile(r"SALDO DE INVENTARIO DE BILLETES ATM AL", re.IGNORECASE)

    def normaliza_divisa_code(code_upper: str) -> str:
        up = (code_upper or "").upper()
        if "USD" in up:
            return "USD"
        if "PYG" in up or "GUARANI" in _strip_accents(up):
            return "PYG"
        return up

    path_entrada = os.path.join(dir_entrada, filename)
    p = Path(path_entrada)
    stem_out = p.stem + "_PROCESADO"
    path_salida = os.path.join(dir_consolidado, f"{stem_out}.xlsx")

    def extrae_agencia_y_fecha(df: pd.DataFrame) -> Dict[str, str]:
        agencia, fecha = "", ""
        for _, row in df.iterrows():
            for cell in row:
                t = _txt(cell)
                if not agencia:
                    m = AGENCIA_RE.search(t)
                    if m:
                        agencia = m.group(1).strip()
                if not fecha and (TITULO_RE.search(t) or "SALDO DE INVENTARIO" in t.upper()):
                    m = DATE_RE.search(t)
                    if m:
                        fecha = m.group(1)
            if agencia and fecha:
                break
        if not fecha:
            for _, row in df.iterrows():
                for cell in row:
                    m = DATE_RE.search(_txt(cell))
                    if m:
                        fecha = m.group(1)
                        break
                if fecha:
                    break
        return {"AGENCIA": agencia, "FECHA_INVENTARIO": fecha}

    def capturar_codigo_total(row_text_upper: str) -> Optional[str]:
        m = re.search(r"TOTAL\s+DE\s+LA\s+MONEDA\s+([A-Z]{3})", row_text_upper, flags=re.IGNORECASE)
        return m.group(1).upper() if m else None

    def buscar_fin_y_codigo(df: pd.DataFrame) -> Tuple[int, Optional[str]]:
        row_end, code = len(df), None
        for i, row in df.iterrows():
            row_up = " | ".join(_upper(c) for c in row.tolist())
            if FIN_MONEDA_UP in row_up:
                row_end = i
                code = capturar_codigo_total(row_up)
                break
        return row_end, code

    def buscar_inicio_por_divisa(df: pd.DataFrame, row_end: int) -> Optional[int]:
        for i, row in df.iterrows():
            if i >= row_end:
                break
            ups = [_upper(c) for c in row.tolist() if _txt(c)]
            if "USD" in ups or "PYG" in ups:
                return i
        return None

    def buscar_inicio_fallback(df: pd.DataFrame, row_end: int) -> Optional[int]:
        for i, row in df.iterrows():
            if i >= row_end:
                break
            nums = [j for j, c in enumerate(row.tolist()) if _to_int(c) is not None]
            if not nums:
                continue
            denom_col = nums[0]
            left_up = " ".join(_upper(row.iloc[j]) for j in range(0, denom_col) if _txt(row.iloc[j]))
            if any(tok in left_up for tok in AGRUP_TOKENS):
                return i
        return None

    def localiza_bloque(df: pd.DataFrame) -> Dict[str, Any]:
        row_end, code_total = buscar_fin_y_codigo(df)
        row_start = buscar_inicio_por_divisa(df, row_end)
        if row_start is None:
            row_start = buscar_inicio_fallback(df, row_end)
        if row_start is None:
            raise ValueError("No se pudo determinar el inicio del bloque (no se encontró USD/PYG ni agrupación con denominación).")
        return {"row_start": row_start, "row_end": row_end, "moneda_codigo": code_total}

    def fila_es_total_o_subtotal(row) -> bool:
        up = " ".join(_upper(c) for c in row.tolist())
        return any(tok in up for tok in STOP_ROW_TOKENS) or FIN_MONEDA_UP in up

    def lista_numeros_con_indices(row) -> List[Tuple[int, int]]:
        out = []
        for j, c in enumerate(row.tolist()):
            v = _to_int(c)
            if v is not None:
                out.append((j, v))
        return out

    def siguiente_numero_a_la_derecha(row, desde_col: int) -> Tuple[int, int]:
        ncols = len(row)
        for j in range(desde_col + 1, ncols):
            v = _to_int(row.iloc[j])
            if v is not None:
                return j, v
        return ncols, 0

    def detectar_agrup_y_tipo(left_cells: List[str]) -> Tuple[Optional[str], Optional[str]]:
        left_up = " ".join(_upper(c) for c in left_cells if _txt(c))
        tipo = None
        for t in TIPO_TOKENS:
            if t in left_up:
                tipo = t
                break
        agrup = None
        for a in AGRUP_TOKENS:
            if a in left_up:
                agrup = a
                break
        return agrup, tipo

    def parsea_cuerpo(df: pd.DataFrame, row_start: int, row_end: int, default_code: Optional[str],
                      agencia: str, fecha: str) -> List[Dict[str, Any]]:
        registros: List[Dict[str, Any]] = []
        cur_divisa = normaliza_divisa_code(default_code or "")
        cur_agrup, cur_tipo = "", ""

        for i in range(row_start, row_end):
            row = df.iloc[i]
            if fila_es_total_o_subtotal(row):
                continue

            ups = [_upper(c) for c in row.tolist() if _txt(c)]
            if "USD" in ups:
                cur_divisa = "USD"
            elif "PYG" in ups:
                cur_divisa = "PYG"

            nums = lista_numeros_con_indices(row)
            if not nums:
                continue
            denom_col, denom_val = nums[0]

            left_cells = [row.iloc[j] for j in range(0, denom_col)]
            agrup, tipo = detectar_agrup_y_tipo(left_cells)
            if agrup:
                cur_agrup = agrup
            if tipo:
                cur_tipo = tipo

            idx = denom_col
            vals: List[int] = []
            for _ in range(5):
                idx, v = siguiente_numero_a_la_derecha(row, idx)
                vals.append(v)
            while len(vals) < 5:
                vals.append(0)

            reg = {
                "FECHA_INVENTARIO": fecha,
                "DIVISA": cur_divisa or "PYG",
                "AGENCIA": agencia,
                "AGRUPACION_EFECTIVO": cur_agrup,
                "TIPO_VALOR": cur_tipo,
                "DENOMINACION": denom_val,
                "DEPOSITO": vals[0] or 0,
                "CJE_DEP": vals[1] or 0,
                "CANJE": vals[2] or 0,
                "MONEDA": vals[3] or 0,
                "IMPORTE_TOTAL": vals[4] or 0,
            }
            if include_zeros or any([reg["DEPOSITO"], reg["CJE_DEP"], reg["CANJE"], reg["MONEDA"], reg["IMPORTE_TOTAL"]]):
                registros.append(reg)

        return registros

    try:
        xls = pd.ExcelFile(path_entrada, engine="openpyxl")
    except Exception as e:
        logger.error(f"[INV_ATM] {filename}: No se pudo abrir el archivo ({e}).")
        return None

    registros: List[Dict[str, Any]] = []
    for sheet in xls.sheet_names:
        try:
            df = pd.read_excel(path_entrada, sheet_name=sheet, header=None, dtype=object, engine="openpyxl").fillna("")
            meta = extrae_agencia_y_fecha(df)
            lim = localiza_bloque(df)
            registros.extend(
                parsea_cuerpo(
                    df,
                    lim["row_start"],
                    lim["row_end"],
                    lim.get("moneda_codigo"),
                    meta.get("AGENCIA", ""),
                    meta.get("FECHA_INVENTARIO", ""),
                )
            )
        except Exception as e:
            logger.warning(f"[INV_ATM] Hoja '{sheet}' omitida: {e}")
            continue

    cols = [
        "FECHA_INVENTARIO",
        "DIVISA",
        "AGENCIA",
        "AGRUPACION_EFECTIVO",
        "TIPO_VALOR",
        "DENOMINACION",
        "DEPOSITO",
        "CJE_DEP",
        "CANJE",
        "MONEDA",
        "IMPORTE_TOTAL",
    ]

    if not registros:
        logger.info(f"[INV_ATM] {filename}: No se extrajeron filas.")
        pd.DataFrame(columns=cols).to_excel(path_salida, index=False)
        logger.info(f"[INV_ATM] Guardado vacío: {path_salida}")
        return path_salida

    df_out = pd.DataFrame(registros)[cols]
    df_out.sort_values(
        by=[
            "FECHA_INVENTARIO",
            "AGENCIA",
            "DIVISA",
            "AGRUPACION_EFECTIVO",
            "TIPO_VALOR",
            "DENOMINACION",
        ],
        inplace=True,
    )

    df_out = fill_agencia_column(df_out, path_entrada, agencia_carpeta)
    df_out.to_excel(path_salida, index=False)
    logger.info(f"[INV_ATM] Guardado: {path_salida}")
    return path_salida


#############################################
### 6) INV_BCO: INVENTARIO DE BANCO (def) ###
#############################################

def get_inv_bco(fecha_ejecucion: datetime,
                filename: str,
                dir_entrada: str,
                dir_consolidado: str,
                agencia_carpeta: Optional[str] = None,
                include_zeros: bool = True) -> Optional[str]:
    """Procesa un archivo 'INV_BILLETES_BANCO*.xls[x]' y genera un Excel procesado.

    Columnas de salida:
      FECHA_INVENTARIO, DIVISA, AGENCIA, AGRUPACION_EFECTIVO, TIPO_VALOR,
      DENOMINACION, DEPOSITO, CJE_DEP, CANJE, MONEDA, IMPORTE_TOTAL
    """
    AGRUP_TOKENS = ["TESORO EFECTIVO", "FAJOS EFECTIVOS", "PICOS EFECTIVO"]
    TIPO_TOKENS = [
        "BILLETES (LADRILLOS)",
        "MONEDAS (BOLSAS)",
        "MONEDAS (PAQUETES)",
        "BILLETES",
        "MONEDAS",
    ]
    FIN_MONEDA_UP = "TOTAL DE LA MONEDA"
    STOP_ROW_TOKENS = {
        "SUB TOTAL",
        "SUBTOTAL",
        "TOTAL DEL DEPÓSITO",
        "TOTAL DEL DEPOSITO",
        "TOTAL DEPÓSITO",
        "TOTAL DEPOSITO",
    }
    DATE_RE = re.compile(r"(\d{2}/\d{2}/\d{4})")
    TITULO_RE = re.compile(r"SALDOS?\s+DE\s+INVENTARIO\s+DE\s+BILLETES\s+AL", re.IGNORECASE)
    AGENCIA_RE = re.compile(r"SUCURSAL:\s*([^)]+)\)", re.IGNORECASE)

    def normaliza_divisa_code(code_upper: str) -> str:
        up = (code_upper or "").upper()
        if "USD" in up:
            return "DOLARES"
        if "PYG" in up or "GUARANI" in _strip_accents(up):
            return "GUARANIES"
        return up

    path_entrada = os.path.join(dir_entrada, filename)
    p = Path(path_entrada)
    stem_out = p.stem + "_PROCESADO"
    path_salida = os.path.join(dir_consolidado, f"{stem_out}.xlsx")

    def extrae_agencia_y_fecha(df: pd.DataFrame) -> Dict[str, str]:
        agencia, fecha = "", ""
        for _, row in df.iterrows():
            for cell in row:
                t = _txt(cell)
                if not agencia:
                    m = AGENCIA_RE.search(t)
                    if m:
                        agencia = m.group(1).strip()
                if not fecha and (TITULO_RE.search(t) or "INVENTARIO" in t.upper()):
                    m = DATE_RE.search(t)
                    if m:
                        fecha = m.group(1)
            if agencia and fecha:
                break
        if not fecha:
            for _, row in df.iterrows():
                for cell in row:
                    m = DATE_RE.search(_txt(cell))
                    if m:
                        fecha = m.group(1)
                        break
                if fecha:
                    break
        return {"AGENCIA": agencia, "FECHA_INVENTARIO": fecha}

    def capturar_codigo_total(row_text_upper: str) -> Optional[str]:
        m = re.search(r"TOTAL\s+DE\s+LA\s+MONEDA\s+([A-Z]{3})", row_text_upper, flags=re.IGNORECASE)
        return m.group(1).upper() if m else None

    def buscar_fin_y_codigo(df: pd.DataFrame) -> Tuple[int, Optional[str]]:
        row_end, code = len(df), None
        for i, row in df.iterrows():
            row_up = " | ".join(_upper(c) for c in row.tolist())
            if FIN_MONEDA_UP in row_up:
                row_end = i
                code = capturar_codigo_total(row_up)
                break
        return row_end, code

    def buscar_inicio_por_divisa(df: pd.DataFrame, row_end: int) -> Optional[int]:
        for i, row in df.iterrows():
            if i >= row_end:
                break
            ups = [_upper(c) for c in row.tolist() if _txt(c)]
            if "USD" in ups or "PYG" in ups:
                return i
        return None

    def buscar_inicio_por_cabecera(df: pd.DataFrame, row_end: int) -> Optional[int]:
        for i, row in df.iterrows():
            row_up = " | ".join(_upper(c) for c in row.tolist())
            if (
                "DIVISA" in row_up
                and "DENOM" in row_up
                and "CJE/DEP" in row_up
                and "IMPORTE" in row_up
            ):
                return i + 1
        return None

    def buscar_inicio_fallback(df: pd.DataFrame, row_end: int) -> Optional[int]:
        for i, row in df.iterrows():
            if i >= row_end:
                break
            nums = [j for j, c in enumerate(row.tolist()) if _to_int(c) is not None]
            if not nums:
                continue
            denom_col = nums[0]
            left_up = " ".join(_upper(row.iloc[j]) for j in range(0, denom_col) if _txt(row.iloc[j]))
            if any(tok in left_up for tok in AGRUP_TOKENS):
                return i
        return None

    def localiza_bloque(df: pd.DataFrame) -> Dict[str, Any]:
        row_end, code_total = buscar_fin_y_codigo(df)
        row_start = buscar_inicio_por_cabecera(df, row_end)
        if row_start is None:
            row_start = buscar_inicio_por_divisa(df, row_end)
        if row_start is None:
            row_start = buscar_inicio_fallback(df, row_end)
        if row_start is None:
            raise ValueError(
                "No se pudo determinar el inicio del bloque (cabecera / USD-PYG / agrupación+denominación)."
            )
        return {"row_start": row_start, "row_end": row_end, "moneda_codigo": code_total}

    def fila_es_total_o_subtotal(row) -> bool:
        up = " ".join(_upper(c) for c in row.tolist())
        return any(tok in up for tok in STOP_ROW_TOKENS) or FIN_MONEDA_UP in up

    def lista_numeros_con_indices(row) -> List[Tuple[int, int]]:
        out = []
        for j, c in enumerate(row.tolist()):
            v = _to_int(c)
            if v is not None:
                out.append((j, v))
        return out

    def siguiente_numero_a_la_derecha(row, desde_col: int) -> Tuple[int, int]:
        ncols = len(row)
        for j in range(desde_col + 1, ncols):
            v = _to_int(row.iloc[j])
            if v is not None:
                return j, v
        return ncols, 0

    def detectar_agrup_y_tipo(left_cells: List[str]) -> Tuple[Optional[str], Optional[str]]:
        left_up = " ".join(_upper(c) for c in left_cells if _txt(c))
        tipo = None
        for t in TIPO_TOKENS:
            if t in left_up:
                tipo = t
                break
        agrup = None
        for a in AGRUP_TOKENS:
            if a in left_up:
                agrup = a
                break
        return agrup, tipo

    def parsea_cuerpo(df: pd.DataFrame, row_start: int, row_end: int, default_code: Optional[str],
                      agencia: str, fecha: str) -> List[Dict[str, Any]]:
        registros: List[Dict[str, Any]] = []
        cur_divisa = normaliza_divisa_code(default_code or "")
        cur_agrup, cur_tipo = "", ""

        for i in range(row_start, row_end):
            row = df.iloc[i]
            if fila_es_total_o_subtotal(row):
                continue

            ups = [_upper(c) for c in row.tolist() if _txt(c)]
            if "USD" in ups:
                cur_divisa = normaliza_divisa_code("USD")
            elif "PYG" in ups:
                cur_divisa = normaliza_divisa_code("PYG")

            nums = lista_numeros_con_indices(row)
            if not nums:
                continue
            denom_col, denom_val = nums[0]

            left_cells = [row.iloc[j] for j in range(0, denom_col)]
            agrup, tipo = detectar_agrup_y_tipo(left_cells)
            if agrup:
                cur_agrup = agrup
            if tipo:
                cur_tipo = tipo

            idx = denom_col
            vals: List[int] = []
            for _ in range(5):
                idx, v = siguiente_numero_a_la_derecha(row, idx)
                vals.append(v)
            while len(vals) < 5:
                vals.append(0)

            reg = {
                "FECHA_INVENTARIO": fecha,
                "DIVISA": cur_divisa or "GUARANIES",
                "AGENCIA": agencia,
                "AGRUPACION_EFECTIVO": cur_agrup,
                "TIPO_VALOR": cur_tipo,
                "DENOMINACION": denom_val,
                "DEPOSITO": vals[0] or 0,
                "CJE_DEP": vals[1] or 0,
                "CANJE": vals[2] or 0,
                "MONEDA": vals[3] or 0,
                "IMPORTE_TOTAL": vals[4] or 0,
            }
            if include_zeros or any([reg["DEPOSITO"], reg["CJE_DEP"], reg["CANJE"], reg["MONEDA"], reg["IMPORTE_TOTAL"]]):
                registros.append(reg)

        return registros

    try:
        xls = pd.ExcelFile(path_entrada, engine="openpyxl")
    except Exception as e:
        logger.error(f"[INV_BCO] {filename}: No se pudo abrir el archivo ({e}).")
        return None

    registros: List[Dict[str, Any]] = []
    for sheet in xls.sheet_names:
        try:
            df = pd.read_excel(path_entrada, sheet_name=sheet, header=None, dtype=object, engine="openpyxl").fillna("")
            meta = extrae_agencia_y_fecha(df)
            lim = localiza_bloque(df)
            registros.extend(
                parsea_cuerpo(
                    df,
                    lim["row_start"],
                    lim["row_end"],
                    lim.get("moneda_codigo"),
                    meta.get("AGENCIA", ""),
                    meta.get("FECHA_INVENTARIO", ""),
                )
            )
        except Exception as e:
            logger.warning(f"[INV_BCO] Hoja '{sheet}' omitida: {e}")
            continue

    cols = [
        "FECHA_INVENTARIO",
        "DIVISA",
        "AGENCIA",
        "AGRUPACION_EFECTIVO",
        "TIPO_VALOR",
        "DENOMINACION",
        "DEPOSITO",
        "CJE_DEP",
        "CANJE",
        "MONEDA",
        "IMPORTE_TOTAL",
    ]

    if not registros:
        logger.info(f"[INV_BCO] {filename}: No se extrajeron filas.")
        pd.DataFrame(columns=cols).to_excel(path_salida, index=False)
        logger.info(f"[INV_BCO] Guardado vacío: {path_salida}")
        return path_salida

    df_out = pd.DataFrame(registros)[cols]
    df_out.sort_values(
        by=[
            "FECHA_INVENTARIO",
            "AGENCIA",
            "DIVISA",
            "AGRUPACION_EFECTIVO",
            "TIPO_VALOR",
            "DENOMINACION",
        ],
        inplace=True,
    )

    df_out = fill_agencia_column(df_out, path_entrada, agencia_carpeta)
    df_out.to_excel(path_salida, index=False)
    logger.info(f"[INV_BCO] Guardado: {path_salida}")
    return path_salida


# =============================
# RECOLECCIÓN Y EJECUCIÓN MAIN
# =============================

def collect_pending_files_prosegur() -> List[Tuple[Path, str]]:
    """Recorre PROSEGUR/ASU|CDE|CNC|ENC|OVD y junta archivos pendientes."""
    results: List[Tuple[Path, str]] = []
    for ag in AGENCIES:
        base = PENDIENTES / ag
        if not base.exists():
            continue
        for p in base.rglob('*'):
            if p.is_file() and p.suffix.lower() in ('.xlsx', '.xls', '.pdf') and not p.name.startswith('~'):
                results.append((p, ag))
    return results


def get_procesado_dir(fecha: datetime, agencia: str) -> Path:
    folder = PROCESADO_DIR / fecha.strftime('%Y-%m-%d') / agencia
    folder.mkdir(parents=True, exist_ok=True)
    return folder


def move_original(path: Path, destino_dir: Path, procesado_ok: bool) -> None:
    try:
        destino_dir.mkdir(parents=True, exist_ok=True)
        shutil.move(str(path), str(destino_dir / path.name))
        logger.info(f"Original movido a {destino_dir / path.name}")
    except Exception as e:
        logger.error(f"No se pudo mover original: {path} -> {destino_dir} ({e})")


def _dispatch_and_process(fecha_ejecucion: datetime, path: Path, agencia: str) -> None:
    """Detecta tipo por prefijo del nombre y ejecuta el parser correspondiente."""
    fname = path.name
    fname_upper = fname.upper()
    procesado_dir = get_procesado_dir(fecha_ejecucion, agencia)
    procesado_ok = False
    try:
        if fname_upper.startswith('EC_ATM'):
            get_ec_atm(fecha_ejecucion, fname, str(path.parent), str(procesado_dir), agencia)
            procesado_ok = True
        elif fname_upper.startswith('EC_BANCO') or fname_upper.startswith('EC_BCO'):
            get_ec_banco(fecha_ejecucion, fname, str(path.parent), str(procesado_dir), agencia)
            procesado_ok = True
        elif fname_upper.startswith('EC_BULTOS_ATM'):
            get_ec_bultos_atm(fecha_ejecucion, fname, str(path.parent), str(procesado_dir), agencia)
            procesado_ok = True
        elif fname_upper.startswith('EC_BULTOS_BCO') or fname_upper.startswith('EC_BULTOS_BANCO'):
            get_ec_bultos_bco(fecha_ejecucion, fname, str(path.parent), str(procesado_dir), agencia)
            procesado_ok = True
        elif fname_upper.startswith('INV_ATM'):
            get_inv_atm(fecha_ejecucion, fname, str(path.parent), str(procesado_dir), agencia)
            procesado_ok = True
        elif fname_upper.startswith('INV_BCO') or fname_upper.startswith('INV_BANCO'):
            get_inv_bco(fecha_ejecucion, fname, str(path.parent), str(procesado_dir), agencia)
            procesado_ok = True
        else:
            logger.warning(f"[SKIP] {fname}: no se reconoce el prefijo.")
        move_original(path, procesado_dir, procesado_ok)
    except Exception as e:
        logger.exception(f"Error procesando {fname}: {e}")
        move_original(path, procesado_dir, False)


if __name__ == '__main__':
    setup_logger_prosegur()
    fecha_ejecucion = datetime.now()
    pendientes = collect_pending_files_prosegur()
    if not pendientes:
        logger.info('No hay archivos en PROSEGUR/ASU|CDE|CNC|ENC|OVD.')
    else:
        for path, ag in pendientes:
            logger.info(f'Procesando: [{ag}] {path.name}')
            _dispatch_and_process(fecha_ejecucion, path, ag)
    logger.info('Fin de ejecución PROSEGUR.')

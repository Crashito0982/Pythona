# -*- coding: utf-8 -*-
"""
CONSOLIDADO PROSEGUR – estructura BRITIMP + SALDOS ANTERIORES

Qué hace este script:
- Escanea PROSEGUR/ASU|CDE|CNC|ENC|OVD en busca de archivos EC_* e INV_*.
- Detecta el tipo de archivo (EC_ATM, EC_BANCO, EC_BULTOS_ATM, EC_BULTOS_BCO, INV_ATM, INV_BCO).
- Para:
  * EC_ATM: agrega SALDO_ANTERIOR_USD y SALDO_ANTERIOR_PYG
  * EC_BANCO: agrega SALDO_ANTERIOR
  * EC_BULTOS_ATM: SALDO_ANTERIOR_PYG y SALDO_ANTERIOR_USD
  * EC_BULTOS_BCO: SALDO_ANTERIOR
- Normaliza agencias: ASU, CDE, CNC, ENC, OVD, incluyendo nombres como
  "Asunción", "Ciudad del Este", "Concepción", etc. y códigos 1_10, 2_10, 5_10, 3_10, 4_10
  en el nombre de archivo.
- Utiliza los parsers INV_ATM e INV_BCO de prosegur_unificado_v4_2.py.

Estructura de carpetas esperada (ejemplo):
PROSEGUR/
    ASU/
        EC_ATM_PY01_10_6112025_1.xlsx
        ...
    CDE/
    CNC/
    ENC/
    OVD/
    PROCESADO/
        2025-11-07/
            ASU/
                ...
    CONSOLIDADO/   (reservado para usos futuros)
    PROSEGUR_log.txt
"""

import os
import re
import sys
import shutil
import unicodedata
from datetime import datetime
from pathlib import Path
from typing import Optional, Union, List, Dict, Tuple, Any

import pandas as pd
from loguru import logger

# Reutilizamos inventarios desde tu script probado
import prosegur_unificado_v4_2 as v4


###############################
#  UBICACIÓN / RUTAS BASE    #
###############################

AGENCIES = ["ASU", "CDE", "CNC", "ENC", "OVD"]


def resolve_root_prosegur() -> Path:
    """
    Si existe carpeta PROSEGUR en el mismo nivel del script, la usa.
    Si no, usa el cwd como raíz PROSEGUR.
    """
    here = Path(__file__).resolve().parent if "__file__" in globals() else Path.cwd()
    if (here / "PROSEGUR").is_dir():
        return here / "PROSEGUR"
    return here


ROOT = resolve_root_prosegur()
PENDIENTES = ROOT  # PROSEGUR/
PROCESADO_DIR = ROOT / "PROCESADO"
CONSOLIDADO_DIR = ROOT / "CONSOLIDADO"

# Crear base
for d in [PROCESADO_DIR, CONSOLIDADO_DIR]:
    d.mkdir(parents=True, exist_ok=True)
for ag in AGENCIES:
    (PENDIENTES / ag).mkdir(parents=True, exist_ok=True)


####################
#   LOGGING        #
####################

def setup_logger_prosegur() -> None:
    logger.remove()
    fmt = "{level} - {message}"
    log_file = ROOT / "PROSEGUR_log.txt"
    logger.add(sys.stdout, format=fmt, level="INFO")
    logger.add(str(log_file), format=fmt, level="INFO", encoding="utf-8")
    logger.info("==== Inicio ejecución PROSEGUR ====")
    logger.info(f"ROOT = {ROOT}")


#########################
#   HELPERS GENERALES   #
#########################

def _strip_accents(s: str) -> str:
    return ''.join(
        c for c in unicodedata.normalize('NFD', s)
        if unicodedata.category(c) != 'Mn'
    )


def _first_non_empty_after(row_vals: List[str], start_idx: int) -> Optional[int]:
    """Devuelve índice de la primera celda no vacía a la derecha de start_idx."""
    if start_idx is None:
        return None
    for i in range(start_idx + 1, len(row_vals)):
        if str(row_vals[i]).strip() != '':
            return i
    return None


def _get_cell(row_vals: List[str], idx: Optional[int], default: str = '') -> str:
    if idx is None or idx >= len(row_vals):
        return default
    v = str(row_vals[idx]).strip()
    return v if v != '' else default


def _only_digits(s: str) -> str:
    """Devuelve sólo dígitos (para RECIBO)."""
    return ''.join(ch for ch in str(s) if ch.isdigit())


def _txt(x) -> str:
    return "" if pd.isna(x) else str(x).strip()


def _upper(x) -> str:
    return re.sub(r"\s+", " ", _txt(x)).upper()


def _to_int(x) -> Optional[int]:
    """Convierte '3.000', '3,000', '3000.0' a int. Devuelve None si no es número."""
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


def _is_zero_like(s: str) -> bool:
    """True si el string representa 0 (con o sin separadores)."""
    t = str(s).replace(',', '').replace('.', '').strip()
    return t == '' or t == '0'


def _leer_hojas_excel(path_entrada: str, sheet_name=None) -> Dict[Union[int, str], pd.DataFrame]:
    """
    Devuelve un dict {nombre_hoja: DataFrame} según sheet_name.
    None -> todas las hojas; int/str -> una sola; lista -> sólo esas.
    """
    if sheet_name is None:
        return pd.read_excel(path_entrada, sheet_name=None, header=None, dtype=str)
    if isinstance(sheet_name, (list, tuple)):
        return pd.read_excel(path_entrada, sheet_name=list(sheet_name), header=None, dtype=str)
    df = pd.read_excel(path_entrada, sheet_name=sheet_name, header=None, dtype=str)
    return {sheet_name: df}


#########################
#   AGENCIAS / SUCURSAL #
#########################

AGENCIA_ALIASES: Dict[str, List[str]] = {
    "ASU": ["ASU", "ASUNCION", "ASUNCIÓN", "ASUNCION.", "ASUNCIÓN.", "ASUNCION CENTRO"],
    "CDE": ["CDE", "CIUDAD DEL ESTE", "C. DEL ESTE", "C DEL ESTE", "CIUDAD DEL E.", "CDE."],
    "CNC": ["CNC", "CONCEPCION", "CONCEPCIÓN"],
    "ENC": ["ENC", "ENCARNACION", "ENCARNACIÓN"],
    "OVD": ["OVD", "CORONEL OVIEDO", "CNEL. OVIEDO", "C. OVIEDO"],
}

AGENCIA_FILE_DIGIT_MAP = {'1': 'ASU', '2': 'CDE', '5': 'CNC', '3': 'ENC', '4': 'OVD'}
AGENCIA_FILE_PATTERN = re.compile(r'(^|[^0-9])([1-5])_10([^0-9]|$)')


def _agencia_code_from_text(text: str) -> Optional[str]:
    if not text:
        return None
    t = _strip_accents(str(text)).upper()
    # Buscar códigos directos
    for code in AGENCIA_ALIASES.keys():
        if re.search(r'\b' + re.escape(code) + r'\b', t):
            return code
    # Buscar aliases descriptivos
    for code, aliases in AGENCIA_ALIASES.items():
        for al in aliases:
            al_norm = _strip_accents(al).upper()
            if re.search(r'\b' + re.escape(al_norm) + r'\b', t):
                return code
    return None


def infer_agencia_from_filename(path: Union[str, Path]) -> str:
    """Intenta deducir AGENCIA usando códigos 1_10..5_10 en el nombre de archivo."""
    name = Path(path).name.upper()
    m = AGENCIA_FILE_PATTERN.search(name)
    if not m:
        return ''
    return AGENCIA_FILE_DIGIT_MAP.get(m.group(2), '')


def resolve_agencia_base(path_entrada: Union[str, Path], agencia_carpeta: Optional[str]) -> str:
    """
    Prioridad:
      1) Código 1_10..5_10 en filename
      2) agencia_carpeta (ASU/CDE/CNC/ENC/OVD) mapeada con aliases
      3) Nombres de carpetas en la ruta que coincidan con aliases
    """
    ag = infer_agencia_from_filename(path_entrada)
    if ag:
        return ag

    if agencia_carpeta:
        code = _agencia_code_from_text(agencia_carpeta)
        if code:
            return code

    p = Path(path_entrada)
    for part in reversed(p.parts):
        code = _agencia_code_from_text(part)
        if code:
            return code

    return ''


def fill_agencia_column(
    df: pd.DataFrame,
    path_entrada: Union[str, Path],
    agencia_carpeta: Optional[str]
) -> pd.DataFrame:
    """
    Normaliza columna AGENCIA:
      - Si ya hay valores, toma el primero y lo normaliza a código (ASU/CDE/CNC/ENC/OVD).
      - Si está vacía, deduce por filename/carpeta.
    """
    if df is None or df.empty:
        return df
    if 'AGENCIA' not in df.columns:
        df['AGENCIA'] = ''

    non_empty = [str(v).strip() for v in df['AGENCIA'].unique() if str(v).strip()]
    base = ''
    if non_empty:
        base = _agencia_code_from_text(non_empty[0]) or non_empty[0]
    else:
        base = resolve_agencia_base(path_entrada, agencia_carpeta)

    if not base:
        return df

    base_code = _agencia_code_from_text(base) or base
    df['AGENCIA'] = base_code
    return df


#########################
#   MONEDAS / SALDOS    #
#########################

def _guess_currency_from_sheet_name(sheet_name: str) -> str:
    """Devuelve GUANARIES/DOLARES/EUROS/REALES/PESOS según nombre hoja."""
    n = _strip_accents(str(sheet_name)).upper()
    if any(k in n for k in ['USD', 'DOLAR', 'DÓLAR', 'DOLARES', 'DÓLARES']):
        return 'DOLARES'
    if any(k in n for k in ['EUR', 'EURO', 'EUROS']):
        return 'EUROS'
    if any(k in n for k in ['BRL', 'REAL', 'REALES']):
        return 'REALES'
    if any(k in n for k in ['ARS', 'PESO', 'PESOS', 'ARG']):
        return 'PESOS'
    if any(k in n for k in ['PYG', 'GUARANI', 'GUARANÍ']):
        return 'GUARANIES'
    return 'GUARANIES'


def _normaliza_moneda_iso(token: str) -> str:
    """Normaliza a ISO aproximado: PYG, USD, EUR, BRL, ARS."""
    t = _strip_accents(str(token)).upper().strip()
    if any(k in t for k in ['PYG', 'GS', 'G$', '₲', 'GUARANI', 'GUARANIES', 'GUARANÍ']):
        return 'PYG'
    if any(k in t for k in ['USD', 'US$', 'U$S', 'DOLAR', 'DÓLAR', 'DOLARES', 'DÓLARES']):
        return 'USD'
    if any(k in t for k in ['EUR', '€', 'EURO', 'EUROS']):
        return 'EUR'
    if any(k in t for k in ['BRL', 'R$', 'REAL', 'REALES']):
        return 'BRL'
    if any(k in t for k in ['ARS', 'PESO', 'PESOS', 'ARG']):
        return 'ARS'
    if '$' in t:
        return 'USD'
    return t or 'PYG'


def _extraer_saldos_desde_fila(strip_cells: List[str], etiqueta: str = 'SALDO ANTERIOR') -> List[str]:
    """
    Dada una fila (lista de strings), devuelve todos los valores que tienen dígitos
    a la derecha de la celda que contiene la etiqueta (por defecto 'Saldo Anterior').
    """
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


#########################
#   FORMATOS SALIDA     #
#########################

def _ordenar_y_renombrar_columnas_ec(df: pd.DataFrame) -> pd.DataFrame:
    df.columns = [str(c).replace(' ', '_').upper() for c in df.columns]
    df = df.rename(columns={'FECHA_OPER': 'FECHA', 'MOTIVO MOVIMIENTO': 'MOTIVO_MOVIMIENTO'})
    orden_final = [
        'FECHA', 'SUCURSAL', 'RECIBO', 'BULTOS', 'GUARANIES', 'DOLARES',
        'ING_EGR', 'CLASIFICACION', 'FECHA_ARCHIVO',
        'MOTIVO_MOVIMIENTO', 'AGENCIA', 'SALDO_ANTERIOR_PYG', 'SALDO_ANTERIOR_USD'
    ]
    for col in orden_final:
        if col not in df.columns:
            df[col] = ''
    return df[orden_final]


def _ordenar_y_renombrar_columnas_ec_banco(df: pd.DataFrame) -> pd.DataFrame:
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


def _ordenar_y_renombrar_columnas_bultos(df: pd.DataFrame) -> pd.DataFrame:
    """Para BULTOS_ATM en formato long (PYG/USD)."""
    df.columns = [str(c).replace(' ', '_').upper() for c in df.columns]
    df = df.rename(columns={'FECHA_OPER': 'FECHA'})
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


###############################
#  PARSER: EC_ATM (EFECTIVO) #
###############################

def get_ec_atm(
    fecha_ejecucion: datetime,
    filename: str,
    dir_entrada: str,
    dir_consolidado: str,
    agencia_carpeta: Optional[str] = None,
    sheet_name: Union[int, str] = 0
) -> Optional[Tuple[str, int]]:
    """
    Procesa EC_ATM_PY*.xlsx (estado de cuenta ATM) y genera un archivo procesado.
    Retorna (ruta_salida, n_registros) o None si no hubo registros.
    """
    path_entrada = os.path.join(dir_entrada, filename)
    p = Path(path_entrada)
    stem_out = p.stem + "_PROCESADO"
    path_salida = os.path.join(dir_consolidado, f"{stem_out}.xlsx")

    df_raw = pd.read_excel(path_entrada, sheet_name=sheet_name, header=None, dtype=str).fillna('')

    rx_fecha_cell = re.compile(r'^\s*\d{1,2}/\d{1,2}/\d{4}\s*$')
    rx_totales = re.compile(r'\b(TOTAL|SUBTOTAL)\b', re.IGNORECASE)

    agencia = ''
    fecha_archivo = ''
    clasificacion = 'ATM'
    ing_egr = ''
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

        # Saldo anterior USD / PYG
        if 'SALDO ANTERIOR' in upper_join and not (saldo_ant_usd or saldo_ant_pyg):
            saldos = _extraer_saldos_desde_fila(strip_cells)
            if len(saldos) >= 1:
                saldo_ant_usd = saldos[0]
            if len(saldos) >= 2:
                saldo_ant_pyg = saldos[1]
            continue

        # Agencia
        if 'PROSEGUR PARAGUAY S.A.' in upper_join:
            m = re.search(r'SUCURSAL:\s*([^)]+)\)', line_join, flags=re.IGNORECASE)
            if m:
                agencia = m.group(1).strip()
            continue

        # Encabezado con fecha archivo
        if 'ESTADO DE CUENTA DE' in upper_join:
            m_f = re.search(r'AL:\s*(\d{1,2}/\d{1,2}/\d{4})', line_join, flags=re.IGNORECASE)
            if m_f:
                fecha_archivo = m_f.group(1)
            continue

        # Ingresos / Egresos
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

        # Buscar celda fecha en la fila
        date_idx = next((i for i, c in enumerate(strip_cells) if rx_fecha_cell.match(c)), None)

        # Si no hay fecha pero hay sección IN/OUT → motivo
        if ing_egr and date_idx is None:
            motivo_actual = line_join.strip()
            continue

        # Fila de detalle
        if ing_egr and motivo_actual and date_idx is not None:
            fecha_oper = strip_cells[date_idx]

            suc_idx = _first_non_empty_after(strip_cells, date_idx)
            sucursal = _get_cell(strip_cells, suc_idx, default='')

            rec_idx = _first_non_empty_after(strip_cells, suc_idx) if suc_idx is not None else None
            recibo_raw = _get_cell(strip_cells, rec_idx, default='')
            recibo = _only_digits(recibo_raw) or recibo_raw

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
        logger.info(f"[EC_ATM] {filename}: No se detectaron registros válidos.")

    df_out = _ordenar_y_renombrar_columnas_ec(df_out)
    df_out = fill_agencia_column(df_out, path_entrada, agencia_carpeta)

    df_out.to_excel(path_salida, index=False)
    nregs = len(df_out)
    logger.info(f"[EC_ATM] Guardado: {path_salida}")
    return path_salida, nregs


################################
#  PARSER: EC_BANCO (EFECTIVO) #
################################

def get_ec_banco(
    fecha_ejecucion: datetime,
    filename: str,
    dir_entrada: str,
    dir_consolidado: str,
    agencia_carpeta: Optional[str] = None,
    sheet_name=None
) -> Optional[Tuple[str, int]]:
    """
    Procesa EC_BANCO/EC_BCO*.xlsx.  Una salida con IMPORTE + MONEDA y SALDO_ANTERIOR por hoja/moneda.
    """
    path_entrada = os.path.join(dir_entrada, filename)
    p = Path(path_entrada)
    stem_out = p.stem + "_PROCESADO"
    path_salida = os.path.join(dir_consolidado, f"{stem_out}.xlsx")

    hojas = _leer_hojas_excel(path_entrada, sheet_name=sheet_name)

    rx_fecha_linea = re.compile(r'^\s*(\d{1,2}/\d{1,2}/\d{4})\b')
    rx_totales = re.compile(r'\b(TOTAL|SUBTOTAL)\b', re.IGNORECASE)
    rx_moneda = re.compile(
        r'\b(GUARAN[IÍ]ES|D[ÓO]LARES|EUROS?|REALES?|PESOS?)\b', re.IGNORECASE
    )

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

            # SALDO ANTERIOR
            if 'SALDO ANTERIOR' in linea_up and not saldo_anterior_hoja:
                valores = _extraer_saldos_desde_fila(strip_cells)
                if valores:
                    saldo_anterior_hoja = valores[0]
                continue

            if 'PROSEGUR PARAGUAY S.A.' in linea_up:
                m = re.search(r'SUCURSAL:\s*([^)]+)\)', linea, flags=re.IGNORECASE)
                if m:
                    agencia = m.group(1).strip()
                continue

            if 'ESTADO DE CUENTA DE' in linea_up:
                m_f = re.search(r'AL[:\s]+(\d{1,2}/\d{1,2}/\d{4})', linea, flags=re.IGNORECASE)
                if m_f:
                    fecha_archivo = m_f.group(1)
                m_mon = rx_moneda.search(linea)
                if m_mon:
                    moneda_actual = _guess_currency_from_sheet_name(m_mon.group(1))
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
                    (i for i, p in enumerate(parts[1:], 1)
                     if sum(ch.isdigit() for ch in _strip_accents(p)) >= 4),
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
                    'CLASIFICACION': 'BCO',
                    'MOTIVO MOVIMIENTO': motivo_actual,
                    'FECHA_OPER': fecha_oper,
                    'SUCURSAL': sucursal,
                    'RECIBO': recibo,
                    'BULTOS': bultos,
                    'MONEDA': _normaliza_moneda_iso(moneda_actual),
                    'MONTO': importe,
                    'SALDO_ANTERIOR': saldo_anterior_hoja,
                })

    df_out = pd.DataFrame(registros, columns=[
        'HOJA_ORIGEN', 'AGENCIA', 'FECHA_ARCHIVO', 'ING_EGR', 'CLASIFICACION',
        'MOTIVO MOVIMIENTO', 'FECHA_OPER', 'SUCURSAL',
        'RECIBO', 'BULTOS', 'MONEDA', 'MONTO', 'SALDO_ANTERIOR'
    ])

    if df_out.empty:
        logger.info(f"[EC_BANCO] {filename}: No se detectaron registros válidos.")
        df_out = pd.DataFrame(columns=df_out.columns)

    df_out = df_out.rename(columns={'MOTIVO MOVIMIENTO': 'MOTIVO_MOVIMIENTO'})
    df_out = _ordenar_y_renombrar_columnas_ec_banco(df_out)
    df_out = fill_agencia_column(df_out, path_entrada, agencia_carpeta)

    df_out.to_excel(path_salida, index=False)
    nregs = len(df_out)
    logger.info(f"[EC_BANCO] Guardado: {path_salida}")
    return path_salida, nregs


######################################
#  PARSER: EC_BULTOS_ATM (BULTOS)   #
######################################

def get_ec_bultos_atm(
    fecha_ejecucion: datetime,
    filename: str,
    dir_entrada: str,
    dir_consolidado: str,
    agencia_carpeta: Optional[str] = None,
    sheet_name: Union[int, str] = 0,
    descartar_usd_cero: bool = True
) -> Optional[Tuple[str, int]]:
    """
    Procesa EC_BULTOS_ATM*.xlsx → formato long (una fila PYG, una USD).
    Extrae SALDO_ANTERIOR_PYG / SALDO_ANTERIOR_USD desde la fila 'Saldo Anterior'.
    """
    path_entrada = os.path.join(dir_entrada, filename)
    p = Path(path_entrada)
    stem_out = p.stem + "_PROCESADO"
    path_salida = os.path.join(dir_consolidado, f"{stem_out}.xlsx")

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
            # Esperado: [cant_pyg, saldo_pyg, cant_usd, saldo_usd]
            if len(saldos) >= 2:
                saldo_ant_pyg = saldos[1]
            if len(saldos) >= 4:
                saldo_ant_usd = saldos[3]
            continue

        if 'PROSEGUR PARAGUAY S.A.' in upper_join:
            m = re.search(r'SUCURSAL:\s*([^)]+)\)', line_join, flags=re.IGNORECASE)
            if m:
                agencia = m.group(1).strip()
            continue

        if 'ESTADO DE CUENTA DE BULTOS DE ATM' in upper_join:
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
            'FECHA_OPER', 'SUCURSAL', 'RECIBO', 'BULTOS', 'MONEDA', 'IMPORTE',
            'ING_EGR', 'CLASIFICACION', 'FECHA_ARCHIVO', 'MOTIVO_MOVIMIENTO',
            'AGENCIA', 'SALDO_ANTERIOR_PYG', 'SALDO_ANTERIOR_USD'
        ])
        logger.info(f"[EC_BULTOS_ATM] {filename}: No se detectaron registros válidos.")

    df_out = _ordenar_y_renombrar_columnas_bultos(df_out)
    df_out = fill_agencia_column(df_out, path_entrada, agencia_carpeta)
    df_out.to_excel(path_salida, index=False)
    nregs = len(df_out)
    logger.info(f"[EC_BULTOS_ATM] Guardado: {path_salida}")
    return path_salida, nregs


#######################################
#  PARSER: EC_BULTOS_BCO (BULTOS BCO) #
#######################################

def get_ec_bultos_bco(
    fecha_ejecucion: datetime,
    filename: str,
    dir_entrada: str,
    dir_consolidado: str,
    agencia_carpeta: Optional[str] = None,
    sheet_name=None
) -> Optional[Tuple[str, int]]:
    """
    Procesa EC_BULTOS_BCO*/EC_BULTOS_BANCO* → formato long.
    Extrae SALDO_ANTERIOR por hoja/moneda.
    """
    path_entrada = os.path.join(dir_entrada, filename)
    p = Path(path_entrada)
    stem_out = p.stem + "_PROCESADO"
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
                m = re.search(r'SUCURSAL:\s*([^)]+)\)', linea, flags=re.IGNORECASE)
                if m:
                    agencia = m.group(1).strip()
                continue

            if 'ESTADO DE CUENTA DE' in linea_up:
                m_f = re.search(r'AL[:\s]+(\d{1,2}/\d{1,2}/\d{4})', linea, flags=re.IGNORECASE)
                if m_f:
                    fecha_archivo = m_f.group(1)
                m_mon = rx_moneda.search(linea)
                if m_mon:
                    moneda_actual = _normaliza_moneda_iso(m_mon.group(1))
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
                parts = parts_all
                if not parts:
                    continue
                fecha_oper = parts[0]

                idx_rec = next(
                    (i for i, p in enumerate(parts[1:], 1)
                     if sum(ch.isdigit() for ch in _strip_accents(p)) >= 4),
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
        'FECHA', 'SUCURSAL', 'RECIBO', 'BULTOS', 'MONEDA', 'IMPORTE',
        'ING_EGR', 'CLASIFICACION', 'FECHA_ARCHIVO', 'MOTIVO_MOVIMIENTO',
        'AGENCIA', 'SALDO_ANTERIOR'
    ]
    df_out = pd.DataFrame(registros, columns=cols)
    if df_out.empty:
        df_out = pd.DataFrame(columns=cols)
        logger.info(f"[EC_BULTOS_BCO] {filename}: No se detectaron registros válidos.")

    df_out = _ordenar_y_renombrar_columnas_bultos_bco(df_out)
    df_out = fill_agencia_column(df_out, path_entrada, agencia_carpeta)
    df_out.to_excel(path_salida, index=False)
    nregs = len(df_out)
    logger.info(f"[EC_BULTOS_BCO] Guardado: {path_salida}")
    return path_salida, nregs


#############################
#  INV_ATM / INV_BCO (v4_2) #
#############################

def get_inv_atm(
    fecha_ejecucion: datetime,
    filename: str,
    dir_entrada: str,
    dir_consolidado: str,
    agencia_carpeta: Optional[str] = None,
    include_zeros: bool = True
) -> Optional[Tuple[str, int]]:
    """
    Wrapper sobre v4.get_inv_atm (tu lógica probada).
    - Llama a v4.get_inv_atm(..., collect_only=True) para obtener un DataFrame.
    - Agrega columna AGENCIA (normalizada).
    - Escribe *_PROCESADO.xlsx en dir_consolidado.
    """
    path_entrada = os.path.join(dir_entrada, filename)
    p = Path(path_entrada)
    stem_out = p.stem + "_PROCESADO"
    path_salida = os.path.join(dir_consolidado, f"{stem_out}.xlsx")

    df = v4.get_inv_atm(
        fecha_ejecucion,
        filename,
        dir_entrada,
        dir_consolidado=None,
        include_zeros=include_zeros,
        collect_only=True,
        output_path=None
    )
    if df is None or df.empty:
        logger.info(f"[INV_ATM] {filename}: No se detectaron registros válidos.")
        return None

    df = fill_agencia_column(df, path_entrada, agencia_carpeta)
    df.to_excel(path_salida, index=False)
    nregs = len(df)
    logger.info(f"[INV_ATM] Guardado: {path_salida}")
    return path_salida, nregs


def get_inv_bco(
    fecha_ejecucion: datetime,
    filename: str,
    dir_entrada: str,
    dir_consolidado: str,
    agencia_carpeta: Optional[str] = None,
    include_zeros: bool = True
) -> Optional[Tuple[str, int]]:
    """
    Wrapper sobre v4.get_inv_bco (tu lógica probada).
    """
    path_entrada = os.path.join(dir_entrada, filename)
    p = Path(path_entrada)
    stem_out = p.stem + "_PROCESADO"
    path_salida = os.path.join(dir_consolidado, f"{stem_out}.xlsx")

    df = v4.get_inv_bco(
        fecha_ejecucion,
        filename,
        dir_entrada,
        dir_consolidado=None,
        include_zeros=include_zeros,
        collect_only=True,
        output_path=None
    )
    if df is None or df.empty:
        logger.info(f"[INV_BCO] {filename}: No se detectaron registros válidos.")
        return None

    df = fill_agencia_column(df, path_entrada, agencia_carpeta)
    df.to_excel(path_salida, index=False)
    nregs = len(df)
    logger.info(f"[INV_BCO] Guardado: {path_salida}")
    return path_salida, nregs


##########################
#  DISPATCHER / MAIN     #
##########################

def collect_pending_files_prosegur() -> List[Tuple[Path, str]]:
    """Busca archivos en PROSEGUR/ASU|CDE|CNC|ENC|OVD."""
    results: List[Tuple[Path, str]] = []
    for ag in AGENCIES:
        base = PENDIENTES / ag
        if not base.exists():
            continue
        for p in base.rglob("*"):
            if p.is_file() and p.suffix.lower() in (".xlsx", ".xls", ".pdf") and not p.name.startswith("~"):
                results.append((p, ag))
    return results


def get_procesado_dir(fecha: datetime, agencia: str) -> Path:
    d = PROCESADO_DIR / fecha.strftime("%Y-%m-%d") / agencia
    d.mkdir(parents=True, exist_ok=True)
    return d


def move_original(path: Path, dest_dir: Path) -> bool:
    try:
        dest_dir.mkdir(parents=True, exist_ok=True)
        shutil.move(str(path), str(dest_dir / path.name))
        return True
    except Exception as e:
        logger.info(f" - > [MOVIDO ERROR] '{path.name}' → '{dest_dir}': {e}")
        return False


def _count_rows_excel(xlsx_path: str) -> int:
    try:
        df = pd.read_excel(xlsx_path)
        return int(df.shape[0])
    except Exception:
        return 0


def _detectar_tipo(fname_upper: str) -> Optional[str]:
    """
    Determina el tipo lógico de archivo según el nombre:
      - EC_ATM*
      - EC_BANCO* / EC_BCO*
      - EC_BULTOS_ATM*
      - EC_BULTOS_BCO* / EC_BULTOS_BANCO*
      - INV_BILLETES_ATM*, INV_ATM* → INV_ATM
      - INV_BILLETES_BANCO*, INV_BCO*, INV_BANCO* → INV_BCO
    """
    if fname_upper.startswith("EC_ATM"):
        return "EC_ATM"
    if fname_upper.startswith("EC_BANCO") or fname_upper.startswith("EC_BCO"):
        return "EC_EFECT_BCO"
    if fname_upper.startswith("EC_BULTOS_ATM"):
        return "EC_BULTOS_ATM"
    if fname_upper.startswith("EC_BULTOS_BCO") or "EC_BULTOS_BANCO" in fname_upper:
        return "EC_BULTOS_BCO"
    if fname_upper.startswith("INV_BILLETES_ATM") or fname_upper.startswith("INV_ATM") \
       or ("INV" in fname_upper and "ATM" in fname_upper):
        return "INV_ATM"
    if fname_upper.startswith("INV_BILLETES_BANCO") or fname_upper.startswith("INV_BCO") \
       or fname_upper.startswith("INV_BANCO") or ("INV" in fname_upper and ("BANCO" in fname_upper or "BCO" in fname_upper)):
        return "INV_BCO"
    return None


def _dispatch_and_process(fecha_ejecucion: datetime, path: Path, agencia: str) -> None:
    fname = path.name
    fname_upper = fname.upper()
    tipo = _detectar_tipo(fname_upper)

    logger.info(f"[ANALIZANDO] '{fname}' en carpeta '{agencia}'")

    if not tipo:
        logger.info(f" - > Tipo detectado: DESCONOCIDO, Agencia: {agencia.title()}")
        dest_dir = get_procesado_dir(fecha_ejecucion, agencia)
        move_original(path, dest_dir)
        return

    logger.info(f" - > Tipo detectado: {tipo}, Agencia: {agencia.title()}")

    out_dir = get_procesado_dir(fecha_ejecucion, agencia)
    parser_name: Optional[str] = None
    out_path: Optional[str] = None
    nregs: int = 0

    try:
        if tipo == "EC_ATM":
            parser_name = "get_ec_atm"
            res = get_ec_atm(fecha_ejecucion, fname, str(path.parent), str(out_dir), agencia_carpeta=agencia)
        elif tipo == "EC_EFECT_BCO":
            parser_name = "get_ec_banco"
            res = get_ec_banco(fecha_ejecucion, fname, str(path.parent), str(out_dir), agencia_carpeta=agencia)
        elif tipo == "EC_BULTOS_ATM":
            parser_name = "get_ec_bultos_atm"
            res = get_ec_bultos_atm(fecha_ejecucion, fname, str(path.parent), str(out_dir), agencia_carpeta=agencia)
        elif tipo == "EC_BULTOS_BCO":
            parser_name = "get_ec_bultos_bco"
            res = get_ec_bultos_bco(fecha_ejecucion, fname, str(path.parent), str(out_dir), agencia_carpeta=agencia)
        elif tipo == "INV_ATM":
            parser_name = "get_inv_atm"
            res = get_inv_atm(fecha_ejecucion, fname, str(path.parent), str(out_dir), agencia_carpeta=agencia)
        elif tipo == "INV_BCO":
            parser_name = "get_inv_bco"
            res = get_inv_bco(fecha_ejecucion, fname, str(path.parent), str(out_dir), agencia_carpeta=agencia)
        else:
            res = None
    except Exception as e:
        logger.info(f" - > [ERROR PARSER] {e}")
        res = None

    if isinstance(res, tuple) and len(res) == 2:
        out_path, nregs = res
    elif isinstance(res, str):
        out_path = res
        if out_path and Path(out_path).exists():
            nregs = _count_rows_excel(out_path)

    if parser_name:
        logger.info(f" - > Parser ejecutado: {parser_name}. Registros obtenidos: {nregs}")
    if out_path:
        logger.info(f" - > [RAW ESCRITO] {nregs} registros guardados en '{Path(out_path).name}'")

    if move_original(path, out_dir):
        logger.info(f" - > [MOVIDO OK] '{fname}' a '{out_dir}'")


def main():
    setup_logger_prosegur()
    fecha_ejecucion = datetime.now()
    pendientes = collect_pending_files_prosegur()

    if not pendientes:
        logger.info("No hay archivos en PROSEGUR/ASU|CDE|CNC|ENC|OVD.")
    else:
        for p, ag in pendientes:
            _dispatch_and_process(fecha_ejecucion, p, ag)

    logger.info("[FIN] Consolidado PROSEGUR")


if __name__ == "__main__":
    main()

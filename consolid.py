# -*- coding: utf-8 -*-
"""
CONSOLIDADO PROSEGUR – UNIFICADO (EC_*, BULTOS_* y SALDOS ANTERIORES)
-------------------------------------------------------------------------------
- Estructura de carpetas estilo BRITIMP:
    PROSEGUR/
        ASU, CDE, CNC, ENC, OVD  -> pendientes
        PROCESADO/AAAA-MM-DD/AGENCIA/ -> procesados
        CONSOLIDADO/ (reservado)
        PROSEGUR_log.txt -> log

- Tipos soportados por ahora (por nombre de archivo):
    EC_ATM*              -> get_ec_atm
    EC_BANCO* / EC_BCO*  -> get_ec_banco
    EC_BULTOS_ATM*       -> get_ec_bultos_atm
    EC_BULTOS_BCO*       -> get_ec_bultos_bco / EC_BULTOS_BANCO*
    INV*ATM*             -> get_inv_atm  (stub básico)
    INV*BANCO* / INV*BCO*-> get_inv_bco  (stub básico)

- Saldos anteriores:
    * EC_ATM:
        fila "Saldo Anterior" -> [USD, PYG] → SALDO_ANTERIOR_USD, SALDO_ANTERIOR_PYG
    * EC_BANCO:
        fila "Saldo Anterior" por hoja/moneda → SALDO_ANTERIOR
    * EC_BULTOS_ATM:
        fila "Saldo Anterior" -> [cant_pyg, saldo_pyg, cant_usd, saldo_usd]
        → SALDO_ANTERIOR_PYG, SALDO_ANTERIOR_USD
    * EC_BULTOS_BCO:
        fila "Saldo Anterior" -> [cant, saldo] → SALDO_ANTERIOR
"""

import os
import re
import shutil
import unicodedata
from datetime import datetime
from pathlib import Path
from typing import Optional, Union, List, Dict, Tuple, Any

import pandas as pd
from loguru import logger
import sys


# ==============================
#  ESTRUCTURA DE CARPETAS
# ==============================

AGENCIES = ['ASU', 'CDE', 'CNC', 'ENC', 'OVD']


def resolve_root_prosegur() -> Path:
    here = Path(__file__).resolve().parent if "__file__" in globals() else Path.cwd()
    if (here / 'PROSEGUR').exists():
        return here / 'PROSEGUR'
    return here


ROOT = resolve_root_prosegur()
PENDIENTES = ROOT
PROCESADO_DIR = ROOT / 'PROCESADO'
CONSOLIDADO_DIR = ROOT / 'CONSOLIDADO'

FULL_PATH = str(PENDIENTES)
FULL_PATH_PROCESADO = str(PROCESADO_DIR)
FULL_PATH_CONSOLIDADO = str(CONSOLIDADO_DIR)

for d in [PENDIENTES, PROCESADO_DIR, CONSOLIDADO_DIR]:
    d.mkdir(parents=True, exist_ok=True)
for ag in AGENCIES:
    (PENDIENTES / ag).mkdir(parents=True, exist_ok=True)


# ==============================
#  LOGGING (LOGURU)
# ==============================

def setup_logger_prosegur() -> None:
    logger.remove()
    fmt = "{level} - {message}"
    log_file = ROOT / "PROSEGUR_log.txt"
    logger.add(sys.stdout, format=fmt, level="INFO")
    logger.add(str(log_file), format=fmt, level="INFO", encoding="utf-8")
    logger.info("==== Inicio ejecución PROSEGUR ====")
    logger.info(f"ROOT = {ROOT}")


# ==============================
#  HELPERS GENERALES
# ==============================

def _strip_accents(s: str) -> str:
    return ''.join(c for c in unicodedata.normalize('NFD', s) if unicodedata.category(c) != 'Mn')


def _first_non_empty_after(row_vals: List[str], start_idx: int) -> Optional[int]:
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
    return ''.join(ch for ch in str(s) if ch.isdigit())


def _extraer_saldos_desde_fila(strip_cells: List[str], etiqueta: str = 'SALDO ANTERIOR') -> List[str]:
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


def encontrar_fecha(texto: str) -> Optional[str]:
    if not texto:
        return None
    m = re.search(r'(\d{1,2}/\d{1,2}/\d{4})', texto)
    return m.group(1) if m else None


def get_agencia(linea_cabecera: str) -> str:
    if not linea_cabecera:
        return ''
    m = re.search(r'SUCURSAL:\s*([^)]+)\)', linea_cabecera, flags=re.IGNORECASE)
    return m.group(1).strip() if m else ''


def _normaliza_moneda(texto: str) -> str:
    t = _strip_accents(str(texto)).upper()
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


def _txt(x) -> str:
    return "" if x is None or (isinstance(x, float) and pd.isna(x)) else str(x).strip()


def _upper(x) -> str:
    return re.sub(r"\s+", " ", _txt(x)).upper()


def _is_zero_like(s: str) -> bool:
    t = str(s).replace(',', '').replace('.', '').strip()
    return t == '' or t == '0'


# ==== Agencia desde filename/carpeta ====

AGENCIA_FILE_DIGIT_MAP = {'1': 'ASU', '2': 'CDE', '5': 'CNC', '3': 'ENC', '4': 'OVD'}
AGENCIA_FILE_PATTERN = re.compile(r'(^|[^0-9])([1-5])_10([^0-9]|$)')


def infer_agencia_from_filename(path: Union[str, Path]) -> str:
    name = Path(path).name.upper()
    m = AGENCIA_FILE_PATTERN.search(name)
    if not m:
        return ''
    return AGENCIA_FILE_DIGIT_MAP.get(m.group(2), '')


def resolve_agencia_base(path_entrada: Union[str, Path], agencia_carpeta: Optional[str]) -> str:
    ag = infer_agencia_from_filename(path_entrada)
    if ag:
        return ag
    return (agencia_carpeta or '').strip()


def fill_agencia_column(df: pd.DataFrame, path_entrada: Union[str, Path], agencia_carpeta: Optional[str]) -> pd.DataFrame:
    if df is None or df.empty or 'AGENCIA' not in df.columns:
        return df
    non_empty = [str(v).strip() for v in df['AGENCIA'].unique() if str(v).strip()]
    if non_empty:
        base = non_empty[0]
    else:
        base = resolve_agencia_base(path_entrada, agencia_carpeta)
    if not base:
        return df
    df['AGENCIA'] = [base if not str(v).strip() else v for v in df['AGENCIA']]
    return df


def _leer_hojas_excel(path_entrada: str, sheet_name=None) -> Dict[Union[int, str], pd.DataFrame]:
    if sheet_name is None:
        return pd.read_excel(path_entrada, sheet_name=None, header=None, dtype=str)
    if isinstance(sheet_name, (list, tuple)):
        return pd.read_excel(path_entrada, sheet_name=list(sheet_name), header=None, dtype=str)
    df = pd.read_excel(path_entrada, sheet_name=sheet_name, header=None, dtype=str)
    return {sheet_name: df}


# ==============================
#  FORMATOS DE SALIDA
# ==============================

def _ordenar_y_renombrar_columnas_ec(df: pd.DataFrame) -> pd.DataFrame:
    df.columns = [str(c).replace(' ', '_').upper() for c in df.columns]
    df = df.rename(columns={'FECHA_OPER': 'FECHA', 'MOTIVO MOVIMIENTO': 'MOTIVO_MOVIMIENTO'})
    orden_final = [
        'FECHA', 'SUCURSAL', 'RECIBO', 'BULTOS', 'GUARANIES', 'DOLARES',
        'ING_EGR', 'CLASIFICACION', 'FECHA_ARCHIVO', 'MOTIVO_MOVIMIENTO',
        'AGENCIA', 'SALDO_ANTERIOR_PYG', 'SALDO_ANTERIOR_USD'
    ]
    for col in orden_final:
        if col not in df.columns:
            df[col] = ''
    return df[orden_final]


def _ordenar_y_renombrar_columnas_ec_banco(df: pd.DataFrame) -> pd.DataFrame:
    df.columns = [str(c).replace(' ', '_').upper() for c in df.columns]
    df = df.rename(columns={'FECHA_OPER': 'FECHA', 'MONTO': 'IMPORTE', 'MOTIVO MOVIMIENTO': 'MOTIVO_MOVIMIENTO'})
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


# ==============================
#  EC_ATM
# ==============================

def get_ec_atm(fecha_ejecucion: datetime,
               filename: str,
               dir_entrada: str,
               dir_consolidado: str,
               agencia_carpeta: Optional[str] = None,
               sheet_name: Union[int, str] = 0) -> Optional[str]:

    path_entrada = os.path.join(dir_entrada, filename)
    p = Path(path_entrada)
    stem_out = p.stem + '_PROCESADO'
    path_salida = os.path.join(dir_consolidado, f'{stem_out}.xlsx')

    df_raw = pd.read_excel(path_entrada, sheet_name=sheet_name, header=None, dtype=str).fillna('')

    rx_fecha_cell = re.compile(r'^\s*\d{1,2}/\d{1,2}/\d{4}\s*$')
    rx_totales = re.compile(r'\\b(TOTAL|SUBTOTAL)\\b', re.IGNORECASE)

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
            m_f = re.search(r'AL:\\s*(\\d{1,2}/\\d{1,2}/\\d{4})', line_join, flags=re.IGNORECASE)
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


# ==============================
#  EC_BANCO (EFECTIVO)
# ==============================

def get_ec_banco(fecha_ejecucion: datetime,
                 filename: str,
                 dir_entrada: str,
                 dir_consolidado: str,
                 sheet_name=None) -> Optional[str]:

    path_entrada = os.path.join(dir_entrada, filename)
    p = Path(path_entrada)
    stem_out = p.stem + '_PROCESADO'
    path_salida = os.path.join(dir_consolidado, f'{stem_out}.xlsx')

    hojas = _leer_hojas_excel(path_entrada, sheet_name=sheet_name)

    rx_fecha_linea = re.compile(r'^\\s*(\\d{1,2}/\\d{1,2}/\\d{4})\\b')
    rx_totales = re.compile(r'\\b(TOTAL|SUBTOTAL)\\b', re.IGNORECASE)
    rx_moneda = re.compile(r'\\b(GUARAN[IÍ]ES|D[ÓO]LARES|EUROS?|REALES?|PESOS?)\\b', re.IGNORECASE)

    mapa_clasif = {
        'BANCO': 'BCO',
        'ATM': 'ATM',
        'BULTOS DE BANCO': 'BULTO BCO',
        'BULTOS DE ATM': 'BULTO ATM',
    }

    registros: List[Dict[str, str]] = []

    for nombre_hoja, df in hojas.items():
        df = df.fillna('')

        saldo_anterior_hoja = ''

        agencia = ''
        fecha_archivo = ''
        clasificacion = ''
        ing_egr = ''
        motivo_actual = ''

        moneda_actual = _guess_currency_from_sheet_name(nombre_hoja) or 'GUARANIES'

        for _, row in df.iterrows():
            linea = ' '.join([str(x).strip() for x in row.values if str(x).strip()])
            if not linea:
                continue

            linea_up = _strip_accents(linea).upper()

            if 'SALDO ANTERIOR' in linea_up and not saldo_anterior_hoja:
                strip_cells = [str(x).strip() for x in row.values]
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
                m_tipo = re.search(r'ESTADO DE CUENTA DE\\s+(.*?)\\s+AL:', linea, flags=re.IGNORECASE)
                if m_tipo:
                    texto = m_tipo.group(1).strip()
                    texto_norm = _strip_accents(texto).upper()
                    clasificacion = mapa_clasif.get(texto_norm, texto.strip())
                m_f = re.search(r'AL:\\s*(\\d{1,2}/\\d{1,2}/\\d{4})', linea, flags=re.IGNORECASE)
                if m_f:
                    fecha_archivo = m_f.group(1)
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

            m_moneda = rx_moneda.search(linea)
            if m_moneda:
                moneda_actual = m_moneda.group(1)

            if ing_egr and not rx_fecha_linea.match(linea):
                motivo_actual = linea.strip()
                continue

            m_date = rx_fecha_linea.match(linea)
            if ing_egr and motivo_actual and m_date:
                parts = linea.split()
                if not parts:
                    continue
                fecha_oper = parts[0]
                idx_rec = next((i for i, p in enumerate(parts[1:], 1)
                                if re.fullmatch(r'\\d{6,}', p)), None)
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
        'MOTIVO MOVIMIENTO', 'FECHA_OPER', 'SUCURSAL', 'RECIBO', 'BULTOS',
        'MONEDA', 'MONTO', 'SALDO_ANTERIOR'
    ])

    if df_out.empty:
        df_out = pd.DataFrame(columns=[
            'HOJA_ORIGEN', 'AGENCIA', 'FECHA_ARCHIVO', 'ING_EGR', 'CLASIFICACION',
            'MOTIVO MOVIMIENTO', 'FECHA_OPER', 'SUCURSAL', 'RECIBO', 'BULTOS',
            'MONEDA', 'MONTO', 'SALDO_ANTERIOR'
        ])
        logger.info(f'[EC_BANCO] {filename}: No se detectaron registros válidos.')

    df_out = df_out.rename(columns={'MOTIVO MOVIMIENTO': 'MOTIVO_MOVIMIENTO'})
    df_out = _ordenar_y_renombrar_columnas_ec_banco(df_out)
    df_out.to_excel(path_salida, index=False)
    logger.info(f'[EC_BANCO] Guardado: {path_salida}')
    return path_salida


# ==============================
#  EC_BULTOS_ATM
# ==============================

def get_ec_bultos_atm(fecha_ejecucion: datetime,
                      filename: str,
                      dir_entrada: str,
                      dir_consolidado: str,
                      sheet_name: Union[int, str] = 0,
                      descartar_usd_cero: bool = True) -> Optional[str]:

    path_entrada = os.path.join(dir_entrada, filename)
    p = Path(path_entrada)
    stem_out = p.stem + '_PROCESADO'
    path_salida = os.path.join(dir_consolidado, f'{stem_out}.xlsx')

    df_x = pd.read_excel(path_entrada, sheet_name=sheet_name, header=None, dtype=str).fillna('')
    rx_fecha_cell = re.compile(r'^\\s*(\\d{1,2}/\\d{1,2}/\\d{4})\\s*$')
    rx_totales = re.compile(r'\\b(TOTAL|SUBTOTAL)\\b', re.IGNORECASE)

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
            m = re.search(r'SUCURSAL:\\s*([^)]+)\\)', line_join, flags=re.IGNORECASE)
            if m:
                agencia = m.group(1).strip()
            continue

        if 'ESTADO DE CUENTA DE BULTOS DE ATM' in upper_join:
            m_f = re.search(r'AL:\\s*(\\d{1,2}/\\d{1,2}/\\d{4})', line_join, flags=re.IGNORECASE)
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
        logger.info(f'[EC_BULTOS_ATM] {filename}: No se detectaron registros válidos.')

    df_out = _ordenar_y_renombrar_columnas_bultos(df_out)
    df_out.to_excel(path_salida, index=False)
    logger.info(f'[EC_BULTOS_ATM] Guardado: {path_salida}')
    return path_salida


# ==============================
#  EC_BULTOS_BCO
# ==============================

def get_ec_bultos_bco(fecha_ejecucion: datetime,
                      filename: str,
                      dir_entrada: str,
                      dir_consolidado: str,
                      sheet_name=None) -> Optional[str]:

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

    rx_fecha_linea = re.compile(r'^\\s*(\\d{1,2}/\\d{1,2}/\\d{4})\\b')
    rx_totales = re.compile(r'\\b(TOTAL|SUBTOTAL)\\b', re.IGNORECASE)
    rx_moneda = re.compile(r'(GUARAN[IÍ]ES|D[ÓO]LARES|EUROS?|REALES?|PESOS?|PYG|GS|G\\$|₲|USD|US\\$|U\\$S|R\\$|BRL|EUR|ARS|€|\\$)',
                           re.IGNORECASE)

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
                m = re.search(r'SUCURSAL:\\s*([^)]+)\\)', linea, flags=re.IGNORECASE)
                if m:
                    agencia = m.group(1).strip()
                continue

            if 'ESTADO DE CUENTA DE' in linea_up:
                m_f = re.search(r'AL[:\\s]+(\\d{1,2}/\\d{1,2}/\\d{4})', linea, flags=re.IGNORECASE)
                if m_f:
                    fecha_archivo = m_f.group(1)
                m_mon_enc = rx_moneda.search(linea)
                if m_mon_enc:
                    moneda_actual = _normaliza_moneda_iso(m_mon_enc.group(1))
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

            if rx_moneda.search(linea) and not rx_fecha_linea.match(linea):
                moneda_actual = _normaliza_moneda_iso(rx_moneda.search(linea).group(1))
                continue

            if ing_egr and not rx_fecha_linea.match(linea):
                motivo_actual = linea.strip()
                continue

            m_date = rx_fecha_linea.match(linea)
            if ing_egr and motivo_actual and m_date:
                parts = parts_all
                if not parts:
                    continue
                fecha_oper = parts[0]
                idx_rec = next((i for i, p in enumerate(parts[1:], 1)
                                if re.fullmatch(r'\\d{6,}', _strip_accents(p))), None)
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
        logger.info(f'[EC_BULTOS_BCO] {filename}: No se detectaron registros válidos.')

    df_out = _ordenar_y_renombrar_columnas_bultos_bco(df_out)
    df_out.to_excel(path_salida, index=False)
    logger.info(f'[EC_BULTOS_BCO] Guardado: {path_salida}')
    return path_salida


# ==============================
#  INV_ATM / INV_BCO (STUB)
# ==============================

def get_inv_atm(fecha_ejecucion: datetime,
                filename: str,
                dir_entrada: str,
                dir_consolidado: str,
                agencia_carpeta: Optional[str] = None) -> Optional[str]:
    """
    STUB temporal para INV_ATM.
    Sólo mueve el archivo a PROCESADO sin generar detalle.
    """
    path_entrada = os.path.join(dir_entrada, filename)
    p = Path(path_entrada)
    stem_out = p.stem + '_PROCESADO'
    out_dir = dir_consolidado
    Path(out_dir).mkdir(parents=True, exist_ok=True)
    path_salida = os.path.join(out_dir, f'{stem_out}.xlsx')

    logger.info(f'[INV_ATM] {filename}: parser de inventario no implementado en este unificado.')
    # Crear Excel vacío con columnas genéricas para no romper flujo
    df_out = pd.DataFrame(columns=['FECHA_INVENTARIO', 'DIVISA', 'DENOMINACION', 'CANTIDAD', 'MONEDA', 'IMPORTE_TOTAL', 'AGENCIA'])
    df_out.to_excel(path_salida, index=False)
    return path_salida


def get_inv_bco(fecha_ejecucion: datetime,
                filename: str,
                dir_entrada: str,
                dir_consolidado: str,
                agencia_carpeta: Optional[str] = None) -> Optional[str]:
    """
    STUB temporal para INV_BCO.
    Sólo mueve el archivo a PROCESADO sin generar detalle.
    """
    path_entrada = os.path.join(dir_entrada, filename)
    p = Path(path_entrada)
    stem_out = p.stem + '_PROCESADO'
    out_dir = dir_consolidado
    Path(out_dir).mkdir(parents=True, exist_ok=True)
    path_salida = os.path.join(out_dir, f'{stem_out}.xlsx')

    logger.info(f'[INV_BCO] {filename}: parser de inventario no implementado en este unificado.')
    df_out = pd.DataFrame(columns=['FECHA_INVENTARIO', 'DIVISA', 'DENOMINACION', 'CANTIDAD', 'MONEDA', 'IMPORTE_TOTAL', 'AGENCIA'])
    df_out.to_excel(path_salida, index=False)
    return path_salida


# ==============================
#  DISPATCHER / MAIN
# ==============================

def collect_pending_files_prosegur() -> List[Tuple[Path, str]]:
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
    d = PROCESADO_DIR / fecha.strftime('%Y-%m-%d') / agencia
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
    if fname_upper.startswith('EC_ATM'):
        return 'EC_ATM'
    if fname_upper.startswith('EC_BANCO') or fname_upper.startswith('EC_BCO'):
        return 'EC_EFECT_BCO'
    if fname_upper.startswith('EC_BULTOS_ATM'):
        return 'EC_BULTOS_ATM'
    if fname_upper.startswith('EC_BULTOS_BCO') or 'EC_BULTOS_BANCO' in fname_upper:
        return 'EC_BULTOS_BCO'
    if 'INV' in fname_upper and 'ATM' in fname_upper:
        return 'INV_ATM'
    if 'INV' in fname_upper and ('BANCO' in fname_upper or 'BCO' in fname_upper):
        return 'INV_BCO'
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
        if tipo == 'EC_ATM':
            parser_name = 'get_ec_atm'
            out_path = get_ec_atm(fecha_ejecucion, fname, str(path.parent), str(out_dir), agencia_carpeta=agencia)
        elif tipo == 'EC_EFECT_BCO':
            parser_name = 'get_ec_banco'
            out_path = get_ec_banco(fecha_ejecucion, fname, str(path.parent), str(out_dir))
        elif tipo == 'EC_BULTOS_ATM':
            parser_name = 'get_ec_bultos_atm'
            out_path = get_ec_bultos_atm(fecha_ejecucion, fname, str(path.parent), str(out_dir))
        elif tipo == 'EC_BULTOS_BCO':
            parser_name = 'get_ec_bultos_bco'
            out_path = get_ec_bultos_bco(fecha_ejecucion, fname, str(path.parent), str(out_dir))
        elif tipo == 'INV_ATM':
            parser_name = 'get_inv_atm'
            out_path = get_inv_atm(fecha_ejecucion, fname, str(path.parent), str(out_dir), agencia_carpeta=agencia)
        elif tipo == 'INV_BCO':
            parser_name = 'get_inv_bco'
            out_path = get_inv_bco(fecha_ejecucion, fname, str(path.parent), str(out_dir), agencia_carpeta=agencia)
    except Exception as e:
        logger.info(f" - > [ERROR PARSER] {e}")
        out_path = None

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
        logger.info('No hay archivos en PROSEGUR/ASU|CDE|CNC|ENC|OVD.')
    else:
        for p, ag in pendientes:
            _dispatch_and_process(fecha_ejecucion, p, ag)
    logger.info('[FIN] Consolidado PROSEGUR')


if __name__ == '__main__':
    main()

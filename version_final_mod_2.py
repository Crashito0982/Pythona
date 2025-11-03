# -*- coding: utf-8 -*-
"""
BRITIMP - Consolidado
"""
from __future__ import annotations
import os
import re
import sys
import shutil
import unicodedata
#import logging
from loguru import logger
from pathlib import Path
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
from sqlConn import sqlConn  # Conector a SQL Server para volcado de datos

try:
    from pypdf import PdfReader
except ImportError:
    PdfReader = None
    logger.warning("ADVERTENCIA: La librería 'pypdf' no está instalada. El procesamiento de archivos PDF no funcionará. Instálala con: pip install pypdf")

try:
    import xlrd
except ImportError:
    xlrd = None
    logger.warning("ADVERTENCIA: La librería 'xlrd' no está instalada. El procesamiento de archivos .xls antiguos podría fallar. Instálala con: pip install xlrd==1.2.0")

# ------------------------------ CONFIGURACIÓN DE RUTAS Y COLUMNAS FINALES ------------------------------

AGENCIA_ALIASES = {
    'ASU': ['ASUNCION', 'ASU'],
    'CDE': ['CIUDAD DEL ESTE', 'CDE'],
    'ENC': ['ENCARNACION', 'ENC'],
    'OVD': ['CNEL. OVIEDO', 'CORONEL OVIEDO', 'OVIEDO', 'OVD'],
}


FINAL_COLUMNS = {
    "EC_EFECT_BCO": ["FECHA_RECIBO", "SUCURSAL", "RECIBO", "BULTOS", "IMPORTE", "MONEDA", "ING_EGR", "CLASIFICACION", "FECHA_ARCHIVO", "MOTIVO_MOVIMIENTO", "AGENCIA"],
    "EC_EFECT_ATM": ["FECHA_RECIBO", "SUCURSAL", "RECIBO", "BULTOS", "MONTO", "MONEDA", "ING_EGR", "CLASIFICACION", "FECHA_ARCHIVO", "MOTIVO_MOVIMIENTO", "AGENCIA"],
    "INV_BILLETES_BCO": ["FECHA_INVENTARIO", "DIVISA", "AGENCIA", "AGRUPACION_EFECTIVO", "TIPO_VALOR", "DENOMINACION", "CALIDAD_DEPOSITO", "CALIDAD_CD", "CALIDAD_CANJE", "MONEDA", "IMPORTE_TOTAL"],
    "INV_BILLETES_ATM": ["FECHA_INVENTARIO", "DIVISA", "AGENCIA", "AGRUPACION_EFECTIVO", "TIPO_VALOR", "DENOMINACION", "CALIDAD_DEPOSITO", "CALIDAD_CD", "CALIDAD_CANJE", "MONEDA", "IMPORTE_TOTAL"],
    "EC_BULTO_ATM": ["FECHA_RECIBO", "SUCURSAL", "RECIBO", "BULTOS", "MONTO", "MONEDA", "ING_EGR", "CLASIFICACION", "FECHA_ARCHIVO", "MOTIVO_MOVIMIENTO", "AGENCIA"],
}


def resolve_root() -> Path:
    """Determina el directorio raíz del proyecto."""
    here = Path(__file__).resolve().parent if "__file__" in globals() else Path.cwd()
    if (here / "PENDIENTES").exists() or (here.name.upper() == "BRITIMP"):
        return here
    if (here / "BRITIMP").exists():
        return here / "BRITIMP"
    return here

ROOT = Path(r"//nfs_airflow_py/cmdat/ea-saa-datos/Transportadoras/Britimp")
PENDIENTES = ROOT  # Aquí existen las carpetas ASU, CDE, ENC y OVD donde CONTROL-M deposita los archivos
PROCESADO = ROOT / "PROCESADO"
CONSOLIDADO = ROOT / "CONSOLIDADO"

# Lista de agencias actualizada
AGENCIES = ["ASU", "CDE", "ENC", "OVD"]

def ensure_dirs() -> None:
    """Asegura que existan las carpetas base de salida en el share de Britimp.

    Las carpetas de agencias (ASU, CDE, ENC, OVD) ya son manejadas por CONTROL-M
    directamente dentro de ROOT, por lo que aquí solo se crean PROCESADO y
    CONSOLIDADO si no existen.
    """
    # Carpetas base de salida (no tocamos las carpetas de agencias: las gestiona CONTROL-M)
    for d in [PROCESADO, CONSOLIDADO]:
        d.mkdir(parents=True, exist_ok=True)

ensure_dirs()

# ------------------------------ LOGGING ------------------------------
# ------------------------------ LOGGING ------------------------------

_LOGGER: Optional[logging.Logger] = None

def today_folder() -> Path:
    """Crea y devuelve la ruta a la carpeta de consolidados del día de hoy."""
    today = datetime.now().strftime("%Y-%m-%d")
    outdir = os.path.join(CONSOLIDADO / today)
    os.makedirs(outdir, exists_ok = True)
#   os.mkdir(parents=True, exists_ok = True)    
    return outdir

#def setup_logger() -> logging.Logger:
#    """Configura el logger para que escriba en archivo y en consola."""
#    global _LOGGER
#    if _LOGGER:
#        return _LOGGER

#    log_dir = today_folder()
#    log_path = log_dir / "BRITIMP_log.txt"
#    logger = logging.getLogger("BRITIMP")
#    logger.setLevel(logging.INFO)
#    logger.handlers = []

#    fmt = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')

#    # Handler para archivo
#    fh = logging.FileHandler(log_path, encoding='utf-8', mode='a')
#    fh.setFormatter(fmt)
#    logger.addHandler(fh)

#    # Handler para consola
#    ch = logging.StreamHandler(sys.stdout)
#    ch.setFormatter(fmt)
#    logger.addHandler(ch)

#    logger.info("=" * 10 + " INICIO DE EJECUCIÓN " + "=" * 10)
#    logger.info(f"Directorio Raíz: {ROOT}")
#    _LOGGER = logger
#    return logger

#def log_info(msg: str): setup_logger().info(msg)
#def log_warn(msg: str): setup_logger().warning(msg)
#def log_error(msg: str): setup_logger().error(msg)

def log_info(msg: str) -> None:
    """Wrapper de compatibilidad: usa loguru.logger.info."""
    logger.info(msg)

def log_warn(msg: str) -> None:
    """Wrapper de compatibilidad: usa loguru.logger.warning."""
    logger.warning(msg)

def log_error(msg: str) -> None:
    """Wrapper de compatibilidad: usa loguru.logger.error."""
    logger.error(msg)




# ------------------------------ HELPERS (Funciones de Utilidad) ------------------------------

DATE_RE = re.compile(r"^\s*(\d{1,2}/\d{1,2}/\d{4})\s*$", re.IGNORECASE)

def has_string_date(row_vals: List[Any]) -> bool:
    """Verifica si alguna celda en la fila es una fecha en formato string dd/mm/yyyy."""
    for c in row_vals:
        if isinstance(c, str) and DATE_RE.match(c.strip()):
            return True
        if isinstance(c, datetime):
            return True
    return False

def excel_serial_to_ddmmyyyy(val: float) -> Optional[str]:
    """Convierte fecha serial de Excel a 'dd/mm/yyyy'."""
    try:
        if isinstance(val, (int, float)) and not pd.isna(val):
            base = datetime(1899, 12, 30)
            return (base + timedelta(days=float(val))).date().strftime("%d/%m/%Y")
    except Exception:
        return None

def to_ddmmyyyy(val: Any) -> Optional[str]:
    """Convierte varios formatos de fecha a 'dd/mm/yyyy'."""
    if isinstance(val, datetime):
        return val.strftime("%d/%m/%Y")
    if isinstance(val, (int, float)) and not pd.isna(val):
        d = excel_serial_to_ddmmyyyy(val)
        if d: return d
    if isinstance(val, str):
        s = val.strip()
        if DATE_RE.match(s):
            return s
        for fmt in ("%Y-%m-%d", "%d-%m-%Y", "%d/%m/%y"):
            try:
                return datetime.strptime(s, fmt).strftime("%d/%m/%Y")
            except ValueError:
                pass
    return None

NUM_SANITIZER = re.compile(r"[^\d,.\-()\u00A0 ]")

def parse_numeric(val: Any) -> Optional[float]:
    """Parsea un valor a número, manejando formatos españoles y negativos con ()."""
    if pd.isna(val): return None
    if isinstance(val, (int, float)): return float(val)
    if isinstance(val, str):
        s = NUM_SANITIZER.sub("", val).replace("\u00A0", " ").strip()
        if not s: return None
        neg = s.startswith("(") and s.endswith(")")
        if neg: s = s[1:-1].strip()
        s = s.replace(" ", "")
        if "," in s and "." in s:
            s = s.replace(".", "") if s.rfind(".") < s.rfind(",") else s.replace(",", "")
            s = s.replace(",", ".")
        elif "," in s:
            parts = s.split(",")
            s = s.replace(",", ".") if len(parts) >= 2 and len(parts[-1]) <= 2 else s.replace(",", "")
        try:
            x = float(s)
            return -x if neg else x
        except (ValueError, TypeError):
            return None
    return None

def clean_digits(val: Any) -> str:
    """Extrae solo los dígitos de un valor."""
    return "" if pd.isna(val) else re.sub(r"\D", "", str(val))

def unir_letras_separadas(texto: str) -> str:
    """Une letras separadas por espacios, ej: 'B I L L E T E S' -> 'BILLETES'."""
    if not texto: return texto
    return re.sub(r'(?:\b[A-ZÁÉÍÓÚÑ](?:\s+[A-ZÁÉÍÓÚÑ])+\b)', lambda m: m.group(0).replace(" ", ""), texto)

def remove_accents(s: str) -> str:
    """Quita acentos y diacríticos de un texto."""
    return "".join(ch for ch in unicodedata.normalize("NFD", s) if not unicodedata.combining(ch))

def name_matches(fname: str, required_groups: List[List[str]]) -> bool:
    """Verifica si un nombre de archivo contiene tokens requeridos."""
    target = remove_accents(fname).upper()
    return all(any(remove_accents(opt).upper() in target for opt in group) for group in required_groups)

# --------------- NORMALIZACIÓN DE AGENCIA / DIVISA ----------------

AGENCIA_PATTERNS: Dict[str, List[str]] = {
    "ASU": [r"\bCASA\s+MATRIZ\b", r"\bASUNCION\b", r"\bASUNCIÓN\b", r"\bASU\b"],
    "CDE": [r"\bCIUDAD\s+DEL\s+ESTE\b", r"\bCDE\b"],
    "ENC": [r"\bENCARNACION\b", r"\bENCARNACIÓN\b", r"\bENC\b"],
    "OVD": [r"\bCNEL\.?\s+OVIEDO\b", r"\bCORONEL\s+OVIEDO\b", r"\bOVIEDO\b", r"\bOVD\b"],
}

def normalize_agencia_to_cod(value: Any) -> str:
    """Normaliza el nombre de una agencia a su código de 3 letras."""
    if not value: return ""
    u = remove_accents(str(value).strip().upper())
    for cod, patterns in AGENCIA_PATTERNS.items():
        if any(re.search(pat, u) for pat in patterns):
            return cod
    return ""

def normalize_divisa_to_iso(value: Any) -> str:
    """Normaliza variantes de moneda a 'PYG' o 'USD'."""
    if not value: return ""
    u = remove_accents(str(value).strip().upper()).replace("₲", "GS").replace("US$", "USD")
    canon = re.sub(r"[^A-Z0-9]", "", u)
    if canon.startswith("GUAR") or canon in {"PYG", "GS", "GUARANI", "GUARANIES"}: return "PYG"
    if canon.startswith("DOL") or "USD" in canon or canon in {"US", "USS"}: return "USD"
    return ""

# ------------------------------ LECTURA Y DETECCIÓN ------------------------------

NEGATIVE_BANKS = ["CONTINENTAL", "BBVA", "GNB", "REGIONAL", "BASA", "VISION", "ATLAS", "SUDAMERIS", "FAMILIAR", "ITAPUA", "AMAMBAY"]

def read_excel_any_version(path: Path) -> Optional[pd.ExcelFile]:
    """Lee un archivo Excel, intentando con varios motores si es necesario."""
    try:
        return pd.ExcelFile(path)
    except Exception:
        try:
            engine = "openpyxl" if path.suffix.lower() == ".xlsx" else "xlrd" if xlrd else None
            if engine:
                return pd.ExcelFile(path, engine=engine)
        except Exception as e:
            log_warn(f"No se pudo leer el archivo Excel '{path.name}' con ningún motor. Error: {e}")
    return None

def file_text_preview(path: Path, max_rows_excel: int = 40) -> str:
    """Extrae un texto de previsualización de un archivo (PDF o Excel)."""
    ext = path.suffix.lower()
    if ext in (".xlsx", ".xls"):
        xl = read_excel_any_version(path)
        if not xl: return ""
        parts = []
        for sh_name in xl.sheet_names:
            df = pd.read_excel(xl, sheet_name=sh_name, header=None, nrows=max_rows_excel)
            if not df.empty:
                parts.extend(df.fillna("").astype(str).agg(" ".join, axis=1).tolist())
        return "\n".join(parts)
    if ext == ".pdf" and PdfReader:
        try:
            return "".join(page.extract_text() or "" for page in PdfReader(path).pages)
        except Exception:
            return ""
    return ""

def detect_cliente_itau(path: Path, tipo_documento: Optional[str]) -> bool:
    """
    Verifica si el documento es de Itaú. Primero busca explícitamente otros bancos
    con el formato "CLIENTE: [BANCO]" para evitar falsos positivos.
    """
    txt = unir_letras_separadas(file_text_preview(path)).upper()
    
    # Búsqueda precisa de otros bancos para descarte
    for bank in NEGATIVE_BANKS:
        pattern = r"CLIENTE:\s*" + re.escape(bank)
        if re.search(pattern, txt):
            log_info(f"[SKIP] {path.name} -> Otro banco detectado explícitamente: '{bank}'")
            return False

    # Si no se descarta, se procede con la lógica de aceptación para Itaú
    fname_upper = remove_accents(path.name).upper()
    if "ITAU" in txt or "ITAU" in fname_upper:
        return True

    # Para Estados de Cuenta (EC), si no se menciona "ITAU" explícitamente, se descarta por seguridad.
    if tipo_documento and "EC_" in tipo_documento:
        if name_matches(path.name, [["EC"], ["ATM", "BCO", "BANCO", "BULTO"], ["EFECTIVO", "BILLETE"]]):
            log_info(f"[SKIP] {path.name} -> Es Estado de Cuenta pero no menciona 'ITAU'.")
            return False

    # Para Inventarios (INV), somos más permisivos. Si el nombre coincide con el patrón, se acepta.
    if tipo_documento and "INV_" in tipo_documento:
        if name_matches(path.name, [["INV"], ["BILLETE"], ["ATM", "BCO", "BANCO", "DOLAR"]]):
            return True

    log_info(f"[SKIP] {path.name} -> No se pudo confirmar que sea de ITAU.")
    return False

def detect_sin_movimientos(path: Path) -> bool:
    """Detecta si el archivo contiene la frase 'SIN MOVIMIENTOS' con espacios flexibles."""
    txt = unir_letras_separadas(file_text_preview(path)).upper()
    return bool(re.search(r"SIN\s+MOVIMIENTOS?", txt))

def parse_agencia_from_text(text: str) -> str:
    """Extrae la agencia desde el texto (buscando 'SUC: ...')."""
    m = re.search(r"SUC:\s*(.+?)\s*(?:[\)\]]|$)", text, flags=re.IGNORECASE)
    if m:
        raw_agencia = m.group(1).strip()
        cod = normalize_agencia_to_cod(raw_agencia)
        return cod or raw_agencia
    return ""

# ------------------------------ DISPATCHER (Selector de Tipo) ------------------------------

def dispatch_tipo(fname: str, text_content: str = "") -> Optional[str]:
    """Determina el tipo de documento basado en el nombre del archivo y su contenido."""
    up = remove_accents(fname).upper()

    # Reglas prioritarias para PDF de inventario por nombre
    if up.endswith('.PDF'):
        if name_matches(up, [["INV"], ["ATM"]]): return "INV_BILLETES_ATM"
        if name_matches(up, [["INV"], ["BANCO"]]): return "INV_BILLETES_BCO"
        if name_matches(up, [["INV"], ["DOLAR"]]): return "INV_BILLETES_BCO"

    # Reglas generales
    if name_matches(up, [["EC"], ["BULTO"], ["ATM"]]): return "EC_BULTO_ATM"
    if name_matches(up, [["EC"], ["EFECT"], ["ATM"]]): return "EC_EFECT_ATM"
    if name_matches(up, [["EC"], ["EFECT"], ["BCO", "BANCO"]]): return "EC_EFECT_BCO"
    if name_matches(up, [["INV"], ["BILLETE"], ["ATM"]]): return "INV_BILLETES_ATM"
    if name_matches(up, [["INV"], ["BILLETE"], ["BCO", "BANCO", "DOLAR"]]): return "INV_BILLETES_BCO"

    # Fallback para PDFs con nombre genérico, usando contenido
    if "INV" in up:
        up_text = text_content.upper()
        if "INVENTARIO DE BILLETES DE ATM" in up_text: return "INV_BILLETES_ATM"
        if "INVENTARIO DE BILLETES DE BANCO" in up_text: return "INV_BILLETES_BCO"
        return "INV_BILLETES_UNKNOWN"

    return None


# ------------------------------ PARSERS (Extractores de Datos) ------------------------------

def parse_ec_bultos_atm_xlsx(path: Path, DEBUG: bool = False) -> pd.DataFrame:
    """Parser para 'Estado de Cuenta Bultos ATM' (.xls y .xlsx)."""
    xl = read_excel_any_version(path)
    if not xl: return pd.DataFrame()
    registros: List[dict] = []
    
    for sheet_name in xl.sheet_names:
        df_raw = pd.read_excel(xl, sheet_name=sheet_name, header=None)
        full_text = "\n".join(df_raw.fillna("").astype(str).agg(" ".join, axis=1))
        agencia = parse_agencia_from_text(full_text)
        m_fecha = re.search(r"ESTADO\s+DE\s+CUENTA.*DEL\s*:\s*(\d{2}/\d{2}/\d{4})", full_text, re.IGNORECASE)
        fecha_archivo = m_fecha.group(1) if m_fecha else None

        section, motivo, started = None, None, False
        for _, row_series in df_raw.iterrows():
            row = row_series.tolist()
            txt = " ".join(map(str, filter(pd.notna, row))).upper()

            if "SALDO ANTERIOR" in txt: section, motivo, started = None, None, False; continue
            if "INGRESOS" in txt and "EGRESOS" not in txt: section, motivo, started = "INGRESOS", None, True; continue
            if "EGRESOS" in txt: section, motivo, started = "EGRESOS", None, True; continue
            if "INFORME DE PROCESOS" in txt: break
            if not started: continue
            if "TOTAL" in txt and not has_string_date(row): motivo = None; continue

            nonempty = [x for x in row if pd.notna(x) and str(x).strip()]
            if len(nonempty) == 1 and not to_ddmmyyyy(nonempty[0]): motivo = str(nonempty[0]).strip(); continue

            fecha, sucursal, recibo, idx = None, "", "", 0
            while idx < len(row):
                if (f := to_ddmmyyyy(row[idx])): fecha = f; idx += 1; break
                idx += 1
            if not fecha: continue
            
            while idx < len(row):
                if (sval := str(row[idx]).strip()): sucursal = sval; idx+=1; break
                idx+=1
            if not sucursal: continue
            
            while idx < len(row):
                if len(digits := clean_digits(row[idx])) >= 5: recibo = digits; idx+=1; break
                idx+=1
            if not recibo: continue
            
            nums = [parse_numeric(c) for c in row[idx:] if pd.notna(c)]
            nums = [n for n in nums if n is not None]
            while len(nums) < 4: nums.append(0.0)
            bgs, mgs, busd, musd = nums[0], nums[1], nums[2], nums[3]

            def add_row(moneda, bultos, monto):
                registros.append({
                    "FECHA_OPERACION": fecha, "SUCURSAL": sucursal, "RECIBO": recibo, "BULTOS": int(bultos) if bultos else None,
                    "MONTO": float(monto or 0.0), "MONEDA": moneda, "ING_EGR": "IN" if section == "INGRESOS" else "OUT",
                    "CLASIFICACION": "ATM", "FECHA_ARCHIVO": fecha_archivo, "MOTIVO_MOVIMIENTO": (motivo or section),
                    "AGENCIA": agencia, "ARCHIVO_ORIGEN": path.name,
                })
            
            if (bgs or mgs): add_row("PYG", bgs, mgs)
            if (busd or musd): add_row("USD", busd, musd)

    return pd.DataFrame(registros)

def parse_ec_efect_bco_xlsx(path: Path, DEBUG: bool = False) -> pd.DataFrame:
    """Parser específico para 'Estado de Cuenta Efectivo Banco' (.xls y .xlsx)."""
    xl = read_excel_any_version(path)
    if not xl: return pd.DataFrame()

    all_dfs = []

    for sheet_name in xl.sheet_names:
        df_raw = pd.read_excel(xl, sheet_name=sheet_name, header=None)

        full_text = " ".join(df_raw.astype(str).agg(" ".join, axis=1))
        moneda, agencia = ("USD" if "MONEDA: DOLAR" in full_text.upper() else "PYG", parse_agencia_from_text(full_text))
        m_fecha = re.search(r"BANCO\s+DEL:\s*(\d{2}/\d{2}/\d{4})", full_text, flags=re.IGNORECASE)
        fecha_archivo = m_fecha.group(1) if m_fecha else None
        extra_cliente = " (DOCUMENTA/ITAU)" if "CLIENTE: DOCUMENTA /ITAU" in full_text.upper() else ""

        registros = []
        current_section, current_motivo = None, None

        for _, row_series in df_raw.iterrows():
            row = row_series.tolist()
            txt = " ".join(map(str, filter(lambda x: pd.notna(x) and str(x).strip(), row))).upper()

            if "INFORME DE PROCESOS" in txt: break
            if "INGRESOS" in txt and "EGRESOS" not in txt: current_section, current_motivo = "INGRESOS", None; continue
            if "EGRESOS" in txt: current_section, current_motivo = "EGRESOS", None; continue
            if not current_section: continue
            if txt.startswith("TOTAL") and not has_string_date(row): current_motivo = None; continue

            nonempty = [x for x in row if pd.notna(x) and str(x).strip()]
            if len(nonempty) == 1 and not to_ddmmyyyy(nonempty[0]):
                current_motivo = str(nonempty[0]).strip()
                continue

            fecha, sucursal, recibo, bultos, idx = None, "", "", None, 0
            while idx < len(row):
                if (f := to_ddmmyyyy(row[idx])): fecha = f; idx += 1; break
                idx += 1
            if not fecha: continue

            while idx < len(row):
                if (sval := str(row[idx]).strip()): sucursal = sval; idx += 1; break
                idx += 1
            if not sucursal: continue

            while idx < len(row):
                if len(digits := clean_digits(row[idx])) >= 5: recibo = digits; idx += 1; break
                idx += 1
            
            if idx < len(row):
                b = parse_numeric(row[idx])
                if b is not None and b == int(b) and 0 <= b < 1000:
                    bultos = int(b)
                    idx += 1 

            importe_candidates = [parse_numeric(c) for c in row[idx:] if parse_numeric(c) is not None]
            if not importe_candidates: continue

            registros.append({
                "FECHA_OPERACION": fecha, "SUCURSAL": sucursal, "RECIBO": recibo, "BULTOS": bultos,
                "IMPORTE": max(importe_candidates), "MONEDA": moneda,
                "ING_EGR": "IN" if current_section == "INGRESOS" else "OUT", "CLASIFICACION": "BCO",
                "FECHA_ARCHIVO": fecha_archivo, "MOTIVO_MOVIMIENTO": (current_motivo or current_section) + extra_cliente,
                "AGENCIA": agencia, "ARCHIVO_ORIGEN": path.name,
            })
        if registros:
            all_dfs.append(pd.DataFrame(registros))

    if not all_dfs:
        return pd.DataFrame()
    return pd.concat(all_dfs, ignore_index=True)

def parse_inv_billetes_pdf_bco(path: Path, DEBUG: bool = False) -> pd.DataFrame:
    """Parser para PDF de Inventario de Banco, basado en INV_BILL_BCO_BRI_PDF.py."""
    if not PdfReader: return pd.DataFrame()
    try:
        texto = "".join(p.extract_text() or "" for p in PdfReader(path).pages)
        texto = unir_letras_separadas(texto)
    except Exception as e:
        log_warn(f"No se pudo leer el PDF '{path.name}'. Error: {e}")
        return pd.DataFrame()

    m_fecha = re.search(r'SALDO DE INVENTARIO DE BILLETES AL:\s*(\d{2}-\d{2}-\d{4})', texto, re.IGNORECASE)
    fecha_inventario = m_fecha.group(1).replace('-', '/') if m_fecha else None

    divisa = "USD" if "DOLAR" in texto.upper() or "DOLAR" in path.name.upper() else "PYG"
    agencia_raw = (re.search(r'SUC:\s*(.+)', texto, re.IGNORECASE) or [None, ""])[1].strip()
    agencia = normalize_agencia_to_cod(agencia_raw) or agencia_raw

    cliente_documenta = "DOCUMENTA /ITAU" in texto.upper()
    datos, agrupacion, tipo_valor = [], None, None
    RE_NUM = re.compile(r'\b\d{1,3}(?:\.\d{3})*\b')

    for linea in texto.split('\n'):
        linea = linea.strip()
        if not linea or "SUB-TOTAL" in linea.upper() or "TOTAL" in linea.upper(): continue

        m_agrup = re.search(r'^(TESORO|PICOS|FAJOS)\s+EFECTIVO', linea, re.IGNORECASE)
        if m_agrup:
            agrupacion = f"{m_agrup.group(0)} (DOCUMENTA/ITAU)" if cliente_documenta else m_agrup.group(0)
            continue

        if re.search(r'^(BILLETES|MONEDAS)', linea, re.IGNORECASE):
            tipo_valor = linea
            continue

        numeros = [n.replace('.', '') for n in RE_NUM.findall(linea)]
        if len(numeros) == 6 and agrupacion and tipo_valor:
            datos.append([fecha_inventario, divisa, agencia, agrupacion, tipo_valor, *numeros])

    cols = ["FECHA_INVENTARIO", "DIVISA", "AGENCIA", "AGRUPACION_EFECTIVO", "TIPO_VALOR",
            "DENOMINACION", "CALIDAD_DEPOSITO", "CALIDAD_CD", "CALIDAD_CANJE", "MONEDA", "IMPORTE_TOTAL"]
    df = pd.DataFrame(datos, columns=cols)
    if not df.empty:
        df['ARCHIVO_ORIGEN'] = path.name
    return df

def parse_inv_billetes_pdf_atm(path: Path, DEBUG: bool = False) -> pd.DataFrame:
    """Parser para PDF de Inventario de ATM, basado en INV_BILL_ATM_BRI_PDF.py."""
    if not PdfReader: return pd.DataFrame()
    try:
        texto = "".join(p.extract_text() or "" for p in PdfReader(path).pages)
        texto = unir_letras_separadas(texto)
    except Exception as e:
        log_warn(f"No se pudo leer el PDF '{path.name}'. Error: {e}")
        return pd.DataFrame()

    m_fecha = re.search(r'SALDO DE INVENTARIO DE BILLETES AL:\s*(\d{2}-\d{2}-\d{4})', texto, re.IGNORECASE)
    fecha_inventario = m_fecha.group(1).replace('-', '/') if m_fecha else None
    
    agencia_raw = (re.search(r'SUC:\s*([A-ZÁÉÍÓÚÑ ]+)', texto, re.IGNORECASE) or [None, ""])[1].strip()
    agencia = normalize_agencia_to_cod(agencia_raw) or agencia_raw

    datos, agrupacion, tipo_valor, divisa = [], None, None, "PYG"
    RE_NUM = re.compile(r'\d{1,3}(?:\.\d{3})*')
    RE_ATM_GRUPO = re.compile(r'^\s*(TESORO|TESOSO|PICOS|FAJOS)\b.*?\s+ATM\b', re.IGNORECASE)

    for linea in texto.split('\n'):
        linea = linea.strip()
        if not linea or "SUB-TOTAL" in linea.upper() or "TOTAL" in linea.upper(): continue

        if RE_ATM_GRUPO.match(linea):
            agrupacion = linea
            divisa = "USD" if "USD" in linea.upper() or "MDA" in linea.upper() else "PYG"
            continue
        
        if re.search(r'^\s*(BILLETES|MONEDAS)\b', linea, re.IGNORECASE):
            tipo_valor = linea
            continue

        numeros = [n.replace('.', '') for n in RE_NUM.findall(linea)]
        if len(numeros) == 6 and agrupacion and tipo_valor:
            datos.append([fecha_inventario, divisa, agencia, agrupacion, tipo_valor, *numeros])

    cols = ["FECHA_INVENTARIO", "DIVISA", "AGENCIA", "AGRUPACION_EFECTIVO", "TIPO_VALOR",
            "DENOMINACION", "CALIDAD_DEPOSITO", "CALIDAD_CD", "CALIDAD_CANJE", "MONEDA", "IMPORTE_TOTAL"]
    df = pd.DataFrame(datos, columns=cols)
    if not df.empty:
        df['ARCHIVO_ORIGEN'] = path.name
    return df

def parse_inv_billetes_xlsx(path: Path, DEBUG: bool = False) -> pd.DataFrame:
    """Parser para Inventarios de Billetes en Excel (BCO y ATM)."""
    xl = read_excel_any_version(path)
    if not xl: return pd.DataFrame()
    registros = []

    for sheet_name in xl.sheet_names:
        df_raw = pd.read_excel(xl, sheet_name=sheet_name, header=None)
        head_text = "\n".join(df_raw.head(15).fillna("").astype(str).agg(" ".join, axis=1))
        agencia_txt = parse_agencia_from_text(head_text)
        agencia_cod = normalize_agencia_to_cod(agencia_txt)
        agencia_out = agencia_cod or agencia_txt
        m_fecha = re.search(r"INVENTARIO\s+DE\s+BILLETES\s+DE\s+(?:ATM|BANCO)\s+AL:\s*(\d{1,2}/\d{1,2}/\d{4})", head_text, re.IGNORECASE)
        fecha_inv = m_fecha.group(1) if m_fecha else None
        for _, row_series in df_raw.iterrows():
            row = row_series.tolist()
            iso_div = normalize_divisa_to_iso(row[0])
            if iso_div not in {"PYG", "USD"}: continue
            agrup = str(row[1] or '').strip().rstrip('.')
            tipo = unir_letras_separadas(str(row[2] or '').strip())
            denom = parse_numeric(row[3])
            if denom is None: continue
            imp = parse_numeric(row[8] if len(row) > 8 else None)
            if imp is None: continue
            registros.append({
                "FECHA_INVENTARIO": fecha_inv, "DIVISA": iso_div, "AGENCIA": agencia_out,
                "AGRUPACION_EFECTIVO": agrup, "TIPO_VALOR": tipo, "DENOMINACION": int(denom),
                "CALIDAD_DEPOSITO": int(parse_numeric(row[4] or 0)),
                "CALIDAD_CD": int(parse_numeric(row[5] or 0)), # Corregido de CJE_DEP
                "CALIDAD_CANJE": int(parse_numeric(row[6] or 0)),
                "MONEDA": int(parse_numeric(row[7] or 0)),
                "IMPORTE_TOTAL": float(imp),
                "ARCHIVO_ORIGEN": path.name,
            })
    return pd.DataFrame(registros)

def parse_ec_efect_atm_xlsx(path: Path, DEBUG: bool = False) -> pd.DataFrame:
    """Parser específico y robusto para 'Estado de Cuenta Efectivo ATM' (.xls y .xlsx)."""
    xl = read_excel_any_version(path)
    if not xl: return pd.DataFrame()
    registros: List[dict] = []
    for sheet_name in xl.sheet_names:
        df_raw = pd.read_excel(xl, sheet_name=sheet_name, header=None)
        full_text = "\n".join(df_raw.fillna("").astype(str).agg(" ".join, axis=1))
        agencia = parse_agencia_from_text(full_text)
        m_fecha = re.search(r"ESTADO\s+DE\s+CUENTA\s+(?:DE\s+EFECTIVO\s+)?DE\s+ATM\s+DEL:\s*(\d{1,2}/\d{1,2}/\d{4})", full_text, re.IGNORECASE)
        fecha_archivo = m_fecha.group(1) if m_fecha else None
        section, motivo, started = None, None, False
        for _, row_series in df_raw.iterrows():
            row = row_series.tolist()
            txt_upper = " ".join(map(str, filter(pd.notna, row))).upper().strip()
            if "INFORME DE PROCESOS" in txt_upper: break
            if txt_upper == "INGRESOS": section, motivo, started = "INGRESOS", None, True; continue
            if txt_upper == "EGRESOS": section, motivo, started = "EGRESOS", None, True; continue
            if not started: continue
            if txt_upper.startswith("TOTAL") and not has_string_date(row): motivo, section = None, None; continue

            nonempty = [x for x in row if pd.notna(x) and str(x).strip()]
            if len(nonempty) == 1 and not to_ddmmyyyy(nonempty[0]): motivo = str(nonempty[0]).strip(); continue

            # Lógica de parseo secuencial
            idx, fecha, sucursal, recibo, bultos = 0, None, "", "", None
            # 1. Fecha
            while idx < len(row):
                if (f := to_ddmmyyyy(row[idx])): fecha = f; idx += 1; break
                idx += 1
            if not fecha: continue
            # 2. Sucursal
            while idx < len(row):
                if (sval := str(row[idx]).strip()): sucursal = sval; idx+=1; break
                idx+=1
            if not sucursal: continue

            # 3. Recibo
            while idx < len(row):
                if len(digits := clean_digits(row[idx])) >= 5: recibo = digits; idx+=1; break
                idx+=1
            if not recibo: continue

            # 4. Bultos (opcional)
            if idx < len(row):
                b = parse_numeric(row[idx])
                if b is not None and 0 <= b < 1000 and float(b).is_integer():
                    bultos, idx = int(b), idx + 1
            # 5. Montos (los últimos números de la fila)
            nums_tail = [parse_numeric(c) for c in row[idx:] if pd.notna(c)]
            nums_tail = [n for n in nums_tail if n is not None]
            monto_gs, monto_usd = 0.0, 0.0
            if len(nums_tail) >= 2:
                monto_gs, monto_usd = nums_tail[-2], nums_tail[-1]
            elif len(nums_tail) == 1:
                monto_gs = nums_tail[0]
            def add_row(moneda, monto):
                registros.append({
                    "FECHA_OPERACION": fecha, "SUCURSAL": sucursal, "RECIBO": recibo, "BULTOS": bultos,
                    "MONTO": monto, "MONEDA": moneda, "ING_EGR": "IN" if section == "INGRESOS" else "OUT",
                    "CLASIFICACION": "ATM", "FECHA_ARCHIVO": fecha_archivo, "MOTIVO_MOVIMIENTO": (motivo or section),
                    "AGENCIA": agencia, "ARCHIVO_ORIGEN": path.name,
                })
            if monto_gs: add_row("PYG", monto_gs)
            if monto_usd: add_row("USD", monto_usd)
    return pd.DataFrame(registros)


# ------------------------------ ORQUESTADOR Y POST-PROCESAMIENTO ------------------------------

OUTPUT_FILES = {
    "EC_EFECT_BCO": "BRITIMP_EFECTBANCO.csv",
    "EC_EFECT_ATM": "BRITIMP_EFECTATM.csv",
    "INV_BILLETES_BCO": "BRITIMP_INVENTARIO_BANCO.csv",
    "INV_BILLETES_ATM": "BRITIMP_INVENTARIO_ATM.csv",
    "EC_BULTO_ATM": "BRITIMP_BULTOS_ATM.csv",
}

def post_process_dataframes(data_dict: Dict[str, pd.DataFrame]) -> Dict[str, pd.DataFrame]:
    """Aplica limpiezas finales a los dataframes: renombra, formatea y elimina columnas."""
    processed_dict = {}
    for tipo, df in data_dict.items():
        if df.empty:
            continue

        df_copy = df.copy()

        # Eliminar ARCHIVO_ORIGEN
        if "ARCHIVO_ORIGEN" in df_copy.columns:
            df_copy = df_copy.drop(columns=["ARCHIVO_ORIGEN"])

        # Procesar archivos de Estado de Cuenta (EC)
        if tipo.startswith("EC_"):
            if "FECHA_OPERACION" in df_copy.columns:
                df_copy = df_copy.rename(columns={"FECHA_OPERACION": "FECHA_RECIBO"})
            
            # Formatear números a enteros (Int64 para admitir nulos)
            for col in ["MONTO", "IMPORTE", "BULTOS"]:
                if col in df_copy.columns:
                    df_copy[col] = pd.to_numeric(df_copy[col], errors='coerce').astype('Int64')

        # Procesar archivos de Inventario (INV)
        if tipo.startswith("INV_"):
            # Mover CJE_DEP a CALIDAD_CD si existe (del parser XLSX)
            if 'CJE_DEP' in df_copy.columns:
                # Llenar CALIDAD_CD con CJE_DEP solo si CALIDAD_CD está vacío
                if 'CALIDAD_CD' not in df_copy:
                    df_copy['CALIDAD_CD'] = None
                df_copy['CALIDAD_CD'] = df_copy['CALIDAD_CD'].fillna(df_copy['CJE_DEP'])
                df_copy = df_copy.drop(columns=['CJE_DEP'])

            # Formatear números a enteros
            for col in ["DENOMINACION", "CALIDAD_DEPOSITO", "CALIDAD_CD", "CALIDAD_CANJE", "MONEDA", "IMPORTE_TOTAL"]:
                if col in df_copy.columns:
                    df_copy[col] = pd.to_numeric(df_copy[col], errors='coerce').astype('Int64')

        # Reordenar y seleccionar columnas finales
        if tipo in FINAL_COLUMNS:
            final_cols = [col for col in FINAL_COLUMNS[tipo] if col in df_copy.columns]
            processed_dict[tipo] = df_copy[final_cols]
        else:
            processed_dict[tipo] = df_copy
            
    return processed_dict

def process_file(path: Path, parent_agency_hint: Optional[str] = None, DEBUG: bool = False) -> Tuple[pd.DataFrame, Optional[str], str, bool]:
    """Procesa un único archivo, desde la detección hasta el parsing."""
    logger.info(f"[ANALIZANDO] '{path.name}' en carpeta '{parent_agency_hint or 'RAIZ'}'")
    preview = file_text_preview(path)
    tipo = dispatch_tipo(path.name, preview)
    if not detect_cliente_itau(path, tipo):
        return pd.DataFrame(), tipo, (parent_agency_hint or ""), False
    if detect_sin_movimientos(path):
        agencia = parse_agencia_from_text(preview) or parent_agency_hint or "N/A"
        logger.info(f"[SKIP] '{path.name}' -> Archivo SIN MOVIMIENTOS (agencia: {agencia})")
        return pd.DataFrame(), tipo, agencia, True
    if tipo == "INV_BILLETES_UNKNOWN":
        logger.info("-> Tipo 'UNKNOWN' necesita ser refinado por contenido...")
        if "ATM" in preview.upper():
            tipo = "INV_BILLETES_ATM"
        else:
            tipo = "INV_BILLETES_BCO"
        logger.info(f"-> Tipo refinado a: {tipo}")
    agencia_final = parse_agencia_from_text(preview) or parent_agency_hint or ""
    logger.info(f"-> Tipo detectado: {tipo or 'Desconocido'}, Agencia: {agencia_final or 'No detectada'}")
    df = pd.DataFrame()
    parser_used = "Ninguno"
    ext = path.suffix.lower()
    try:
        if tipo == "EC_BULTO_ATM" and ext in (".xlsx", ".xls"):
            parser_used = "parse_ec_bultos_atm_xlsx"; df = parse_ec_bultos_atm_xlsx(path, DEBUG)
        elif tipo == "EC_EFECT_ATM" and ext in (".xlsx", ".xls"):
            parser_used = "parse_ec_efect_atm_xlsx"; df = parse_ec_efect_atm_xlsx(path, DEBUG)
        elif tipo == "EC_EFECT_BCO" and ext in (".xlsx", ".xls"):
            parser_used = "parse_ec_efect_bco_xlsx"; df = parse_ec_efect_bco_xlsx(path, DEBUG)
        elif tipo == "INV_BILLETES_ATM":
            if ext == ".pdf": parser_used = "parse_inv_billetes_pdf_atm"; df = parse_inv_billetes_pdf_atm(path, DEBUG)
            elif ext in (".xlsx", ".xls"): parser_used = "parse_inv_billetes_xlsx"; df = parse_inv_billetes_xlsx(path, DEBUG)
        elif tipo == "INV_BILLETES_BCO":
            if ext == ".pdf": parser_used = "parse_inv_billetes_pdf_bco"; df = parse_inv_billetes_pdf_bco(path, DEBUG)
            elif ext in (".xlsx", ".xls"): parser_used = "parse_inv_billetes_xlsx"; df = parse_inv_billetes_xlsx(path, DEBUG)
        else:
            logger.error(f"-> No hay un parser definido para tipo='{tipo}' y extensión='{ext}'")

    except Exception as e:
        logger.error(f"-> ¡ERROR! El parser '{parser_used}' falló para '{path.name}'. Detalles: {e}", exc_info=True)

    logger.info(f"-> Parser ejecutado: {parser_used}. Registros obtenidos: {len(df)}")
    return df, tipo, agencia_final, True

# ------------------------------ ESCRITURA Y MOVIMIENTO DE ARCHIVOS ------------------------------

def write_all_consolidated(all_data: Dict[str, pd.DataFrame]) -> None:
    """Escribe todos los DataFrames acumulados a sus respectivos archivos CSV, sobrescribiendo."""
    outdir = today_folder()
    logger.info("--- INICIANDO ESCRITURA DE ARCHIVOS CONSOLIDADOS ---")
    for tipo, df in all_data.items():
        if tipo not in OUTPUT_FILES or df.empty:
            continue
        outpath = outdir / OUTPUT_FILES[tipo]
        try:
            df.to_csv(outpath, index=False, encoding="utf-8-sig", sep=';')
            logger.info(f"[CSV ESCRITO] {len(df)} registros guardados en '{outpath.name}'")
        except Exception as e:
            logger.error(f"[ERROR DE ESCRITURA] No se pudo guardar '{outpath.name}'. Detalles: {e}")


def enviar_email_adjunto(fecha_hoy: str) -> None:
    """
    Enviar email con archivo adjunto (reporte de pronóstico).
    """
    ruta_adjunto = f"Reporte_pronostico_{fecha_hoy}.xlsx"
    nombre_adjunto = f"Reporte_pronostico_{fecha_hoy}.xlsx"

    try:
        email.enviarEmailadjunto(
            destinatario=[
                # "usuario1@interbanco.com.py",
                # "usuario2@interbanco.com.py",
            ],
            asunto=f"Reporte modelo estadistico {fecha_hoy}",
            mensaje="Buenos dias, \n va en adjunto el reporte del modelo de Boveda",
            ruta_adjunto=ruta_adjunto,
            nombre_adjunto=nombre_adjunto,
        )
        logger.info(f"Correo enviado con el adjunto '{ruta_adjunto}'.")
    except Exception as e:
        logger.error(f"No se pudo enviar el correo con el adjunto '{ruta_adjunto}'. Detalles: {e}")


def volcar_a_sql(dataframes: Dict[str, pd.DataFrame]) -> None:
    """Vuelca los datos consolidados al AT de datos (at_cmdts) usando sqlConn."""
    if not dataframes:
        logger.info('No hay DataFrames consolidados para volcar a la base de datos.')
        return

    try:
        conn_ = sqlConn(server='w2ks12-154.interbanco.com.py,1431', database='at_cmdts')
        logger.info('Conexión a at_cmdts creada correctamente.')
    except Exception as e:
        logger.error(f'No se pudo crear la conexión con at_cmdts. Detalles: {e}')
        return

    for tipo, df in dataframes.items():
        if df.empty:
            continue
        output_file = OUTPUT_FILES.get(tipo)
        if not output_file:
            logger.warning(f"No se encontró OUTPUT_FILES para el tipo '{tipo}'. Se omite el volcado a SQL.")
            continue
        table_name = f"PLSX_{Path(output_file).stem}"  # Ej: PLSX_BRITIMP_INVENTARIO_BANCO
        try:
            logger.info(f"Volcando {len(df)} registros a la tabla '{table_name}' (if_exists='replace').")
            conn_.crea_tabla(df, table_name, if_exists='replace')
        except Exception as e:
            logger.error(f"Error al volcar datos a la tabla '{table_name}'. Detalles: {e}")

    try:
        conn_.desconecta()
        logger.info('Conexión a at_cmdts cerrada correctamente.')
    except Exception as e:
        logger.warning(f'No se pudo cerrar la conexión a at_cmdts limpiamente: {e}')

def move_original(path: Path, agencia: str, procesado_ok: bool) -> None:
    """Mueve el archivo original a la carpeta de PROCESADO, en subcarpetas por fecha y agencia."""
    today = datetime.now().strftime('%Y-%m-%d')
    agencia_norm = (agencia or '').upper()
    if agencia_norm not in AGENCIES:
        agencia_norm = 'SIN_AGENCIA'
    agencia_dir = PROCESADO / today / agencia_norm
    agencia_dir.mkdir(parents=True, exist_ok=True)

    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    status = 'OK' if procesado_ok else 'ERROR'
    new_name = f"{path.stem}_{status}_{timestamp}{path.suffix}"

    try:
        shutil.move(str(path), str(agencia_dir / new_name))
        logger.info(f"-> [MOVIDO] '{path.name}' a '{agencia_dir / new_name}'")
    except Exception as e:
        logger.error(f"-> ¡ERROR! No se pudo mover el archivo '{path.name}'. Detalles: {e}")

# ------------------------------ SCANNER PRINCIPAL ------------------------------

def collect_pending_files() -> List[Tuple[Path, Optional[str]]]:
    """Recopila todos los archivos pendientes en las carpetas de agencias."""
    results = []
    for agencia in AGENCIES:
        for p in (PENDIENTES / agencia).rglob('*'):
            if p.is_file() and p.suffix.lower() in (".xlsx", ".xls", ".pdf") and not p.name.startswith("~"):
                results.append((p, agencia))
    return results

# ------------------------------ MAIN ------------------------------

def run(DEBUG: bool = False) -> Dict[str, int]:
    """Función principal que orquesta todo el proceso."""
#    setup_logger()
    pendientes = collect_pending_files()
    logger.info(f"Archivos encontrados para procesar: {len(pendientes)}")

    all_dataframes: Dict[str, List[pd.DataFrame]] = {k: [] for k in OUTPUT_FILES}

    for path, agencia_hint in pendientes:
        df, tipo, agencia_final, es_itau = process_file(path, parent_agency_hint=agencia_hint, DEBUG=DEBUG)
        procesado_exitoso = es_itau and (not df.empty or (tipo is not None and detect_sin_movimientos(path)))
        if es_itau and tipo and tipo in all_dataframes and not df.empty:
            all_dataframes[tipo].append(df)
        move_original(path, agencia_final or agencia_hint, procesado_exitoso)
        logger.info("-" * 50)
    # Consolidar y escribir los resultados finales
    final_consolidated: Dict[str, pd.DataFrame] = {}
    for tipo, df_list in all_dataframes.items():
        if df_list:
            final_consolidated[tipo] = pd.concat(df_list, ignore_index=True)

    final_formatted = post_process_dataframes(final_consolidated)

    write_all_consolidated(final_formatted)

    # Enviar el correo con el reporte de pronóstico antes de volcar los datos a SQL
    try:
        fecha_hoy = datetime.now().strftime("%Y-%m-%d")
        enviar_email_adjunto(fecha_hoy)
    except Exception as e:
        logger.error(f"[EMAIL] Error al intentar enviar el correo con el reporte de pronóstico: {e}")

    # Volcar los consolidados al AT de datos (at_cmdts)
    try:
        volcar_a_sql(final_formatted)
    except Exception as e:
        logger.error(f"[SQL] Error inesperado al volcar los datos consolidados a la base de datos: {e}")

    # Actualizar estadísticas después del post-procesamiento
    stats = {k: 0 for k in OUTPUT_FILES.keys()}
    for tipo, df in final_formatted.items():
        stats[tipo] = len(df)

    # Generar resumen de forma más compatible y clara
    resumen_partes = []
    for k, v in stats.items():
        resumen_partes.append(f"{k}: {v}")
    resumen_texto = ", ".join(resumen_partes)
    logger.info(f"[RESUMEN FINAL] Registros añadidos: {resumen_texto}")
    
    logger.info("=" * 10 + " FIN DE EJECUCIÓN " + "=" * 10 + "\n")
    return stats

if __name__ == "__main__":
    #DEBUG = "DEBUG" in sys.argv
    run()
import os
import sys
from datetime import datetime
from typing import Optional

import pandas as pd
from sqlConn import sqlConn


# Ruta base de BRITIMP en el share de red
ROOT = r"//nfs_airflow_py/cmdat/ea-saa-datos/Transportadoras/Britimp"
CONSOLIDADO_BASE = os.path.join(ROOT, "CONSOLIDADO")


def ruta_consolidado_fecha(fecha: str) -> str:
    """
    Devuelve la carpeta de CONSOLIDADO para la fecha dada (YYYY-MM-DD).
    """
    return os.path.join(CONSOLIDADO_BASE, fecha)


def checkea_archivos_creados(fecha: str) -> int:
    """
    Verifica que existan todos los archivos consolidados esperados para BRITIMP
    en la carpeta CONSOLIDADO/{fecha}.

    Archivos esperados:
        - BRITIMP_EFECTBANCO.csv
        - BRITIMP_EFECTATM.csv
        - BRITIMP_INVENTARIO_BANCO.csv
        - BRITIMP_INVENTARIO_ATM.csv
        - BRITIMP_BULTOS_ATM.csv
    """
    carpeta = ruta_consolidado_fecha(fecha)
    print(f"Carpeta de consolidados a revisar: {carpeta}")

    if not os.path.isdir(carpeta):
        print("NO EXISTE LA CARPETA DE CONSOLIDADOS PARA ESA FECHA")
        return -1

    esperados = {
        "EC_BCO": "BRITIMP_EFECTBANCO.csv",
        "EC_ATM": "BRITIMP_EFECTATM.csv",
        "INV_BCO": "BRITIMP_INVENTARIO_BANCO.csv",
        "INV_ATM": "BRITIMP_INVENTARIO_ATM.csv",
        "BULTOS_ATM": "BRITIMP_BULTOS_ATM.csv",
    }

    archivos_en_carpeta = set(os.listdir(carpeta))
    ok = 0

    for clave, nombre in esperados.items():
        if nombre in archivos_en_carpeta:
            print(f"ARCHIVO {clave} ({nombre}) CREADO")
            ok += 1
        else:
            print(f"FALTA ARCHIVO {clave} ({nombre})")

    if ok == len(esperados):
        print("SE TIENEN TODOS LOS ARCHIVOS CREADOS EN LA CARPETA CONSOLIDADO")
        return ok
    else:
        print(
            f"NO SE CREARON TODOS LOS ARCHIVOS. ESPERADOS={len(esperados)}, ENCONTRADOS={ok}"
        )
        return -1


def _leer_csv_consolidado(fecha: str, nombre_archivo: str) -> Optional[pd.DataFrame]:
    """
    Lee un CSV consolidado específico para la fecha dada.
    """
    carpeta = ruta_consolidado_fecha(fecha)
    path = os.path.join(carpeta, nombre_archivo)
    try:
        df = pd.read_csv(path, sep=";", encoding="utf-8-sig")
    except Exception as e:
        print(f"NO SE PUDO LEER ARCHIVO {nombre_archivo}: {e}")
        return None

    if df.empty:
        print(f"DATAFRAME DE {nombre_archivo} VACÍO")
        return None

    print(f"DATAFRAME {nombre_archivo} TRAE DATOS: {df.shape}")
    return df


# ----------------------------------------------------------------------
#  EC BANCO (EFECTIVO BANCO)
# ----------------------------------------------------------------------
def puebla_tabla_ec_banco(fecha: str) -> bool:
    df = _leer_csv_consolidado(fecha, "BRITIMP_EFECTBANCO.csv")
    if df is None:
        return False

    # Conversión de fechas (ajustar formato si fuera necesario)
    try:
        df["FECHA_RECIBO"] = pd.to_datetime(df["FECHA_RECIBO"], errors="coerce").dt.date
        df["FECHA_ARCHIVO"] = pd.to_datetime(df["FECHA_ARCHIVO"], errors="coerce").dt.date
    except Exception as e:
        print(f"ADVERTENCIA: No se pudo convertir fechas en EC_BANCO: {e}")

    df["FECHA_CREACION"] = pd.to_datetime(datetime.now())

    try:
        conn_ = sqlConn(predef_conn="AT_CMDTS", agendado=False)
    except Exception as e:
        print(f"NO SE PUDO CREAR CONEXIÓN A AT_CMDTS PARA EC_BANCO: {e}")
        return False

    try:
        conn_.crea_tabla(df, "PLXS_EC_BCO_BRITIMP_2025", if_exists="append")
        conn_.desconecta()
        print("TABLA EC_BANCO BRITIMP POBLADA")
        return True
    except Exception as e:
        conn_.desconecta()
        print(f"PROBLEMA CON LA CREACIÓN/INSERCIÓN EN TABLA EC_BANCO: {e}")
        return False


# ----------------------------------------------------------------------
#  INV BILLETES BANCO
# ----------------------------------------------------------------------
def puebla_tabla_inv_billetes_banco(fecha: str) -> bool:
    df = _leer_csv_consolidado(fecha, "BRITIMP_INVENTARIO_BANCO.csv")
    if df is None:
        return False

    try:
        df["FECHA_INVENTARIO"] = pd.to_datetime(
            df["FECHA_INVENTARIO"], errors="coerce"
        ).dt.date
    except Exception as e:
        print(f"ADVERTENCIA: No se pudo convertir FECHA_INVENTARIO en INV_BILLETES_BCO: {e}")

    df["FECHA_CREACION"] = pd.to_datetime(datetime.now())

    try:
        conn_ = sqlConn(predef_conn="AT_CMDTS", agendado=False)
    except Exception as e:
        print(f"NO SE PUDO CREAR CONEXIÓN A AT_CMDTS PARA INV_BILLETES_BCO: {e}")
        return False

    try:
        conn_.crea_tabla(df, "PLXS_BRITIMP_INV_BILLETES_BANCO_2025", if_exists="append")
        conn_.desconecta()
        print("TABLA INV_BILLETES BANCO BRITIMP POBLADA")
        return True
    except Exception as e:
        conn_.desconecta()
        print(f"PROBLEMA CON LA CREACIÓN/INSERCIÓN EN TABLA INV_BILLETES_BCO: {e}")
        return False


# ----------------------------------------------------------------------
#  INV BILLETES ATM
# ----------------------------------------------------------------------
def puebla_tabla_inv_billetes_atm(fecha: str) -> bool:
    df = _leer_csv_consolidado(fecha, "BRITIMP_INVENTARIO_ATM.csv")
    if df is None:
        return False

    try:
        df["FECHA_INVENTARIO"] = pd.to_datetime(
            df["FECHA_INVENTARIO"], errors="coerce"
        ).dt.date
    except Exception as e:
        print(f"ADVERTENCIA: No se pudo convertir FECHA_INVENTARIO en INV_BILLETES_ATM: {e}")

    df["FECHA_CREACION"] = pd.to_datetime(datetime.now())

    try:
        conn_ = sqlConn(predef_conn="AT_CMDTS", agendado=False)
    except Exception as e:
        print(f"NO SE PUDO CREAR CONEXIÓN A AT_CMDTS PARA INV_BILLETES_ATM: {e}")
        return False

    try:
        conn_.crea_tabla(df, "PLXS_BRITIMP_INV_BILLETES_ATM_2025", if_exists="append")
        conn_.desconecta()
        print("TABLA INV_BILLETES ATM BRITIMP POBLADA")
        return True
    except Exception as e:
        conn_.desconecta()
        print(f"PROBLEMA CON LA CREACIÓN/INSERCIÓN EN TABLA INV_BILLETES_ATM: {e}")
        return False


# ----------------------------------------------------------------------
#  EC ATM (EFECTIVO ATM)
# ----------------------------------------------------------------------
def puebla_tabla_ec_atm(fecha: str) -> bool:
    df = _leer_csv_consolidado(fecha, "BRITIMP_EFECTATM.csv")
    if df is None:
        return False

    try:
        df["FECHA_RECIBO"] = pd.to_datetime(df["FECHA_RECIBO"], errors="coerce").dt.date
        df["FECHA_ARCHIVO"] = pd.to_datetime(df["FECHA_ARCHIVO"], errors="coerce").dt.date
    except Exception as e:
        print(f"ADVERTENCIA: No se pudo convertir fechas en EC_ATM: {e}")

    df["FECHA_CREACION"] = pd.to_datetime(datetime.now())

    try:
        conn_ = sqlConn(predef_conn="AT_CMDTS", agendado=False)
    except Exception as e:
        print(f"NO SE PUDO CREAR CONEXIÓN A AT_CMDTS PARA EC_ATM: {e}")
        return False

    try:
        conn_.crea_tabla(df, "PLXS_BRITIMP_EFFECTATM", if_exists="append")
        conn_.desconecta()
        print("TABLA EC_ATM BRITIMP POBLADA")
        return True
    except Exception as e:
        conn_.desconecta()
        print(f"PROBLEMA CON LA CREACIÓN/INSERCIÓN EN TABLA EC_ATM: {e}")
        return False


# ----------------------------------------------------------------------
#  BULTOS ATM
# ----------------------------------------------------------------------
def puebla_tabla_bultos_atm(fecha: str) -> bool:
    df = _leer_csv_consolidado(fecha, "BRITIMP_BULTOS_ATM.csv")
    if df is None:
        return False

    try:
        df["FECHA_RECIBO"] = pd.to_datetime(df["FECHA_RECIBO"], errors="coerce").dt.date
        df["FECHA_ARCHIVO"] = pd.to_datetime(df["FECHA_ARCHIVO"], errors="coerce").dt.date
    except Exception as e:
        print(f"ADVERTENCIA: No se pudo convertir fechas en BULTOS_ATM: {e}")

    df["FECHA_CREACION"] = pd.to_datetime(datetime.now())

    try:
        conn_ = sqlConn(predef_conn="AT_CMDTS", agendado=False)
    except Exception as e:
        print(f"NO SE PUDO CREAR CONEXIÓN A AT_CMDTS PARA BULTOS_ATM: {e}")
        return False

    try:
        conn_.crea_tabla(df, "PLXS_BRITIMP_BULTOS_ATM", if_exists="append")
        conn_.desconecta()
        print("TABLA BULTOS_ATM BRITIMP POBLADA")
        return True
    except Exception as e:
        conn_.desconecta()
        print(f"PROBLEMA CON LA CREACIÓN/INSERCIÓN EN TABLA BULTOS_ATM: {e}")
        return False


if __name__ == "__main__":
    # Podés pasar la fecha como argumento (YYYY-MM-DD),
    # y si no, usa la fecha de hoy.
    if len(sys.argv) > 1:
        FECHA = sys.argv[1]
    else:
        date_obj = datetime.now().date()
        FECHA = date_obj.strftime("%Y-%m-%d")

    print(f"FECHA: {FECHA}")  # YYYY-MM-DD

    cant_archivos = checkea_archivos_creados(FECHA)
    print(f"CANTIDAD DE ARCHIVOS CREADOS: {cant_archivos}")
    if cant_archivos == 5:
        continuar = puebla_tabla_ec_banco(FECHA)
        if continuar:
            continuar = puebla_tabla_inv_billetes_banco(FECHA)
        if continuar:
            continuar = puebla_tabla_inv_billetes_atm(FECHA)
        if continuar:
            continuar = puebla_tabla_ec_atm(FECHA)
        if continuar:
            continuar = puebla_tabla_bultos_atm(FECHA)
        if continuar:
            print("FINALIZÓ PROCESO DE SUBIDA A TABLAS BRITIMP CON ÉXITO")
        else:
            print("HUBO ERRORES EN ALGUNA DE LAS SUBIDAS DE TABLAS.")
    else:
        print(
            "ARCHIVOS NECESARIOS PARA POBLAR LAS TABLAS NO FUERON CREADOS. "
            "REVISAR PROCESO DE CREACIÓN DE ARCHIVOS BRITIMP."
        )

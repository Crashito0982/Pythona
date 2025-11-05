import os
import sys
from datetime import datetime
from typing import Optional, Tuple

import pandas as pd
from sqlConn import sqlConn
from mail import email

# PRODUCCIÓN
ROOT = r"//nfs_airflow_py/cmdat/ea-saa-datos/Transportadoras/Britimp"
CONSOLIDADO_BASE = os.path.join(ROOT, "CONSOLIDADO")


def ruta_consolidado_fecha(fecha: str) -> str:
    return os.path.join(CONSOLIDADO_BASE, fecha)


def buscar_log(fecha: str) -> Optional[str]:
    carpeta = ruta_consolidado_fecha(fecha)
    posibles_nombres = ["BRITIMP_LOG.txt", "BRITIMP_log.txt"]
    for nombre in posibles_nombres:
        path = os.path.join(carpeta, nombre)
        if os.path.isfile(path):
            return path
    return None


def enviar_email_error(fecha_hoy: str, detalle: str, ruta_log: Optional[str] = None) -> None:
    asunto = f"BRITIMP - Error en el proceso ({fecha_hoy})"
    mensaje = (
        "Se produjo un error durante el proceso de BRITIMP.\n\n"
        f"Fecha de proceso: {fecha_hoy}\n"
        f"Detalle: {detalle}"
    )
    destinatarios = [
        "Leonardo Doldan <correo1jemplo@ejemplo.com.py>",
        "Carlos Rodriguez <correo2jemplo@ejemplo.com.py>",
    ]
    if ruta_log is not None and os.path.isfile(ruta_log):
        try:
            email.enviarEmailAdjunto(
                destinatario=destinatarios,
                asunto=asunto,
                mensaje=mensaje,
                ruta_adjunto=ruta_log,
                nombre_adjunto=os.path.basename(ruta_log),
            )
        except Exception as e:
            print(f"[WARN] No se pudo enviar correo de error con adjunto: {e}")
    else:
        try:
            email.enviarEmail(
                destinatario=destinatarios,
                asunto=asunto,
                mensaje=mensaje,
            )
        except Exception as e:
            print(f"[WARN] No se pudo enviar correo de error sin adjunto: {e}")


def enviar_email_exito(fecha_hoy: str, ruta_log: Optional[str] = None) -> None:
    asunto = f"BRITIMP - Proceso de consolidado y carga OK ({fecha_hoy})"
    mensaje = (
        "Proceso BRITIMP finalizado correctamente.\n\n"
        f"Fecha de proceso: {fecha_hoy}\n"
        "Se generaron los archivos consolidados y se poblaron las tablas en AT_CMDTS sin errores."
    )
    destinatarios = [
        "Leonardo Doldan <correo1jemplo@ejemplo.com.py>",
        "Carlos Rodriguez <correo2jemplo@ejemplo.com.py>",
    ]
    if ruta_log is not None and os.path.isfile(ruta_log):
        try:
            email.enviarEmailAdjunto(
                destinatario=destinatarios,
                asunto=asunto,
                mensaje=mensaje,
                ruta_adjunto=ruta_log,
                nombre_adjunto=os.path.basename(ruta_log),
            )
        except Exception as e:
            print(f"[WARN] No se pudo enviar correo de éxito con adjunto: {e}")
    else:
        try:
            email.enviarEmail(
                destinatario=destinatarios,
                asunto=asunto,
                mensaje=mensaje,
            )
        except Exception as e:
            print(f"[WARN] No se pudo enviar correo de éxito sin adjunto: {e}")


def checkea_archivos_creados(fecha: str) -> Tuple[int, list]:
    carpeta = ruta_consolidado_fecha(fecha)
    print(f"Carpeta de consolidados a revisar: {carpeta}")
    if not os.path.isdir(carpeta):
        print("NO EXISTE LA CARPETA DE CONSOLIDADOS PARA ESA FECHA")
        return -1, ["CARPETA_NO_EXISTE"]
    esperados = {
        "EC_BCO": "BRITIMP_EFECTBANCO.csv",
        "EC_ATM": "BRITIMP_EFECTATM.csv",
        "INV_BCO": "BRITIMP_INVENTARIO_BANCO.csv",
        "INV_ATM": "BRITIMP_INVENTARIO_ATM.csv",
        "BULTOS_ATM": "BRITIMP_BULTOS_ATM.csv",
    }
    archivos_en_carpeta = set(os.listdir(carpeta))
    ok = 0
    faltantes = []
    for clave, nombre in esperados.items():
        if nombre in archivos_en_carpeta:
            print(f"ARCHIVO {clave} ({nombre}) CREADO")
            ok += 1
        else:
            print(f"FALTA ARCHIVO {clave} ({nombre})")
            faltantes.append(nombre)
    if ok == len(esperados):
        print("SE TIENEN TODOS LOS ARCHIVOS CREADOS EN LA CARPETA CONSOLIDADO")
    else:
        print(
            f"NO SE CREARON TODOS LOS ARCHIVOS. ESPERADOS={len(esperados)}, ENCONTRADOS={ok}"
        )
    return ok, faltantes


def _leer_csv_consolidado(fecha: str, nombre_archivo: str) -> Tuple[Optional[pd.DataFrame], Optional[str]]:
    carpeta = ruta_consolidado_fecha(fecha)
    path = os.path.join(carpeta, nombre_archivo)
    try:
        df = pd.read_csv(path, sep=";", encoding="utf-8-sig")
    except Exception as e:
        msg = f"NO SE PUDO LEER ARCHIVO {nombre_archivo}: {e}"
        print(msg)
        return None, msg
    if df.empty:
        msg = f"DATAFRAME DE {nombre_archivo} VACÍO"
        print(msg)
        return None, msg
    print(f"DATAFRAME {nombre_archivo} TRAE DATOS: {df.shape}")
    return df, None


def puebla_tabla_ec_banco(fecha: str) -> None:
    nombre_archivo = "BRITIMP_EFECTBANCO.csv"
    df, err = _leer_csv_consolidado(fecha, nombre_archivo)
    if df is None:
        raise ValueError(err or f"Error desconocido al leer {nombre_archivo}")
    try:
        df["FECHA_RECIBO"] = pd.to_datetime(
            df["FECHA_RECIBO"], format="%d/%m/%Y", errors="coerce"
        ).dt.date
        df["FECHA_ARCHIVO"] = pd.to_datetime(
            df["FECHA_ARCHIVO"], format="%d/%m/%Y", errors="coerce"
        ).dt.date
    except Exception as e:
        print(f"ADVERTENCIA: No se pudo convertir fechas en EC_BANCO: {e}")
    df["FECHA_CREACION"] = pd.to_datetime(datetime.now())
    try:
        conn_ = sqlConn(predef_conn="AT_CMDTS", agendado=False)
    except Exception as e:
        raise RuntimeError(f"NO SE PUDO CREAR CONEXIÓN A AT_CMDTS PARA EC_BANCO: {e}")
    try:
        conn_.crea_tabla(df, "PLXS_EC_BCO_BRITIMP_2025", if_exists="append")
        conn_.desconecta()
        print("TABLA EC_BANCO BRITIMP POBLADA")
    except Exception as e:
        conn_.desconecta()
        raise RuntimeError(f"PROBLEMA CON LA CREACIÓN/INSERCIÓN EN TABLA EC_BANCO: {e}")


def puebla_tabla_inv_billetes_banco(fecha: str) -> None:
    nombre_archivo = "BRITIMP_INVENTARIO_BANCO.csv"
    df, err = _leer_csv_consolidado(fecha, nombre_archivo)
    if df is None:
        raise ValueError(err or f"Error desconocido al leer {nombre_archivo}")
    try:
        df["FECHA_INVENTARIO"] = pd.to_datetime(
            df["FECHA_INVENTARIO"], format="%d/%m/%Y", errors="coerce"
        ).dt.date
    except Exception as e:
        print(
            "ADVERTENCIA: No se pudo convertir FECHA_INVENTARIO en INV_BILLETES_BCO: "
            f"{e}"
        )
    df["FECHA_CREACION"] = pd.to_datetime(datetime.now())
    try:
        conn_ = sqlConn(predef_conn="AT_CMDTS", agendado=False)
    except Exception as e:
        raise RuntimeError(f"NO SE PUDO CREAR CONEXIÓN A AT_CMDTS PARA INV_BILLETES_BCO: {e}")
    try:
        conn_.crea_tabla(df, "PLXS_BRITIMP_INV_BILLETES_BANCO_2025", if_exists="append")
        conn_.desconecta()
        print("TABLA INV_BILLETES BANCO BRITIMP POBLADA")
    except Exception as e:
        conn_.desconecta()
        raise RuntimeError(f"PROBLEMA CON LA CREACIÓN/INSERCIÓN EN TABLA INV_BILLETES_BCO: {e}")


def puebla_tabla_inv_billetes_atm(fecha: str) -> None:
    nombre_archivo = "BRITIMP_INVENTARIO_ATM.csv"
    df, err = _leer_csv_consolidado(fecha, nombre_archivo)
    if df is None:
        raise ValueError(err or f"Error desconocido al leer {nombre_archivo}")
    try:
        df["FECHA_INVENTARIO"] = pd.to_datetime(
            df["FECHA_INVENTARIO"], format="%d/%m/%Y", errors="coerce"
        ).dt.date
    except Exception as e:
        print(
            "ADVERTENCIA: No se pudo convertir FECHA_INVENTARIO en INV_BILLETES_ATM: "
            f"{e}"
        )
    df["FECHA_CREACION"] = pd.to_datetime(datetime.now())
    try:
        conn_ = sqlConn(predef_conn="AT_CMDTS", agendado=False)
    except Exception as e:
        raise RuntimeError(f"NO SE PUDO CREAR CONEXIÓN A AT_CMDTS PARA INV_BILLETES_ATM: {e}")
    try:
        conn_.crea_tabla(df, "PLXS_BRITIMP_INV_BILLETES_ATM_2025", if_exists="append")
        conn_.desconecta()
        print("TABLA INV_BILLETES ATM BRITIMP POBLADA")
    except Exception as e:
        conn_.desconecta()
        raise RuntimeError(f"PROBLEMA CON LA CREACIÓN/INSERCIÓN EN TABLA INV_BILLETES_ATM: {e}")


def puebla_tabla_ec_atm(fecha: str) -> None:
    nombre_archivo = "BRITIMP_EFECTATM.csv"
    df, err = _leer_csv_consolidado(fecha, nombre_archivo)
    if df is None:
        raise ValueError(err or f"Error desconocido al leer {nombre_archivo}")
    try:
        df["FECHA_RECIBO"] = pd.to_datetime(
            df["FECHA_RECIBO"], format="%d/%m/%Y", errors="coerce"
        ).dt.date
        df["FECHA_ARCHIVO"] = pd.to_datetime(
            df["FECHA_ARCHIVO"], format="%d/%m/%Y", errors="coerce"
        ).dt.date
    except Exception as e:
        print(f"ADVERTENCIA: No se pudo convertir fechas en EC_ATM: {e}")
    df["FECHA_CREACION"] = pd.to_datetime(datetime.now())
    try:
        conn_ = sqlConn(predef_conn="AT_CMDTS", agendado=False)
    except Exception as e:
        raise RuntimeError(f"NO SE PUDO CREAR CONEXIÓN A AT_CMDTS PARA EC_ATM: {e}")
    try:
        conn_.crea_tabla(df, "PLXS_BRITIMP_EFECTATM", if_exists="append")
        conn_.desconecta()
        print("TABLA EC_ATM BRITIMP POBLADA")
    except Exception as e:
        conn_.desconecta()
        raise RuntimeError(f"PROBLEMA CON LA CREACIÓN/INSERCIÓN EN TABLA EC_ATM: {e}")


def puebla_tabla_bultos_atm(fecha: str) -> None:
    nombre_archivo = "BRITIMP_BULTOS_ATM.csv"
    df, err = _leer_csv_consolidado(fecha, nombre_archivo)
    if df is None:
        raise ValueError(err or f"Error desconocido al leer {nombre_archivo}")
    try:
        df["FECHA_RECIBO"] = pd.to_datetime(
            df["FECHA_RECIBO"], format="%d/%m/%Y", errors="coerce"
        ).dt.date
        df["FECHA_ARCHIVO"] = pd.to_datetime(
            df["FECHA_ARCHIVO"], format="%d/%m/%Y", errors="coerce"
        ).dt.date
    except Exception as e:
        print(f"ADVERTENCIA: No se pudo convertir fechas en BULTOS_ATM: {e}")
    df["FECHA_CREACION"] = pd.to_datetime(datetime.now())
    try:
        conn_ = sqlConn(predef_conn="AT_CMDTS", agendado=False)
    except Exception as e:
        raise RuntimeError(f"NO SE PUDO CREAR CONEXIÓN A AT_CMDTS PARA BULTOS_ATM: {e}")
    try:
        conn_.crea_tabla(df, "PLXS_BRITIMP_BULTOS_ATM", if_exists="append")
        conn_.desconecta()
        print("TABLA BULTOS_ATM BRITIMP POBLADA")
    except Exception as e:
        conn_.desconecta()
        raise RuntimeError(f"PROBLEMA CON LA CREACIÓN/INSERCIÓN EN TABLA BULTOS_ATM: {e}")


def run(fecha: Optional[str] = None) -> None:
    if fecha is None:
        fecha = datetime.now().strftime("%Y-%m-%d")
    print(f"FECHA: {fecha}")
    ruta_log = buscar_log(fecha)
    cant_archivos, faltantes = checkea_archivos_creados(fecha)
    print(f"CANTIDAD DE ARCHIVOS CREADOS: {cant_archivos}")
    if cant_archivos != 5:
        detalle = (
            "Archivos necesarios para poblar las tablas no fueron creados o están incompletos. "
            f"Faltantes: {', '.join(faltantes) if faltantes else 'DESCONOCIDO'}"
        )
        enviar_email_error(fecha, detalle, ruta_log)
        return
    try:
        puebla_tabla_ec_banco(fecha)
        puebla_tabla_inv_billetes_banco(fecha)
        puebla_tabla_inv_billetes_atm(fecha)
        puebla_tabla_ec_atm(fecha)
        puebla_tabla_bultos_atm(fecha)
    except ValueError as ve:
        detalle = f"Error de archivo/DF en el proceso BRITIMP: {ve}"
        enviar_email_error(fecha, detalle, ruta_log)
        return
    except RuntimeError as re_err:
        detalle = f"Error de base de datos en el proceso BRITIMP: {re_err}"
        enviar_email_error(fecha, detalle, ruta_log)
        return
    except Exception as e:
        detalle = f"Error inesperado en el proceso BRITIMP: {e}"
        enviar_email_error(fecha, detalle, ruta_log)
        return
    print("FINALIZÓ PROCESO DE SUBIDA A TABLAS BRITIMP CON ÉXITO")
    enviar_email_exito(fecha, ruta_log)


if __name__ == "__main__":
    if len(sys.argv) > 1:
        run(sys.argv[1])
    else:
        run()

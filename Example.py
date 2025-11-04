    # Normalización específica para EC_ATM y BULTOS_ATM:
    for tipo in ["EC_EFECT_ATM", "EC_BULTO_ATM"]:
        if tipo not in final:
            continue
        df = final[tipo]

        # Renombrar MONTO -> IMPORTE si hace falta
        if "MONTO" in df.columns and "IMPORTE" not in df.columns:
            df = df.rename(columns={"MONTO": "IMPORTE"})

        # Reordenar columnas para que coincidan con la tabla final
        columnas_sql = [
            "FECHA_RECIBO",
            "SUCURSAL",
            "RECIBO",
            "BULTOS",          # según tu descripción, es BULTOS (plural)
            "MONEDA",
            "IMPORTE",
            "ING_EGR",
            "CLASIFICACION",
            "FECHA_ARCHIVO",
            "MOTIVO_MOVIMIENTO",
            "AGENCIA",
        ]

        # Aseguramos que todas existan (si falta alguna, la creamos vacía)
        for col in columnas_sql:
            if col not in df.columns:
                df[col] = None

        df = df[columnas_sql]
        final[tipo] = df

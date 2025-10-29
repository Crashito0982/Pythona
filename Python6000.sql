USE AT_CMDTS
GO

SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

/*
    SP: dbo.SP_CRM_LEGIT_OPTIMIZADO
    Objetivo: Pipeline optimizado para consolidar CONTRATOS + CÉDULAS de clientes con eventos ENTREGADO,
              priorizando buenas prácticas: SQL estático, #temp tables con esquema e índices, filtros tempranos,
              semi-joins sargables y deduplicación controlada.
    Salida:   dbo.crm_legit_axnt_base_modelo_OPT (tabla final para pruebas)
*/
CREATE OR ALTER PROCEDURE dbo.SP_CRM_LEGIT_OPTIMIZADO
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;
    --SET STATISTICS IO ON;  -- habilitar manualmente en sesión
    --SET STATISTICS TIME ON;-- habilitar manualmente en sesión

    /* ====== Variables de control ====== */
    DECLARE @DIAS_ATRAS INT = -15;
    DECLARE @DIAS_ATRAS_EVENTOS_OUT INT = -30;
    DECLARE @AMD_inicio  CHAR(8) = CONVERT(char(8), CONVERT(date, DATEADD(day, @DIAS_ATRAS, GETDATE())), 112);
    DECLARE @AMD_EVENTOS_OUT CHAR(8) = CONVERT(char(8), CONVERT(date, DATEADD(day, @DIAS_ATRAS_EVENTOS_OUT, GETDATE())), 112);
    DECLARE @Fecha1 DATE = CONVERT(date, DATEADD(day, @DIAS_ATRAS, GETDATE()));

    /* Switches opcionales */
    DECLARE @EliminarCISoloSinContrato BIT = 0; -- 1 = borra CIs de clientes que no tengan ningún otro doc (p.ej. contrato)

    /* =========================================================
       PASO 1: LOGÍSTICA (ENTREGADOS) sin SQL dinámico + #temp
       ========================================================= */
    IF OBJECT_ID('tempdb..#LS_EVENTO_ENTREGADO') IS NOT NULL DROP TABLE #LS_EVENTO_ENTREGADO;
    CREATE TABLE #LS_EVENTO_ENTREGADO
    (
        docid VARCHAR(20) NOT NULL,
        evtsc INT         NOT NULL,
        CONSTRAINT PK_#LS_EVENTO_ENTREGADO PRIMARY KEY (docid)
    );

    INSERT #LS_EVENTO_ENTREGADO (docid, evtsc)
    SELECT a.docid, MAX(a.evtsc) AS evtsc
    FROM ODSP_LGLIB_LGMEVT AS a
    WHERE a.evttdi = 1
      AND a.evtfch >= @AMD_EVENTOS_OUT
      AND NOT EXISTS (
            SELECT 1
            FROM ODSP_LGLIB_LGMEVT AS x
            WHERE x.docid = a.docid
              AND x.evted <> 'T'
              AND x.evttdi IN (16,19,70)
      )
    GROUP BY a.docid;

    IF OBJECT_ID('tempdb..#EventosEntBase') IS NOT NULL DROP TABLE #EventosEntBase;
    CREATE TABLE #EventosEntBase
    (
        docid VARCHAR(20) NOT NULL,
        evtsc INT         NOT NULL,
        CONSTRAINT PK_#EventosEntBase PRIMARY KEY (docid, evtsc)
    );

    INSERT #EventosEntBase(docid, evtsc)
    SELECT a.docid, a.evtsc
    FROM ODSP_LGLIB_LGMEVT AS a
    JOIN #LS_EVENTO_ENTREGADO AS b
      ON b.docid = a.docid AND b.evtsc = a.evtsc
    WHERE a.evtfch >= @AMD_inicio
      AND a.evttdi = 1;

    IF OBJECT_ID('tempdb..#EventosLogistica') IS NOT NULL DROP TABLE #EventosLogistica;
    CREATE TABLE #EventosLogistica
    (
        descrip_doc              VARCHAR(200) NULL,
        docid                    VARCHAR(20)  NOT NULL,
        nro_guia                 VARCHAR(50)  NULL,
        nombre_cliente           VARCHAR(200) NULL,
        nro_cliente              BIGINT       NULL,
        tipo_documento_logistica VARCHAR(50)  NULL,
        nro_documento            VARCHAR(50)  NULL,
        estado_cabecera          VARCHAR(10)  NULL,
        fecha_act                INT          NULL,  -- yyyymmdd (112)
        id_doc_cod_barra         VARCHAR(100) NULL,
        tipo_tarjeta             VARCHAR(10)  NULL,
        cod_tipo_tarjeta         VARCHAR(10)  NULL,
        tipo_producto            VARCHAR(50)  NULL,
        tipo_producto_det        INT          NULL,
        ciudad_cliente           VARCHAR(100) NULL,
        CONSTRAINT PK_#EventosLogistica PRIMARY KEY (docid)
    );

    INSERT #EventosLogistica
    (
        descrip_doc, docid, nro_guia, nombre_cliente, nro_cliente,
        tipo_documento_logistica, nro_documento, estado_cabecera, fecha_act,
        id_doc_cod_barra, tipo_tarjeta, cod_tipo_tarjeta, tipo_producto,
        tipo_producto_det, ciudad_cliente
    )
    SELECT
        tdc.TDCDS,
        CAST(doc.DOCID AS VARCHAR(20))          AS docid,
        doc.DOCGU                               AS nro_guia,
        doc.docnm                               AS nombre_cliente,
        TRY_CONVERT(BIGINT, doc.DOCCTI)         AS nro_cliente,
        doc.DOCTDC                              AS tipo_documento_logistica,
        doc.DOCTDN                              AS nro_documento,
        doc.DOCES                               AS estado_cabecera,
        doc.docedf                              AS fecha_act,
        doc.doccb                               AS id_doc_cod_barra,
        doc.docerm                              AS tipo_tarjeta,
        doc.docori                              AS cod_tipo_tarjeta,
        doc.docpri                              AS tipo_producto,
        tdc.TDCID                               AS tipo_producto_det,
        doc.doccd                               AS ciudad_cliente
    FROM ODSP_LGLIB_LGMDOC AS doc
    JOIN #EventosEntBase    AS eb  ON eb.docid = doc.DOCID
    LEFT JOIN ODSP_LGLIB_LGMTDC AS tdc 
      ON doc.DOCPRI = tdc.tdcpri AND doc.DOCTDI = tdc.TDCID
    WHERE doc.docedf >= @AMD_inicio
      AND (
            (doc.TDCPRI = 'TARJ.C' AND tdc.TDCID IN (1,36,37,70,89,116,132))
         OR (doc.TDCPRI = '105'    AND tdc.TDCID = 89)
      );

    CREATE INDEX IX_#EventosLogistica_nro_cliente         ON #EventosLogistica(nro_cliente);
    CREATE INDEX IX_#EventosLogistica_docid               ON #EventosLogistica(docid);

    /* =========================================================
       PASO 2: ADI (Titularidad) -> mapear ID_LOGISTICA a CLIENTE_TITULAR
       ========================================================= */
    IF OBJECT_ID('tempdb..#ADI2') IS NOT NULL DROP TABLE #ADI2;
    CREATE TABLE #ADI2
    (
        NRO_TJT               VARCHAR(50) NULL,
        TIPO_TC               CHAR(1)     NULL,
        NRO_CLIENTE_ADICIONAL BIGINT      NULL,
        NRO_CLIENTE_TITULAR   BIGINT      NULL,
        ID_LOGISTICA          VARCHAR(20) NOT NULL,
        NUMERO_CLIENTE        BIGINT      NULL,
        NUMERO_DOCUMENTO      VARCHAR(50) NULL,
        CONSTRAINT PK_#ADI2 PRIMARY KEY (ID_LOGISTICA)
    );

    INSERT #ADI2
    SELECT
        SUBSTRING(a.NUTARJET, 1, LEN(a.NUTARJET) - 3) AS NRO_TJT,
        a.TITARJET                                     AS TIPO_TC,
        TRY_CONVERT(BIGINT, a.CONUMECL)                AS NRO_CLIENTE_ADICIONAL,
        TRY_CONVERT(BIGINT, a.CONUCLTI)                AS NRO_CLIENTE_TITULAR,
        CAST(b.DOCID AS VARCHAR(20))                   AS ID_LOGISTICA,
        TRY_CONVERT(BIGINT, b.DOCCTI)                  AS NUMERO_CLIENTE,
        b.DOCTDN                                       AS NUMERO_DOCUMENTO
    FROM CRUDOCMDIR.ODSP.dbo.V_TARJETA_PBTARJETA AS a
    JOIN ODSP_LGLIB_LGMDOC AS b
      ON SUBSTRING(a.NUTARJET, 1, LEN(a.NUTARJET) - 3) = b.DOCNRD COLLATE SQL_Latin1_General_CP1_CI_AI
    WHERE b.DOCID IS NOT NULL
      AND b.DOCNRD <> ''
      AND b.doccti <> '0.0'
      AND b.doccti IS NOT NULL;

    IF OBJECT_ID('tempdb..#EventosLogisticaNew') IS NOT NULL DROP TABLE #EventosLogisticaNew;
    SELECT el.*, adi.NRO_CLIENTE_TITULAR
    INTO   #EventosLogisticaNew
    FROM   #EventosLogistica el
    JOIN   #ADI2            adi ON adi.ID_LOGISTICA = el.docid;

    CREATE INDEX IX_#EventosLogisticaNew_nro_cliente          ON #EventosLogisticaNew(nro_cliente);
    CREATE INDEX IX_#EventosLogisticaNew_nro_cliente_titular  ON #EventosLogisticaNew(NRO_CLIENTE_TITULAR);

    /* Set de clientes logísticos (evita OR en joins posteriores) */
    IF OBJECT_ID('tempdb..#ClientesLogistica') IS NOT NULL DROP TABLE #ClientesLogistica;
    CREATE TABLE #ClientesLogistica (valor BIGINT NOT NULL PRIMARY KEY);

    INSERT #ClientesLogistica(valor)
    SELECT DISTINCT nro_cliente
    FROM #EventosLogisticaNew
    WHERE nro_cliente IS NOT NULL;

    INSERT #ClientesLogistica(valor)
    SELECT DISTINCT NRO_CLIENTE_TITULAR
    FROM #EventosLogisticaNew
    WHERE NRO_CLIENTE_TITULAR IS NOT NULL
      AND NOT EXISTS (SELECT 1 FROM #ClientesLogistica x WHERE x.valor = #EventosLogisticaNew.NRO_CLIENTE_TITULAR);

    /* =========================================================
       PASO 3: CONTRATOS desde Axentria (solo clientes del set logístico)
       ========================================================= */
    IF OBJECT_ID('tempdb..#AxntContratosRaw') IS NOT NULL DROP TABLE #AxntContratosRaw;
    CREATE TABLE #AxntContratosRaw
    (
        anho              INT,
        anho_mes          INT,
        anho_mes_dia      INT,
        vcn_iddo          INT,
        id_nombre         INT,
        campo             VARCHAR(50),
        valor             BIGINT NULL,
        fecha_creacion    DATETIME2(0),
        tipo_documento    VARCHAR(100),
        identificar_documento INT NULL,
        co_fechaCreacion  DATETIME2(0) NULL,
        nombre_logico     VARCHAR(255) NULL,
        VE_IDDO           INT NULL,
        VE_ID             INT NULL,
        alfs_camino       VARCHAR(255) NULL,
        extension_archivo VARCHAR(128) NULL,
        tipo_archivo      VARCHAR(100) NULL,
        particion         VARCHAR(2) NULL,
        nombre_fisico     VARCHAR(260) NULL,
        renombrar_archivo CHAR(2) NULL
    );

    INSERT #AxntContratosRaw
    SELECT
        CAST(CONVERT(nvarchar(4), e.ve_fechaCreacion, 112) AS INT)   AS anho,
        CAST(CONVERT(nvarchar(6), e.ve_fechaCreacion, 112) AS INT)   AS anho_mes,
        CAST(CONVERT(nvarchar(8), e.ve_fechaCreacion,112)  AS INT)   AS anho_mes_dia,
        a.vcn_iddo,
        a.vcn_idcm                                            AS id_nombre,
        CASE WHEN a.vcn_idcm = 1517 THEN 'nro_guia'
             WHEN a.vcn_idcm = 21   THEN 'nro_cliente'
             ELSE g.cm_nombre END                              AS campo,
        TRY_CONVERT(BIGINT, a.vcn_valor)                      AS valor,
        e.ve_fechaCreacion                                    AS fecha_creacion,
        c.td_nombre                                           AS tipo_documento,
        d.co_id                                               AS identificar_documento,
        d.co_fechaCreacion,
        d.co_nombre                                           AS nombre_logico,
        e.VE_IDDO,
        e.VE_ID,
        f.alfs_camino,
        CAST(ISNULL(i.FO_EXTENSION,'') AS VARCHAR(128))       AS extension_archivo,
        i.FO_DESCRIPCION                                      AS tipo_archivo,
        RIGHT(f.alfs_camino, 2)                               AS particion,
        CONCAT(RIGHT(f.alfs_camino, 2), '\\',a.vcn_iddo, '_', e.VE_ID) AS nombre_fisico,
        'SI'                                                  AS renombrar_archivo
    FROM ODSP_AXNT_VALORCAMPONUM a
    LEFT JOIN ODSP_axnt_documento  b ON a.vcn_iddo = b.do_id
    LEFT JOIN ODSP_axnt_tipodoc    c ON a.vcn_idtd = c.td_id
    LEFT JOIN ODSP_axnt_contenido  d ON a.vcn_iddo = d.co_id
    LEFT JOIN ODSP_axnt_version    e ON a.vcn_iddo = e.ve_iddo
    LEFT JOIN ODSP_axnt_AlmacenFS  f ON e.ve_idalmacen = f.alfs_id
    LEFT JOIN ODSP_axnt_campo      g ON a.vcn_idcm = g.cm_id
    LEFT JOIN ODSP_axnt_formato    i ON b.do_idfo = i.fo_id
    WHERE e.ve_fechaCreacion >= @Fecha1
      AND c.td_id = 134
      AND a.vcn_idcm IN (21,1517)
      AND EXISTS (
            SELECT 1 FROM #ClientesLogistica p
            WHERE p.valor = TRY_CONVERT(BIGINT, CASE WHEN a.vcn_idcm=21 THEN a.vcn_valor END)
      );

    INSERT #AxntContratosRaw
    SELECT
        CAST(CONVERT(nvarchar(4), e.ve_fechaCreacion, 112) AS INT)   AS anho,
        CAST(CONVERT(nvarchar(6), e.ve_fechaCreacion, 112) AS INT)   AS anho_mes,
        CAST(CONVERT(nvarchar(8), e.ve_fechaCreacion,112)  AS INT)   AS anho_mes_dia,
        a.vcn_iddo,
        a.vcn_idcm                                            AS id_nombre,
        CASE WHEN a.vcn_idcm = 1517 THEN 'nro_guia'
             WHEN a.vcn_idcm = 21   THEN 'nro_cliente'
             ELSE g.cm_nombre END                              AS campo,
        TRY_CONVERT(BIGINT, a.vcn_valor)                      AS valor,
        e.ve_fechaCreacion                                    AS fecha_creacion,
        c.td_nombre                                           AS tipo_documento,
        d.co_id                                               AS identificar_documento,
        d.co_fechaCreacion,
        d.co_nombre                                           AS nombre_logico,
        e.VE_IDDO,
        e.VE_ID,
        f.alfs_camino,
        CAST(ISNULL(i.FO_EXTENSION,'') AS VARCHAR(128))       AS extension_archivo,
        i.FO_DESCRIPCION                                      AS tipo_archivo,
        RIGHT(f.alfs_camino, 2)                               AS particion,
        CONCAT(RIGHT(f.alfs_camino, 2), '\\',a.vcn_iddo, '_', e.VE_ID) AS nombre_fisico,
        'SI'                                                  AS renombrar_archivo
    FROM CRUDOCMDIR.ODSP.dbo.AXNT_VALORCAMPONUM_2025 a
    LEFT JOIN CRUDOCMDIR.ODSP.dbo.axnt_documento_2025  b ON a.vcn_iddo = b.do_id  AND a.fecha_cierre=b.fecha_cierre
    LEFT JOIN CRUDOCMDIR.ODSP.dbo.axnt_tipodoc_2025    c ON a.vcn_idtd = c.td_id  AND a.fecha_cierre=c.fecha_cierre
    LEFT JOIN CRUDOCMDIR.ODSP.dbo.axnt_contenido_2025  d ON a.vcn_iddo = d.co_id  AND a.fecha_cierre=d.fecha_cierre
    LEFT JOIN CRUDOCMDIR.ODSP.dbo.axnt_version_2025    e ON a.vcn_iddo = e.ve_iddo AND a.fecha_cierre=e.fecha_cierre
    LEFT JOIN CRUDOCMDIR.ODSP.dbo.axnt_AlmacenFS_2025  f ON e.ve_idalmacen = f.alfs_id AND a.fecha_cierre=f.fecha_cierre
    LEFT JOIN CRUDOCMDIR.ODSP.dbo.axnt_campo_2025      g ON a.vcn_idcm = g.cm_id  AND a.fecha_cierre=g.fecha_cierre
    LEFT JOIN CRUDOCMDIR.ODSP.dbo.axnt_formato_2025    i ON b.do_idfo = i.fo_id   AND a.fecha_cierre=i.fecha_cierre
    WHERE e.ve_fechaCreacion >= @Fecha1
      AND c.td_id = 134
      AND a.vcn_idcm IN (21,1517)
      AND EXISTS (
            SELECT 1 FROM #ClientesLogistica p
            WHERE p.valor = TRY_CONVERT(BIGINT, CASE WHEN a.vcn_idcm=21 THEN a.vcn_valor END)
      );

    CREATE INDEX IX_#AxntContratosRaw_iddo ON #AxntContratosRaw(vcn_iddo);

    /* Pivot de contratos */
    IF OBJECT_ID('tempdb..#AxntContratosPivot') IS NOT NULL DROP TABLE #AxntContratosPivot;
    SELECT *
    INTO   #AxntContratosPivot
    FROM (
        SELECT anho, anho_mes, anho_mes_dia, fecha_creacion, vcn_iddo, VE_ID,
               nombre_fisico, nombre_logico, tipo_documento, renombrar_archivo,
               campo, valor
        FROM #AxntContratosRaw
    ) s
    PIVOT (
        MAX(valor) FOR campo IN ([nro_guia],[nro_cliente])
    ) p;

    /* Dedupe contratos: 1 por (nro_cliente, día) más reciente */
    IF OBJECT_ID('tempdb..#AxntContratosFinal') IS NOT NULL DROP TABLE #AxntContratosFinal;
    SELECT anho, anho_mes, anho_mes_dia, fecha_creacion,
           vcn_iddo, VE_ID, nombre_fisico, nombre_logico,
           tipo_documento, renombrar_archivo, nro_guia,
           CAST(nro_cliente AS BIGINT) AS nro_cliente
    INTO   #AxntContratosFinal
    FROM (
        SELECT *,
               ROW_NUMBER() OVER (PARTITION BY CAST(nro_cliente AS BIGINT), CAST(fecha_creacion AS DATE)
                                  ORDER BY fecha_creacion DESC) AS rn
        FROM #AxntContratosPivot
    ) q
    WHERE rn = 1
      AND nro_cliente IS NOT NULL;

    CREATE INDEX IX_#AxntContratosFinal_nro_cliente ON #AxntContratosFinal(nro_cliente);

    /* Set de clientes con contrato */
    IF OBJECT_ID('tempdb..#ClientesConContrato') IS NOT NULL DROP TABLE #ClientesConContrato;
    SELECT DISTINCT nro_cliente AS valor
    INTO   #ClientesConContrato
    FROM   #AxntContratosFinal;

    /* =========================================================
       PASO 4: CÉDULAS (1 por cliente) limitadas a clientes con contrato
       ========================================================= */
    IF OBJECT_ID('tempdb..#CIsFinal') IS NOT NULL DROP TABLE #CIsFinal;
    CREATE TABLE #CIsFinal
    (
        anho INT, anho_mes INT, anho_mes_dia INT,
        vcn_iddo INT NULL, VE_ID INT NULL,
        nombre_fisico VARCHAR(260), nombre_logico VARCHAR(255),
        tipo_documento VARCHAR(100), renombrar_archivo CHAR(2),
        nro_guia VARCHAR(50) NULL, nro_cliente BIGINT, fecha_creacion DATETIME2(0)
    );

    /* 4.1) Desde LS_ULTIMO_CI_AXNT_FULL */
    ;WITH CIsLS AS (
        SELECT
            ROW_NUMBER() OVER (PARTITION BY TRY_CONVERT(BIGINT, b.valor)
                               ORDER BY b.fecha_creacion DESC) rn,
            TRY_CONVERT(INT, LEFT(b.anho_mes_dia, 4))  AS anho,
            TRY_CONVERT(INT, LEFT(b.anho_mes_dia, 6))  AS anho_mes,
            TRY_CONVERT(INT, b.anho_mes_dia)           AS anho_mes_dia,
            NULL AS vcn_iddo,
            NULL AS VE_ID,
            b.nombre_fisico,
            b.nombre_logico,
            'CEDULA DE IDENTIDAD' AS tipo_documento,
            'SI' AS renombrar_archivo,
            NULL AS nro_guia,
            TRY_CONVERT(BIGINT, b.valor) AS nro_cliente,
            b.fecha_creacion
        FROM dbo.LS_ULTIMO_CI_AXNT_FULL b
        WHERE TRY_CONVERT(BIGINT, b.valor) IN (SELECT valor FROM #ClientesConContrato)
    )
    INSERT #CIsFinal
    SELECT anho, anho_mes, anho_mes_dia, vcn_iddo, VE_ID, nombre_fisico, nombre_logico,
           tipo_documento, renombrar_archivo, nro_guia, nro_cliente, fecha_creacion
    FROM CIsLS
    WHERE rn = 1;

    /* 4.2) Fallback en Axentria para los que no aparecieron en LS */
    ;WITH faltantes AS (
        SELECT valor FROM #ClientesConContrato c
        WHERE NOT EXISTS (SELECT 1 FROM #CIsFinal f WHERE f.nro_cliente = c.valor)
    ), CIsAX AS (
        SELECT *,
               ROW_NUMBER() OVER (PARTITION BY TRY_CONVERT(BIGINT, a.vcn_valor)
                                  ORDER BY e.ve_fechaCreacion DESC, e.VE_ID DESC) rn
        FROM ODSP_AXNT_VALORCAMPONUM a
        LEFT JOIN ODSP_axnt_tipodoc   c ON a.vcn_idtd = c.td_id
        LEFT JOIN ODSP_axnt_contenido d ON a.vcn_iddo = d.co_id
        LEFT JOIN ODSP_axnt_version   e ON a.vcn_iddo = e.ve_iddo
        LEFT JOIN ODSP_axnt_AlmacenFS f ON e.ve_idalmacen = f.alfs_id
        WHERE c.td_id = 104
          AND a.vcn_idcm = 21
          AND TRY_CONVERT(BIGINT, a.vcn_valor) IN (SELECT valor FROM faltantes)
    )
    INSERT #CIsFinal
    SELECT
        CAST(CONVERT(nvarchar(4),  e.ve_fechaCreacion, 112) AS INT) AS anho,
        CAST(CONVERT(nvarchar(6),  e.ve_fechaCreacion, 112) AS INT) AS anho_mes,
        CAST(CONVERT(nvarchar(8),  e.ve_fechaCreacion,112)  AS INT) AS anho_mes_dia,
        a.vcn_iddo,
        e.VE_ID,
        CONCAT(RIGHT(f.alfs_camino, 2), '\\',a.vcn_iddo, '_', e.VE_ID) AS nombre_fisico,
        d.co_nombre as nombre_logico,
        'CEDULA DE IDENTIDAD' AS tipo_documento,
        'SI' as renombrar_archivo,
        NULL as nro_guia,
        TRY_CONVERT(BIGINT, a.vcn_valor) as nro_cliente,
        e.ve_fechaCreacion as fecha_creacion
    FROM CIsAX a
    JOIN ODSP_axnt_version   e ON a.vcn_iddo = e.ve_iddo
    JOIN ODSP_axnt_contenido d ON a.vcn_iddo = d.co_id
    JOIN ODSP_axnt_AlmacenFS f ON e.ve_idalmacen = f.alfs_id
    WHERE a.rn = 1;

    /* =========================================================
       PASO 5: COMBINADO y final join con set logístico (semi-join)
       ========================================================= */
    IF OBJECT_ID('tempdb..#Combinado') IS NOT NULL DROP TABLE #Combinado;
    SELECT anho, anho_mes, anho_mes_dia, fecha_creacion, vcn_iddo, VE_ID,
           nombre_fisico, nombre_logico, tipo_documento, renombrar_archivo,
           nro_guia, CAST(nro_cliente AS BIGINT) AS nro_cliente
    INTO   #Combinado
    FROM (
        SELECT * FROM #AxntContratosFinal
        UNION ALL
        SELECT * FROM #CIsFinal
    ) t;

    /* Opcional: borrar CIs de clientes sin ningún otro documento */
    IF (@EliminarCISoloSinContrato = 1)
    BEGIN
        DELETE ci
        FROM #Combinado AS ci
        WHERE ci.tipo_documento = 'CEDULA DE IDENTIDAD'
          AND NOT EXISTS (
                SELECT 1
                FROM #Combinado x
                WHERE x.nro_cliente = ci.nro_cliente
                  AND x.tipo_documento <> 'CEDULA DE IDENTIDAD'
          );
    END

    /* Final: solo clientes del set logístico (evita OR en join) */
    IF OBJECT_ID('dbo.crm_legit_axnt_base_modelo_OPT') IS NOT NULL DROP TABLE dbo.crm_legit_axnt_base_modelo_OPT;
    SELECT DISTINCT
        uc.anho, uc.anho_mes, uc.anho_mes_dia, uc.fecha_creacion, uc.vcn_iddo,
        uc.VE_ID, uc.nombre_fisico, uc.nombre_logico, uc.tipo_documento,
        uc.renombrar_archivo, uc.nro_guia, uc.nro_cliente
    INTO dbo.crm_legit_axnt_base_modelo_OPT
    FROM #Combinado AS uc
    WHERE EXISTS (SELECT 1 FROM #ClientesLogistica c WHERE c.valor = uc.nro_cliente);

    /* Índice útil para la dtsx/consumo */
    CREATE INDEX IX_crm_legit_axnt_base_modelo_OPT_cliente ON dbo.crm_legit_axnt_base_modelo_OPT(nro_cliente, tipo_documento);

    /* Limpieza explícita (opcional, #temp se descarta al finalizar el scope) */
    -- DROP TABLE IF EXISTS ...
END
GO

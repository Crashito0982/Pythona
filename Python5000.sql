/* ======== FASE 1: LOGÍSTICA OPTIMIZADA ======== */
-- Recomendado al inicio del SP:
-- SET XACT_ABORT ON; 
-- SET NOCOUNT ON; 
-- SET ARITHABORT ON; 
-- SET ANSI_WARNINGS ON;

-- 1) Último evento ENTREGADO por DOCID (sin SQL dinámico)
IF OBJECT_ID('tempdb..#LS_EVENTO_ENTREGADO') IS NOT NULL DROP TABLE #LS_EVENTO_ENTREGADO;
CREATE TABLE #LS_EVENTO_ENTREGADO
(
    docid  varchar(20) NOT NULL,
    evtsc  int         NOT NULL,
    CONSTRAINT PK_#LS_EVENTO_ENTREGADO PRIMARY KEY (docid)
);

INSERT #LS_EVENTO_ENTREGADO (docid, evtsc)
SELECT a.docid, MAX(a.evtsc)
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

-- 2) Base de eventos (solo la fila de esa última secuencia)
IF OBJECT_ID('tempdb..#EventosEntBase') IS NOT NULL DROP TABLE #EventosEntBase;
CREATE TABLE #EventosEntBase
(
    docid  varchar(20) NOT NULL,
    evtsc  int         NOT NULL,
    CONSTRAINT PK_#EventosEntBase PRIMARY KEY (docid, evtsc)
);

INSERT #EventosEntBase(docid, evtsc)
SELECT a.docid, a.evtsc
FROM ODSP_LGLIB_LGMEVT AS a
JOIN #LS_EVENTO_ENTREGADO AS b
  ON b.docid = a.docid AND b.evtsc = a.evtsc
WHERE a.evtfch >= @AMD_inicio
  AND a.evttdi = 1;

-- 3) Enriquecimiento con LGMDOC (ventana por fecha_act)
IF OBJECT_ID('tempdb..#EventosLogistica') IS NOT NULL DROP TABLE #EventosLogistica;
CREATE TABLE #EventosLogistica
(
    descrip_doc              varchar(200) NULL,
    docid                    varchar(20)  NOT NULL,
    nro_guia                 varchar(50)  NULL,
    nombre_cliente           varchar(200) NULL,
    nro_cliente              bigint       NULL,
    tipo_documento_logistica varchar(50)  NULL,
    nro_documento            varchar(50)  NULL,
    estado_cabecera          varchar(10)  NULL,
    fecha_act                int          NULL,   -- yyyymmdd en LGMDOC
    id_doc_cod_barra         varchar(100) NULL,
    tipo_tarjeta             varchar(10)  NULL,
    cod_tipo_tarjeta         varchar(10)  NULL,
    tipo_producto            varchar(50)  NULL,
    tipo_producto_det        int          NULL,
    ciudad_cliente           varchar(100) NULL,
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
    CAST(doc.DOCID AS varchar(20))                           AS docid,
    doc.DOCGU                                                AS nro_guia,
    doc.docnm                                                AS nombre_cliente,
    TRY_CONVERT(bigint, doc.DOCCTI)                          AS nro_cliente,
    doc.DOCTDC                                               AS tipo_documento_logistica,
    doc.DOCTDN                                               AS nro_documento,
    doc.DOCES                                                AS estado_cabecera,
    doc.docedf                                               AS fecha_act,       -- yyyymmdd (112)
    doc.doccb                                                AS id_doc_cod_barra,
    doc.docerm                                               AS tipo_tarjeta,
    doc.docori                                               AS cod_tipo_tarjeta,
    doc.docpri                                               AS tipo_producto,
    tdc.TDCID                                                AS tipo_producto_det,
    doc.doccd                                                AS ciudad_cliente
FROM ODSP_LGLIB_LGMDOC AS doc
JOIN #EventosEntBase      AS eb  ON eb.docid = doc.DOCID
LEFT JOIN ODSP_LGLIB_LGMTDC AS tdc 
       ON doc.DOCPRI = tdc.tdcpri AND doc.DOCTDI = tdc.TDCID
WHERE doc.docedf >= @AMD_inicio
  AND (
        (doc.TDCPRI = 'TARJ.C' AND tdc.TDCID IN (1,36,37,70,89,116,132))
        OR (doc.TDCPRI = '105' AND tdc.TDCID = 89)
      );

-- Índices que ayudan a lo que viene
CREATE INDEX IX_#EventosLogistica_nro_cliente ON #EventosLogistica(nro_cliente);
CREATE INDEX IX_#EventosLogistica_docid       ON #EventosLogistica(docid);

-- 4) ADI/titulares mínimos (lo mantenemos en #temp)
IF OBJECT_ID('tempdb..#ADI2') IS NOT NULL DROP TABLE #ADI2;
CREATE TABLE #ADI2
(
    NRO_TJT               varchar(50) NULL,
    TIPO_TC               char(1)     NULL,
    NRO_CLIENTE_ADICIONAL bigint      NULL,
    NRO_CLIENTE_TITULAR   bigint      NULL,
    ID_LOGISTICA          varchar(20) NOT NULL,
    NUMERO_CLIENTE        bigint      NULL,
    NUMERO_DOCUMENTO      varchar(50) NULL,
    CONSTRAINT PK_#ADI2 PRIMARY KEY (ID_LOGISTICA)
);

INSERT #ADI2
SELECT
    SUBSTRING(a.NUTARJET, 1, LEN(a.NUTARJET) - 3) AS NRO_TJT,
    a.TITARJET                                     AS TIPO_TC,
    TRY_CONVERT(bigint, a.CONUMECL)               AS NRO_CLIENTE_ADICIONAL,
    TRY_CONVERT(bigint, a.CONUCLTI)               AS NRO_CLIENTE_TITULAR,
    CAST(b.DOCID AS varchar(20))                  AS ID_LOGISTICA,
    TRY_CONVERT(bigint, b.DOCCTI)                 AS NUMERO_CLIENTE,
    b.DOCTDN                                       AS NUMERO_DOCUMENTO
FROM CRUDOCMDIR.ODSP.dbo.V_TARJETA_PBTARJETA AS a
JOIN ODSP_LGLIB_LGMDOC AS b
  ON SUBSTRING(a.NUTARJET, 1, LEN(a.NUTARJET) - 3) = b.DOCNRD COLLATE SQL_Latin1_General_CP1_CI_AI
WHERE b.DOCID IS NOT NULL
  AND b.DOCNRD <> ''
  AND b.doccti <> '0.0'
  AND b.doccti IS NOT NULL;

-- 5) Logística + titular
IF OBJECT_ID('tempdb..#EventosLogisticaNew') IS NOT NULL DROP TABLE #EventosLogisticaNew;
SELECT el.*, adi.NRO_CLIENTE_TITULAR
INTO   #EventosLogisticaNew
FROM   #EventosLogistica el
JOIN   #ADI2            adi ON adi.ID_LOGISTICA = el.docid;

CREATE INDEX IX_#EventosLogisticaNew_nro_cliente        ON #EventosLogisticaNew(nro_cliente);
CREATE INDEX IX_#EventosLogisticaNew_nro_cliente_titular ON #EventosLogisticaNew(NRO_CLIENTE_TITULAR);

-- 6) SET de clientes a activar
IF OBJECT_ID('tempdb..#ClientesParaActivar') IS NOT NULL DROP TABLE #ClientesParaActivar;
CREATE TABLE #ClientesParaActivar (valor bigint NOT NULL PRIMARY KEY);

INSERT #ClientesParaActivar(valor)
SELECT DISTINCT nro_cliente
FROM #EventosLogisticaNew
WHERE nro_cliente IS NOT NULL;

INSERT #ClientesParaActivar(valor)
SELECT DISTINCT NRO_CLIENTE_TITULAR
FROM #EventosLogisticaNew
WHERE NRO_CLIENTE_TITULAR IS NOT NULL
  AND NOT EXISTS (SELECT 1 FROM #ClientesParaActivar x WHERE x.valor = #EventosLogisticaNew.NRO_CLIENTE_TITULAR);

-- === Compatibilidad temporal con el resto del SP ===
-- (En la próxima fase, cambiamos el resto del código para leer directamente de #temp y eliminamos estas físicas)
IF OBJECT_ID('dbo.ls_eventos_entregados_temp_new_PRUEBA') IS NOT NULL DROP TABLE dbo.ls_eventos_entregados_temp_new_PRUEBA;
SELECT * INTO dbo.ls_eventos_entregados_temp_new_PRUEBA FROM #EventosLogisticaNew;

IF OBJECT_ID('dbo.ClientesParaActivar_PRUEBA') IS NOT NULL DROP TABLE dbo.ClientesParaActivar_PRUEBA;
SELECT * INTO dbo.ClientesParaActivar_PRUEBA FROM #ClientesParaActivar;

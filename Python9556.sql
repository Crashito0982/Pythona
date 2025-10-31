USE AT_CMDTS
GO

SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

/*
================================================================================
SP: CRM_LEGIT_FIRMAS_PRUEBA_001 (versión comentada)
---------------------------------------------------------------------------------
Propósito general
- Armar una base final con documentos de AXENTRIA (CONTRATOS y CÉDULAS) para
  clientes que tienen eventos de logística recientes (entregados) y que están
  pendientes de activación (sin eventos de activación/destruido/firma varía).
- Mantener la **metodología** del código original, pero con algunos ajustes
  puntuales de robustez/legibilidad (p. ej., TRY_CONVERT a BIGINT donde aplica),
  y con comentarios que expliquen cada bloque.

Resumen del flujo
1) Parámetros de fecha y lógica de mes.
2) Eventos de logística (últimos EVTTDI=1, excluyendo activados/destruidos/
   firma varía). Se enriquece con datos de documento.
3) Cruce con tablas “ADI” para obtener el titular cuando exista.
4) Lista de clientes únicos a activar (nro_cliente y/o titular).
5) AXENTRIA – CONTRATOS: filtra por clientes objetivo (pushdown), pivotea y
   deduplica por cliente/día.
6) AXENTRIA – CÉDULAS:
   - Primero intenta tomar la última cédula desde LS_ULTIMO_CI_AXNT_FULL,
     *únicamente* para clientes con contrato (criterio de negocio pedido).
   - Si falta, busca en AXENTRIA (td_id=104) y toma la más reciente.
7) Une CONTRATOS + CÉDULAS y finalmente reduce a aquellos clientes que
   efectivamente aparecen en los eventos de logística (EXISTS semijoin).
8) Limpieza de temporales.

Notas sobre performance (no funcionales, pero útiles):
- TRY_CONVERT(BIGINT, ...): evita fallas por valores no numéricos en campos
  que deberían ser números (DOCCTI / vcn_valor idcm=21). Esto reduce abortos
  de ejecución sin escanear toda la tabla.
- Subquery IN con v.vcn_idcm = 21 en contratos: empuja la selectividad hacia
  AXNT_VALORCAMPONUM y reduce el set de vcn_iddo a pivotear.
- EXISTS en el join final evita multiplicar filas por lado logística.
- Se recomienda (fuera de este SP) evaluar índices en columnas filtradas:
  ODSP_AXNT_VALORCAMPONUM(vcn_idcm, vcn_valor), ODSP_axnt_version(ve_iddo,
  ve_fechaCreacion), ODSP_LGLIB_LGMEVT(evttdi, evtfch, docid),
  ODSP_LGLIB_LGMDOC(docedf, docid, doccti).
================================================================================
*/

ALTER PROCEDURE [dbo].[CRM_LEGIT_FIRMAS_PRUEBA_001]
AS
    -- =============================
    -- Declaración de variables
    -- =============================
    DECLARE @DIAS_ATRAS numeric(3);                 -- Ventana principal para filtrar eventos/documents (días hacia atrás)
    DECLARE @DIAS_ATRAS_EVENTOS_OUT numeric(3);     -- Ventana extendida para prefiltrar eventos entregados "válidos"
    DECLARE @AMD_inicio varchar(8);                 -- YYYYMMDD inicio ventana
    DECLARE @AMD_EVENTOS_OUT varchar(8);            -- YYYYMMDD inicio ventana eventos out
    DECLARE @AMD_fin_mes varchar(8);                -- YYYYMMDD fin de mes de la ventana
    DECLARE @AMD01_base varchar(8);                 -- (no usado, conservado por compatibilidad)
    DECLARE @Fecha1 DATE;                           -- Fecha inicio como DATE (usada con AXENTRIA)
    DECLARE @ANHO varchar(4);
    DECLARE @mes_actual varchar(2);
    DECLARE @mes_inicio varchar(2);
    DECLARE @sql nvarchar(4000), @sql_0 nvarchar(4000), @sql_1 nvarchar(4000), @sql_2 nvarchar(4000);
    DECLARE @ANHO_MES_ACTUAL varchar(6);

BEGIN
    SET NOCOUNT ON;

    -- ======================================================
    -- PASO 0: Seteo de fechas y parámetros de contexto
    -- ======================================================
    set @DIAS_ATRAS = -15;                 -- Ventana principal (15 días hacia atrás)
    SET @DIAS_ATRAS_EVENTOS_OUT = -30;     -- Ventana extendida (30 días) para filtrar eventos entregados candidatos

    -- Fechas en formatos usados por fuentes (YYYYMMDD) y DATE
    set @AMD_EVENTOS_OUT = cast(CONVERT(varchar, CONVERT(date, DATEADD(day, @DIAS_ATRAS_EVENTOS_OUT, GETDATE())), 112) as varchar(8));
    set @AMD_inicio      = cast(CONVERT(varchar, CONVERT(date, DATEADD(day, @DIAS_ATRAS,        GETDATE())), 112) as varchar(8));
    set @AMD_fin_mes     = CONVERT(varchar, EOMONTH(CONVERT(date, DATEADD(day, @DIAS_ATRAS, GETDATE())),0), 112);
    set @ANHO_MES_ACTUAL = CONVERT(varchar, (YEAR(EOMONTH(GETDATE(), 0))*100+MONTH(EOMONTH(GETDATE(), 0))));
    set @mes_actual      = cast(CONVERT(varchar, MONTH(CONVERT(date, GETDATE())), 112) as varchar(2));
    set @mes_inicio      = cast(CONVERT(varchar, MONTH(CONVERT(date, DATEADD(day, @DIAS_ATRAS, GETDATE()))), 112) as varchar(2));
    SET @Fecha1          = CONVERT(date, DATEADD(day, @DIAS_ATRAS, GETDATE()));

    -- Compatibilidad con lógica original (año según cruce de mes)
    IF @mes_inicio = '12' 
        BEGIN
            SET @ANHO = CONVERT(varchar, (YEAR (GETDATE()-367)))
        END
    IF @mes_inicio != '12' 
        BEGIN
            SET @ANHO = CONVERT(varchar, (YEAR (GETDATE())))
        END;

    -- ======================================================
    -- PASO 1: Lógica de Logística (Eventos de entrega)
    -- ======================================================
    -- Objetivo: obtener, por DOCID, el último EVTTDI=1 (entregado) de los
    -- últimos 30 días (@AMD_EVENTOS_OUT), excluyendo aquellos que ya tengan
    -- eventos finales (activación 16, destruido 19, firma varía 70) con EVTED != 'T'.
    IF @mes_actual >= @mes_inicio
    BEGIN
        -- 1.1) Pre-filtrado por DOCID con último EVTSC de evento entregado "válido"
        DROP TABLE IF EXISTS [LS_EVENTO_ENTREGADO_TMP_PRUEBA];
        SET @SQL_0 = '
          select a.docid, max(a.evtsc) AS evtsc
          into [LS_EVENTO_ENTREGADO_TMP_PRUEBA]
          from [ODSP_LGLIB_LGMEVT] a
          where a.evttdi=1
            and a.evtfch >= '''+@AMD_EVENTOS_OUT+'''
            and a.docid not in (
                select distinct DOCID
                from [ODSP_LGLIB_LGMEVT]
                where evted!=''T'' and EVTTDI in (16, 19, 70)  -- activación/destruido/firma varía
            )
          group by a.DOCID';
        EXEC sp_executesql @SQL_0;

        -- 1.2) Traer las filas de eventos coincidentes (ya con EVTSC máximo x DOCID)
        DROP TABLE IF EXISTS [ag_eventos_entregados_base_temp_PRUEBA];
        set @sql_1 = 'select a.DOCID, a.EVTSC, a.EVTIID, a.EVTLTI, a.EVTTDI, a.EVTFCH, a.EVTFCA,
                             a.EVTES, a.EVTESD, a.EVTED, a.EVMC, a.EVTM I, a.EVTHOR, a.EVTOBS, a.EVTDRE,
                             a.EVTTRE, a.EVTCRE, a.EVTPRE, a.EVTZON, a.EVTMID, a.EVTMDSC, a.FECHA_CIERRE, a.INGRESADO
                      into [ag_eventos_entregados_base_temp_PRUEBA]
                      from [ODSP_LGLIB_LGMEVT] a
                      inner join [LS_EVENTO_ENTREGADO_TMP_PRUEBA] b on b.docid=a.docid
                      where a.evtfch >= '''+@AMD_inicio+'''  -- 15 días para la etapa detallada
                        and a.evttdi=1
                        and b.evtsc=a.evtsc';
        EXEC sp_executesql @sql_1;

        -- 1.3) Enriquecimiento con cabecera de documento (cliente, guía, etc.)
        --      NOTA: TRY_CONVERT(BIGINT, DOCCTI) normaliza nro_cliente posible texto
        DROP TABLE IF EXISTS [ag_eventos_entregados_temp_PRUEBA];
        set @sql_2 = 'select
                        b.TDCDS as descrip_doc,
                        cast(a.DOCID as varchar(20)) as docid,
                        a.DOCGU as nro_guia,
                        a.docnm as nombre_cliente,
                        TRY_CONVERT(BIGINT, a.DOCCTI) as nro_cliente,
                        a.DOCTDC AS tipo_documento_logistica,
                        a.DOCTDN AS nro_documento,
                        a.DOCES as estado_cabecera,
                        a.docedf as fecha_act,
                        a.doccb as id_doc_cod_barra,
                        a.docerm as tipo_tarjeta,
                        a.docori as cod_tipo_tarjeta,
                        a.docpri as tipo_producto,
                        b.TDCID as tipo_producto_det,
                        a.doccd as ciudad_cliente
                      into [ag_eventos_entregados_temp_PRUEBA]
                      from [ODSP_LGLIB_LGMDOC] a
                      LEFT JOIN [ODSP_LGLIB_LGMTDC] b on a.DOCPRI = b.tdcpri and a.DOCTDI = B.TDCID
                      inner join [ag_eventos_entregados_base_temp_PRUEBA] c on c.DOCID=a.DOCID
                      where a.docedf >= '''+@AMD_inicio+'''  -- 15 días para cabecera
                        and ((TDCPRI in (''TARJ.C'') and TDCID in (1,36,37,70,89,116,132))
                             or (TDCPRI = ''105'' and TDCID = 89))';
        EXEC sp_executesql @sql_2;
    END

    -- ======================================================
    -- PASO 2: Titularidad (tablas ADI)
    -- ======================================================
    -- Objetivo: obtener NRO_CLIENTE_TITULAR para DOCID cuando exista y
    -- anexarlo a la tabla de eventos (LEFT JOIN para no perder eventos).
    DROP TABLE IF EXISTS AT_CMDTS.DBO.LS_TMP_TC_ADI_PRUEBA;
    select
        a.ID_LOGISTICA, a.NUMERO_CLIENTE, a.NUMERO_DOCUMENTO, SUBSTRING(a.NRO_TJT,1,16) AS NRO_TJT,
        B.TITARJET AS 'TIPO_TC', B.CONUMECL AS 'NRO_CLIENTE_ADICIONAL',
        B.CONUCLTI AS 'NRO_CLIENTE_TITULAR', B.FERECIBO, B.FEEMBOZA
    into AT_CMDTS.DBO.LS_TMP_TC_ADI_PRUEBA
    from CORPDW_V_R_SOL_ALTA_TC A
        INNER JOIN CORPDW_TARJETA_PBTARJETA B ON B.NUTARJET=A.NRO_TJT
    where ID_LOGISTICA IS NOT NULL;

    DROP TABLE IF EXISTS AT_CMDTS.DBO.LS_TMP_TC_ADI_2_PRUEBA;
    select
        SUBSTRING(A.NUTARJET, 1, LEN(A.NUTARJET) - 3) AS NRO_TJT,
        A.TITARJET AS 'TIPO_TC', A.CONUMECL AS 'NRO_CLIENTE_ADICIONAL',
        TRY_CONVERT(BIGINT, A.CONUCLTI) AS NRO_CLIENTE_TITULAR, B.DOCID as 'ID_LOGISTICA',
        TRY_CONVERT(BIGINT, B.DOCCTI) as NUMERO_CLIENTE, B.DOCTDN as 'NUMERO_DOCUMENTO'
    into AT_CMDTS.DBO.LS_TMP_TC_ADI_2_PRUEBA
    from CRUDOCMDIR.ODSP.dbo.V_TARJETA_PBTARJETA A
    inner join ODSP_LGLIB_LGMDOC B
        ON SUBSTRING(A.NUTARJET, 1, LEN(A.NUTARJET) - 3) = B.DOCNRD COLLATE SQL_Latin1_General_CP1_CI_AI
    where B.DOCID IS NOT NULL
      and B.DOCNRD <> ''
      and B.doccti <> '0.0'
      and B.doccti IS NOT NULL;

    DROP TABLE IF EXISTS [AT_CMDTS].[dbo].[ls_eventos_entregados_temp_new_PRUEBA];
    select a.*, b.NRO_CLIENTE_TITULAR 
    into   [AT_CMDTS].[dbo].[ls_eventos_entregados_temp_new_PRUEBA]
    from   [AT_CMDTS].[dbo].[ag_eventos_entregados_temp_PRUEBA] a
    LEFT JOIN AT_CMDTS.DBO.LS_TMP_TC_ADI_2_PRUEBA b  -- LEFT para NO perder eventos
           on b.ID_LOGISTICA=a.docid;

    -- ======================================================
    -- PASO 3: Lista de clientes únicos a activar
    -- ======================================================
    -- Incluye: nro_cliente (de eventos) y NRO_CLIENTE_TITULAR (si existe).
    DROP TABLE IF EXISTS [AT_CMDTS].[dbo].[ClientesParaActivar_PRUEBA];
    SELECT DISTINCT TRY_CONVERT(BIGINT, nro_cliente) AS valor
    INTO [AT_CMDTS].[dbo].[ClientesParaActivar_PRUEBA]
    FROM [ag_eventos_entregados_temp_PRUEBA]
    WHERE TRY_CONVERT(BIGINT, nro_cliente) IS NOT NULL
    UNION 
    SELECT DISTINCT NRO_CLIENTE_TITULAR
    FROM [ls_eventos_entregados_temp_new_PRUEBA]
    WHERE NRO_CLIENTE_TITULAR IS NOT NULL; 

    -- ======================================================
    -- PASO 4: CONTRATOS desde AXENTRIA (filtrado por clientes objetivo)
    -- ======================================================
    -- 4.1) Extrae solo campos necesarios (nro_cliente y nro_guia) y pushdown
    --      por vcn_idcm=21 (nro_cliente) para los clientes a activar.
    DROP TABLE IF EXISTS [ag_axnt_contratos_origen_PRUEBA];
    select X.*
    into [ag_axnt_contratos_origen_PRUEBA]
    FROM (
        SELECT
            cast(CONVERT(nvarchar(4), e.ve_fechaCreacion, 112) as int) as anho,
            cast(CONVERT(nvarchar(6), e.ve_fechaCreacion, 112) as int) as anho_mes,
            cast(CONVERT(nvarchar(8), e.ve_fechaCreacion,112) as int) as anho_mes_dia,
            a.vcn_iddo, a.vcn_idcm as id_nombre, 
            CASE WHEN a.vcn_idcm = 1517 THEN 'nro_guia' WHEN a.vcn_idcm = 21 THEN 'nro_cliente' ELSE g.cm_nombre END as campo, 
            TRY_CONVERT(BIGINT, a.vcn_valor) as valor,       -- normaliza posible texto -> BIGINT
            e.ve_fechaCreacion as fecha_creacion, c.td_nombre as tipo_documento,
            d.co_id as identificar_documento, d.co_fechaCreacion, d.co_nombre as nombre_logico,
            e.VE_IDDO, e.VE_ID, f.alfs_camino, 
            CAST(ISNULL(i.FO_EXTENSION,'') AS VARCHAR(128)) as extension_archivo, 
            i.FO_DESCRIPCION as tipo_archivo, RIGHT(f.alfs_camino, 2) as particion,
            concat(RIGHT(f.alfs_camino, 2), '\',a.vcn_iddo, '_', e.VE_ID) AS nombre_fisico,
            'SI' as renombrar_archivo
        from [ODSP_AXNT_VALORCAMPONUM] a
            left join [ODSP_axnt_documento] b on a.vcn_iddo = b.do_id
            left join [ODSP_axnt_tipodoc] c on a.vcn_idtd = c.td_id
            left join [ODSP_axnt_contenido] d on a.vcn_iddo = d.co_id
            left join [ODSP_axnt_version] e on a.vcn_iddo = e.ve_iddo
            left join [ODSP_axnt_AlmacenFS] f on e.ve_idalmacen = f.alfs_id
            left join [ODSP_axnt_campo] g on a.vcn_idcm = g.cm_id
            left join [ODSP_axnt_formato] i on b.do_idfo = i.fo_id
        where e.ve_fechaCreacion>= @Fecha1          -- coherente con ventana del proceso
          AND c.td_id IN (134)                      -- Solo CONTRATOS
          AND a.vcn_idcm IN (21, 1517)              -- Solo nro_cliente y nro_guia
          AND a.vcn_iddo IN (
                SELECT v.vcn_iddo FROM [ODSP_AXNT_VALORCAMPONUM] v
                WHERE v.vcn_idcm = 21
                  AND TRY_CONVERT(BIGINT, v.vcn_valor) IN (SELECT valor FROM [AT_CMDTS].[dbo].[ClientesParaActivar_PRUEBA])
          )
        UNION
        SELECT
            cast(CONVERT(nvarchar(4), e.ve_fechaCreacion, 112) as int) as anho,
            cast(CONVERT(nvarchar(6), e.ve_fechaCreacion, 112) as int) as anho_mes,
            cast(CONVERT(nvarchar(8), e.ve_fechaCreacion, 112) as int) as anho_mes_dia,
            a.vcn_iddo, a.vcn_idcm as id_nombre, 
            CASE WHEN a.vcn_idcm = 1517 THEN 'nro_guia' WHEN a.vcn_idcm = 21 THEN 'nro_cliente' ELSE g.cm_nombre END as campo,
            TRY_CONVERT(BIGINT, a.vcn_valor) as valor,
            e.ve_fechaCreacion as fecha_creacion, c.td_nombre as tipo_documento,
            d.co_id as identificar_documento, d.co_fechaCreacion, d.co_nombre as nombre_logico,
            e.VE_IDDO, e.VE_ID, f.alfs_camino, 
            CAST(ISNULL(i.FO_EXTENSION,'') AS VARCHAR(128)) as extension_archivo, 
            i.FO_DESCRIPCION as tipo_archivo, RIGHT(f.alfs_camino, 2) as particion,
            concat(RIGHT(f.alfs_camino, 2), '\',a.vcn_iddo, '_', e.VE_ID) AS nombre_fisico,
            'SI' as renombrar_archivo
        from [CRUDOCMDIR].[ODSP].[dbo].[AXNT_VALORCAMPONUM_2025] a
            left join [CRUDOCMDIR].[ODSP].[dbo].[axnt_documento_2025] b on (a.vcn_iddo = b.do_id and a.fecha_cierre=b.fecha_cierre)
            left join [CRUDOCMDIR].[ODSP].[dbo].[axnt_tipodoc_2025] c on (a.vcn_idtd = c.td_id and a.fecha_cierre=c.fecha_cierre)
            left join [CRUDOCMDIR].[ODSP].[dbo].[axnt_contenido_2025] d on (a.vcn_iddo = d.co_id and a.fecha_cierre=d.fecha_cierre)
            left join [CRUDOCMDIR].[ODSP].[dbo].[axnt_version_2025] e on (a.vcn_iddo = e.ve_iddo and a.fecha_cierre=e.fecha_cierre)
            left join [CRUDOCMDIR].[ODSP].[dbo].[axnt_AlmacenFS_2025] f on (e.ve_idalmacen = f.alfs_id and a.fecha_cierre=f.fecha_cierre)
            left join [CRUDOCMDIR].[ODSP].[dbo].[axnt_campo_2025] g on (a.vcn_idcm = g.cm_id and a.fecha_cierre=g.fecha_cierre)
            left join [CRUDOCMDIR].[ODSP].[dbo].[axnt_formato_2025] i on (b.do_idfo = i.fo_id and a.fecha_cierre=i.fecha_cierre)
        where e.ve_fechaCreacion>= @Fecha1
          AND c.td_id IN (134)
          AND a.vcn_idcm IN (21, 1517)
          AND a.vcn_iddo IN (
                SELECT v.vcn_iddo FROM [CRUDOCMDIR].[ODSP].[dbo].[AXNT_VALORCAMPONUM_2025] v
                WHERE v.vcn_idcm = 21
                  AND TRY_CONVERT(BIGINT, v.vcn_valor) IN (SELECT valor FROM [AT_CMDTS].[dbo].[ClientesParaActivar_PRUEBA])
          )
    ) X;

    -- 4.2) Pivot de contratos para dejar columnas [nro_guia] y [nro_cliente]
    DROP TABLE IF EXISTS [ag_axnt_contratos_pivot_PRUEBA];
    SELECT *
    INTO [ag_axnt_contratos_pivot_PRUEBA]
    FROM (
        SELECT anho, anho_mes, anho_mes_dia, fecha_creacion, vcn_iddo, VE_ID,
               nombre_fisico, nombre_logico, tipo_documento, renombrar_archivo,
               campo, valor
        FROM [ag_axnt_contratos_origen_PRUEBA]
    ) t
    PIVOT (
        MAX(valor) FOR campo IN ([nro_guia], [nro_cliente]) 
    ) AS pivot_table;

    -- 4.3) Deduplicado de contratos: por (nro_cliente, fecha_creacion día)
    DROP TABLE IF EXISTS [ag_axnt_contratos_final_PRUEBA];
    SELECT
        anho, anho_mes, anho_mes_dia, fecha_creacion,
        vcn_iddo, VE_ID, nombre_fisico, nombre_logico,
        tipo_documento, renombrar_archivo, nro_guia, nro_cliente
    INTO [ag_axnt_contratos_final_PRUEBA]
    FROM (
        SELECT
            anho, anho_mes, anho_mes_dia, fecha_creacion,
            vcn_iddo, VE_ID, nombre_fisico, nombre_logico,
            tipo_documento, renombrar_archivo, nro_guia, CAST(nro_cliente AS BIGINT) AS nro_cliente,
            ROW_NUMBER() OVER (PARTITION BY nro_cliente, CAST(fecha_creacion AS DATE) ORDER BY fecha_creacion DESC) AS rn
        FROM [ag_axnt_contratos_pivot_PRUEBA]
    ) tmp
    WHERE rn = 1;

    -- 4.4) Clientes con contrato (usado para limitar CIs)
    DROP TABLE IF EXISTS [ClientesConContrato_PRUEBA];
    SELECT DISTINCT nro_cliente AS valor
    INTO [ClientesConContrato_PRUEBA]
    FROM [ag_axnt_contratos_final_PRUEBA]
    WHERE nro_cliente IS NOT NULL;

    -- ======================================================
    -- PASO 5: CÉDULAS (preferencia: LS_ULTIMO_CI_AXNT_FULL; fallback: AXENTRIA)
    -- ======================================================
    DROP TABLE IF EXISTS [ag_axnt_cedulas_final_PRUEBA];
    DROP TABLE IF EXISTS [ClientesEncontradosCI_PRUEBA];

    -- 5.1) Última CI por cliente desde tabla consolidada (solo clientes con contrato)
    SELECT DISTINCT TRY_CONVERT(BIGINT, valor) AS valor
    INTO [ClientesEncontradosCI_PRUEBA]
    FROM dbo.LS_ULTIMO_CI_AXNT_FULL
    WHERE TRY_CONVERT(BIGINT, valor) IS NOT NULL
      AND TRY_CONVERT(BIGINT, valor) IN (SELECT valor FROM [ClientesConContrato_PRUEBA]);

    SELECT 
        anho, anho_mes, anho_mes_dia, vcn_iddo, VE_ID, nombre_fisico, nombre_logico, 
        tipo_documento, renombrar_archivo, nro_guia, nro_cliente, fecha_creacion 
    INTO [ag_axnt_cedulas_final_PRUEBA] 
    FROM (
        SELECT
            ROW_NUMBER() OVER(PARTITION BY b.valor ORDER BY b.fecha_creacion DESC) as rn,
            CAST(LEFT(b.anho_mes_dia, 4) AS INT) AS anho,
            CAST(LEFT(b.anho_mes_dia, 6) AS INT) AS anho_mes,
            b.anho_mes_dia,
            NULL AS vcn_iddo, NULL AS VE_ID, b.nombre_fisico, b.nombre_logico,
            'CEDULA DE IDENTIDAD' AS tipo_documento, 'SI' AS renombrar_archivo,
            NULL AS nro_guia, TRY_CONVERT(BIGINT, b.valor) as nro_cliente, b.fecha_creacion 
        FROM dbo.LS_ULTIMO_CI_AXNT_FULL b
        WHERE b.valor IN (SELECT valor FROM [ClientesEncontradosCI_PRUEBA]) 
    ) AS CedulasDesdeTabla
    WHERE rn = 1; 

    -- 5.2) Fallback: si no hubo coincidencia en la tabla consolidada, buscar en AXENTRIA
    INSERT INTO [ag_axnt_cedulas_final_PRUEBA] (
        anho, anho_mes, anho_mes_dia, vcn_iddo, VE_ID, nombre_fisico, nombre_logico,
        tipo_documento, renombrar_archivo, nro_guia, nro_cliente, fecha_creacion
    )
    SELECT 
        anho, anho_mes, anho_mes_dia, vcn_iddo, VE_ID, nombre_fisico, nombre_logico,
        tipo_documento, renombrar_archivo, nro_guia, nro_cliente, fecha_creacion
    FROM (
        SELECT
            ROW_NUMBER() OVER(PARTITION BY TRY_CONVERT(BIGINT, a.vcn_valor) ORDER BY e.ve_fechaCreacion DESC, e.VE_ID DESC) as rn,
            cast(CONVERT(nvarchar(4), e.ve_fechaCreacion, 112) as int) as anho,
            cast(CONVERT(nvarchar(6), e.ve_fechaCreacion, 112) as int) as anho_mes,
            cast(CONVERT(nvarchar(8), e.ve_fechaCreacion,112) as int) as anho_mes_dia,
            a.vcn_iddo, e.VE_ID,
            concat(RIGHT(f.alfs_camino, 2), '\',a.vcn_iddo, '_', e.VE_ID) AS nombre_fisico,
            d.co_nombre as nombre_logico, c.td_nombre as tipo_documento, 'SI' as renombrar_archivo,
            NULL as nro_guia, TRY_CONVERT(BIGINT, a.vcn_valor) as nro_cliente, e.ve_fechaCreacion as fecha_creacion
        from [ODSP_AXNT_VALORCAMPONUM] a
            left join [ODSP_axnt_tipodoc] c on a.vcn_idtd = c.td_id
            left join [ODSP_axnt_contenido] d on a.vcn_iddo = d.co_id
            left join [ODSP_axnt_version] e on a.vcn_iddo = e.ve_iddo
            left join [ODSP_axnt_AlmacenFS] f on e.ve_idalmacen = f.alfs_id
        where c.td_id = 104       -- Cédula
          AND a.vcn_idcm = 21     -- Nro Cliente
          AND TRY_CONVERT(BIGINT, a.vcn_valor) IN (
                SELECT valor FROM [ClientesConContrato_PRUEBA]
                WHERE valor NOT IN (SELECT valor FROM [ClientesEncontradosCI_PRUEBA])
          )
        UNION ALL 
        SELECT
            ROW_NUMBER() OVER(PARTITION BY TRY_CONVERT(BIGINT, a.vcn_valor) ORDER BY e.ve_fechaCreacion DESC, e.VE_ID DESC) as rn,
            cast(CONVERT(nvarchar(4), e.ve_fechaCreacion, 112) as int) as anho,
            cast(CONVERT(nvarchar(6), e.ve_fechaCreacion, 112) as int) as anho_mes,
            cast(CONVERT(nvarchar(8), e.ve_fechaCreacion, 112) as int) as anho_mes_dia,
            a.vcn_iddo, e.VE_ID,
            concat(RIGHT(f.alfs_camino, 2), '\',a.vcn_iddo, '_', e.VE_ID) AS nombre_fisico,
            d.co_nombre as nombre_logico, c.td_nombre as tipo_documento, 'SI' as renombrar_archivo,
            NULL as nro_guia, TRY_CONVERT(BIGINT, a.vcn_valor) as nro_cliente, e.ve_fechaCreacion as fecha_creacion
        from [CRUDOCMDIR].[ODSP].[dbo].[AXNT_VALORCAMPONUM_2025] a
            left join [CRUDOCMDIR].[ODSP].[dbo].[axnt_tipodoc_2025] c on (a.vcn_idtd = c.td_id and a.fecha_cierre=c.fecha_cierre)
            left join [CRUDOCMDIR].[ODSP].[dbo].[axnt_contenido_2025] d on (a.vcn_iddo = d.co_id and a.fecha_cierre=d.fecha_cierre)
            left join [CRUDOCMDIR].[ODSP].[dbo].[axnt_version_2025] e on (a.vcn_iddo = e.ve_iddo and a.fecha_cierre=e.fecha_cierre)
            left join [CRUDOCMDIR].[ODSP].[dbo].[axnt_AlmacenFS_2025] f on (e.ve_idalmacen = f.alfs_id and a.fecha_cierre=f.fecha_cierre)
        where c.td_id = 104
          AND a.vcn_idcm = 21
          AND TRY_CONVERT(BIGINT, a.vcn_valor) IN (
                SELECT valor FROM [ClientesConContrato_PRUEBA]
                WHERE valor NOT IN (SELECT valor FROM [ClientesEncontradosCI_PRUEBA])
          )
    ) AS CedulasAxentria
    WHERE rn = 1;

    DROP TABLE IF EXISTS [ClientesEncontradosCI_PRUEBA];

    -- ======================================================
    -- PASO 6: Unir CONTRATOS + CÉDULAS (alineando columnas)
    -- ======================================================
    DROP TABLE IF EXISTS [ag_axnt_combinado_PRUEBA];
    SELECT 
        anho, anho_mes, anho_mes_dia, fecha_creacion, vcn_iddo, VE_ID,
        nombre_fisico, nombre_logico, tipo_documento, renombrar_archivo, nro_guia, nro_cliente
    INTO [ag_axnt_combinado_PRUEBA]
    FROM (
        SELECT 
            anho, anho_mes, anho_mes_dia, fecha_creacion, vcn_iddo, VE_ID,
            nombre_fisico, nombre_logico, tipo_documento, renombrar_archivo, nro_guia,
            CAST(nro_cliente AS BIGINT) AS nro_cliente
        FROM [ag_axnt_contratos_final_PRUEBA]
        UNION ALL
        SELECT 
            anho, anho_mes, anho_mes_dia, fecha_creacion, vcn_iddo, VE_ID,
            nombre_fisico, nombre_logico, tipo_documento, renombrar_archivo, nro_guia,
            CAST(nro_cliente AS BIGINT) AS nro_cliente
        FROM [ag_axnt_cedulas_final_PRUEBA]
    ) AS CombinedData;

    -- ======================================================
    -- PASO 7: Tabla final filtrada por presencia en logística (EXISTS)
    -- ======================================================
    DROP TABLE IF EXISTS [crm_legit_axnt_base_modelo_PRUEBA];
    SELECT DISTINCT
        uc.anho, uc.anho_mes, uc.anho_mes_dia, uc.fecha_creacion, uc.vcn_iddo,
        uc.VE_ID, uc.nombre_fisico, uc.nombre_logico, uc.tipo_documento,
        uc.renombrar_archivo, uc.nro_guia, uc.nro_cliente
    INTO [crm_legit_axnt_base_modelo_PRUEBA]
    FROM [ag_axnt_combinado_PRUEBA] uc 
    WHERE EXISTS (
        SELECT 1
        FROM [ls_eventos_entregados_temp_new_PRUEBA] b 
        WHERE b.nro_cliente = uc.nro_cliente 
           OR b.NRO_CLIENTE_TITULAR = uc.nro_cliente
    );

    -- ======================================================
    -- PASO 8: Opcional – deduplicado final por día/cliente/tipo_documento
    -- ======================================================
    /*
    DELETE T
    FROM (
        SELECT *, 
               duplicados = ROW_NUMBER() OVER (
                   PARTITION BY nro_cliente, tipo_documento, CAST(fecha_creacion AS DATE) 
                   ORDER BY fecha_creacion DESC 
               )
        FROM [crm_legit_axnt_base_modelo_PRUEBA]
    ) AS T
    WHERE duplicados > 1;
    */

    -- ======================================================
    -- PASO 9: Limpieza de temporales
    -- ======================================================
    DROP TABLE IF EXISTS [ag_eventos_entregados_base_temp_PRUEBA];
    DROP TABLE IF EXISTS [ag_eventos_entregados_temp_PRUEBA];
    DROP TABLE IF EXISTS [ag_axnt_contratos_origen_PRUEBA]; 
    DROP TABLE IF EXISTS [ag_axnt_contratos_pivot_PRUEBA]; 
    DROP TABLE IF EXISTS [ag_axnt_contratos_final_PRUEBA]; 
    DROP TABLE IF EXISTS [ag_axnt_cedulas_final_PRUEBA]; 
    DROP TABLE IF EXISTS [ag_axnt_combinado_PRUEBA]; 
    DROP TABLE IF EXISTS [LS_EVENTO_ENTREGADO_TMP_PRUEBA];
    DROP TABLE IF EXISTS [ls_eventos_entregados_temp_new_PRUEBA];
    DROP TABLE IF EXISTS AT_CMDTS.DBO.LS_TMP_TC_ADI_PRUEBA;
    DROP TABLE IF EXISTS AT_CMDTS.DBO.LS_TMP_TC_ADI_2_PRUEBA;
    DROP TABLE IF EXISTS [AT_CMDTS].[dbo].[ClientesParaActivar_PRUEBA];
    DROP TABLE IF EXISTS [ClientesConContrato_PRUEBA];
 
END
GO

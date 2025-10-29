USE AT_CMDTS
GO

SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

ALTER PROCEDURE [dbo].[CRM_LEGIT_FIRMAS_PRUEBA_001]
AS
    -- Declaramos las variables a utilizar
    DECLARE @DIAS_ATRAS numeric(3);
    DECLARE @DIAS_ATRAS_EVENTOS_OUT numeric(3);
    DECLARE @AMD_inicio varchar(8);
    DECLARE @AMD_EVENTOS_OUT varchar(8);
    DECLARE @AMD_fin_mes varchar(8);
    DECLARE @AMD01_base varchar(8); 
    DECLARE @Fecha1 DATE; 
    DECLARE @ANHO varchar(4);
    DECLARE @mes_actual varchar(2);
    DECLARE @mes_inicio varchar(2);
    DECLARE @sql nvarchar(4000);
    DECLARE @sql_0 nvarchar(4000);
    DECLARE @sql_1 nvarchar(4000);
    DECLARE @sql_2 nvarchar(4000);
    DECLARE @ANHO_MES_ACTUAL varchar(6);

BEGIN
    SET NOCOUNT ON;

    -- PASO 0: Seteo de Variables
    set @DIAS_ATRAS = -15;
    SET @DIAS_ATRAS_EVENTOS_OUT = -30;
    set @AMD_EVENTOS_OUT = cast (CONVERT(varchar, convert(date, dateadd(day, @DIAS_ATRAS_EVENTOS_OUT, GETDATE())), 112) as varchar(8)); 
    set @AMD_inicio = cast(CONVERT(varchar, convert(date, dateadd(day, @DIAS_ATRAS, GETDATE())), 112) as varchar(8)); 
    set @AMD_fin_mes = CONVERT(varchar, eomonth (convert(date, dateadd(day, @DIAS_ATRAS, GETDATE())),0), 112);
    set @ANHO_MES_ACTUAL = convert(varchar, (year (eomonth(GETDATE(), 0))*100+month (eomonth(GETDATE(), 0))));
    set @mes_actual = cast(CONVERT(varchar, month (convert(date, GETDATE())), 112) as varchar(2)); 
    set @mes_inicio = cast(CONVERT(varchar, month (convert(date, dateadd(day, @DIAS_ATRAS, GETDATE()))), 112) as varchar(2)); 
    SET @Fecha1 = convert(date, dateadd(day, @DIAS_ATRAS, GETDATE()));
    
    -- Lógica de meses (sin cambios)
    IF @mes_inicio = '12' 
        BEGIN
            SET @ANHO = convert(varchar, (YEAR (GETDATE()-367)))
        END
    IF @mes_inicio != '12' 
        BEGIN
            SET @ANHO = convert(varchar, (YEAR (GETDATE())))
        END;

    -- PASO 1: Lógica de Logística (Eventos)
    IF @mes_actual >= @mes_inicio
    BEGIN
        -- 1.1: Buscar eventos pendientes
        DROP TABLE IF EXISTS [LS_EVENTO_ENTREGADO_TMP_PRUEBA];
        SET @SQL_0 = '
          select
              a.docid,
              max(a.evtsc) AS evtsc
          into [LS_EVENTO_ENTREGADO_TMP_PRUEBA]
          from [ODSP_LGLIB_LGMEVT] a
          where 1=1
              and a.evttdi=1
              and a.evtfch >= '''+@AMD_EVENTOS_OUT+'''
              and a.docid not in (
                  select distinct DOCID
                  from [ODSP_LGLIB_LGMEVT]
                  where 1=1
                      and evted!=''T''
                      and EVTTDI in (16, 19, 70)
              )
              group by a.DOCID'; 
        EXEC sp_executesql @SQL_0;

        -- 1.2: Obtener detalles del evento
        DROP TABLE IF EXISTS [ag_eventos_entregados_base_temp_PRUEBA];
        set @sql_1 = 'select a.DOCID, a.EVTSC, a.EVTIID, a.EVTLTI, a.EVTTDI, a.EVTFCH, a.EVTFCA,
                        a.EVTES, a.EVTESD, a.EVTED, a.EVTMC, a.EVTMI, a.EVTHOR, a.EVTOBS, a.EVTDRE,
                        a.EVTTRE, a.EVTCRE, a.EVTPRE, a.EVTZON, a.EVTMID, a.EVTMDSC, a.FECHA_CIERRE, a.INGRESADO
                      into [ag_eventos_entregados_base_temp_PRUEBA]
                      from [ODSP_LGLIB_LGMEVT] a
                      inner join [LS_EVENTO_ENTREGADO_TMP_PRUEBA] b on b.docid=a.docid
                      where a.evtfch >= '''+@AMD_inicio+'''
                      and b.docid=a.docid
                      and a.evttdi=1
                      and b.evtsc=a.evtsc'; 
        EXEC sp_executesql @sql_1;

        -- 1.3: Enriquecer con datos del documento
        DROP TABLE IF EXISTS [ag_eventos_entregados_temp_PRUEBA];
        set @sql_2 = 'select
                        b.TDCDS as descrip_doc
                        ,cast(a.DOCID as varchar(20)) as docid
                        ,a.DOCGU as nro_guia
                        ,a.docnm as nombre_cliente
                        ,cast(a.DOCCTI as INT) as nro_cliente
                        ,a.DOCTDC AS tipo_documento_logistica 
                        ,a.DOCTDN AS nro_documento
                        ,a.DOCES as estado_cabecera
                        ,a.docedf as fecha_act
                        ,a.doccb as id_doc_cod_barra
                        ,a.docerm as tipo_tarjeta
                        ,a.docori as cod_tipo_tarjeta
                        ,a.docpri as tipo_producto
                        ,b.TDCID as tipo_producto_det
                        ,a.doccd as ciudad_cliente
                      into [ag_eventos_entregados_temp_PRUEBA]
                      from [ODSP_LGLIB_LGMDOC] a
                      LEFT JOIN [ODSP_LGLIB_LGMTDC] b on a.DOCPRI = b.tdcpri and a.DOCTDI = B.TDCID
                      inner join [ag_eventos_entregados_base_temp_PRUEBA] c on c.DOCID=a.DOCID
                      where a.docedf >= '''+@AMD_inicio+''' and
                      ((TDCPRI in (''TARJ.C'') and TDCID in (1,36,37,70,89,116,132))
                      or (TDCPRI = ''105'' and TDCID = 89))'; 
        EXEC sp_executesql @sql_2;
    END
    ELSE
    BEGIN
        -- Si no hay eventos en el rango, crear una tabla vacía con el MISMO ESQUEMA esperado
        IF OBJECT_ID('[ag_eventos_entregados_temp_PRUEBA]') IS NOT NULL DROP TABLE [ag_eventos_entregados_temp_PRUEBA];
        SELECT TOP (0)
            b.TDCDS as descrip_doc,
            cast(a.DOCID as varchar(20)) as docid,
            a.DOCGU as nro_guia,
            a.docnm as nombre_cliente,
            cast(a.DOCCTI as INT) as nro_cliente,
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
        INTO [ag_eventos_entregados_temp_PRUEBA]
        FROM [ODSP_LGLIB_LGMDOC] a
        LEFT JOIN [ODSP_LGLIB_LGMTDC] b ON 1=0
        WHERE 1=0;
    END;

    -- PASO 2: Lógica de Clientes Titulares (Tablas ADI)
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
        A.CONUCLTI AS 'NRO_CLIENTE_TITULAR', B.DOCID as 'ID_LOGISTICA',
        B.DOCCTI as 'NUMERO_CLIENTE', B.DOCTDN as 'NUMERO_DOCUMENTO'
    into AT_CMDTS.DBO.LS_TMP_TC_ADI_2_PRUEBA
    from CRUDOCMDIR.ODSP.dbo.V_TARJETA_PBTARJETA A
    inner join ODSP_LGLIB_LGMDOC B
        ON SUBSTRING(A.NUTARJET, 1, LEN(A.NUTARJET) - 3) = B.DOCNRD COLLATE SQL_Latin1_General_CP1_CI_AI
    where 1=1
        and B.DOCID IS NOT NULL
        and B.DOCNRD <> ''
        and B.doccti <> '0.0'
        and B.doccti IS NOT NULL;

    DROP TABLE IF EXISTS [AT_CMDTS].[dbo].[ls_eventos_entregados_temp_new_PRUEBA];
    select
        a.*, b.NRO_CLIENTE_TITULAR 
    into
        [AT_CMDTS].[dbo].[ls_eventos_entregados_temp_new_PRUEBA]
    from
        [AT_CMDTS].[dbo].[ag_eventos_entregados_temp_PRUEBA] a
        inner join AT_CMDTS.DBO.LS_TMP_TC_ADI_2_PRUEBA b
            on b.ID_LOGISTICA=a.docid;

    -- PASO 3: Crear lista de clientes únicos
    DROP TABLE IF EXISTS [AT_CMDTS].[dbo].[ClientesParaActivar_PRUEBA];
    SELECT DISTINCT CAST(nro_cliente AS INT) AS valor
    INTO [AT_CMDTS].[dbo].[ClientesParaActivar_PRUEBA]
    FROM [ls_eventos_entregados_temp_new_PRUEBA]
    WHERE nro_cliente IS NOT NULL 
    UNION 
    SELECT DISTINCT CAST(NRO_CLIENTE_TITULAR AS INT) AS valor
    FROM [ls_eventos_entregados_temp_new_PRUEBA]
    WHERE NRO_CLIENTE_TITULAR IS NOT NULL; 

    -- PASO 4: Procesar Contratos
    -- 4.1: Obtener datos raw de contratos
    DROP TABLE IF EXISTS [ag_axnt_contratos_raw_PRUEBA];
    select X.*
    into [ag_axnt_contratos_raw_PRUEBA]
    FROM (
        SELECT
            cast(CONVERT(nvarchar(4), e.ve_fechaCreacion, 112) as int) as anho,
            cast(CONVERT(nvarchar(6), e.ve_fechaCreacion, 112) as int) as anho_mes,
            cast(CONVERT(nvarchar(8), e.ve_fechaCreacion,112) as int) as anho_mes_dia,
            a.vcn_iddo, a.vcn_idcm as id_nombre, 
            CASE WHEN a.vcn_idcm = 1517 THEN 'nro_guia' WHEN a.vcn_idcm = 21 THEN 'nro_cliente' ELSE g.cm_nombre END as campo, 
            floor(a.vcn_valor) as valor,
            e.ve_fechaCreacion as fecha_creacion, c.td_nombre as tipo_documento,
            d.co_id as identificar_documento, d.co_fechaCreacion, d.co_nombre as nombre_logico,
            e.VE_IDDO, e.VE_ID, f.alfs_camino, 
            CAST(ISNULL(i.FO_EXTENSION,'') AS VARCHAR(128)) as extension_archivo, 
            i.FO_DESCRIPCION as tipo_archivo, RIGHT(f.alfs_camino, 2) as particion,
            concat(RIGHT(f.alfs_camino, 2), '\\',a.vcn_iddo, '_', e.VE_ID) AS nombre_fisico,
            'SI' as renombrar_archivo
        from [ODSP_AXNT_VALORCAMPONUM] a
            left join [ODSP_axnt_documento] b on a.vcn_iddo = b.do_id
            left join [ODSP_axnt_tipodoc] c on a.vcn_idtd = c.td_id
            left join [ODSP_axnt_contenido] d on a.vcn_iddo = d.co_id
            left join [ODSP_axnt_version] e on a.vcn_iddo = e.ve_iddo
            left join [ODSP_axnt_AlmacenFS] f on e.ve_idalmacen = f.alfs_id
            left join [ODSP_axnt_campo] g on a.vcn_idcm = g.cm_id
            left join [ODSP_axnt_formato] i on b.do_idfo = i.fo_id
        where e.ve_fechaCreacion>= @Fecha1
            AND c.td_id IN (134) -- Solo Contratos
            AND a.vcn_idcm IN (21, 1517) -- Solo nro_cliente y nro_guia
            AND a.vcn_iddo IN (
                SELECT v.vcn_iddo FROM [ODSP_AXNT_VALORCAMPONUM] v
                WHERE v.vcn_idcm = 21 AND floor(v.vcn_valor) IN (SELECT valor FROM [AT_CMDTS].[dbo].[ClientesParaActivar_PRUEBA])
            )
        UNION
        SELECT
            cast(CONVERT(nvarchar(4), e.ve_fechaCreacion, 112) as int) as anho,
            cast(CONVERT(nvarchar(6), e.ve_fechaCreacion, 112) as int) as anho_mes,
            cast(CONVERT(nvarchar(8), e.ve_fechaCreacion, 112) as int) as anho_mes_dia,
            a.vcn_iddo, a.vcn_idcm as id_nombre, 
            CASE WHEN a.vcn_idcm = 1517 THEN 'nro_guia' WHEN a.vcn_idcm = 21 THEN 'nro_cliente' ELSE g.cm_nombre END as campo,
            floor(a.vcn_valor) as valor,
            e.ve_fechaCreacion as fecha_creacion, c.td_nombre as tipo_documento,
            d.co_id as identificar_documento, d.co_fechaCreacion, d.co_nombre as nombre_logico,
            e.VE_IDDO, e.VE_ID, f.alfs_camino, 
            CAST(ISNULL(i.FO_EXTENSION,'') AS VARCHAR(128)) as extension_archivo, 
            i.FO_DESCRIPCION as tipo_archivo, RIGHT(f.alfs_camino, 2) as particion,
            concat(RIGHT(f.alfs_camino, 2), '\\',a.vcn_iddo, '_', e.VE_ID) AS nombre_fisico,
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
            AND c.td_id IN (134) -- Solo Contratos
            AND a.vcn_idcm IN (21, 1517)
            AND a.vcn_iddo IN (
                SELECT v.vcn_iddo FROM [CRUDOCMDIR].[ODSP].[dbo].[AXNT_VALORCAMPONUM_2025] v
                WHERE v.vcn_idcm = 21 AND floor(v.vcn_valor) IN (SELECT valor FROM [AT_CMDTS].[dbo].[ClientesParaActivar_PRUEBA])
            )
    ) X;

    -- 4.2: Pivotar contratos
    DROP TABLE IF EXISTS [ag_axnt_contratos_pivot_PRUEBA];
    SELECT *
    INTO [ag_axnt_contratos_pivot_PRUEBA]
    FROM (
        SELECT
            anho, anho_mes, anho_mes_dia, fecha_creacion, vcn_iddo, VE_ID,
            nombre_fisico, nombre_logico, tipo_documento, renombrar_archivo,
            campo, valor
        FROM [ag_axnt_contratos_raw_PRUEBA]
    ) t
    PIVOT (
        MAX(valor)
        FOR campo IN ([nro_guia], [nro_cliente]) 
    ) AS pivot_table;

    -- 4.3: Eliminar duplicados de contratos (SIN incluir la columna rn en la tabla final)
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
            tipo_documento, renombrar_archivo, nro_guia, nro_cliente,
            ROW_NUMBER() OVER (PARTITION BY nro_cliente, CAST(fecha_creacion AS DATE) ORDER BY fecha_creacion DESC) AS rn
        FROM [ag_axnt_contratos_pivot_PRUEBA]
    ) tmp
    WHERE rn = 1;

    -- PASO 4.4: Lista de clientes con contrato (para limitar CIs a clientes que SÍ tienen contrato)
    DROP TABLE IF EXISTS [ClientesConContrato_PRUEBA];
    SELECT DISTINCT CAST(nro_cliente AS INT) AS valor
    INTO [ClientesConContrato_PRUEBA]
    FROM [ag_axnt_contratos_final_PRUEBA]
    WHERE nro_cliente IS NOT NULL;

    -- PASO 5: Obtener Cédulas
    DROP TABLE IF EXISTS [ag_axnt_cedulas_final_PRUEBA];
    DROP TABLE IF EXISTS [ClientesEncontradosCI_PRUEBA];
    -- Solo consideramos CIs de clientes que tienen CONTRATO
    SELECT DISTINCT CAST(valor AS INT) AS valor
    INTO [ClientesEncontradosCI_PRUEBA]
    FROM dbo.LS_ULTIMO_CI_AXNT_FULL
    WHERE CAST(valor AS INT) IN (SELECT valor FROM [ClientesConContrato_PRUEBA]);

    -- 5.1: CIs desde LS_ULTIMO_CI_AXNT_FULL
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
            NULL AS nro_guia, b.valor as nro_cliente, b.fecha_creacion 
        FROM dbo.LS_ULTIMO_CI_AXNT_FULL b
        WHERE b.valor IN (SELECT valor FROM [ClientesEncontradosCI_PRUEBA]) 
    ) AS CedulasDesdeTabla
    WHERE rn = 1; 

    -- 5.2: CIs desde Axentria (Fallback)
    INSERT INTO [ag_axnt_cedulas_final_PRUEBA] (
        anho, anho_mes, anho_mes_dia, vcn_iddo, VE_ID, nombre_fisico, nombre_logico,
        tipo_documento, renombrar_archivo, nro_guia, nro_cliente, fecha_creacion
    )
    SELECT 
        anho, anho_mes, anho_mes_dia, vcn_iddo, VE_ID, nombre_fisico, nombre_logico,
        tipo_documento, renombrar_archivo, nro_guia, nro_cliente, fecha_creacion
    FROM (
        SELECT
            ROW_NUMBER() OVER(PARTITION BY floor(a.vcn_valor) ORDER BY e.ve_fechaCreacion DESC, e.VE_ID DESC) as rn,
            cast(CONVERT(nvarchar(4), e.ve_fechaCreacion, 112) as int) as anho,
            cast(CONVERT(nvarchar(6), e.ve_fechaCreacion, 112) as int) as anho_mes,
            cast(CONVERT(nvarchar(8), e.ve_fechaCreacion,112) as int) as anho_mes_dia,
            a.vcn_iddo, e.VE_ID,
            concat(RIGHT(f.alfs_camino, 2), '\\',a.vcn_iddo, '_', e.VE_ID) AS nombre_fisico,
            d.co_nombre as nombre_logico, c.td_nombre as tipo_documento, 'SI' as renombrar_archivo,
            NULL as nro_guia, floor(a.vcn_valor) as nro_cliente, e.ve_fechaCreacion as fecha_creacion
        from [ODSP_AXNT_VALORCAMPONUM] a
            left join [ODSP_axnt_tipodoc] c on a.vcn_idtd = c.td_id
            left join [ODSP_axnt_contenido] d on a.vcn_iddo = d.co_id
            left join [ODSP_axnt_version] e on a.vcn_iddo = e.ve_iddo
            left join [ODSP_axnt_AlmacenFS] f on e.ve_idalmacen = f.alfs_id
        where c.td_id = 104 -- Cédula
            AND a.vcn_idcm = 21 -- Nro Cliente
            AND floor(a.vcn_valor) IN (SELECT valor FROM [ClientesConContrato_PRUEBA] WHERE valor NOT IN (SELECT valor FROM [ClientesEncontradosCI_PRUEBA]))
        UNION ALL 
        SELECT
            ROW_NUMBER() OVER(PARTITION BY floor(a.vcn_valor) ORDER BY e.ve_fechaCreacion DESC, e.VE_ID DESC) as rn,
            cast(CONVERT(nvarchar(4), e.ve_fechaCreacion, 112) as int) as anho,
            cast(CONVERT(nvarchar(6), e.ve_fechaCreacion, 112) as int) as anho_mes,
            cast(CONVERT(nvarchar(8), e.ve_fechaCreacion, 112) as int) as anho_mes_dia,
            a.vcn_iddo, e.VE_ID,
            concat(RIGHT(f.alfs_camino, 2), '\\',a.vcn_iddo, '_', e.VE_ID) AS nombre_fisico,
            d.co_nombre as nombre_logico, c.td_nombre as tipo_documento, 'SI' as renombrar_archivo,
            NULL as nro_guia, floor(a.vcn_valor) as nro_cliente, e.ve_fechaCreacion as fecha_creacion
        from [CRUDOCMDIR].[ODSP].[dbo].[AXNT_VALORCAMPONUM_2025] a
            left join [CRUDOCMDIR].[ODSP].[dbo].[axnt_tipodoc_2025] c on (a.vcn_idtd = c.td_id and a.fecha_cierre=c.fecha_cierre)
            left join [CRUDOCMDIR].[ODSP].[dbo].[axnt_contenido_2025] d on (a.vcn_iddo = d.co_id and a.fecha_cierre=d.fecha_cierre)
            left join [CRUDOCMDIR].[ODSP].[dbo].[axnt_version_2025] e on (a.vcn_iddo = e.ve_iddo and a.fecha_cierre=e.fecha_cierre)
            left join [CRUDOCMDIR].[ODSP].[dbo].[axnt_AlmacenFS_2025] f on (e.ve_idalmacen = f.alfs_id and a.fecha_cierre=f.fecha_cierre)
        where c.td_id = 104 -- Cédula
            AND a.vcn_idcm = 21 -- Nro Cliente
            AND floor(a.vcn_valor) IN (SELECT valor FROM [ClientesConContrato_PRUEBA] WHERE valor NOT IN (SELECT valor FROM [ClientesEncontradosCI_PRUEBA]))
    ) AS CedulasAxentria
    WHERE rn = 1;

    DROP TABLE IF EXISTS [ClientesEncontradosCI_PRUEBA];

    -- PASO 6: Combinar Contratos y Cédulas (alineando columnas explícitamente)
    DROP TABLE IF EXISTS [ag_axnt_combinado_PRUEBA];
    SELECT 
        anho, anho_mes, anho_mes_dia, fecha_creacion, vcn_iddo, VE_ID,
        nombre_fisico, nombre_logico, tipo_documento, renombrar_archivo, nro_guia, nro_cliente
    INTO [ag_axnt_combinado_PRUEBA]
    FROM (
        SELECT 
            anho, anho_mes, anho_mes_dia, fecha_creacion, vcn_iddo, VE_ID,
            nombre_fisico, nombre_logico, tipo_documento, renombrar_archivo, nro_guia, nro_cliente
        FROM [ag_axnt_contratos_final_PRUEBA]
        UNION ALL
        SELECT 
            anho, anho_mes, anho_mes_dia, fecha_creacion, vcn_iddo, VE_ID,
            nombre_fisico, nombre_logico, tipo_documento, renombrar_archivo, nro_guia, nro_cliente
        FROM [ag_axnt_cedulas_final_PRUEBA]
    ) AS CombinedData;

    -- PASO 7: Creación de tabla final (Join con Logística)
    DROP TABLE IF EXISTS [crm_legit_axnt_base_modelo_PRUEBA];
    -- Usamos EXISTS (semi-join) para evitar multiplicar filas por múltiples eventos/logística
    -- y DISTINCT para blindarnos ante filas idénticas provenientes de etapas previas
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

    -- PASO 8: Lógica de duplicados (opcional)
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

    -- PASO 9: Eliminación de tablas temporales
    DROP TABLE IF EXISTS [ag_eventos_entregados_base_temp_PRUEBA];
    DROP TABLE IF EXISTS [ag_eventos_entregados_temp_PRUEBA];
    DROP TABLE IF EXISTS [ag_axnt_contratos_raw_PRUEBA]; 
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

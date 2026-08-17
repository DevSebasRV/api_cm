CREATE PROCEDURE [dbo].[SP_DESTAJO_X_CLASE_MECANICO_RETOOL]


@Start_Date date,
@End_Date date,
@Tecnico nvarchar(155)

AS

CREATE TABLE #DestajoXClase
(
Usuario_Asignado nvarchar(155),
Orden Int,
Cliente nvarchar(100),
Factura nvarchar(100),
Fecha_Factura datetime,
Codigo nvarchar(50),
Nombre nvarchar(120),
Cantidad Decimal (10,2),
Importe Decimal (10,2),
Costo Decimal(10,2),
Util_Ref Decimal(10,2),
MO_Mto Decimal(10,2),
MO_Corr Decimal(10,2),
Vendedor nvarchar(155),
Depto nvarchar(20),
ODS_Cliente nvarchar(100),
status nvarchar (50)
)


-- FAC

INSERT INTO #DestajoXClase 
SELECT T0.U_Name as 'Usuario Asignado', T1.CallID as 'Orden',
T1.custmrName as 'Cliente',  'Factura ' + cast(T3.DocNum as varchar) 'Factura',T3.DocDate'Fecha Factura', T4.ItemCode as 'Codigo', T4.Dscription as 'Nombre',SUM(T4.Quantity) as 'Cantidad',
SUM(t4.LineTotal) 'Importe', 
CASE WHEN substring(T4.Dscription, 1, 4) = 'M.O.' THEN 0 ELSE SUM( isnull(T4.Quantity*T4.GrossBuyPr,0)) END AS 'Costo',
 CASE WHEN substring(T4.Dscription, 1, 4) <> 'M.O.' THEN isnull(T4.LineTotal-CASE WHEN substring(T4.Dscription, 1, 4) = 'M.O.' THEN 0 ELSE isnull(T4.Quantity*T4.GrossBuyPr,0) END,0) ELSE 0 END as 'Util - Ref' 
--CASE WHEN substring(T4.Dscription, 1, 6) = 'ARMADO' AND T5.ItmsGrpCod=125 THEN isnull(T4.Quantity*T6.PRICE,0) ELSE 0 END AS 'Armado'
--, CASE WHEN substring(T4.ItemCode, 1, 6) = 'MO-MTO' AND T5.ITMSGrpCod=125 THEN isnull(T4.Quantity*T6.Price,0) ELSE 0 END AS 'Mano de obra Mto'
 ,CASE 
   WHEN substring(T4.ItemCode, 1,6) = 'MO-MTO' AND T0.Fax = 'A' THEN SUM (isnull (T4.Quantity*T5.U_Dest_A,0))
   WHEN substring(T4.ItemCode, 1,6) = 'MO-MTO' AND T0.Fax = 'B' THEN SUM(isnull (T4.Quantity*T5.U_Dest_B,0))
   WHEN substring(T4.ItemCode, 1,7) = 'MOLLANT' AND T0.Fax = 'B' THEN SUM( isnull (T4.Quantity*T5.U_Dest_B,0))
   WHEN substring(T4.ItemCode, 1,7) = 'MOLLANT' AND T0.Fax = 'A' THEN SUM (isnull (T4.Quantity*T5.U_Dest_A,0))
   WHEN substring(T4.ItemCode, 1,15) = 'MO-ACCESORIOS-B' AND T0.Fax = 'A' THEN SUM (isnull (T4.Quantity*T5.U_Dest_B,0))
   WHEN substring(T4.ItemCode, 1,15) = 'MO-ACCESORIOS-B' AND T0.Fax = 'B' THEN SUM (isnull (T4.Quantity*T5.U_Dest_B,0))
   WHEN substring(T4.ItemCode, 1,8) = 'MO-MTO-K' AND T0.Fax = 'A' THEN SUM (isnull (T4.Quantity*T5.U_Dest_A,0))
   WHEN substring(T4.ItemCode, 1,8) = 'MO-MTO-K' AND T0.Fax = 'B' THEN SUM (isnull (T4.Quantity*T5.U_Dest_B,0))
   WHEN substring(T4.ItemCode, 1,8) = 'MO-MTO-KM' AND T0.Fax = 'A' THEN SUM (isnull (T4.Quantity*T5.U_Dest_A,0))
   WHEN substring(T4.ItemCode, 1,8) = 'MO-MTO-KM' AND T0.Fax = 'B' THEN SUM (isnull (T4.Quantity*T5.U_Dest_B,0))
   WHEN substring(T4.ItemCode, 1,8) = 'MO-MTO-YM' AND T0.Fax = 'A' THEN SUM (isnull (T4.Quantity*T5.U_Dest_A,0))
   WHEN substring(T4.ItemCode, 1,8) = 'MO-MTO-YM' AND T0.Fax = 'B' THEN SUM (isnull (T4.Quantity*T5.U_Dest_B,0))
   WHEN substring(T4.ItemCode, 1,8) = 'MO-MTO-CFM' AND T0.Fax = 'A' THEN SUM (isnull (T4.Quantity*T5.U_Dest_A,0))
   WHEN substring(T4.ItemCode, 1,8) = 'MO-MTO-CFM' AND T0.Fax = 'B' THEN SUM (isnull (T4.Quantity*T5.U_Dest_B,0))
   WHEN substring(T4.ItemCode, 1,8) = 'MO-MTO-T' AND T0.Fax = 'A' THEN SUM (isnull (T4.Quantity*T5.U_Dest_A,0)) 
   WHEN substring(T4.ItemCode, 1,8) = 'MO-MTO-T' AND T0.Fax = 'B' THEN SUM (isnull (T4.Quantity*T5.U_Dest_B,0))
   WHEN substring(T4.ItemCode, 1,8) = 'MO-MTO-TM' AND T0.Fax = 'A' THEN SUM (isnull (T4.Quantity*T5.U_Dest_A,0))
   WHEN substring(T4.ItemCode, 1,8) = 'MO-MTO-TM' AND T0.Fax = 'B' THEN SUM (isnull (T4.Quantity*T5.U_Dest_B,0)) 
   WHEN substring(T4.ItemCode, 1,22) = 'MO-REVISION-SEMINUEVA' AND T0.Fax = 'A' THEN SUM (isnull (T4.Quantity*T5.U_Dest_A,0))
   WHEN substring(T4.ItemCode, 1,22) = 'MO-REVISION-SEMINUEVA' AND T0.Fax = 'B' THEN SUM (isnull (T4.Quantity*T5.U_Dest_B,0))
   WHEN substring(T4.ItemCode, 1,8) = 'MO-MTO-Y' AND T0.Fax = 'A' THEN SUM (isnull (T4.Quantity*T5.U_Dest_A,0))
   WHEN substring(T4.ItemCode, 1,8) = 'MO-MTO-Y' AND T0.Fax = 'B' THEN SUM (isnull (T4.Quantity*T5.U_Dest_B,0))

   
  

   
  ELSE 0
  END AS 'Mano de obra Mto'
  

--, CASE WHEN substring(T4.ItemCode, 1,10) = 'MO-PUBLICO' 
, CASE 
   WHEN substring(T4.ItemCode, 1,10) = 'MO-PUBLICO' AND T0.Fax = 'A' THEN SUM( isnull (T4.Quantity*T5.U_Dest_A,0))
   WHEN substring(T4.ItemCode, 1,10) = 'MO-PUBLICO' AND T0.Fax = 'B' THEN SUM (isnull (T4.Quantity*T5.U_Dest_B,0))
   WHEN substring(T4.ItemCode, 1,17) = 'MO-PUBLICO-DUCATI' AND T0.Fax = 'A' THEN SUM ( isnull (T4.Quantity*T5.U_Dest_A,0))
   WHEN substring(T4.ItemCode, 1,17) = 'MO-PUBLICO-DUCATI' AND T0.Fax = 'B' THEN SUM ( isnull (T4.Quantity*T5.U_Dest_B,0) )  
   WHEN substring(T4.ItemCode, 1,10) = 'MO-RECLAMO' AND T0.Fax = 'A' THEN SUM (isnull (T4.Quantity*T5.U_Dest_A,0))
   WHEN substring(T4.ItemCode, 1,10) = 'MO-RECLAMO' AND T0.Fax = 'B' THEN SUM (isnull (T4.Quantity*T5.U_Dest_B,0))
   WHEN substring(T4.ItemCode, 1,10) = 'MO-INTERNA' AND T0.Fax = 'A' THEN SUM (isnull (T4.Quantity*T5.U_Dest_A,0))
   WHEN substring(T4.ItemCode, 1,10) = 'MO-INTERNA' AND T0.Fax = 'B' THEN SUM (isnull (T4.Quantity*T5.U_Dest_B,0))
   WHEN substring(T4.ItemCode, 1,7) = 'MO-CORT' AND T0.Fax = 'A' THEN SUM (isnull (T4.Quantity*T5.U_Dest_A,0))
   WHEN substring(T4.ItemCode, 1,7) = 'MO-CORT' AND T0.Fax = 'B' THEN SUM (isnull (T4.Quantity*T5.U_Dest_B,0))  
   WHEN substring(T4.ItemCode, 1,5) = 'MO-GA' AND T0.Fax = 'A' THEN SUM ( isnull (T4.Quantity*T5.U_Dest_A,0))
   WHEN substring(T4.ItemCode, 1,5) = 'MO-GA' AND T0.Fax = 'B' THEN SUM (isnull (T4.Quantity*T5.U_Dest_B,0))
   WHEN substring(T4.ItemCode, 1,10) = 'MO-SEGUROS' AND T0.Fax = 'A' THEN SUM (isnull (T4.Quantity*T5.U_Dest_A,0))
   WHEN substring(T4.ItemCode, 1,10) = 'MO-SEGUROS' AND T0.Fax = 'B' THEN SUM (isnull (T4.Quantity*T5.U_Dest_B,0))
   WHEN substring(T4.ItemCode, 1,10) = 'MO-PUBLICO-KM' AND T0.Fax = 'A' THEN SUM (isnull (T4.Quantity*T5.U_Dest_A,0))
   WHEN substring(T4.ItemCode, 1,10) = 'MO-PUBLICO-KM' AND T0.Fax = 'B' THEN SUM (isnull (T4.Quantity*T5.U_Dest_B,0))
   WHEN substring(T4.ItemCode, 1,10) = 'MO-PUBLICO-YM' AND T0.Fax = 'A' THEN SUM (isnull (T4.Quantity*T5.U_Dest_A,0))
   WHEN substring(T4.ItemCode, 1,10) = 'MO-PUBLICO-YM' AND T0.Fax = 'B' THEN SUM (isnull (T4.Quantity*T5.U_Dest_B,0))
   WHEN substring(T4.ItemCode, 1,10) = 'MO-PUBLICO-CFM' AND T0.Fax = 'A' THEN SUM (isnull (T4.Quantity*T5.U_Dest_A,0))
   WHEN substring(T4.ItemCode, 1,10) = 'MO-PUBLICO-CFM' AND T0.Fax = 'B' THEN SUM (isnull (T4.Quantity*T5.U_Dest_B,0))
   WHEN substring(T4.ItemCode, 1,10) = 'MO-PUBLICO-T' AND T0.Fax = 'A' THEN SUM (isnull (T4.Quantity*T5.U_Dest_A,0))
   WHEN substring(T4.ItemCode, 1,10) = 'MO-PUBLICO-T' AND T0.Fax = 'B' THEN SUM (isnull (T4.Quantity*T5.U_Dest_B,0))
   WHEN substring(T4.ItemCode, 1,10) = 'MO-PUBLICO-TM' AND T0.Fax = 'A' THEN SUM (isnull (T4.Quantity*T5.U_Dest_A,0))
   WHEN substring(T4.ItemCode, 1,10) = 'MO-PUBLICO-TM' AND T0.Fax = 'B' THEN SUM (isnull (T4.Quantity*T5.U_Dest_B,0))
   WHEN substring(T4.ItemCode, 1,10) = 'MO-INTERNA-T' AND T0.Fax = 'A' THEN SUM (isnull (T4.Quantity*T5.U_Dest_A,0))
   WHEN substring(T4.ItemCode, 1,10) = 'MO-INTERNA-T' AND T0.Fax = 'B' THEN SUM (isnull (T4.Quantity*T5.U_Dest_B,0))
   WHEN substring(T4.ItemCode, 1,10) = 'MO-GA-T' AND T0.Fax = 'A' THEN SUM (isnull (T4.Quantity*T5.U_Dest_A,0))
   WHEN substring(T4.ItemCode, 1,10) = 'MO-GA-T' AND T0.Fax = 'B' THEN SUM (isnull (T4.Quantity*T5.U_Dest_B,0))
   WHEN substring(T4.ItemCode, 1,10) = 'MO-RECLAMO-T' AND T0.Fax = 'A' THEN SUM (isnull (T4.Quantity*T5.U_Dest_A,0))
   WHEN substring(T4.ItemCode, 1,10) = 'MO-RECLAMO-T' AND T0.Fax = 'B' THEN SUM (isnull (T4.Quantity*T5.U_Dest_B,0))
   WHEN substring(T4.ItemCode, 1,10) = 'MO-CORT-T' AND T0.Fax = 'A' THEN SUM (isnull (T4.Quantity*T5.U_Dest_A,0))
   WHEN substring(T4.ItemCode, 1,10) = 'MO-CORT-T' AND T0.Fax = 'B' THEN SUM (isnull (T4.Quantity*T5.U_Dest_B,0))
  
   
   ELSE 0
  END AS 'Mano de obra Correctivo'
  

--, CASE WHEN T4.LineTotal<>0 AND T4.LineTotal>(T4.Quantity*T6.Price) THEN (((T4.Quantity*T6.Price)/T4.Linetotal)*100) ELSE 0 END as '% M.O.'
, T7.SlpName as 'Vendedor',T8.[Name] AS 'Departamento', CONCAT (T1.CallID,'-',T1.custmrName,'FN'), T9.[Name]
FROM OUSR T0
INNER JOIN OSCL T1 ON T0.USERID = T1.assignee
INNER JOIN SCL4 T2 ON T1.callID = T2.SrcvCallID
INNER JOIN OINV T3 ON T2.DocAbs = T3.DocEntry
INNER JOIN INV1 T4 ON T3.DocEntry = T4.DocEntry
INNER JOIN OITM T5 ON T4.itemCode = T5.ItemCode
INNER JOIN ITM1 T6 ON T5.ItemCode = T6.ItemCode
INNER JOIN OSLP T7 ON T3.SlpCode = T7.SlpCode
INNER JOIN OUDP T8 ON T0.Department = T8.Code
INNER JOIN OSCS T9 ON T1.[status] = T9.[statusID]

where T0.U_Name like  '%' and t3.DocDate between @Start_Date and @End_Date AND  T0.U_Name = @Tecnico AND T1.createDate >= '20200101' 
and t3.series in (4,6,51,52,53,78,50,119) and T1.U_pagado Like 'No' and T2.[Object] = '13' and  T6.[PriceList] = '26' 
and (T4.[BaseType] = '15' OR T4.[BaseType] = '17') and (T1.status = -1 OR T1.status = 26 OR T1.status = 25)
AND T5.ItmsGrpCod = '125' AND T4.ItemCode LIKE 'MO%'

GROUP BY T0.U_NAME, T1.CallID, T3.DocNum, T1.custmrName, T1.U_pagado, T1.DocNum, t4.LineTotal, T4.Dscription, T4.Quantity, T4.GrossBuyPr, 
T6.Price, T4.Dscription, T5.ItmsGrpCod, T4.Quantity, T6.PRICE, T4.ItemCode, T4.Dscription, T3.DocDate, T7.SlpName, T0.Fax, 
T5.U_Dest_A, T5.U_Dest_B, T8.[Name],T9.[Name]

--UNION ALL
-- ND
INSERT INTO #DestajoXClase 
SELECT T0.U_Name as 'Usuario Asignado', T1.CallID as 'Orden',
T1.custmrName as 'Cliente',  'Factura ' + cast(T3.DocNum as varchar) 'Factura', T3.DocDate'Fecha Factura', T4.ItemCode as 'Codigo', T4.Dscription as 'Nombre',SUM(T4.Quantity) as 'Cantidad',
SUM(t4.LineTotal) 'Importe', 
CASE WHEN substring(T4.Dscription, 1, 4) = 'M.O.' THEN 0 ELSE SUM(isnull(T4.Quantity*T4.GrossBuyPr,0)) END AS 'Costo',
 CASE WHEN substring(T4.Dscription, 1, 4) <> 'M.O.' THEN isnull(T4.LineTotal-CASE WHEN substring(T4.Dscription, 1, 4) = 'M.O.' THEN 0 ELSE isnull(T4.Quantity*T4.GrossBuyPr,0) END,0) ELSE 0 END as 'Util - Ref' 
--CASE WHEN substring(T4.Dscription, 1, 6) = 'ARMADO' AND T5.ItmsGrpCod=125 THEN isnull(T4.Quantity*T8.PRICE,0) ELSE 0 END AS 'Armado'
--, CASE WHEN substring(T4.ItemCode, 1, 6) = 'MO-MTO' AND T5.ITMSGrpCod=125  THEN isnull(T4.Quantity*T8.Price,0) ELSE 0 END AS 'Mano de obra Mto'
,CASE 
   WHEN substring(T4.ItemCode, 1,6) = 'MO-MTO' AND T0.Fax = 'A' THEN SUM(isnull (T4.Quantity*T5.U_Dest_A,0))
   WHEN substring(T4.ItemCode, 1,6) = 'MO-MTO' AND T0.Fax = 'B' THEN SUM(isnull (T4.Quantity*T5.U_Dest_B,0))
   WHEN substring(T4.ItemCode, 1,7) = 'MOLLANT' AND T0.Fax = 'B' THEN SUM (isnull (T4.Quantity*T5.U_Dest_B,0))
   WHEN substring(T4.ItemCode, 1,7) = 'MOLLANT' AND T0.Fax = 'A' THEN SUM (isnull (T4.Quantity*T5.U_Dest_A,0))
   WHEN substring(T4.ItemCode, 1,8) = 'MO-MTO-KM' AND T0.Fax = 'A' THEN SUM (isnull (T4.Quantity*T5.U_Dest_A,0))
   WHEN substring(T4.ItemCode, 1,8) = 'MO-MTO-KM' AND T0.Fax = 'B' THEN SUM (isnull (T4.Quantity*T5.U_Dest_B,0))
   WHEN substring(T4.ItemCode, 1,8) = 'MO-MTO-YM' AND T0.Fax = 'A' THEN SUM (isnull (T4.Quantity*T5.U_Dest_A,0))
   WHEN substring(T4.ItemCode, 1,8) = 'MO-MTO-YM' AND T0.Fax = 'B' THEN SUM (isnull (T4.Quantity*T5.U_Dest_B,0))
   WHEN substring(T4.ItemCode, 1,8) = 'MO-MTO-CFM' AND T0.Fax = 'A' THEN SUM (isnull (T4.Quantity*T5.U_Dest_A,0))
   WHEN substring(T4.ItemCode, 1,8) = 'MO-MTO-CFM' AND T0.Fax = 'B' THEN SUM (isnull (T4.Quantity*T5.U_Dest_B,0))
   WHEN substring(T4.ItemCode, 1,8) = 'MO-MTO-T' AND T0.Fax = 'A' THEN SUM (isnull (T4.Quantity*T5.U_Dest_A,0))
   WHEN substring(T4.ItemCode, 1,8) = 'MO-MTO-T' AND T0.Fax = 'B' THEN SUM (isnull (T4.Quantity*T5.U_Dest_B,0))
   WHEN substring(T4.ItemCode, 1,8) = 'MO-MTO-TM' AND T0.Fax = 'A' THEN SUM (isnull (T4.Quantity*T5.U_Dest_A,0))
   WHEN substring(T4.ItemCode, 1,8) = 'MO-MTO-TM' AND T0.Fax = 'B' THEN SUM (isnull (T4.Quantity*T5.U_Dest_B,0))
    WHEN substring(T4.ItemCode, 1,22) = 'MO-REVISION-SEMINUEVA' AND T0.Fax = 'A' THEN SUM (isnull (T4.Quantity*T5.U_Dest_A,0))
   WHEN substring(T4.ItemCode, 1,22) = 'MO-REVISION-SEMINUEVA' AND T0.Fax = 'B' THEN SUM (isnull (T4.Quantity*T5.U_Dest_B,0))
   WHEN substring(T4.ItemCode, 1,13) = 'MO-INTERNA-Y' AND T0.Fax = 'A' THEN SUM(isnull (T4.Quantity*T5.U_Dest_A,0))
   WHEN substring(T4.ItemCode, 1,13) = 'MO-INTERNA-Y' AND T0.Fax = 'B' THEN SUM(isnull (T4.Quantity*T5.U_Dest_B,0))
   WHEN substring(T4.ItemCode, 1,13) = 'MO-INTERNA-CF' AND T0.Fax = 'A' THEN SUM(isnull (T4.Quantity*T5.U_Dest_A,0))
   WHEN substring(T4.ItemCode, 1,13) = 'MO-INTERNA-CF' AND T0.Fax = 'B' THEN SUM(isnull (T4.Quantity*T5.U_Dest_B,0)) 
   
   
   
  ELSE 0
  END AS 'Mano de obra Mto'
  
, CASE 
   WHEN substring(T4.ItemCode, 1,10) = 'MO-PUBLICO' AND T0.Fax = 'A' THEN SUM(isnull (T4.Quantity*T5.U_Dest_A,0))
   WHEN substring(T4.ItemCode, 1,10) = 'MO-PUBLICO' AND T0.Fax = 'B' THEN SUM (isnull (T4.Quantity*T5.U_Dest_B,0))
   WHEN substring(T4.ItemCode, 1,17) = 'MO-PUBLICO-DUCATI' AND T0.Fax = 'A' THEN SUM (isnull (T4.Quantity*T5.U_Dest_A,0))
   WHEN substring(T4.ItemCode, 1,17) = 'MO-PUBLICO-DUCATI' AND T0.Fax = 'B' THEN SUM (isnull (T4.Quantity*T5.U_Dest_B,0))
   WHEN substring(T4.ItemCode, 1,10) = 'MO-RECLAMO' AND T0.Fax = 'A' THEN SUM (isnull (T4.Quantity*T5.U_Dest_A,0))
   WHEN substring(T4.ItemCode, 1,10) = 'MO-RECLAMO' AND T0.Fax = 'B' THEN SUM( isnull (T4.Quantity*T5.U_Dest_B,0))
   WHEN substring(T4.ItemCode, 1,10) = 'MO-INTERNA' AND T0.Fax = 'A' THEN SUM (isnull (T4.Quantity*T5.U_Dest_A,0))
   WHEN substring(T4.ItemCode, 1,10) = 'MO-INTERNA' AND T0.Fax = 'B' THEN SUM (isnull (T4.Quantity*T5.U_Dest_B,0))
   WHEN substring(T4.ItemCode, 1,7) = 'MO-CORT' AND T0.Fax = 'A' THEN SUM (isnull (T4.Quantity*T5.U_Dest_A,0))
   WHEN substring(T4.ItemCode, 1,7) = 'MO-CORT' AND T0.Fax = 'B' THEN SUM (isnull (T4.Quantity*T5.U_Dest_B,0))
   WHEN substring(T4.ItemCode, 1,5) = 'MO-GA' AND T0.Fax = 'A' THEN SUM (isnull (T4.Quantity*T5.U_Dest_A,0))
   WHEN substring(T4.ItemCode, 1,5) = 'MO-GA' AND T0.Fax = 'B' THEN SUM (isnull (T4.Quantity*T5.U_Dest_B,0)) 
   WHEN substring(T4.ItemCode, 1,10) = 'MO-SEGUROS' AND T0.Fax = 'A' THEN SUM (isnull (T4.Quantity*T5.U_Dest_A,0))
   WHEN substring(T4.ItemCode, 1,10) = 'MO-SEGUROS' AND T0.Fax = 'B' THEN SUM (isnull (T4.Quantity*T5.U_Dest_B,0) )
   WHEN substring(T4.ItemCode, 1,10) = 'MO-PUBLICO-KM' AND T0.Fax = 'A' THEN SUM (isnull (T4.Quantity*T5.U_Dest_A,0))
WHEN substring(T4.ItemCode, 1,10) = 'MO-PUBLICO-KM' AND T0.Fax = 'B' THEN SUM (isnull (T4.Quantity*T5.U_Dest_B,0))
WHEN substring(T4.ItemCode, 1,10) = 'MO-PUBLICO-YM' AND T0.Fax = 'A' THEN SUM (isnull (T4.Quantity*T5.U_Dest_A,0))
WHEN substring(T4.ItemCode, 1,10) = 'MO-PUBLICO-YM' AND T0.Fax = 'B' THEN SUM (isnull (T4.Quantity*T5.U_Dest_B,0))
WHEN substring(T4.ItemCode, 1,10) = 'MO-PUBLICO-CFM' AND T0.Fax = 'A' THEN SUM (isnull (T4.Quantity*T5.U_Dest_A,0))
WHEN substring(T4.ItemCode, 1,10) = 'MO-PUBLICO-CFM' AND T0.Fax = 'B' THEN SUM (isnull (T4.Quantity*T5.U_Dest_B,0))
WHEN substring(T4.ItemCode, 1,10) = 'MO-PUBLICO-T' AND T0.Fax = 'A' THEN SUM (isnull (T4.Quantity*T5.U_Dest_A,0))
WHEN substring(T4.ItemCode, 1,10) = 'MO-PUBLICO-T' AND T0.Fax = 'B' THEN SUM (isnull (T4.Quantity*T5.U_Dest_B,0))
WHEN substring(T4.ItemCode, 1,10) = 'MO-PUBLICO-TM' AND T0.Fax = 'A' THEN SUM (isnull (T4.Quantity*T5.U_Dest_A,0))
WHEN substring(T4.ItemCode, 1,10) = 'MO-PUBLICO-TM' AND T0.Fax = 'B' THEN SUM (isnull (T4.Quantity*T5.U_Dest_B,0))
WHEN substring(T4.ItemCode, 1,10) = 'MO-INTERNA-T' AND T0.Fax = 'A' THEN SUM (isnull (T4.Quantity*T5.U_Dest_A,0))
WHEN substring(T4.ItemCode, 1,10) = 'MO-INTERNA-T' AND T0.Fax = 'B' THEN SUM (isnull (T4.Quantity*T5.U_Dest_B,0))
WHEN substring(T4.ItemCode, 1,10) = 'MO-GA-T' AND T0.Fax = 'A' THEN SUM (isnull (T4.Quantity*T5.U_Dest_A,0))
WHEN substring(T4.ItemCode, 1,10) = 'MO-GA-T' AND T0.Fax = 'B' THEN SUM (isnull (T4.Quantity*T5.U_Dest_B,0))
WHEN substring(T4.ItemCode, 1,10) = 'MO-RECLAMO-T' AND T0.Fax = 'A' THEN SUM (isnull (T4.Quantity*T5.U_Dest_A,0))
WHEN substring(T4.ItemCode, 1,10) = 'MO-RECLAMO-T' AND T0.Fax = 'B' THEN SUM (isnull (T4.Quantity*T5.U_Dest_B,0))
WHEN substring(T4.ItemCode, 1,10) = 'MO-CORT-T' AND T0.Fax = 'A' THEN SUM (isnull (T4.Quantity*T5.U_Dest_A,0))
WHEN substring(T4.ItemCode, 1,10) = 'MO-CORT-T' AND T0.Fax = 'B' THEN SUM (isnull (T4.Quantity*T5.U_Dest_B,0))

   
   
   
   ELSE 0
  END AS 'Mano de obra Correctivo'

--, CASE WHEN T4.LineTotal<>0 AND T4.LineTotal>(T4.Quantity*T8.Price) THEN (((T4.Quantity*T8.Price)/T4.Linetotal)*100) ELSE 0 END as '% M.O.'
, T11.SlpName as 'Vendedor', T12.[Name] AS 'Departamento', CONCAT (T1.CallID,'-',T1.custmrName,'FN'), T13.[Name]
from OUSR T0
INNER JOIN OSCL T1 on T0.Internal_K = T1.Assignee
INNER JOIN SCL4 T2 on T1.callID = T2.SrcvCallID and T2.object = '15'
INNER JOIN ODLN T10 on T10.DocNum = T2.docNumber 
INNER JOIN DLN1 T6 on T6.DocEntry = T10.DocEntry  
INNER JOIN ITM1 T8 on T6.ItemCode = T8.ItemCode and T8.PriceList = '26'
LEFT JOIN INV1 T4 on T6.docEntry = T4.BaseEntry AND T6.LineNum = T4.BaseLine AND T4.BaseType = '15'
LEFT JOIN OINV T3 on T4.DocEntry = T3.DocEntry
LEFT JOIN OITM T5 on T4.ItemCode = T5.ItemCode
LEFT JOIN ITM1 T7 on T4.ItemCode = T7.ItemCode and T7.PriceList = '1'
INNER JOIN OSLP T11 on T11.SlpCode = T10.SlpCode
INNER JOIN OUDP T12 ON T0.Department = T12.Code
INNER JOIN OSCS T13 ON T1.[status] = T13.[statusID]

where T0.U_Name like '%'  and t3.DocDate between @Start_Date and @End_Date AND  T0.U_Name = @Tecnico  AND T1.createDate >= '20200101'   
and t3.series in (53,78)and T1.U_pagado Like 'No' and  T8.[PriceList] = 26 and T4.[BaseType] = '15' and (T1.status = -1 OR T1.status = 26 OR T1.status = 25)
AND T5.ItmsGrpCod = '125' AND T4.ItemCode LIKE 'MO%'

GROUP BY T0.U_NAME, T1.CallID, T3.DocNum, T1.custmrName, T1.U_pagado, T1.DocNum, t4.LineTotal, T4.Dscription, T4.Quantity, T4.GrossBuyPr, T8.Price, T4.Dscription, T5.ItmsGrpCod, T4.Quantity, /*T8.PRICE,*/ T4.ItemCode, T4.Dscription, T3.DocDate,T11.SlpName, T0.Fax, T5.U_Dest_A, T5.U_Dest_B, T12.[Name],T13.[Name]

--UNION ALL
-- NC

INSERT INTO #DestajoXClase 
SELECT T0.U_Name as 'Usuario Asignado', T1.CallID as 'Orden',
T1.custmrName as 'Cliente', 'N. Credito ' + cast(T8.DocNum as varchar) + char(13) + '...Fact ' + cast(T3.DocNum as varchar) 'Factura', T3.DocDate'Fecha Factura',T4.ItemCode as 'Codigo', T4.Dscription as 'Nombre',SUM(-T4.Quantity) as 'Cantidad',
SUM (-T7.LineTotal) AS 'Importe', 
-CASE WHEN substring(T4.Dscription, 1, 4) = 'M.O.' THEN 0 ELSE SUM(isnull(T7.Quantity*T7.GrossBuyPr,0)) END AS 'Costo',


 (IsNull(CASE WHEN
substring(T4.Dscription, 1, 4) = 'M.O.' THEN 0 ELSE isnull(T7.Quantity*T6.Price,0) END,0)-
IsNull(CASE WHEN substring(T4.Dscription, 1, 4) = 'M.O.' THEN 0 ELSE isnull(T7.Quantity*T7.GrossBuyPr,0) END,0)) AS 'Utilidad de refacciones' 
--, -CASE WHEN substring(T4.Dscription, 1, 6) = 'ARMADO' AND T5.ItmsGrpCod=125 THEN isnull(T7.Quantity*T6.PRICE,0) ELSE 0 END AS 'Armado'
--, -CASE WHEN substring(T4.ItemCode, 1, 6) = 'MO-MTO' AND T5.ITMSGrpCod=125 THEN isnull(T7.Quantity*T6.Price,0) ELSE 0 END 'Mano de obra Mto'
,-CASE 
   WHEN substring(T4.ItemCode, 1,6) = 'MO-MTO' AND T0.Fax = 'A' THEN SUM(isnull (T4.Quantity*T5.U_Dest_A,0))
   WHEN substring(T4.ItemCode, 1,6) = 'MO-MTO' AND T0.Fax = 'B' THEN SUM( isnull (T4.Quantity*T5.U_Dest_B,0))
   WHEN substring(T4.ItemCode, 1,7) = 'MOLLANT' AND T0.Fax = 'B' THEN SUM (isnull (T4.Quantity*T5.U_Dest_B,0))
   WHEN substring(T4.ItemCode, 1,7) = 'MOLLANT' AND T0.Fax = 'A' THEN SUM (isnull (T4.Quantity*T5.U_Dest_A,0))
   WHEN substring(T4.ItemCode, 1,8) = 'MO-MTO-KM' AND T0.Fax = 'A' THEN SUM (isnull (T4.Quantity*T5.U_Dest_A,0))
WHEN substring(T4.ItemCode, 1,8) = 'MO-MTO-KM' AND T0.Fax = 'B' THEN SUM (isnull (T4.Quantity*T5.U_Dest_B,0))
WHEN substring(T4.ItemCode, 1,8) = 'MO-MTO-YM' AND T0.Fax = 'A' THEN SUM (isnull (T4.Quantity*T5.U_Dest_A,0))
WHEN substring(T4.ItemCode, 1,8) = 'MO-MTO-YM' AND T0.Fax = 'B' THEN SUM (isnull (T4.Quantity*T5.U_Dest_B,0))
WHEN substring(T4.ItemCode, 1,8) = 'MO-MTO-CFM' AND T0.Fax = 'A' THEN SUM (isnull (T4.Quantity*T5.U_Dest_A,0))
WHEN substring(T4.ItemCode, 1,8) = 'MO-MTO-CFM' AND T0.Fax = 'B' THEN SUM (isnull (T4.Quantity*T5.U_Dest_B,0))
WHEN substring(T4.ItemCode, 1,8) = 'MO-MTO-T' AND T0.Fax = 'A' THEN SUM (isnull (T4.Quantity*T5.U_Dest_A,0))
WHEN substring(T4.ItemCode, 1,8) = 'MO-MTO-T' AND T0.Fax = 'B' THEN SUM (isnull (T4.Quantity*T5.U_Dest_B,0))
WHEN substring(T4.ItemCode, 1,8) = 'MO-MTO-TM' AND T0.Fax = 'A' THEN SUM (isnull (T4.Quantity*T5.U_Dest_A,0))
WHEN substring(T4.ItemCode, 1,8) = 'MO-MTO-TM' AND T0.Fax = 'B' THEN SUM (isnull (T4.Quantity*T5.U_Dest_B,0))
WHEN substring(T4.ItemCode, 1,8) = 'MO-INTERNA-Y' AND T0.Fax = 'A' THEN SUM (isnull (T4.Quantity*T5.U_Dest_A,0))
WHEN substring(T4.ItemCode, 1,8) = 'MO-INTERNA-Y' AND T0.Fax = 'B' THEN SUM (isnull (T4.Quantity*T5.U_Dest_B,0))
  
  
  
  ELSE 0
  END AS 'Mano de obra Mto'

, -CASE 
   WHEN substring(T7.ItemCode, 1,10) = 'MO-PUBLICO' AND T0.Fax = 'A' THEN SUM (isnull (T7.Quantity*T5.U_Dest_A,0))
   WHEN substring(T7.ItemCode, 1,10) = 'MO-PUBLICO' AND T0.Fax = 'B' THEN SUM (isnull (T7.Quantity*T5.U_Dest_B,0))
   WHEN substring(T7.ItemCode, 1,17) = 'MO-PUBLICO-DUCATI' AND T0.Fax = 'A' THEN SUM (isnull (T7.Quantity*T5.U_Dest_A,0))
      WHEN substring(T7.ItemCode, 1,17) = 'MO-PUBLICO-DUCATI' AND T0.Fax = 'B' THEN SUM(isnull (T7.Quantity*T5.U_Dest_B,0))
   WHEN substring(T7.ItemCode, 1,10) = 'MO-RECLAMO' AND T0.Fax = 'A' THEN SUM (isnull (T7.Quantity*T5.U_Dest_A,0))
   WHEN substring(T7.ItemCode, 1,10) = 'MO-RECLAMO' AND T0.Fax = 'B' THEN SUM (isnull (T7.Quantity*T5.U_Dest_B,0))
   WHEN substring(T7.ItemCode, 1,10) = 'MO-INTERNA' AND T0.Fax = 'A' THEN SUM (isnull (T7.Quantity*T5.U_Dest_A,0))
   WHEN substring(T7.ItemCode, 1,10) = 'MO-INTERNA' AND T0.Fax = 'B' THEN SUM (isnull (T7.Quantity*T5.U_Dest_B,0))
   WHEN substring(T7.ItemCode, 1,7) = 'MO-CORT' AND T0.Fax = 'A' THEN SUM (isnull (T7.Quantity*T5.U_Dest_A,0))
   WHEN substring(T7.ItemCode, 1,7) = 'MO-CORT' AND T0.Fax = 'B' THEN SUM (isnull (T7.Quantity*T5.U_Dest_B,0))
   WHEN substring(T7.ItemCode, 1,5) = 'MO-GA' AND T0.Fax = 'A' THEN SUM (isnull (T7.Quantity*T5.U_Dest_A,0))
   WHEN substring(T7.ItemCode, 1,5) = 'MO-GA' AND T0.Fax = 'B' THEN SUM (isnull (T7.Quantity*T5.U_Dest_B,0)) 
   WHEN substring(T7.ItemCode, 1,10) = 'MO-SEGUROS' AND T0.Fax = 'A' THEN SUM ( isnull (T7.Quantity*T5.U_Dest_A,0))
   WHEN substring(T7.ItemCode, 1,10) = 'MO-SEGUROS' AND T0.Fax = 'B' THEN SUM (isnull (T7.Quantity*T5.U_Dest_B,0)) 
   WHEN substring(T4.ItemCode, 1,10) = 'MO-PUBLICO-KM' AND T0.Fax = 'A' THEN SUM (isnull (T4.Quantity*T5.U_Dest_A,0))
WHEN substring(T4.ItemCode, 1,10) = 'MO-PUBLICO-KM' AND T0.Fax = 'B' THEN SUM (isnull (T4.Quantity*T5.U_Dest_B,0))
WHEN substring(T4.ItemCode, 1,10) = 'MO-PUBLICO-YM' AND T0.Fax = 'A' THEN SUM (isnull (T4.Quantity*T5.U_Dest_A,0))
WHEN substring(T4.ItemCode, 1,10) = 'MO-PUBLICO-YM' AND T0.Fax = 'B' THEN SUM (isnull (T4.Quantity*T5.U_Dest_B,0))
WHEN substring(T4.ItemCode, 1,10) = 'MO-PUBLICO-CFM' AND T0.Fax = 'A' THEN SUM (isnull (T4.Quantity*T5.U_Dest_A,0))
WHEN substring(T4.ItemCode, 1,10) = 'MO-PUBLICO-CFM' AND T0.Fax = 'B' THEN SUM (isnull (T4.Quantity*T5.U_Dest_B,0))
WHEN substring(T4.ItemCode, 1,10) = 'MO-PUBLICO-T' AND T0.Fax = 'A' THEN SUM (isnull (T4.Quantity*T5.U_Dest_A,0))
WHEN substring(T4.ItemCode, 1,10) = 'MO-PUBLICO-T' AND T0.Fax = 'B' THEN SUM (isnull (T4.Quantity*T5.U_Dest_B,0))
WHEN substring(T4.ItemCode, 1,10) = 'MO-PUBLICO-TM' AND T0.Fax = 'A' THEN SUM (isnull (T4.Quantity*T5.U_Dest_A,0))
WHEN substring(T4.ItemCode, 1,10) = 'MO-PUBLICO-TM' AND T0.Fax = 'B' THEN SUM (isnull (T4.Quantity*T5.U_Dest_B,0))
WHEN substring(T4.ItemCode, 1,10) = 'MO-INTERNA-T' AND T0.Fax = 'A' THEN SUM (isnull (T4.Quantity*T5.U_Dest_A,0))
WHEN substring(T4.ItemCode, 1,10) = 'MO-INTERNA-T' AND T0.Fax = 'B' THEN SUM (isnull (T4.Quantity*T5.U_Dest_B,0))
WHEN substring(T4.ItemCode, 1,10) = 'MO-GA-T' AND T0.Fax = 'A' THEN SUM (isnull (T4.Quantity*T5.U_Dest_A,0))
WHEN substring(T4.ItemCode, 1,10) = 'MO-GA-T' AND T0.Fax = 'B' THEN SUM (isnull (T4.Quantity*T5.U_Dest_B,0))
WHEN substring(T4.ItemCode, 1,10) = 'MO-RECLAMO-T' AND T0.Fax = 'A' THEN SUM (isnull (T4.Quantity*T5.U_Dest_A,0))
WHEN substring(T4.ItemCode, 1,10) = 'MO-RECLAMO-T' AND T0.Fax = 'B' THEN SUM (isnull (T4.Quantity*T5.U_Dest_B,0))
WHEN substring(T4.ItemCode, 1,10) = 'MO-CORT-T' AND T0.Fax = 'A' THEN SUM (isnull (T4.Quantity*T5.U_Dest_A,0))
WHEN substring(T4.ItemCode, 1,10) = 'MO-CORT-T' AND T0.Fax = 'B' THEN SUM (isnull (T4.Quantity*T5.U_Dest_B,0))


   
   
   ELSE 0
  END AS 'Mano de obra Correctivo'

--, -CASE WHEN T4.LineTotal<>0 AND T4.LineTotal>(T4.Quantity*T6.Price) THEN (((T4.Quantity*T6.Price)/T4.Linetotal)*100) ELSE 0 END as '% M.O.'
, T9.SlpName as 'Vendedor', T10.[Name] AS 'Departamento', CONCAT (T1.CallID,'-',T1.custmrName,'FN'),T11.[Name]

FROM OUSR T0
INNER JOIN OSCL T1 ON T0.USERID = T1.assignee
INNER JOIN SCL4 T2 ON T1.callID = T2.SrcvCallID
INNER JOIN OINV T3 ON T2.DocAbs = T3.DocEntry
INNER JOIN INV1 T4 ON T3.DocEntry = T4.DocEntry
INNER JOIN OITM T5 ON T4.itemCode = T5.ItemCode
INNER JOIN ITM1 T6 ON T5.ItemCode = T6.ItemCode
INNER JOIN RIN1 T7 ON T4.DocEntry = T7.BaseEntry and T4.LineNum = T7.BaseLine
INNER JOIN ORIN T8 ON T7.DocEntry = T8.DocEntry
INNER JOIN OSLP T9 ON T9.SlpCode = T3.SlpCode
INNER JOIN OUDP T10 ON T0.Department = T10.Code
INNER JOIN OSCS T11 ON T1.[status] = T11.[statusID]

where T0.U_Name like '%' and t3.DocDate between @Start_Date and @End_Date AND  T0.U_Name = @Tecnico AND T1.createDate >= '20200101' 
and t3.series in (4,6,51,52,53,78,50,120) and T1.U_pagado Like 'No'   and T2.[Object] = '13' and  T6.[PriceList] = 26 and T4.[BaseType] = '15' 
and (T1.status = -1 OR T1.status = 26 OR T1.status = 25)
AND T5.ItmsGrpCod = '125' AND T4.ItemCode LIKE 'MO%'

GROUP BY T0.U_NAME, T1.CallID, T3.DocNum, T1.custmrName, T1.U_pagado, T8.DocNum, T7.LineTotal, T4.Dscription, T7.Quantity, T7.GrossBuyPr, T6.Price, T4.Dscription, T5.ITMSGrpCod, T7.Quantity, T6.Price, T4.Quantity, T6.PRICE, T4.ItemCode, T4.Dscription, T4.LineTotal, T3.DocDate, T9.SlpName,  T0.Fax, T5.U_Dest_A, T5.U_Dest_B, T7.ItemCode, T10.[Name],T11.[Name]
order by 1, 2, 4



------

select * from #DestajoXClase

DROP TABLE #DestajoXClase
const sql = require('mssql');
const pool = require('../config/dbconfig')

exports.getById = async (id) => {
    return await pool.request()
        .input('param1', sql.VarChar(50), id)
        .query('SELECT * FROM Asset WHERE ID = @param1')
}

exports.getAll = async () => {
    return await pool.request()
        .query('SELECT * FROM Asset')
}

exports.getAssetBySiteId = async (siteid) => {
const q=`SELECT a.AssetTypeID, a.AssetTypeName, 
(SELECT a2.AssetTypeID, a2.AssetTypeName, a2.ID, a2.System, a2.UniqueID, a2.Name 
 FROM Asset a2 
 INNER JOIN AssetSite ass ON a2.ID = ass.AssetID 
 WHERE ass.SiteID = '${siteid}' AND a2.AssetTypeID = a.AssetTypeID 
 FOR JSON PATH) AS Assets 
FROM Asset a 
INNER JOIN AssetSite ass ON a.ID = ass.AssetID 
INNER JOIN Unit ut ON a.UnitID = ut.ID 
WHERE ass.SiteID = '${siteid}' AND ut.UnitType  IN (1,2,3)
GROUP BY a.AssetTypeID, a.AssetTypeName`

    return await pool.request()
    .query(q)
}

exports.getAssetByAssetId = async (assetIds) => {
    const assetIdValues = assetIds.map(asset => `CONVERT(uniqueidentifier, '${asset}')`).join(',');
    return await pool.request()
    .query(`SELECT ID,Name FROM Asset WHERE ID IN (${assetIdValues})`)
}

exports.getAssetByDepartment = async (departmentIds) => {
    const departmentIdValues = departmentIds.map(dept => `CONVERT(uniqueidentifier, '${dept}')`).join(',');
    return await pool.request()
        .query(`SELECT * FROM AssetSite WHERE SiteID IN (${departmentIdValues})`)
}

exports.getChargerMap=async(siteID)=>{
    const q=` SELECT a.ID , a.Name, a.UnitID ,a.UnitID2, u.UnitType FROM Asset a  
    JOIN AssetSite as2 on as2.AssetID =a.ID
    JOIN Unit u on a.UnitID = u.ID
    WHERE as2.SiteID ='${siteID}' AND u.UnitType IN (5,6,7,8);`

    return await pool.request()
    .query(q)
}

exports.getFaultCodeByCharger=async(siteID)=>{
    const q=`SELECT COUNT(*) as TotalCount 
    FROM Asset a
    INNER JOIN AssetSite as2 on as2.AssetID =a.ID 
    WHERE a.[System] IN (5,6,7) AND as2.SiteID ='${siteID}';`

    const q1=`SELECT a.ID, a.Name, COUNT(*) AS Count
    FROM Asset a
    JOIN AlarmCharger ac ON ac.AssetID = a.ID
    WHERE ac.SiteID = '${siteID}'
      AND ac.EventCode >= 1152
      AND ac.EventCode <= 1165
    GROUP BY a.ID , a.Name;`
    
    return await pool.request()
    .query(q1)
}

exports.getFaultCodeByFaultCode=async(siteID)=>{
    const q=`SELECT LEFT(CONVERT(VARCHAR(MAX), ac.EventData, 2), 8) AS Name , count(*) as Count
            FROM AlarmCharger ac 
            WHERE ac.SiteID = '${siteID}'
            AND ac.EventCode >= 1152
            AND ac.EventCode <= 1165
            GROUP BY LEFT(CONVERT(VARCHAR(MAX), ac.EventData, 2), 8);`

const q1=`SELECT 
CASE LEFT(CONVERT(VARCHAR(MAX), ac.EventData, 2), 8)
  WHEN '00000001' THEN 'Charger Power Section Fault'
  WHEN '00000002' THEN 'Charger High Temperature'
  WHEN '00000004' THEN 'Invalid Battery Parameters'
  WHEN '00000008' THEN 'Charger Cannot Control Output Current'
  WHEN '00000010' THEN 'High Battery Temperature'
  WHEN '00000020' THEN 'Low Battery Temperature'
  WHEN '00000040' THEN 'High Battery Volatge'
  WHEN '00000080' THEN 'Low Battery Volatge'
  WHEN '00000100' THEN 'High Battery Resistance'
  WHEN '00000200' THEN 'Battery Temperature Sensor Out of Range'
  WHEN '00000400' THEN 'CAN Communication Fault to Battery Module'
  WHEN '00000800' THEN 'Pilot Fault to Battery Module'
  WHEN '00001000' THEN 'Charge Timeout Exceeded'
  WHEN '00002000' THEN 'Charge Ah Limit Exceeded'
  WHEN '00004000' THEN 'Contactor Fault'
  WHEN '00008000' THEN 'Battery Module Fault'
  WHEN '00010000' THEN 'No Battery Voltage Detected'
  WHEN '00020000' THEN 'No Charge Profile Created for Measured Battery Voltage'
  ELSE LEFT(CONVERT(VARCHAR(MAX), ac.EventData, 2), 8)
END AS Name,
COUNT(*) AS Count
FROM 
AlarmCharger ac
WHERE 
ac.SiteID = '${siteID}'
AND ac.EventCode >= 1152
AND ac.EventCode <= 1165
GROUP BY 
LEFT(CONVERT(VARCHAR(MAX), ac.EventData, 2), 8)
`

    return await pool.request()
    .query(q1)
}

exports.getUnitCount=async(siteID)=>{
    const q=`SELECT LastUpdate
    FROM Asset 
    INNER JOIN AssetSite ON Asset.ID = AssetSite.AssetID
    WHERE AssetSite.SiteID = '${siteID}'`

    return await pool.request()
    .query(q)
}

exports.getMaintenanceStatusReport=async(assetIds)=>{
    const assetIdValues = assetIds.map(asset => `CONVERT(uniqueidentifier, '${asset}')`).join(',');

    const q=`SELECT  
                SUM(CASE WHEN EventCode = 264 THEN 1 ELSE 0 END) AS Maintenance,
                SUM(CASE WHEN EventCode = 779 THEN 1 ELSE 0 END) AS Upcoming,
                SUM(CASE WHEN EventCode = 1289 THEN 1 ELSE 0 END) AS Completed,
                SUM(CASE WHEN EventCode = 785 THEN 1 ELSE 0 END) AS Overdue,
                COUNT(*) AS Total
            FROM Alarm
            WHERE AssetID IN (${assetIdValues});`

        return await pool.request()
            .query(q)
}

exports.getMaintenanceStatusDetails=async(assetIds)=>{
    const assetIdValues = assetIds.map(asset => `CONVERT(uniqueidentifier, '${asset}')`).join(',');
const q5=`SELECT 
a.Name AS AssetName,
a.AssetTypeName,
a.Frequency,
(
    SELECT TOP 1 
        CASE 
            WHEN EventCode IN (204, 778) THEN 'Overdue' 
            WHEN EventCode IN (779, 780) THEN 'Upcoming' 
            WHEN EventCode = 1289 THEN 'Completed' 
        END AS Status 
    FROM Alarm 
    WHERE AssetID = a.ID AND EventCode IN (204, 778, 779, 780, 1289)
    ORDER BY Date DESC
) AS Status,
(
    SELECT TOP 1 
        CAST(ROUND(CAST(SUBSTRING(CONVERT(varbinary, HMRData), 1, 4) AS int) / 3600.0, 0) AS int)
    FROM AlarmHMR h
    WHERE AssetID = a.ID AND CONVERT(date, Date) = (
        SELECT TOP 1 
            CONVERT(date, Date) 
        FROM Alarm 
        WHERE AssetID = a.ID AND EventCode = 1289
        ORDER BY Date DESC
    )
    ORDER BY Date DESC
        
) AS LastPerformed,
(
    SELECT TOP 1
        CAST(ROUND(CAST(SUBSTRING(CONVERT(varbinary, h.HMRData), 1, 4) AS int) / 3600.0, 0) AS int)
    FROM 
        AlarmHMR h 
    WHERE 
        h.AssetID = a.ID 
        AND EXISTS (
            SELECT 1 FROM Alarm WHERE AssetID = a.ID AND EventCode IN (204,778,779,780,1289)
        )
    ORDER BY 
        h.Date DESC 
) AS CurrentHMR
FROM 
Asset a 
WHERE 
a.ID IN (${assetIdValues})
`
return await pool.request()
            .query(q5)
}

exports.getPowerUsage = async(siteID,date)=>{
      const q3=`SELECT FORMAT(number, '00') + ':00' AS Hour,
      COUNT(CASE WHEN AlarmPowerUsage.InstantaneouskW > 0 THEN 1 END) AS Charger,
      COALESCE(MAX(AlarmPowerUsage.InstantaneouskW), 0) AS MaxkW
    FROM master..spt_values
    LEFT JOIN AlarmPowerUsage 
    ON FORMAT(DATEPART(hour, AlarmPowerUsage.Date), '00') + ':00' = FORMAT(number, '00') + ':00'
        AND AlarmPowerUsage.SiteID = '${siteID}'
        AND CONVERT(date, AlarmPowerUsage.Date) = '${date}'
    WHERE type = 'P' AND number BETWEEN 0 AND 23
    GROUP BY FORMAT(number, '00') + ':00'
    ORDER BY Hour;`

    
    return await pool.request()
            .query(q3)
}

exports.getPowerUsageCurrent=async(siteID,date)=>{

const q=`SELECT 
        FORMAT(DATEADD(HOUR, number, startHour), 'yyyy-MM-dd HH:00') AS Hour, 
        COALESCE(COUNT(CASE WHEN AlarmPowerUsage.InstantaneouskW > 0 THEN 1 END), 0) AS Charger, 
        COALESCE(MAX(AlarmPowerUsage.InstantaneouskW), 0) AS MaxkW 
        FROM (
        SELECT 
        DATEADD(HOUR, -23, DATEADD(HOUR, DATEDIFF(HOUR, 0, GETDATE()), 0)) AS startHour,
        0 AS number
        UNION ALL
        SELECT 
        DATEADD(HOUR, -23, DATEADD(HOUR, DATEDIFF(HOUR, 0, GETDATE()), 0)) AS startHour,
        number + 1 AS number
        FROM master..spt_values 
        WHERE type = 'P' AND number BETWEEN 0 AND 22
        ) AS hours
        LEFT JOIN AlarmPowerUsage 
        ON FORMAT(AlarmPowerUsage.Date, 'yyyy-MM-dd HH:00') = FORMAT(DATEADD(HOUR, number, startHour), 'yyyy-MM-dd HH:00') 
        AND AlarmPowerUsage.SiteID = '${siteID}'
        AND AlarmPowerUsage.Date >= DATEADD(HOUR, -23, DATEADD(HOUR, DATEDIFF(HOUR, 0, GETDATE()), 0))
        AND AlarmPowerUsage.Date < DATEADD(HOUR, DATEDIFF(HOUR, 0, '${date}'), 0)
        WHERE hours.startHour >= DATEADD(HOUR, -23, DATEADD(HOUR, DATEDIFF(HOUR, 0, GETDATE()), 0))
        GROUP BY FORMAT(DATEADD(HOUR, number, startHour), 'yyyy-MM-dd HH:00')
        ORDER BY Hour;`

    return await pool.request()
            .query(q)
}

exports.getMapDetails=async(siteID)=>{
const w=`SELECT a.ID as AssetID, a.Name, a.Lon, a.Lat, c1.Port as Port1, c1.ID as ID1,c2.Port as Port2, c2.ID as ID2, c1.status as Status1,c2.status as Status2,
            CASE 
            WHEN u1.UniqueID = CONVERT(VARBINARY(8), c1.VirtualUniqueID, 2) AND a.UnitID = u1.ID
                THEN COALESCE(a.Name, STUFF(CONVERT(VARCHAR(50), CONVERT(VARBINARY(8), CAST(c1.BatteryModule AS BIGINT)), 2), 1, PATINDEX('%[^0]%', CONVERT(VARCHAR(50), CONVERT(VARBINARY(8), CAST(c1.BatteryModule AS BIGINT)), 2)) - 1, ''))
            ELSE STUFF(CONVERT(VARCHAR(50), CONVERT(VARBINARY(8), CAST(c1.BatteryModule AS BIGINT)), 2), 1, PATINDEX('%[^0]%', CONVERT(VARCHAR(50), CONVERT(VARBINARY(8), CAST(c1.BatteryModule AS BIGINT)), 2)) - 1, '') 
            END AS Paired1,
            CASE 
            WHEN u2.UniqueID = CONVERT(VARBINARY(8), c2.VirtualUniqueID, 2) AND a.UnitID = u2.ID
                THEN COALESCE(a.Name, STUFF(CONVERT(VARCHAR(50), CONVERT(VARBINARY(8), CAST(c2.BatteryModule AS BIGINT)), 2), 1, PATINDEX('%[^0]%', CONVERT(VARCHAR(50), CONVERT(VARBINARY(8), CAST(c2.BatteryModule AS BIGINT)), 2)) - 1, ''))
            ELSE STUFF(CONVERT(VARCHAR(50), CONVERT(VARBINARY(8), CAST(c2.BatteryModule AS BIGINT)), 2), 1, PATINDEX('%[^0]%', CONVERT(VARCHAR(50), CONVERT(VARBINARY(8), CAST(c2.BatteryModule AS BIGINT)), 2)) - 1, '')
            END AS Paired2,
            CONVERT(INT, CONVERT(VARBINARY(4), SUBSTRING(c1.EventData, 7, 4), 2)) * 100 AS Power1,
            CONVERT(INT, CONVERT(VARBINARY(2), SUBSTRING(c1.EventData, 5, 2), 2)) * 100 AS Voltage1,
            CONVERT(INT, CONVERT(VARBINARY(1), SUBSTRING(c1.EventData, 19, 1), 2)) AS SoC1,

            CONVERT(INT, CONVERT(VARBINARY(4), SUBSTRING(c2.EventData, 7, 4), 2)) * 100 AS Power2,
            CONVERT(INT, CONVERT(VARBINARY(2), SUBSTRING(c2.EventData, 5, 2), 2)) * 100 AS Voltage2,
            CONVERT(INT, CONVERT(VARBINARY(1), SUBSTRING(c2.EventData, 19, 1), 2)) AS SoC2

        FROM Asset a
        JOIN AssetSite jas ON a.ID = jas.AssetID
        LEFT JOIN (
            SELECT acd.AssetID, acd.Port, acd.ID, acd.EventData, 
                    CASE 
                    WHEN latest.Ended IS NULL THEN 'Faulted'
                    WHEN latest.EventCode = 1792 THEN 'Charging'
                    WHEN latest.EventCode = 1794 THEN 'Equalizing'
                    WHEN latest.EventCode = 1796 THEN 'Idling'
                    END AS status, 
                    acd.BatteryModule, acd.VirtualUniqueID, u.ID AS UnitID, u.UniqueID AS Converted, 
                    ROW_NUMBER() OVER (PARTITION BY acd.AssetID, acd.Port ORDER BY acd.Date DESC) AS rn
                    FROM AlarmChargeDetail acd
                    JOIN AlarmCharger ac ON acd.AssetID = ac.AssetID AND acd.Port = ac.Port
                    LEFT JOIN Unit u ON acd.UnitID = u.ID
                        LEFT JOIN (
                        SELECT AssetID, Port, Ended, EventCode
                        FROM (
                        SELECT AssetID, Port, Ended, EventCode, ROW_NUMBER() OVER (PARTITION BY AssetID, Port ORDER BY Date DESC) AS rn
                        FROM AlarmCharger
                        WHERE Ended IS NOT NULL AND EventCode IN (1792, 1794, 1796)
                        ) latest
                        WHERE rn = 1
                        ) latest ON acd.AssetID = latest.AssetID AND acd.Port = latest.Port
                        WHERE acd.Port = 1
        ) c1 ON a.ID = c1.AssetID AND c1.rn = 1
        LEFT JOIN Unit u1 ON c1.UnitID = u1.ID AND c1.Converted = CONVERT(VARBINARY(8), c1.VirtualUniqueID, 2)

        LEFT JOIN (
            SELECT acd.AssetID, acd.Port, acd.ID, acd.EventData, 
                CASE 
                WHEN latest.Ended IS NULL THEN 'Faulted'
                WHEN latest.EventCode = 1792 THEN 'Charging'
                WHEN latest.EventCode = 1794 THEN 'Equalizing'
                WHEN latest.EventCode = 1796 THEN 'Idling'
                END AS status, 
            acd.BatteryModule, acd.VirtualUniqueID, u.ID AS UnitID, u.UniqueID AS Converted, 
            ROW_NUMBER() OVER (PARTITION BY acd.AssetID, acd.Port ORDER BY acd.Date DESC) AS rn
            FROM AlarmChargeDetail acd
            JOIN AlarmCharger ac ON acd.AssetID = ac.AssetID AND acd.Port = ac.Port
            LEFT JOIN Unit u ON acd.UnitID = u.ID
            LEFT JOIN (
            SELECT AssetID, Port, Ended, EventCode
            FROM (
            SELECT AssetID, Port, Ended, EventCode, ROW_NUMBER() OVER (PARTITION BY AssetID, Port ORDER BY Date DESC) AS rn
            FROM AlarmCharger
            WHERE Ended IS NOT NULL AND EventCode IN (1792, 1794, 1796)
            ) latest
            WHERE rn = 1
            ) latest ON acd.AssetID = latest.AssetID AND acd.Port = latest.Port
            WHERE acd.Port = 2
        ) c2 ON a.ID = c2.AssetID AND c2.rn = 1
        LEFT JOIN Unit u2 ON c2.UnitID = u2.ID AND c2.Converted = CONVERT(VARBINARY(8), c2.VirtualUniqueID, 2)
    WHERE jas.SiteID = '${siteID}' 
    AND a.System IN (5,6,7,11)
    AND a.Lon IS NOT NULL
    AND a.Lat IS NOT NULL;`

const q1=`Select a.ID As AssetID, a.Name, a.Lon, a.Lat, c1.Port As Port1, c1.ID As ID1,
c2.Port As Port2, c2.ID As ID2, c1.status As Status1, c2.status As Status2,
Case
  When u1.UniqueID = Convert(VARBINARY(8),c1.VirtualUniqueID,2) And a.UnitID = u1.ID Then Coalesce(a.Name,Stuff(Convert(VARCHAR(50),Convert(VARBINARY(8),Cast(c1.BatteryModule As BIGINT)),2), 1, PatIndex('%[^0]%',Convert(VARCHAR(50),Convert(VARBINARY(8),Cast(c1.BatteryModule As BIGINT)),2)) - 1, ''))
  Else Stuff(Convert(VARCHAR(50),Convert(VARBINARY(8),Cast(c1.BatteryModule As BIGINT)),2), 1, PatIndex('%[^0]%',Convert(VARCHAR(50),Convert(VARBINARY(8),Cast(c1.BatteryModule As BIGINT)),2)) - 1, '')
End As Paired1, 
Case
  When u2.UniqueID = Convert(VARBINARY(8),c2.VirtualUniqueID,2) And a.UnitID = u2.ID Then Coalesce(a.Name,Stuff(Convert(VARCHAR(50),Convert(VARBINARY(8),Cast(c2.BatteryModule As BIGINT)),2), 1, PatIndex('%[^0]%',Convert(VARCHAR(50),Convert(VARBINARY(8),Cast(c2.BatteryModule As BIGINT)),2)) - 1, ''))
  Else Stuff(Convert(VARCHAR(50),Convert(VARBINARY(8),Cast(c2.BatteryModule As BIGINT)),2), 1, PatIndex('%[^0]%',Convert(VARCHAR(50),Convert(VARBINARY(8),Cast(c2.BatteryModule As BIGINT)),2)) - 1, '')
End As Paired2, 
Convert(INT,Convert(VARBINARY(4),SubString(c1.EventData, 7,4),2)) * 100 As Power1,
Convert(INT,Convert(VARBINARY(2),SubString(c1.EventData, 5, 2),2)) *100 As Voltage1, 
Convert(INT,Convert(VARBINARY(1),SubString(c1.EventData, 19,1),2)) As SoC1,

Convert(INT,Convert(VARBINARY(4),SubString(c2.EventData, 7,4),2)) * 100 As Power2,
Convert(INT,Convert(VARBINARY(2),SubString(c2.EventData, 5, 2),2)) *100 As Voltage2, 
Convert(INT,Convert(VARBINARY(1),SubString(c2.EventData, 19,1),2)) As SoC2 
From Asset a
Inner Join AssetSite jas On a.ID = jas.AssetID
Left Join (Select acd.AssetID, acd.Port, acd.ID, acd.EventData, Case
      When latest.Ended Is Null Then 'Faulted'
      When latest.EventCode = 1792 Then 'Charging'
      When latest.EventCode = 1794 Then 'Equalizing'
      When latest.EventCode = 1796 Then 'Idling'
    End As status, acd.BatteryModule, acd.VirtualUniqueID, u.ID As UnitID,
    u.UniqueID As Converted, Row_Number() Over (Partition By acd.AssetID,
    acd.Port Order By acd.Date Desc) As rn From AlarmChargeDetail acd
    Inner Join AlarmCharger ac On acd.AssetID = ac.AssetID And
        acd.Port = ac.Port
    Left Join Unit u On acd.UnitID = u.ID
    Left Join (Select latest.AssetID, latest.Port, latest.Ended,
        latest.EventCode
      From (Select AssetID, Port, Ended, EventCode, Row_Number() Over
            (Partition By AssetID, Port Order By Date Desc) As rn
          From AlarmCharger
          Where Ended Is Not Null And EventCode In (1792, 1794, 1796)) latest
      Where latest.rn = 1) latest On acd.AssetID = latest.AssetID And
        acd.Port = latest.Port
  Where acd.Port = 1) c1 On a.ID = c1.AssetID And c1.rn = 1
Left Join Unit u1 On c1.UnitID = u1.ID And
    c1.Converted = Convert(VARBINARY(8),c1.VirtualUniqueID,2)
Left Join (Select acd.AssetID, acd.Port, acd.ID, acd.EventData, Case
      When latest.Ended Is Null Then 'Faulted'
      When latest.EventCode = 1792 Then 'Charging'
      When latest.EventCode = 1794 Then 'Equalizing'
      When latest.EventCode = 1796 Then 'Idling'
    End As status, acd.BatteryModule, acd.VirtualUniqueID, u.ID As UnitID,
    u.UniqueID As Converted, Row_Number() Over (Partition By acd.AssetID,
    acd.Port Order By acd.Date Desc) As rn From AlarmChargeDetail acd
    Inner Join AlarmCharger ac On acd.AssetID = ac.AssetID And
        acd.Port = ac.Port
    Left Join Unit u On acd.UnitID = u.ID
    Left Join (Select latest.AssetID, latest.Port, latest.Ended,
        latest.EventCode
      From (Select AssetID, Port, Ended, EventCode, Row_Number() Over
            (Partition By AssetID, Port Order By Date Desc) As rn
          From AlarmCharger
          Where Ended Is Not Null And EventCode In (1792, 1794, 1796)) latest
      Where latest.rn = 1) latest On acd.AssetID = latest.AssetID And
        acd.Port = latest.Port
  Where acd.Port = 2) c2 On a.ID = c2.AssetID And c2.rn = 1
Left Join Unit u2 On c2.UnitID = u2.ID And
    c2.Converted = Convert(VARBINARY(8),c2.VirtualUniqueID,2)
Where jas.SiteID = '${siteID}' And a.System In (5, 6, 7, 11) `    
    return await pool.request()
            .query(w)    
}
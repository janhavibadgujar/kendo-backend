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

    return await pool.request()
    .query(q)
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

const q=`SELECT a.ID, a.Name, a.Lon, a.Lat, 
CONVERT(INT, CONVERT(VARBINARY(4), SUBSTRING(c1.EventData, 7, 4), 2)) AS Power1,
CONVERT(INT, CONVERT(VARBINARY(2), SUBSTRING(c1.EventData, 5, 2), 2)) * 100 AS Voltage1,
CONVERT(INT, CONVERT(VARBINARY(1), SUBSTRING(c1.EventData, 19, 1), 2)) AS SoC1,
c1.Port as Port1, c1.ID as ID1,
CASE WHEN c1.Port IS NULL THEN NULL 
     WHEN c1.Ended IS NULL THEN 'Faulted' 
     ELSE CASE c1.EventCode 
          WHEN 1792 THEN 'Charging' 
          WHEN 1794 THEN 'Equalizing' 
          WHEN 1796 THEN 'Idling' 
          ELSE 'N/A' 
          END 
     END AS Status1,
CONVERT(INT, CONVERT(VARBINARY(4), SUBSTRING(c2.EventData, 7, 4), 2)) AS Power2,
CONVERT(INT, CONVERT(VARBINARY(2), SUBSTRING(c2.EventData, 5, 2), 2)) * 100 AS Voltage2,
CONVERT(INT, CONVERT(VARBINARY(1), SUBSTRING(c2.EventData, 19, 1), 2)) AS SoC2,
c2.Port as Port2, c2.ID as ID2,
CASE WHEN c2.Port IS NULL THEN NULL 
     WHEN c2.Ended IS NULL THEN 'Faulted' 
     ELSE CASE c2.EventCode 
          WHEN 1792 THEN 'Charging' 
          WHEN 1794 THEN 'Equalizing' 
          WHEN 1796 THEN 'Idling' 
          ELSE 'N/A' 
          END 
     END AS Status2
FROM Asset a
JOIN AssetSite jas ON a.ID = jas.AssetID
LEFT JOIN (
  SELECT acd.AssetID, acd.Port, acd.ID, acd.EventData, Ended, EventCode, ROW_NUMBER() OVER (PARTITION BY acd.AssetID, acd.Port ORDER BY acd.Date DESC) AS rn
  FROM AlarmChargeDetail acd
  JOIN AlarmCharger ac ON acd.AssetID = ac.AssetID AND acd.Port = ac.Port
  WHERE acd.Port = 1
  ) c1 ON a.ID = c1.AssetID AND c1.rn = 1
  LEFT JOIN (
    SELECT acd.AssetID, acd.Port, acd.ID, acd.EventData, Ended, EventCode, ROW_NUMBER() OVER (PARTITION BY acd.AssetID, acd.Port ORDER BY acd.Date DESC) AS rn
    FROM AlarmChargeDetail acd
    JOIN AlarmCharger ac ON acd.AssetID = ac.AssetID AND acd.Port = ac.Port
    WHERE acd.Port = 2
    ) c2 ON a.ID = c2.AssetID AND c2.rn = 1
WHERE jas.SiteID= '${siteID}' 
AND a.System IN (5,6,7,11)
AND a.Lon IS NOT NULL
AND a.Lat IS NOT NULL;
`

    return await pool.request()
            .query(q)    
}
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

const q=`WITH cte_latest_alarm_charger AS (
    SELECT AssetID, Port, Ended, EventCode, EventData,
        ROW_NUMBER() OVER (PARTITION BY AssetID ORDER BY Date DESC) AS rn
    FROM AlarmCharger
), cte_latest_event_code AS (
    SELECT AssetID, EventCode,
        ROW_NUMBER() OVER (PARTITION BY AssetID ORDER BY Date DESC) AS rn
    FROM AlarmCharger
    WHERE EventCode IN (1792, 1794, 1796)
), cte_latest_charge_detail AS (
    SELECT AssetID, BatteryModule, VirtualUniqueID, UnitID,
        CONVERT(VARCHAR(MAX), CONVERT(VARBINARY(MAX), VirtualUniqueID), 2) AS VirtualUniqueIDHex
    FROM AlarmChargeDetail
), cte_converted_unique_id AS (
    SELECT AssetID, VirtualUniqueID, UnitID, 
        CASE WHEN Unit.ID IS NOT NULL THEN CONVERT(VARCHAR(MAX), Unit.UniqueId) ELSE CONVERT(VARCHAR(MAX), CONVERT(VARBINARY(MAX), BatteryModule), 2) END AS ConvertedUniqueID
    FROM cte_latest_charge_detail
    LEFT JOIN Unit ON Unit.ID = cte_latest_charge_detail.UnitID AND Unit.UniqueId = CONVERT(VARCHAR(MAX), cte_latest_charge_detail.VirtualUniqueID, 2)
), cte_asset AS (
    SELECT ID, Name, UnitID
    FROM Asset
    WHERE UnitID IN (SELECT UnitID FROM cte_converted_unique_id)
), cte_selected_assets AS (
    SELECT AssetSite.AssetID, cte_latest_alarm_charger.Port, cte_latest_alarm_charger.Ended, 
        cte_latest_event_code.EventCode, cte_latest_alarm_charger.EventData
    FROM AssetSite
    LEFT JOIN cte_latest_alarm_charger ON cte_latest_alarm_charger.AssetID = AssetSite.AssetID AND cte_latest_alarm_charger.rn = 1
    LEFT JOIN cte_latest_event_code ON cte_latest_event_code.AssetID = AssetSite.AssetID AND cte_latest_event_code.rn = 1
    WHERE AssetSite.SiteID = '${siteID}'
)
SELECT cte_selected_assets.AssetID, cte_asset.Name, cte_selected_assets.Port, 
    CASE WHEN cte_selected_assets.Ended IS NULL THEN 'Faulted' ELSE 
        CASE cte_selected_assets.EventCode 
            WHEN 1792 THEN 'Charging' 
            WHEN 1794 THEN 'Equalizing' 
            WHEN 1796 THEN 'Idling' 
        END 
    END AS EndedStatus, 
    CONVERT(BIGINT, SUBSTRING(cte_selected_assets.EventData, 7, 4), 2) * 100 AS Power, 
    CONVERT(BIGINT, SUBSTRING(cte_selected_assets.EventData, 9, 2), 2) * 100 AS Voltage, 
    CONVERT(INT, SUBSTRING(cte_selected_assets.EventData, 19, 1), 2) AS SoC
FROM cte_selected_assets
LEFT JOIN cte_asset ON cte_asset.ID = cte_selected_assets.AssetID`

const q1=`WITH AssetSite_CTE AS (
    SELECT AssetID
    FROM AssetSite
    WHERE SiteID = '${siteID}'
), AlarmCharger_CTE AS (
    SELECT Port, AssetID, Ended, EventCode, CONVERT(VARCHAR(MAX), EventData, 2) AS EventData, ROW_NUMBER() OVER (PARTITION BY AssetID ORDER BY Date DESC) AS RowNum
    FROM AlarmCharger
), AlarmChargeDetail_CTE AS (
    SELECT BatteryModule, VirtualUniqueID, UnitID, AssetID, CONVERT(VARCHAR(MAX), Date, 120) AS Date, ROW_NUMBER() OVER (PARTITION BY AssetID ORDER BY Date DESC) AS RowNum
    FROM AlarmChargeDetail
), Unit_CTE AS (
    SELECT u.ID, u.UniqueId, a.Name
    FROM Unit u
    LEFT JOIN Asset a ON a.UnitID = u.ID
), Converted_VirtualUniqueID_CTE AS (
    SELECT AssetID, CAST(CONVERT(VARCHAR(MAX), CONVERT(VARBINARY(MAX), VirtualUniqueID), 2) AS INT) AS Converted_VirtualUniqueID, Date
    FROM AlarmChargeDetail_CTE
    WHERE RowNum = 1
), Filtered_AlarmCharger_CTE AS (
    SELECT Port, AssetID, Ended, EventCode, EventData, RowNum
    FROM AlarmCharger_CTE
    WHERE RowNum = 1
)
SELECT a.AssetID, ac.Port, 
    CASE WHEN ac.Ended IS NULL THEN 'Faulted'
         ELSE CASE ac.EventCode
                  WHEN 1792 THEN 'Charging'
                  WHEN 1794 THEN 'Equalizing'
                  WHEN 1796 THEN 'Idling'
              END
    END AS Ended, 
    CONVERT(INT, CONVERT(VARBINARY(4), SUBSTRING(ac.EventData, 7, 4), 2)) * 100 AS Power,
    CONVERT(INT, CONVERT(VARBINARY(2), SUBSTRING(ac.EventData, 5, 2), 2)) * 100 AS Voltage,
    CONVERT(INT, CONVERT(VARBINARY(1), SUBSTRING(ac.EventData, 19, 1), 2)) AS SoC,
    ac.EventData, ac.Ended, 
    ac.EventCode, ac.RowNum,
    ac2.EventData AS BatteryModule, cvu.Converted_VirtualUniqueID,
    u.Name AS UnitName
FROM AssetSite_CTE a
LEFT JOIN Filtered_AlarmCharger_CTE ac ON a.AssetID = ac.AssetID
LEFT JOIN AlarmChargeDetail_CTE acd ON a.AssetID = acd.AssetID AND acd.RowNum = 1
LEFT JOIN Converted_VirtualUniqueID_CTE cvu ON a.AssetID = cvu.AssetID
LEFT JOIN Unit_CTE u ON acd.UnitID = u.ID AND (u.UniqueId = cvu.Converted_VirtualUniqueID OR acd.BatteryModule = CONVERT(VARBINARY(MAX), CONVERT(INT, CONVERT(VARBINARY(MAX), cvu.Converted_VirtualUniqueID)), 2))
LEFT JOIN AlarmCharger_CTE ac2 ON a.AssetID = ac2.AssetID AND ac2.EventCode = 1798 AND ac2.RowNum = 1
`

const d=`WITH LatestCharger AS (
    SELECT ac.AssetID, ac.Port, ac.Ended, ac.EventCode, ac.EventData
    FROM AlarmCharger ac
    WHERE ac.Date = (SELECT MAX(Date) FROM AlarmCharger WHERE AssetID = ac.AssetID)
), LatestChargeDetail AS (
    SELECT acd.AssetID, acd.BatteryModule, acd.VirtualUniqueID, acd.UnitID
    FROM AlarmChargeDetail acd
    WHERE acd.Date = (SELECT MAX(Date) FROM AlarmChargeDetail WHERE AssetID = acd.AssetID)
), Units AS (
    SELECT u.ID, u.UniqueId, a.Name
    FROM Unit u
    LEFT JOIN Asset a ON a.UnitID = u.ID
)
SELECT asst.AssetID, lc.Port, 
    CASE WHEN lc.Ended IS NULL THEN
        CASE lc.EventCode
            WHEN 1792 THEN 'Charging'
            WHEN 1794 THEN 'Equalizing'
            WHEN 1796 THEN 'Idling'
        END
    ELSE CONVERT(varchar(30), lc.Ended, 120)
    END AS ChargerStatus,
    CONVERT(INT, CONVERT(VARBINARY(4), SUBSTRING(lc.EventData, 7, 4), 2)) * 100 AS Power,
    CONVERT(INT, CONVERT(VARBINARY(2), SUBSTRING(lc.EventData, 5, 2), 2)) * 100 AS Voltage,
    CONVERT(INT, CONVERT(VARBINARY(1), SUBSTRING(lc.EventData, 19, 1), 2)) AS SoC,
    CASE WHEN ld.VirtualUniqueID LIKE '0x%' THEN CONVERT(int, ld.VirtualUniqueID, 16)
        ELSE CONVERT(int, '0x' + ld.VirtualUniqueID, 16)
    END AS ConvertedVirtualUniqueID,
    CASE WHEN u.ID IS NOT NULL THEN u.Name ELSE CONVERT(varchar(30), CONVERT(varbinary(4), ld.BatteryModule), 2) END AS BatteryModule
FROM AssetSite asst
JOIN LatestCharger lc ON lc.AssetID = asst.AssetID
LEFT JOIN LatestChargeDetail ld ON ld.AssetID = asst.AssetID
LEFT JOIN Units u ON u.ID = ld.UnitID AND 
    (ld.VirtualUniqueID LIKE '0x%' AND u.UniqueId = CONVERT(varchar(30), CONVERT(varbinary(4), SUBSTRING(ld.VirtualUniqueID, 3, LEN(ld.VirtualUniqueID)), 1), 2))
WHERE asst.SiteID = '${siteID}'
`

const d1=`SELECT 
ad.BatteryModule,
ad.VirtualUniqueID,
ad.UnitID,
COALESCE(a.Name, CONVERT(VARCHAR(50), ad.BatteryModule, 2)) AS AssetName,
COALESCE(u.SerialNumber, '') AS UnitSerialNumber
FROM AssetSite ASite
INNER JOIN Asset AS a ON ASite.AssetID = a.ID
CROSS APPLY (
SELECT TOP 1 
    ac.Port, 
    ac.Ended, 
    ac.EventCode,
    CONVERT(INT, SUBSTRING(CONVERT(VARCHAR(32), ac.EventData, 2), 7, 2), 1) * 100 AS Voltage,
    CONVERT(INT, SUBSTRING(CONVERT(VARCHAR(32), ac.EventData, 2), 9, 4), 1) * 100 AS Power,
    CASE SUBSTRING(CONVERT(VARCHAR(32), ac.EventData, 2), 19, 2) 1
        WHEN '00' THEN 0
        ELSE CONVERT(INT, SUBSTRING(CONVERT(VARCHAR(32), ac.EventData, 2), 19, 2), 1) 
    END AS SoC
FROM AlarmCharger AS ac
WHERE ac.AssetID = a.ID
ORDER BY ac.Date DESC
) acd
LEFT JOIN AlarmChargeDetail AS ad ON ad.AssetID = a.ID
CROSS APPLY (
SELECT TOP 1 u.ID, a.Name
FROM Unit AS u
WHERE u.ID = ad.UnitID AND u.UniqueId = CONVERT(VARCHAR(50), CONVERT(BIGINT, SUBSTRING(ad.VirtualUniqueID, 3, LEN(ad.VirtualUniqueID))), 2)
) u
WHERE ASite.SiteID = '${siteID}'
`
    return await pool.request()
            .query(d1)    
}
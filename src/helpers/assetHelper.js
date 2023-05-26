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
    
    const q=`SELECT a.ID, a.Name, COUNT(*) AS Count
    FROM Asset a
    JOIN AlarmCharger ac ON ac.AssetID = a.ID
    WHERE ac.SiteID = '${siteID}'
      AND ac.EventCode >= 1152
      AND ac.EventCode <= 1165
    GROUP BY a.ID , a.Name;`
    
    return await pool.request()
    .query(q)
}

exports.getFaultCodeByFaultCode=async(siteID)=>{
   
    const q=`SELECT 
CASE LEFT(CONVERT(VARCHAR(MAX), ac.EventData, 2), 8)
  WHEN '00000001' THEN 'Charger Power Section Fault'
  WHEN '00000002' THEN 'Charger High Temperature'
  WHEN '00000004' THEN 'Invalid Battery Parameters'
  WHEN '00000008' THEN 'Charger Cannot Control Output Current'
  WHEN '00000010' THEN 'High Battery Temperature'
  WHEN '00000020' THEN 'Low Battery Temperature'
  WHEN '00000040' THEN 'High Battery Voltage'
  WHEN '00000080' THEN 'Low Battery Voltage'
  WHEN '00000100' THEN 'High Battery Resistance'
  WHEN '00000200' THEN 'Battery Temperature Sensor Out of Range'
  WHEN '00000400' THEN 'Can Communication Fault to Battery Module'
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
    .query(q)
}

exports.getUnitCount=async(siteID)=>{
    const q=`SELECT LastUpdate
    FROM Asset 
    INNER JOIN AssetSite ON Asset.ID = AssetSite.AssetID
    WHERE AssetSite.SiteID = '${siteID}'`

    const q1=`SELECT COUNT(*) AS OnlineCount,
    (SELECT COUNT(*) FROM Asset INNER JOIN AssetSite ON Asset.ID = AssetSite.AssetID WHERE AssetSite.SiteID = '${siteID}') AS OfflineCount
FROM Asset 
INNER JOIN AssetSite ON Asset.ID = AssetSite.AssetID
WHERE AssetSite.SiteID = '${siteID}'
AND LastUpdate >= DATEADD(MINUTE, -5, GETDATE())
`

    return await pool.request()
    .query(q1)
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
    const q=`SELECT 
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
            .query(q)
}

exports.getPowerUsage = async(siteID,date)=>{
      const q=`SELECT FORMAT(number, '00') + ':00' AS Hour,
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

    const w=`SELECT FORMAT(number, '00') + ':00' AS Hour,
    COUNT(CASE WHEN AlarmPowerUsage.InstantaneouskW > 0 THEN 1 END) AS Charger,
    COALESCE(MAX(AlarmPowerUsage.InstantaneouskW), 0) AS MaxkW
    FROM master..spt_values
    LEFT JOIN AlarmPowerUsage
    ON FORMAT(DATEPART(hour, DATEADD(hour,
    CASE
    WHEN (SELECT Timezone FROM Site WHERE ID = '${siteID}') IN ('11', '12', '13', '14', '15') THEN -6
    WHEN (SELECT Timezone FROM Site WHERE ID = '${siteID}') = '19' THEN -4.5
    WHEN (SELECT Timezone FROM Site WHERE ID = '${siteID}') = '61' THEN 4.5
    WHEN (SELECT Timezone FROM Site WHERE ID = '${siteID}') IN ('62', '63', '64') THEN 5
    ELSE -12
    END,
    AlarmPowerUsage.Date)), '00') + ':00' = FORMAT(number, '00') + ':00'
    AND AlarmPowerUsage.SiteID = '${siteID}'
    AND CONVERT(date, AlarmPowerUsage.Date) = '${date}'
    WHERE type = 'P' AND number BETWEEN 0 AND 23
    GROUP BY FORMAT(number, '00') + ':00'
    ORDER BY Hour;`

    
    return await pool.request()
            .query(q)
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



const w=`WITH HourOffsets AS (
    SELECT Site.ID, 
    CASE 
    WHEN Site.Timezone IN ('1') THEN -12.0
        WHEN Site.Timezone IN ('2') THEN -11.0
        WHEN Site.Timezone IN ('3') THEN -10.0
        WHEN Site.Timezone IN ('4') THEN -9.0
        WHEN Site.Timezone IN ('5', '6') THEN -8.0
        WHEN Site.Timezone IN ('7', '8', '9', '10') THEN -7.0
        WHEN Site.Timezone IN ('11', '12', '13', '14', '15') THEN -6.0
        WHEN Site.Timezone IN ('16', '17', '18') THEN -5.0
        WHEN Site.Timezone IN ('19') THEN -4.5
        WHEN Site.Timezone IN ('20', '21', '22', '23') THEN -4.0
        WHEN Site.Timezone IN ('24') THEN -3.5
        WHEN Site.Timezone IN ('25', '26', '27', '28', '29') THEN -3.0
        WHEN Site.Timezone IN ('30') THEN -2.0
        WHEN Site.Timezone IN ('31', '32') THEN -1.0
        WHEN Site.Timezone IN ('33', '34', '35') THEN 0.0
        WHEN Site.Timezone IN ('36', '37', '38', '39', '40') THEN 1.0
        WHEN Site.Timezone IN ('41', '42', '43', '44', '45', '46', '47', '48', '49') THEN 2.0
        WHEN Site.Timezone IN ('50', '51', '52', '53', '54') THEN 3.0
        WHEN Site.Timezone IN ('55') THEN 3.5
        WHEN Site.Timezone IN ('56', '57', '58', '59', '60') THEN 4.0
        WHEN Site.Timezone IN ('61') THEN 4.5
        WHEN Site.Timezone IN ('62', '63', '64') THEN 5.0
        WHEN Site.Timezone IN ('65', '66') THEN 5.5
        WHEN Site.Timezone IN ('67') THEN 5.75
        WHEN Site.Timezone IN ('68', '69') THEN 6.0
        WHEN Site.Timezone IN ('70') THEN 6.5
        WHEN Site.Timezone IN ('71', '72') THEN 7.0
        WHEN Site.Timezone IN ('73', '74', '75', '76', '77') THEN 8.0
        WHEN Site.Timezone IN ('78', '79', '80') THEN 9.0
        WHEN Site.Timezone IN ('81', '82') THEN 9.5
        WHEN Site.Timezone IN ('83', '84', '85', '86', '87') THEN 10.0
        WHEN Site.Timezone IN ('88') THEN 11.0
        WHEN Site.Timezone IN ('89', '90') THEN 12.0
        WHEN Site.Timezone IN ('91') THEN 13.0
    ELSE '0'
END AS HourOffset

    FROM Site 
    WHERE Site.ID = '${siteID}'
  ) 
  
  SELECT FORMAT(DATEADD(HOUR, HO.HourOffset + number, startHour), 'yyyy-MM-dd HH:00') AS Hour, 
    COALESCE(COUNT(CASE WHEN AlarmPowerUsage.InstantaneouskW > 0 THEN 1 END), 0) AS Charger, 
    COALESCE(MAX(AlarmPowerUsage.InstantaneouskW), 0) AS MaxkW 
  FROM (
    SELECT DATEADD(HOUR, -23, DATEADD(HOUR, DATEDIFF(HOUR, 0, GETDATE()), 0)) AS startHour, 0 AS number 
    UNION ALL 
    SELECT DATEADD(HOUR, -23, DATEADD(HOUR, DATEDIFF(HOUR, 0, GETDATE()), 0)) AS startHour, number + 1 AS number 
    FROM master..spt_values 
    WHERE type = 'P' AND number BETWEEN 0 AND 22 
  ) AS hours 
  CROSS JOIN HourOffsets AS HO 
  LEFT JOIN AlarmPowerUsage ON FORMAT(DATEADD(HOUR, HO.HourOffset + number, startHour), 'yyyy-MM-dd HH:00') = FORMAT(DATEADD(HOUR, -1 * (HO.HourOffset + number), GETDATE()), 'yyyy-MM-dd HH:00') 
    AND AlarmPowerUsage.SiteID = '${siteID}' 
    AND AlarmPowerUsage.Date >= DATEADD(HOUR, -23, DATEADD(HOUR, DATEDIFF(HOUR, 0, GETDATE()), 0)) 
    AND AlarmPowerUsage.Date < DATEADD(HOUR, DATEDIFF(HOUR, 0, '${date}'), 0) 
  WHERE hours.startHour >= DATEADD(HOUR, -23, DATEADD(HOUR, DATEDIFF(HOUR, 0, GETDATE()), 0)) 
  GROUP BY FORMAT(DATEADD(HOUR, HO.HourOffset + number, startHour), 'yyyy-MM-dd HH:00'), HO.HourOffset 
  ORDER BY Hour`

const w1=`WITH HourOffsets AS (
    SELECT Site.ID, 
      CASE 
      WHEN Site.Timezone IN ('1') THEN -12.0
      WHEN Site.Timezone IN ('2') THEN -11.0
      WHEN Site.Timezone IN ('3') THEN -10.0
      WHEN Site.Timezone IN ('4') THEN -9.0
      WHEN Site.Timezone IN ('5', '6') THEN -8.0
      WHEN Site.Timezone IN ('7', '8', '9', '10') THEN -7.0
      WHEN Site.Timezone IN ('11', '12', '13', '14', '15') THEN -6.0
      WHEN Site.Timezone IN ('16', '17', '18') THEN -5.0
      WHEN Site.Timezone IN ('19') THEN -4.5
      WHEN Site.Timezone IN ('20', '21', '22', '23') THEN -4.0
      WHEN Site.Timezone IN ('24') THEN -3.5
      WHEN Site.Timezone IN ('25', '26', '27', '28', '29') THEN -3.0
      WHEN Site.Timezone IN ('30') THEN -2.0
      WHEN Site.Timezone IN ('31', '32') THEN -1.0
      WHEN Site.Timezone IN ('33', '34', '35') THEN 0.0
      WHEN Site.Timezone IN ('36', '37', '38', '39', '40') THEN 1.0
      WHEN Site.Timezone IN ('41', '42', '43', '44', '45', '46', '47', '48', '49') THEN 2.0
      WHEN Site.Timezone IN ('50', '51', '52', '53', '54') THEN 3.0
      WHEN Site.Timezone IN ('55') THEN 3.5
      WHEN Site.Timezone IN ('56', '57', '58', '59', '60') THEN 4.0
      WHEN Site.Timezone IN ('61') THEN 4.5
      WHEN Site.Timezone IN ('62', '63', '64') THEN 5.0
      WHEN Site.Timezone IN ('65', '66') THEN 5.5
      WHEN Site.Timezone IN ('67') THEN 5.75
      WHEN Site.Timezone IN ('68', '69') THEN 6.0
      WHEN Site.Timezone IN ('70') THEN 6.5
      WHEN Site.Timezone IN ('71', '72') THEN 7.0
      WHEN Site.Timezone IN ('73', '74', '75', '76', '77') THEN 8.0
      WHEN Site.Timezone IN ('78', '79', '80') THEN 9.0
      WHEN Site.Timezone IN ('81', '82') THEN 9.5
      WHEN Site.Timezone IN ('83', '84', '85', '86', '87') THEN 10.0
      WHEN Site.Timezone IN ('88') THEN 11.0
      WHEN Site.Timezone IN ('89', '90') THEN 12.0
      WHEN Site.Timezone IN ('91') THEN 13.0
        ELSE 0.0
      END AS HourOffset
    FROM Site 
    WHERE Site.ID = '${siteID}'
  )
  SELECT FORMAT(DATEADD(HOUR, HO.HourOffset + number, startHour), 'yyyy-MM-dd HH:00') AS Hour, 
    COALESCE(COUNT(CASE WHEN AlarmPowerUsage.InstantaneouskW > 0 THEN 1 END), 0) AS Charger, 
    COALESCE(MAX(AlarmPowerUsage.InstantaneouskW), 0) AS MaxkW 
  FROM (
    SELECT DATEADD(HOUR, -23, (SELECT DATEADD(HOUR, HO.HourOffset, DATEADD(HOUR, DATEDIFF(HOUR, 0, GETUTCDATE()), 0)) FROM HourOffsets AS HO)) AS startHour, 0 AS number 
    UNION ALL 
    SELECT DATEADD(HOUR, -23, (SELECT DATEADD(HOUR, HO.HourOffset, DATEADD(HOUR, DATEDIFF(HOUR, 0, GETUTCDATE()), 0)) FROM HourOffsets AS HO)) AS startHour, number + 1 AS number 
    FROM master..spt_values 
    WHERE type = 'P' AND number BETWEEN 0 AND 22 
  ) AS hours 
  CROSS JOIN HourOffsets AS HO 
  LEFT JOIN AlarmPowerUsage ON FORMAT(DATEADD(HOUR, HO.HourOffset + number, startHour), 'yyyy-MM-dd HH:00') = FORMAT(DATEADD(HOUR, -1 * (HO.HourOffset + number), GETUTCDATE()), 'yyyy-MM-dd HH:00') 
    AND AlarmPowerUsage.SiteID = '${siteID}' 
    AND AlarmPowerUsage.Date >= DATEADD(HOUR, -23, (SELECT DATEADD(HOUR, HO.HourOffset, DATEADD(HOUR, DATEDIFF(HOUR, 0, GETUTCDATE()), 0)) FROM HourOffsets AS HO)) 
    AND AlarmPowerUsage.Date < DATEADD(HOUR, DATEDIFF(HOUR, 0, '${date}'), 0) 
  WHERE hours.startHour >= DATEADD(HOUR, -23, (SELECT DATEADD(HOUR, HO.HourOffset, DATEADD(HOUR, DATEDIFF(HOUR, 0, GETUTCDATE()), 0)) FROM HourOffsets AS HO)) 
  GROUP BY FORMAT(DATEADD(HOUR, HO.HourOffset + number, startHour), 'yyyy-MM-dd HH:00'), HO.HourOffset 
  ORDER BY Hour
  `

    return await pool.request()
            .query(w1)
}

exports.getMapDetails=async(siteID)=>{
 const q=`SELECT a.ID as AssetID, a.LastUpdate as LastSeen, a.Name, a.Lon, a.Lat, 
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
    CONVERT(INT, CONVERT(VARBINARY(4), SUBSTRING(c1.EventData, 7, 4), 2)) AS Power1,
    CONVERT(INT, CONVERT(VARBINARY(2), SUBSTRING(c1.EventData, 5, 2), 2)) AS Voltage1,
    CONVERT(INT, CONVERT(VARBINARY(1), SUBSTRING(c1.EventData, 19, 1), 2)) AS SoC1,
    c1.Port as Port1, c1.ID as ID1,
   
    CONVERT(INT, CONVERT(VARBINARY(4), SUBSTRING(c2.EventData, 7, 4), 2)) AS Power2,
    CONVERT(INT, CONVERT(VARBINARY(2), SUBSTRING(c2.EventData, 5, 2), 2)) AS Voltage2,
    CONVERT(INT, CONVERT(VARBINARY(1), SUBSTRING(c2.EventData, 19, 1), 2)) AS SoC2,
    c2.Port as Port2, c2.ID as ID2
    FROM Asset a
    JOIN AssetSite jas ON a.ID = jas.AssetID
    LEFT JOIN (
      SELECT acd.AssetID, acd.Port, acd.ID, acd.EventData, acd.BatteryModule, acd.VirtualUniqueID, u.ID AS UnitID, u.UniqueID AS Converted, 
        ROW_NUMBER() OVER (PARTITION BY acd.AssetID, acd.Port ORDER BY acd.Date DESC) AS rn
        FROM AlarmChargeDetail acd
        LEFT JOIN Unit u ON acd.UnitID = u.ID
      WHERE acd.Port = 1
    ) c1 ON a.ID = c1.AssetID AND c1.rn = 1
    LEFT JOIN Unit u1 ON c1.UnitID = u1.ID AND c1.Converted = CONVERT(VARBINARY(8), c1.VirtualUniqueID, 2)
    LEFT JOIN (
      SELECT acd.AssetID, acd.Port, acd.ID, acd.EventData, acd.BatteryModule, acd.VirtualUniqueID, u.ID AS UnitID, u.UniqueID AS Converted, 
        ROW_NUMBER() OVER (PARTITION BY acd.AssetID, acd.Port ORDER BY acd.Date DESC) AS rn
        FROM AlarmChargeDetail acd
        LEFT JOIN Unit u ON acd.UnitID = u.ID
      WHERE acd.Port = 2
    ) c2 ON a.ID = c2.AssetID AND c2.rn = 1
    LEFT JOIN Unit u2 ON c2.UnitID = u2.ID AND c2.Converted = CONVERT(VARBINARY(8), c2.VirtualUniqueID, 2)
    WHERE jas.SiteID = '${siteID}' 
    AND a.System IN (5,6,7,11)
    AND a.Lon IS NOT NULL
    AND a.Lat IS NOT NULL;
`
    return await pool.request()
            .query(q)    
}

exports.getMap=async(siteID)=>{
const q=`SELECT a.ID as AssetID, c1.status as Status1, c2.status as Status2
    FROM Asset a
    JOIN AssetSite jas ON a.ID = jas.AssetID
    OUTER APPLY (
        SELECT TOP 1 acd.AssetID, acd.Port, acd.ID, acd.EventData,
        CASE 
            WHEN latest.Ended IS NULL THEN 'Faulted'
            WHEN latest.EventCode = 1792 THEN 'Charging'
            WHEN latest.EventCode = 1794 THEN 'Equalizing'
            WHEN latest.EventCode = 1796 THEN 'Idling'
        END AS status
        FROM AlarmChargeDetail acd
        JOIN AlarmCharger ac ON acd.AssetID = ac.AssetID AND acd.Port = ac.Port
        OUTER APPLY (
            SELECT TOP 1 AssetID, Port, Ended, EventCode
            FROM AlarmCharger
            WHERE AssetID = acd.AssetID AND Port = acd.Port AND Ended IS NOT NULL AND EventCode IN (1792, 1794, 1796)
            ORDER BY Date DESC
        ) latest
        WHERE acd.Port = 1 AND acd.AssetID = a.ID
        ORDER BY acd.Date DESC
    ) c1
    OUTER APPLY (
        SELECT TOP 1 acd.AssetID, acd.Port, acd.ID, acd.EventData,
        CASE 
            WHEN latest.Ended IS NULL THEN 'Faulted'
            WHEN latest.EventCode = 1792 THEN 'Charging'
            WHEN latest.EventCode = 1794 THEN 'Equalizing'
            WHEN latest.EventCode = 1796 THEN 'Idling'
        END AS status
        FROM AlarmChargeDetail acd
        JOIN AlarmCharger ac ON acd.AssetID = ac.AssetID AND acd.Port = ac.Port
        OUTER APPLY (
            SELECT TOP 1 AssetID, Port, Ended, EventCode
            FROM AlarmCharger
            WHERE AssetID = acd.AssetID AND Port = acd.Port AND Ended IS NOT NULL AND EventCode IN (1792, 1794, 1796)
            ORDER BY Date DESC
        ) latest
        WHERE acd.Port = 2 AND acd.AssetID = a.ID
        ORDER BY acd.Date DESC
    ) c2
    WHERE jas.SiteID = '${siteID}' 
    AND a.System IN (5,6,7,11)
    AND a.Lon IS NOT NULL
    AND a.Lat IS NOT NULL;
    `

const q1=`SELECT 
a.ID, 
a.Name, 

CASE
    WHEN c1.Ended IS NULL THEN 'Faulted'
    WHEN c1.EventCode = 1792 THEN 'Charging'
    WHEN c1.EventCode = 1794 THEN 'Equalizing'
    WHEN c1.EventCode = 1796 THEN 'Idling'
END AS Status1,
CASE
    WHEN c2.Ended IS NULL THEN 'Faulted'
    WHEN c2.EventCode = 1792 THEN 'Charging'
    WHEN c2.EventCode = 1794 THEN 'Equalizing'
    WHEN c2.EventCode = 1796 THEN 'Idling'
END AS Status2,
CONVERT(INT, CONVERT(VARBINARY(4), SUBSTRING(c1.EventData, 7, 4), 2)) AS Power1,
    CONVERT(INT, CONVERT(VARBINARY(2), SUBSTRING(c1.EventData, 5, 2), 2)) AS Voltage1,
    CONVERT(INT, CONVERT(VARBINARY(1), SUBSTRING(c1.EventData, 19, 1), 2)) AS SoC1,
    CONVERT(INT, CONVERT(VARBINARY(4), SUBSTRING(c2.EventData, 7, 4), 2)) AS Power2,
    CONVERT(INT, CONVERT(VARBINARY(2), SUBSTRING(c2.EventData, 5, 2), 2)) AS Voltage2,
    CONVERT(INT, CONVERT(VARBINARY(1), SUBSTRING(c2.EventData, 19, 1), 2)) AS SoC2

FROM Asset a
JOIN AssetSite jas ON a.ID = jas.AssetID
LEFT JOIN (
SELECT
    ac1.AssetID,
    ac1.Port,
    ac1.ID,
    ac1.Ended,
    ac1.EventCode,
    ac1.EventData
FROM AlarmCharger ac1
INNER JOIN (
    SELECT AssetID, MAX(Date) AS MaxDate
    FROM AlarmCharger
    WHERE EventCode = 1798
    GROUP BY AssetID
) ac2 ON ac1.AssetID = ac2.AssetID AND ac1.Date = ac2.MaxDate
) c1 ON a.ID = c1.AssetID
LEFT JOIN (
SELECT
    ac1.AssetID,
    ac1.Port,
    ac1.ID,
    ac1.Ended,
    ac1.EventCode,
    ac1.EventData
FROM AlarmCharger ac1
INNER JOIN (
    SELECT AssetID, MAX(Date) AS MaxDate
    FROM AlarmCharger
    WHERE EventCode = 1798
    GROUP BY AssetID
) ac2 ON ac1.AssetID = ac2.AssetID AND ac1.Date = ac2.MaxDate
) c2 ON a.ID = c2.AssetID
WHERE jas.SiteID = '${siteID}' 
AND a.System IN (5, 6, 7, 11)
AND a.Lat IS NOT NULL
AND a.Lon IS NOT NULL;
`

    return await pool.request()
            .query(q)
}

exports.getMapSystem5=async(siteID)=>{
    const q=`WITH cte1 AS (
    SELECT AssetID FROM AssetSite WHERE SiteID = '${siteID}'
), 
cte2 AS (
    SELECT Name, UnitID FROM Asset WHERE System = 5 AND ID IN (SELECT AssetID FROM cte1)
), 
cte3 AS (
    SELECT TOP 1 EventData, Date FROM AlarmCharger WHERE AssetID IN (SELECT AssetID FROM cte1) AND Port = 1 AND EventCode = 1798 ORDER BY Date DESC
), 
cte4 AS (
    SELECT CONVERT(INT, CONVERT(VARBINARY(4), SUBSTRING(EventData, 7, 4), 2)) AS Power, CONVERT(INT, CONVERT(VARBINARY(2), SUBSTRING(EventData, 11, 2), 2)) AS Voltage, CONVERT(INT, CONVERT(VARBINARY(1), SUBSTRING(EventData, 19, 1), 2)) AS SoC FROM cte3
), 
cte5 AS (
    SELECT BatteryModule, VirtualUniqueID, UnitID, Date FROM AlarmChargeDetail WHERE AssetID IN (SELECT AssetID FROM cte1) AND Port = 1
), 
cte6 AS (
    SELECT TOP 1 CONVERT(INT, CONVERT(VARBINARY(4), SUBSTRING(VirtualUniqueID, 1, 4), 2)) AS ConvertedVirtualUniqueID, BatteryModule, Date FROM cte5 ORDER BY Date DESC
), 
cte7 AS (
    SELECT Name FROM Asset WHERE UnitID = (SELECT ID FROM Unit WHERE UniqueId = CONVERT(VARCHAR(8), (SELECT ConvertedVirtualUniqueID FROM cte6), 10))
)
SELECT Asset.ID AS AssetID, Unit.LastSeenDate AS LastSeen1, Asset.Lat, Asset.Lon,
       CASE 
           WHEN AlarmCharger.Ended IS NULL THEN 'Faulted'
           WHEN AlarmCharger.EventCode = 1792 THEN 'Charging'
           WHEN AlarmCharger.EventCode = 1794 THEN 'Equalizing'
           WHEN AlarmCharger.EventCode = 1796 THEN 'Idling'
       END AS Status1,
       CASE 
           WHEN Asset.UnitID = Unit.ID AND Unit.UniqueId = CONVERT(VARCHAR(8), (SELECT ConvertedVirtualUniqueID FROM cte6), 10) THEN Asset.Name
           ELSE CONVERT(VARCHAR(MAX), CONVERT(VARBINARY(MAX), cte5.BatteryModule, 2))
       END AS Paired1,
       cte4.Power as Power1, cte4.Voltage as Voltage1, cte4.SoC as SoC1
FROM Unit 
LEFT JOIN Asset ON Asset.UnitID = Unit.ID 
LEFT JOIN cte4 ON 1 = 1
LEFT JOIN cte5 ON cte5.UnitID = Unit.ID AND Unit.UniqueId = CONVERT(VARCHAR(8), (SELECT ConvertedVirtualUniqueID FROM cte6), 10)
LEFT JOIN AlarmCharger ON AlarmCharger.AssetID = Asset.ID AND AlarmCharger.Port = 1
WHERE Asset.ID IN (SELECT AssetID FROM cte1)
AND Asset.Lat IS NOT NULL
AND Asset.Lon IS NOT NULL
ORDER BY Asset.ID
`
const w=`SELECT
AssetSite.AssetID,
Asset.Name,
Asset.Lat,
Asset.Lon,
CASE
    WHEN AlarmCharger.Ended IS NULL THEN 'Faulted'
    WHEN AlarmCharger.Ended IS NOT NULL AND AlarmCharger.EventCode = 1792 THEN 'Charging'
    WHEN AlarmCharger.Ended IS NOT NULL AND AlarmCharger.EventCode = 1794 THEN 'Equalizing'
    WHEN AlarmCharger.Ended IS NOT NULL AND AlarmCharger.EventCode = 1796 THEN 'Idling'
END AS Status1,
CONVERT(INT, CONVERT(VARBINARY(4), SUBSTRING(AlarmCharger.EventData, 7, 4), 2)) AS Power1,
CONVERT(INT, CONVERT(VARBINARY(2), SUBSTRING(AlarmCharger.EventData, 5, 2), 2)) AS Voltage1,
CONVERT(INT, CONVERT(VARBINARY(1), SUBSTRING(AlarmCharger.EventData, 19, 1), 2)) AS SoC1,
CASE
    WHEN Unit.UniqueID = CONVERT(VARBINARY(8), AlarmChargeDetail.VirtualUniqueID, 2) AND Asset.UnitID = Unit.ID THEN COALESCE(Asset.Name, STUFF(CONVERT(VARCHAR(50), CONVERT(VARBINARY(8), CAST(AlarmChargeDetail.BatteryModule AS BIGINT)), 2), 1, PATINDEX('%[^0]%', CONVERT(VARCHAR(50), CONVERT(VARBINARY(8), CAST(AlarmChargeDetail.BatteryModule AS BIGINT)), 2)) - 1, ''))
    ELSE STUFF(CONVERT(VARCHAR(50), CONVERT(VARBINARY(8), CAST(AlarmChargeDetail.BatteryModule AS BIGINT)), 2), 1, PATINDEX('%[^0]%', CONVERT(VARCHAR(50), CONVERT(VARBINARY(8), CAST(AlarmChargeDetail.BatteryModule AS BIGINT)), 2)) - 1, '')
END AS Paired1,
Unit.LastSeenDate AS LastSeen1
FROM
AssetSite
INNER JOIN Asset ON AssetSite.AssetID = Asset.ID
LEFT JOIN (
SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY AssetID, Port ORDER BY Date DESC) AS RowNumber
FROM
    AlarmCharger
WHERE
    Port = 1
) AS AlarmCharger ON AssetSite.AssetID = AlarmCharger.AssetID AND AlarmCharger.RowNumber = 1
LEFT JOIN (
SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY AssetID, Port ORDER BY Date DESC) AS RowNumber
FROM
    AlarmChargeDetail
WHERE
    Port = 1
) AS AlarmChargeDetail ON AssetSite.AssetID = AlarmChargeDetail.AssetID AND AlarmChargeDetail.RowNumber = 1
LEFT JOIN Unit ON AlarmChargeDetail.UnitID = Unit.ID
WHERE
AssetSite.SiteID = '${siteID}'
AND Asset.System = 5
AND Asset.Lat IS NOT NULL
AND Asset.Lon IS NOT NULL
AND (AlarmCharger.AssetID IS NOT NULL OR AlarmChargeDetail.AssetID IS NOT NULL);
`
    return await pool.request()
            .query(w)
}

exports.getMapSystem6=async(siteID)=>{
    const q=`WITH CTE AS (
        SELECT AssetID
        FROM AssetSite
        WHERE SiteID = '${siteID}'
    ),
    CTE2 AS (
        SELECT ID, Name, UnitID, UnitID2
        FROM Asset
        WHERE Asset.System = 6
            AND ID IN (SELECT AssetID FROM CTE)
            AND Asset.Lat IS NOT NULL
            AND Asset.Lon IS NOT NULL
    ),
    CTE3 AS (
        SELECT TOP 1 
            ac.EventData,
            ac.Ended,
            ac.EventCode
        FROM AlarmCharger ac
        WHERE ac.AssetID IN (SELECT AssetID FROM CTE)
            AND ac.Port = 1
        ORDER BY ac.Date DESC
    ),
    CTE4 AS (
        SELECT CONVERT(INT, SUBSTRING(EventData, 7, 4), 1) AS Power
        FROM CTE3
    ),
    CTE5 AS (
        SELECT CONVERT(INT, SUBSTRING(EventData, 11, 2), 1) AS Voltage
        FROM CTE3
    ),
    CTE6 AS (
        SELECT CONVERT(INT, SUBSTRING(EventData, 19, 1), 1) AS SoC
        FROM CTE3
    ),
    CTE7 AS (
        SELECT TOP 1 BatteryModule, CONVERT(INT, VirtualUniqueID, 1) AS VirtualUniqueID, UnitID, Date
        FROM AlarmChargeDetail
        WHERE AssetID IN (SELECT AssetID FROM CTE)
            AND Port = 1
        ORDER BY Date DESC
    ),
    CTE8 AS (
        SELECT CASE
            WHEN Unit.ID IS NOT NULL THEN Asset.Name
            ELSE CONVERT(VARCHAR(MAX), CTE7.BatteryModule, 2)
        END AS Name
        FROM CTE7
        LEFT JOIN Unit ON Unit.ID = CTE7.UnitID
        LEFT JOIN Asset ON Asset.UnitID = Unit.ID
    ),
    CTE9 AS (
        SELECT Unit.LastSeenDate
        FROM Unit
        WHERE Unit.ID IN (SELECT UnitID FROM CTE7)
    ),
    CTE10 AS (
        SELECT TOP 1 
            ac.EventData,
            ac.Ended,
            ac.EventCode
        FROM AlarmCharger ac
        WHERE ac.AssetID IN (SELECT AssetID FROM CTE)
            AND ac.Port = 2
        ORDER BY ac.Date DESC
    ),
    CTE11 AS (
        SELECT CONVERT(INT, SUBSTRING(EventData, 7, 4), 1) AS Power
        FROM CTE10
    ),
    CTE12 AS (
        SELECT CONVERT(INT, SUBSTRING(EventData, 11, 2), 1) AS Voltage
        FROM CTE10
    ),
    CTE13 AS (
        SELECT CONVERT(INT, SUBSTRING(EventData, 19, 1), 1) AS SoC
        FROM CTE10
    ),
    CTE14 AS (
        SELECT TOP 1 BatteryModule, CONVERT(INT, VirtualUniqueID, 1) AS VirtualUniqueID, UnitID, Date
        FROM AlarmChargeDetail
        WHERE AssetID IN (SELECT AssetID FROM CTE)
            AND Port = 2
        ORDER BY Date DESC
    ),
    CTE15 AS (
        SELECT CASE
            WHEN Unit.ID IS NOT NULL THEN Asset.Name
            ELSE CONVERT(VARCHAR(MAX), CTE14.BatteryModule, 2)
            END AS Name
            FROM CTE14
            LEFT JOIN Unit ON Unit.ID = CTE14.UnitID
            LEFT JOIN Asset ON Asset.UnitID2 = Unit.ID
    ),
    CTE16 AS (
        SELECT Unit.LastSeenDate
        FROM Unit
        WHERE Unit.ID IN (SELECT UnitID FROM CTE14)
    )
    
    SELECT
        CTE2.ID AS AssetID,
        CTE2.Name,
        CTE4.Power AS Power1,
        CTE5.Voltage AS Voltage1,
        CTE6.SoC AS SoC1,
        CTE8.Name AS Paired1,
        CTE9.LastSeenDate AS LastSeen1,
        CTE11.Power AS Power2,
        CTE12.Voltage AS Voltage2,
        CTE13.SoC AS SoC2,
        CTE15.Name AS Paired2,
        CTE16.LastSeenDate AS LastSeen2
    FROM CTE2
    LEFT JOIN CTE3 ON 1=1 
    LEFT JOIN CTE4 ON 1=1 
    LEFT JOIN CTE5 ON 1=1
    LEFT JOIN CTE6 ON 1=1
    LEFT JOIN CTE7 ON 1=1
    LEFT JOIN CTE8 ON 1=1
    LEFT JOIN CTE9 ON 1=1
    LEFT JOIN CTE10 ON 1=1
    LEFT JOIN CTE11 ON 1=1
    LEFT JOIN CTE12 ON 1=1
    LEFT JOIN CTE13 ON 1=1
    LEFT JOIN CTE14 ON 1=1
    LEFT JOIN CTE15 ON 1=1
    LEFT JOIN CTE16 ON 1=1
    `

    const q1=`WITH CTE AS (
        SELECT AssetID
        FROM AssetSite
        WHERE SiteID = '${siteID}'
    ),
    CTE2 AS (
        SELECT ID, Name, UnitID, UnitID2, Lat, Lon
        FROM Asset
        WHERE Asset.System = 6
            AND ID IN (SELECT AssetID FROM CTE)
            AND Asset.Lat IS NOT NULL
            AND Asset.Lon IS NOT NULL
    ),
    CTE3 AS (
        SELECT TOP 1 
            ac.EventData,
            ac.Ended,
            ac.EventCode
        FROM AlarmCharger ac
        WHERE ac.AssetID IN (SELECT AssetID FROM CTE)
            AND ac.Port = 1
        ORDER BY ac.Date DESC
    ),
    CTE4 AS (
        SELECT CONVERT(INT, SUBSTRING(EventData, 7, 4), 1) AS Power
        FROM CTE3
    ),
    CTE5 AS (
        SELECT CONVERT(INT, SUBSTRING(EventData, 11, 2), 1) AS Voltage
        FROM CTE3
    ),
    CTE6 AS (
        SELECT CONVERT(INT, SUBSTRING(EventData, 19, 1), 1) AS SoC
        FROM CTE3
    ),
    CTE7 AS (
        SELECT TOP 1 
            BatteryModule,
            CONVERT(INT, VirtualUniqueID, 1) AS VirtualUniqueID,
            UnitID,
            Date
        FROM AlarmChargeDetail
        WHERE AssetID IN (SELECT AssetID FROM CTE)
            AND Port = 1
        ORDER BY Date DESC
    ),
    CTE10 AS (
        SELECT TOP 1 
            ac.EventData,
            ac.Ended,
            ac.EventCode
        FROM AlarmCharger ac
        WHERE ac.AssetID IN (SELECT AssetID FROM CTE)
            AND ac.Port = 2
        ORDER BY ac.Date DESC
    ),
    CTE11 AS (
        SELECT CONVERT(INT, SUBSTRING(EventData, 7, 4), 1) AS Power
        FROM CTE10
    ),
    CTE12 AS (
        SELECT CONVERT(INT, SUBSTRING(EventData, 11, 2), 1) AS Voltage
        FROM CTE10
    ),
    CTE13 AS (
        SELECT CONVERT(INT, SUBSTRING(EventData, 19, 1), 1) AS SoC
        FROM CTE10
    ),
    CTE14 AS (
        SELECT TOP 1 
            BatteryModule,
            CONVERT(INT, VirtualUniqueID, 1) AS VirtualUniqueID,
            UnitID,
            Date
        FROM AlarmChargeDetail
        WHERE AssetID IN (SELECT AssetID FROM CTE)
            AND Port = 2
        ORDER BY Date DESC
    )
    
    SELECT
        CTE2.ID AS AssetID,
        CTE2.Name,
        CTE2.Lat,
        CTE2.Lon,
        CTE4.Power AS Power1,
        CTE5.Voltage AS Voltage1,
        CTE6.SoC AS SoC1,
        CASE
            WHEN CTE3.Ended IS NULL THEN 'Faulted'
            WHEN CTE3.Ended IS NOT NULL AND CTE3.EventCode IN (1792, 1794, 1796) THEN
                CASE CTE3.EventCode
                    WHEN 1792 THEN 'Charging'
                    WHEN 1794 THEN 'Equalizing'
                    WHEN 1796 THEN 'Idling'
                END
        END AS Status1,
        (
            SELECT CASE
                WHEN u1.ID IS NOT NULL THEN a1.Name
                ELSE CONVERT(VARCHAR(MAX), ad1.BatteryModule, 2)
            END
            FROM Asset a1
            LEFT JOIN Unit u1 ON u1.ID = a1.UnitID
            LEFT JOIN AlarmChargeDetail ad1 ON ad1.UnitID = u1.ID
            WHERE a1.ID = CTE2.ID AND ad1.Port = 1
        ) AS Paired1,
        (
            SELECT u1.LastSeenDate
            FROM Unit u1
            WHERE u1.ID = CTE2.UnitID
        ) AS LastSeen1,
        CTE11.Power AS Power2,
        CTE12.Voltage AS Voltage2,
        CTE13.SoC AS SoC2,
        CASE
            WHEN CTE10.Ended IS NULL THEN 'Faulted'
            WHEN CTE10.Ended IS NOT NULL AND CTE10.EventCode IN (1792, 1794, 1796) THEN
                CASE CTE10.EventCode
                    WHEN 1792 THEN 'Charging'
                    WHEN 1794 THEN 'Equalizing'
                    WHEN 1796 THEN 'Idling'
                END
        END AS Status2,
        (
            SELECT CASE
                WHEN u2.ID IS NOT NULL THEN a2.Name
                ELSE CONVERT(VARCHAR(MAX), ad2.BatteryModule, 2)
            END
            FROM Asset a2
            LEFT JOIN Unit u2 ON u2.ID = a2.UnitID2
            LEFT JOIN AlarmChargeDetail ad2 ON ad2.UnitID = u2.ID
            WHERE a2.ID = CTE2.ID AND ad2.Port = 2
        ) AS Paired2,
        (
            SELECT u2.LastSeenDate
            FROM Unit u2
            WHERE u2.ID = CTE2.UnitID2
        ) AS LastSeen2
    FROM CTE2
    LEFT JOIN CTE3 ON 1=1 
    LEFT JOIN CTE4 ON 1=1 
    LEFT JOIN CTE5 ON 1=1
    LEFT JOIN CTE6 ON 1=1
    LEFT JOIN CTE7 ON 1=1
    LEFT JOIN CTE10 ON 1=1
    LEFT JOIN CTE11 ON 1=1
    LEFT JOIN CTE12 ON 1=1
    LEFT JOIN CTE13 ON 1=1;
`

const q2=`WITH CTE AS (
    SELECT AssetID
    FROM AssetSite
    WHERE SiteID = '${siteID}'
),
CTE2 AS (
    SELECT ID, Name, UnitID, UnitID2, Lat, Lon
    FROM Asset
    WHERE Asset.System = 6
        AND ID IN (SELECT AssetID FROM CTE)
        AND Asset.Lat IS NOT NULL
        AND Asset.Lon IS NOT NULL
),
CTE3 AS (
    SELECT TOP 1 
        ac.EventData,
        ac.Ended,
        ac.EventCode
    FROM AlarmCharger ac
    WHERE ac.AssetID IN (SELECT AssetID FROM CTE)
        AND ac.Port = 1
    ORDER BY ac.Date DESC
),
CTE4 AS (
    SELECT CONVERT(INT, SUBSTRING(EventData, 7, 4), 1) AS Power
    FROM CTE3
),
CTE5 AS (
    SELECT CONVERT(INT, SUBSTRING(EventData, 11, 2), 1) AS Voltage
    FROM CTE3
),
CTE6 AS (
    SELECT CONVERT(INT, SUBSTRING(EventData, 19, 1), 1) AS SoC
    FROM CTE3
),
CTE7 AS (
    SELECT TOP 1 
        BatteryModule,
        CONVERT(INT, VirtualUniqueID, 1) AS VirtualUniqueID,
        UnitID,
        Date
    FROM AlarmChargeDetail
    WHERE AssetID IN (SELECT AssetID FROM CTE)
        AND Port = 1
    ORDER BY Date DESC
),
CTE10 AS (
    SELECT TOP 1 
        ac.EventData,
        ac.Ended,
        ac.EventCode
    FROM AlarmCharger ac
    WHERE ac.AssetID IN (SELECT AssetID FROM CTE)
        AND ac.Port = 2
    ORDER BY ac.Date DESC
),
CTE11 AS (
    SELECT CONVERT(INT, SUBSTRING(EventData, 7, 4), 1) AS Power
    FROM CTE10
),
CTE12 AS (
    SELECT CONVERT(INT, SUBSTRING(EventData, 11, 2), 1) AS Voltage
    FROM CTE10
),
CTE13 AS (
    SELECT CONVERT(INT, SUBSTRING(EventData, 19, 1), 1) AS SoC
    FROM CTE10
),
CTE14 AS (
    SELECT TOP 1 
        BatteryModule,
        CONVERT(INT, VirtualUniqueID, 1) AS VirtualUniqueID,
        UnitID,
        Date
    FROM AlarmChargeDetail
    WHERE AssetID IN (SELECT AssetID FROM CTE)
        AND Port = 2
    ORDER BY Date DESC
)

SELECT
    CTE2.ID AS AssetID,
    CTE2.Name,
    CTE2.Lat,
    CTE2.Lon,
    CTE4.Power AS Power1,
    CTE5.Voltage AS Voltage1,
    CTE6.SoC AS SoC1,
    CASE
        WHEN EXISTS (SELECT 1 FROM AlarmCharger WHERE AssetID IN (SELECT AssetID FROM CTE) AND Port = 1) THEN
            CASE
                WHEN CTE3.Ended IS NULL THEN 'Faulted'
                WHEN CTE3.Ended IS NOT NULL AND CTE3.EventCode IN (1792, 1794, 1796) THEN
                    CASE CTE3.EventCode
                        WHEN 1792 THEN 'Charging'
                        WHEN 1794 THEN 'Equalizing'
                        WHEN 1796 THEN 'Idling'
                    END
            END
        ELSE NULL
    END AS Status1,
    (
        SELECT CASE
            WHEN u1.ID = a1.UnitID THEN COALESCE(a1.Name,STUFF(CONVERT(VARCHAR(50), CONVERT(VARBINARY(8), CAST(ad1.BatteryModule AS BIGINT)), 2), 1, PATINDEX('%[^0]%', CONVERT(VARCHAR(50), CONVERT(VARBINARY(8), CAST(ad1.BatteryModule AS BIGINT)), 2)) - 1, ''))
            ELSE STUFF(CONVERT(VARCHAR(50), CONVERT(VARBINARY(8), CAST(ad1.BatteryModule AS BIGINT)), 2), 1, PATINDEX('%[^0]%', CONVERT(VARCHAR(50), CONVERT(VARBINARY(8), CAST(ad1.BatteryModule AS BIGINT)), 2)) - 1, '')
        END
        FROM Asset a1
        LEFT JOIN Unit u1 ON u1.ID = a1.UnitID
        LEFT JOIN AlarmChargeDetail ad1 ON ad1.UnitID = u1.ID
        WHERE a1.ID = CTE2.ID AND ad1.Port = 1
    ) AS Paired1,
    (
        SELECT u1.LastSeenDate
        FROM Unit u1
        WHERE u1.ID = CTE2.UnitID
    ) AS LastSeen1,
    CTE11.Power AS Power2,
    CTE12.Voltage AS Voltage2,
    CTE13.SoC AS SoC2,
    CASE
        WHEN EXISTS (SELECT 1 FROM AlarmCharger WHERE AssetID IN (SELECT AssetID FROM CTE) AND Port = 2) THEN
            CASE
                WHEN CTE10.Ended IS NULL THEN 'Faulted'
                WHEN CTE10.Ended IS NOT NULL AND CTE10.EventCode IN (1792, 1794, 1796) THEN
                    CASE CTE10.EventCode
                        WHEN 1792 THEN 'Charging'
                        WHEN 1794 THEN 'Equalizing'
                        WHEN 1796 THEN 'Idling'
                    END
            END
        ELSE NULL
    END AS Status2,
    (
        SELECT CASE
            WHEN u2.ID = a2.UnitID2 THEN COALESCE(a2.Name,STUFF(CONVERT(VARCHAR(50), CONVERT(VARBINARY(8), CAST(ad2.BatteryModule AS BIGINT)), 2), 1, PATINDEX('%[^0]%', CONVERT(VARCHAR(50), CONVERT(VARBINARY(8), CAST(ad2.BatteryModule AS BIGINT)), 2)) - 1, ''))
            ELSE STUFF(CONVERT(VARCHAR(50), CONVERT(VARBINARY(8), CAST(ad2.BatteryModule AS BIGINT)), 2), 1, PATINDEX('%[^0]%', CONVERT(VARCHAR(50), CONVERT(VARBINARY(8), CAST(ad2.BatteryModule AS BIGINT)), 2)) - 1, '')
        END
        FROM Asset a2
        LEFT JOIN Unit u2 ON u2.ID = a2.UnitID2
        LEFT JOIN AlarmChargeDetail ad2 ON ad2.UnitID = u2.ID
        WHERE a2.ID = CTE2.ID AND ad2.Port = 2
    ) AS Paired2,
    (
        SELECT u2.LastSeenDate
        FROM Unit u2
        WHERE u2.ID = CTE2.UnitID2
    ) AS LastSeen2
FROM CTE2
LEFT JOIN CTE3 ON 1=1 
LEFT JOIN CTE4 ON 1=1 
LEFT JOIN CTE5 ON 1=1
LEFT JOIN CTE6 ON 1=1
LEFT JOIN CTE7 ON 1=1
LEFT JOIN CTE10 ON 1=1
LEFT JOIN CTE11 ON 1=1
LEFT JOIN CTE12 ON 1=1
LEFT JOIN CTE13 ON 1=1;
`
    return await pool.request()
            .query(q2)
}

exports.getMapSystem=async(siteID)=>{
const q=`SELECT
    AssetID,
    Name,
    Lat,
    Lon,
    Power1,
    Voltage1,
    SoC1,
    Paired1,
    LastSeen1,
    CASE
        WHEN Status1Ended IS NULL THEN 'Faulted'
        WHEN Status1EventCode = 1792 THEN 'Charging'
        WHEN Status1EventCode = 1794 THEN 'Equalizing'
        WHEN Status1EventCode = 1796 THEN 'Idling'
        ELSE NULL
    END AS Status1,
    Power2,
    Voltage2,
    SoC2,
    Paired2,
    LastSeen2,
    CASE
        WHEN Status2Ended IS NULL THEN 'Faulted'
        WHEN Status2EventCode = 1792 THEN 'Charging'
        WHEN Status2EventCode = 1794 THEN 'Equalizing'
        WHEN Status2EventCode = 1796 THEN 'Idling'
        ELSE NULL
    END AS Status2
FROM (
    SELECT
        Asset.ID AS AssetID,
        Asset.Name, Asset.Lat, Asset.Lon,
        CONVERT(INT, CONVERT(VARBINARY(4), SUBSTRING(AlarmCharger2.EventData, 7, 4), 2)) AS Power1,
        CONVERT(INT, CONVERT(VARBINARY(2), SUBSTRING(AlarmCharger2.EventData, 5, 2), 2)) AS Voltage1,
        CONVERT(INT, CONVERT(VARBINARY(1), SUBSTRING(AlarmCharger2.EventData, 19, 1), 2)) AS SoC1,
        CASE
            WHEN Unit1.ID IS NOT NULL THEN Asset.Name
            ELSE STUFF(CONVERT(VARCHAR(50), CONVERT(VARBINARY(8), CAST(AlarmChargeDetail1.BatteryModule AS BIGINT)), 2), 1, PATINDEX('%[^0]%', CONVERT(VARCHAR(50), CONVERT(VARBINARY(8), CAST(AlarmChargeDetail1.BatteryModule AS BIGINT)), 2)) - 1, '') 
        END AS Paired1,
        CASE
            WHEN Asset.UnitID = Unit1.ID THEN Unit1.LastSeenDate
            ELSE NULL
        END AS LastSeen1,
        CONVERT(INT, CONVERT(VARBINARY(4), SUBSTRING(AlarmCharger4.EventData, 7, 4), 2)) AS Power2,
        CONVERT(INT, CONVERT(VARBINARY(2), SUBSTRING(AlarmCharger4.EventData, 5, 2), 2)) AS Voltage2,
        CONVERT(INT, CONVERT(VARBINARY(1), SUBSTRING(AlarmCharger4.EventData, 19, 1), 2)) AS SoC2,
        CASE
            WHEN Unit2.ID IS NOT NULL THEN Asset.Name
            ELSE STUFF(CONVERT(VARCHAR(50), CONVERT(VARBINARY(8), CAST(AlarmChargeDetail2.BatteryModule AS BIGINT)), 2), 1, PATINDEX('%[^0]%', CONVERT(VARCHAR(50), CONVERT(VARBINARY(8), CAST(AlarmChargeDetail2.BatteryModule AS BIGINT)), 2)) - 1, '') 
        END AS Paired2,
        CASE
            WHEN Asset.UnitID = Unit2.ID THEN Unit2.LastSeenDate
            ELSE NULL
        END AS LastSeen2,
        AlarmCharger2.Ended AS Status1Ended,
        AlarmCharger2.EventCode AS Status1EventCode,
        AlarmCharger4.Ended AS Status2Ended,
        AlarmCharger4.EventCode AS Status2EventCode,
        ROW_NUMBER() OVER (PARTITION BY Asset.ID ORDER BY Asset.ID) AS RowNumber
    FROM
        AssetSite
        JOIN Asset ON AssetSite.AssetID = Asset.ID
        LEFT JOIN (
            SELECT
                AssetID,
                EventData,
                Ended,
                EventCode
            FROM
                AlarmCharger
            WHERE
                Port = 1
                AND EventCode = 1798
                AND AssetID IN (SELECT AssetID FROM AssetSite WHERE SiteID = '${siteID}')
                AND Date = (
                    SELECT MAX(Date)
                    FROM AlarmCharger
                    WHERE Port = 1
                        AND EventCode = 1798
                        AND AssetID = AlarmCharger.AssetID
                )
        ) AS AlarmCharger2 ON Asset.ID = AlarmCharger2.AssetID
        LEFT JOIN (
            SELECT
                AssetID,
                EventData,
                Ended,
                EventCode
            FROM
                AlarmCharger
            WHERE
                Port = 2
                AND EventCode = 1798
                AND AssetID IN (SELECT AssetID FROM AssetSite WHERE SiteID = '${siteID}')
                AND Date = (
                    SELECT MAX(Date)
                    FROM AlarmCharger
                    WHERE Port = 2
                        AND EventCode = 1798
                        AND AssetID = AlarmCharger.AssetID
                )
        ) AS AlarmCharger4 ON Asset.ID = AlarmCharger4.AssetID
        LEFT JOIN (
            SELECT
                AssetID,
                BatteryModule,
                VirtualUniqueID,
                UnitID
            FROM
                AlarmChargeDetail
            WHERE
                Port = 1
                AND AssetID IN (SELECT AssetID FROM AssetSite WHERE SiteID = '${siteID}')
                AND Date = (
                    SELECT MAX(Date)
                    FROM AlarmChargeDetail
                    WHERE Port = 1
                        AND AssetID = AlarmChargeDetail.AssetID
                )
        ) AS AlarmChargeDetail1 ON Asset.ID = AlarmChargeDetail1.AssetID
        LEFT JOIN Unit AS Unit1 ON AlarmChargeDetail1.UnitID = Unit1.ID
        LEFT JOIN (
            SELECT
                AssetID,
                BatteryModule,
                VirtualUniqueID,
                UnitID
            FROM
                AlarmChargeDetail
            WHERE
                Port = 2
                AND AssetID IN (SELECT AssetID FROM AssetSite WHERE SiteID = '${siteID}')
                AND Date = (
                    SELECT MAX(Date)
                    FROM AlarmChargeDetail
                    WHERE Port = 2
                        AND AssetID = AlarmChargeDetail.AssetID
                )
        ) AS AlarmChargeDetail2 ON Asset.ID = AlarmChargeDetail2.AssetID
        LEFT JOIN Unit AS Unit2 ON AlarmChargeDetail2.UnitID = Unit2.ID
    WHERE
        Asset.System IN (7, 11)
        AND Asset.Lat IS NOT NULL
        AND Asset.Lon IS NOT NULL
) AS Subquery
WHERE
    RowNumber = 1;
`
const q1=`SELECT
AssetID,
Name,
Lat,
Lon,
Power1,
Voltage1,
SoC1,
Paired1,
LastSeen1,
CASE
    WHEN (SELECT COUNT(*) FROM AlarmCharger WHERE AssetID = Subquery.AssetID AND Port = 1) = 0 THEN NULL 
    WHEN Status1Ended IS NULL THEN 'Faulted'
    WHEN Status1EventCode = 1792 THEN 'Charging'
    WHEN Status1EventCode = 1794 THEN 'Equalizing'
    WHEN Status1EventCode = 1796 THEN 'Idling'
    ELSE NULL
END AS Status1,
Power2,
Voltage2,
SoC2,
Paired2,
LastSeen2,
CASE
    WHEN (SELECT COUNT(*) FROM AlarmCharger WHERE AssetID = Subquery.AssetID AND Port = 2) = 0 THEN NULL 
    WHEN Status2Ended IS NULL THEN 'Faulted'
    WHEN Status2EventCode = 1792 THEN 'Charging'
    WHEN Status2EventCode = 1794 THEN 'Equalizing'
    WHEN Status2EventCode = 1796 THEN 'Idling'
    ELSE NULL
END AS Status2
FROM (
SELECT
    Asset.ID AS AssetID,
    Asset.Name,
    Asset.Lat,
    Asset.Lon,
    CONVERT(INT, CONVERT(VARBINARY(4), SUBSTRING(AlarmCharger2.EventData, 7, 4), 2)) AS Power1,
    CONVERT(INT, CONVERT(VARBINARY(2), SUBSTRING(AlarmCharger2.EventData, 5, 2), 2)) AS Voltage1,
    CONVERT(INT, CONVERT(VARBINARY(1), SUBSTRING(AlarmCharger2.EventData, 19, 1), 2)) AS SoC1,
    CASE
        WHEN Unit1.ID = Asset.UnitID THEN COALESCE(Asset.Name,STUFF(CONVERT(VARCHAR(50), CONVERT(VARBINARY(8), CAST(AlarmChargeDetail1.BatteryModule AS BIGINT)), 2), 1, PATINDEX('%[^0]%', CONVERT(VARCHAR(50), CONVERT(VARBINARY(8), CAST(AlarmChargeDetail1.BatteryModule AS BIGINT)), 2)) - 1, ''))
        ELSE STUFF(CONVERT(VARCHAR(50), CONVERT(VARBINARY(8), CAST(AlarmChargeDetail1.BatteryModule AS BIGINT)), 2), 1, PATINDEX('%[^0]%', CONVERT(VARCHAR(50), CONVERT(VARBINARY(8), CAST(AlarmChargeDetail1.BatteryModule AS BIGINT)), 2)) - 1, '') 
    END AS Paired1,
    CASE
        WHEN Asset.UnitID = Unit1.ID THEN Unit1.LastSeenDate
        ELSE NULL
    END AS LastSeen1,
    CONVERT(INT, CONVERT(VARBINARY(4), SUBSTRING(AlarmCharger4.EventData, 7, 4), 2)) AS Power2,
    CONVERT(INT, CONVERT(VARBINARY(2), SUBSTRING(AlarmCharger4.EventData, 5, 2), 2)) AS Voltage2,
    CONVERT(INT, CONVERT(VARBINARY(1), SUBSTRING(AlarmCharger4.EventData, 19, 1), 2)) AS SoC2,
    CASE
        WHEN Unit2.ID = Asset.UnitID THEN COALESCE(Asset.Name,STUFF(CONVERT(VARCHAR(50), CONVERT(VARBINARY(8), CAST(AlarmChargeDetail2.BatteryModule AS BIGINT)), 2), 1, PATINDEX('%[^0]%', CONVERT(VARCHAR(50), CONVERT(VARBINARY(8), CAST(AlarmChargeDetail2.BatteryModule AS BIGINT)), 2)) - 1, ''))
        ELSE STUFF(CONVERT(VARCHAR(50), CONVERT(VARBINARY(8), CAST(AlarmChargeDetail2.BatteryModule AS BIGINT)), 2), 1, PATINDEX('%[^0]%', CONVERT(VARCHAR(50), CONVERT(VARBINARY(8), CAST(AlarmChargeDetail2.BatteryModule AS BIGINT)), 2)) - 1, '') 
    END AS Paired2,
    CASE
        WHEN Asset.UnitID = Unit2.ID THEN Unit2.LastSeenDate
        ELSE NULL
    END AS LastSeen2,
    AlarmCharger2.Ended AS Status1Ended,
    AlarmCharger2.EventCode AS Status1EventCode,
    AlarmCharger4.Ended AS Status2Ended,
    AlarmCharger4.EventCode AS Status2EventCode,
    ROW_NUMBER() OVER (PARTITION BY Asset.ID ORDER BY Asset.ID) AS RowNumber
FROM
    AssetSite
    JOIN Asset ON AssetSite.AssetID = Asset.ID
    LEFT JOIN (
        SELECT
            AssetID,
            EventData,
            Ended,
            EventCode
        FROM
            AlarmCharger
        WHERE
            Port = 1
            AND EventCode = 1798
            AND AssetID IN (SELECT AssetID FROM AssetSite WHERE SiteID = '${siteID}')
            AND Date = (
                SELECT MAX(Date)
                FROM AlarmCharger
                WHERE Port = 1
                    AND EventCode = 1798
                    AND AssetID = AlarmCharger.AssetID
            )
    ) AS AlarmCharger2 ON Asset.ID = AlarmCharger2.AssetID
    LEFT JOIN (
        SELECT
            AssetID,
            EventData,
            Ended,
            EventCode
        FROM
            AlarmCharger
        WHERE
            Port = 2
            AND EventCode = 1798
            AND AssetID IN (SELECT AssetID FROM AssetSite WHERE SiteID = '${siteID}')
            AND Date = (
                SELECT MAX(Date)
                FROM AlarmCharger
                WHERE Port = 2
                    AND EventCode = 1798
                    AND AssetID = AlarmCharger.AssetID
            )
    ) AS AlarmCharger4 ON Asset.ID = AlarmCharger4.AssetID
    LEFT JOIN (
        SELECT
            AssetID,
            BatteryModule,
            VirtualUniqueID,
            UnitID
        FROM
            AlarmChargeDetail
        WHERE
            Port = 1
            AND AssetID IN (SELECT AssetID FROM AssetSite WHERE SiteID = '${siteID}')
            AND Date = (
                SELECT MAX(Date)
                FROM AlarmChargeDetail
                WHERE Port = 1
                    AND AssetID = AlarmChargeDetail.AssetID
            )
    ) AS AlarmChargeDetail1 ON Asset.ID = AlarmChargeDetail1.AssetID
    LEFT JOIN Unit AS Unit1 ON AlarmChargeDetail1.UnitID = Unit1.ID
    LEFT JOIN (
        SELECT
            AssetID,
            BatteryModule,
            VirtualUniqueID,
            UnitID
        FROM
            AlarmChargeDetail
        WHERE
            Port = 2
            AND AssetID IN (SELECT AssetID FROM AssetSite WHERE SiteID = '${siteID}')
            AND Date = (
                SELECT MAX(Date)
                FROM AlarmChargeDetail
                WHERE Port = 2
                    AND AssetID = AlarmChargeDetail.AssetID
            )
    ) AS AlarmChargeDetail2 ON Asset.ID = AlarmChargeDetail2.AssetID
    LEFT JOIN Unit AS Unit2 ON AlarmChargeDetail2.UnitID = Unit2.ID
WHERE
    Asset.System IN (7, 11)
    AND Asset.Lat IS NOT NULL
    AND Asset.Lon IS NOT NULL
) AS Subquery
WHERE
RowNumber = 1;
`

    return await pool.request()
            .query(q1)
}
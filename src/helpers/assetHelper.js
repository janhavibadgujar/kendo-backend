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

exports.getMaps=async(siteID)=>{
    const q=`SELECT
    a.AssetID,
    asset.Name,
    asset.UnitID,
    acd.BatteryModule,
    CONVERT(decimal(18,0), CONVERT(varbinary(max), ac.EventData, 2) % 0x100000000) AS Power,
    CONVERT(decimal(2,0), CONVERT(varbinary(max), ac.EventData, 2) / 0x100000000 % 0x100) AS Voltage,
    CONVERT(decimal(1,0), CONVERT(varbinary(max), ac.EventData, 2) / 0x10000000000 % 0x100) AS SoC,
    CONVERT(decimal(18,0), CONVERT(varbinary(max), acd.VirtualUniqueID, 2)) AS ConvertedVirtualUniqueID,
    CASE
      WHEN u.ID IS NOT NULL THEN asset.Name
      ELSE CONVERT(varbinary(max), acd.BatteryModule, 2)
    END AS BatteryModuleOrHex,
    u.LastSeenDate
  FROM
    AssetSite AS ast
    INNER JOIN Asset AS asset ON ast.AssetID = asset.AssetID
    LEFT JOIN (
      SELECT
        ac.AssetID,
        ac.Port,
        ac.EventData,
        MAX(ac.Date) AS MaxDate
      FROM
        AlarmCharger AS ac
      WHERE
        ac.Port = 1
        AND ac.EventCode = 1798
      GROUP BY
        ac.AssetID,
        ac.Port
    ) AS ac ON ast.AssetID = ac.AssetID
    LEFT JOIN (
      SELECT
        acd.AssetID,
        acd.Port,
        acd.BatteryModule,
        acd.VirtualUniqueID,
        acd.UnitID,
        acd.Date
      FROM
        AlarmChargeDetail AS acd
      WHERE
        acd.Port = 1
      GROUP BY
        acd.AssetID,
        acd.Port,
        acd.BatteryModule,
        acd.VirtualUniqueID,
        acd.UnitID,
        acd.Date
    ) AS acd ON ast.AssetID = acd.AssetID
    LEFT JOIN Unit AS u ON acd.UnitID = u.ID
  WHERE
    ast.SiteID = '${siteID}'
    AND asset.System = 5
    AND (ac.MaxDate IS NULL OR ac.Date = ac.MaxDate)
    AND (acd.Date IS NULL OR acd.Date = (
      SELECT MAX(Date)
      FROM AlarmChargeDetail
      WHERE AssetID = ast.AssetID
        AND Port = 1
    ))
    AND (u.ID IS NULL OR u.UniqueId = acd.ConvertedVirtualUniqueID)
    AND (u.ID IS NULL OR asset.UnitID = u.ID);
  `

  const q1=`SELECT
  AssetSite.AssetID,
  Asset.Name,
  Asset.UnitID,
  AlarmCharger.Ended,
  CASE
      WHEN AlarmCharger.Ended IS NULL THEN 'Faulted'
      ELSE
          CASE AlarmCharger.EventCode
              WHEN 1792 THEN 'Charging'
              WHEN 1794 THEN 'Equalizing'
              WHEN 1796 THEN 'Idling'
          END
  END AS EventStatus,
  CONVERT(DECIMAL(10, 2), CONVERT(INT, CONVERT(VARBINARY, SUBSTRING(CONVERT(VARCHAR(MAX), AlarmCharger.EventData, 2), 7, 4), 1, 4)))) AS Power,
  CONVERT(INT, CONVERT(VARBINARY, SUBSTRING(CONVERT(VARCHAR(MAX), AlarmCharger.EventData, 2), 11, 2), 1, 2))) AS Voltage,
  CONVERT(INT, CONVERT(VARBINARY, SUBSTRING(CONVERT(VARCHAR(MAX), AlarmCharger.EventData, 2), 19, 1), 1, 1))) AS SoC,
  AlarmChargeDetail.BatteryModule,
  CONVERT(INT, CONVERT(VARBINARY, AlarmChargeDetail.VirtualUniqueID, 1, 4)) AS DecimalVirtualUniqueID,
  CASE
      WHEN Unit.ID IS NOT NULL AND Asset.UnitID = Unit.ID THEN Asset.Name
      ELSE CONVERT(VARCHAR(MAX), AlarmChargeDetail.BatteryModule, 2)
  END AS BatteryModuleHex,
  Unit.LastSeenDate
FROM
  AssetSite
  JOIN Asset ON Asset.ID = AssetSite.AssetID
  LEFT JOIN (
      SELECT
          AssetID,
          Port,
          MAX(Date) AS MaxDate
      FROM
          AlarmCharger
      WHERE
          Port = 1
      GROUP BY
          AssetID,
          Port
  ) AS LatestAlarmCharger ON LatestAlarmCharger.AssetID = AssetSite.AssetID
  LEFT JOIN AlarmCharger ON AlarmCharger.AssetID = LatestAlarmCharger.AssetID AND AlarmCharger.Port = LatestAlarmCharger.Port AND AlarmCharger.Date = LatestAlarmCharger.MaxDate
  LEFT JOIN (
      SELECT TOP 1
          AssetID,
          Port,
          EventData,
          Date
      FROM
          AlarmCharger
      WHERE
          EventCode = 1798 AND Port = 1
      ORDER BY
          Date DESC
  ) AS LatestEventData ON LatestEventData.AssetID = AssetSite.AssetID
  LEFT JOIN AlarmChargeDetail ON AlarmChargeDetail.AssetID = AssetSite.AssetID AND AlarmChargeDetail.Port = 1 AND AlarmChargeDetail.Date = (SELECT MAX(Date) FROM AlarmChargeDetail WHERE AssetID = AssetSite.AssetID AND Port = 1)
  LEFT JOIN Unit ON Unit.ID = AlarmChargeDetail.UnitID AND Unit.UniqueID = CONVERT(INT, CONVERT(VARBINARY, AlarmChargeDetail.VirtualUniqueID, 1, 4))
WHERE
  AssetSite.SiteID = '${siteID}'
  AND Asset.System = 5;
`

const w=`WITH LatestAlarmCharger AS (
    SELECT ac.AssetID, ac.Port, ac.Ended, ac.EventCode, ac.EventData,
        ROW_NUMBER() OVER (PARTITION BY ac.AssetID, ac.Port ORDER BY ac.Date DESC) AS RowNum
    FROM AlarmCharger ac
    WHERE ac.Port = 1
), LatestAlarmChargeDetail AS (
    SELECT acd.AssetID, acd.Port, acd.BatteryModule, acd.VirtualUniqueID, acd.UnitID,
        CONVERT(INT, SUBSTRING(acd.VirtualUniqueID, 7, 4), 2) AS Power,
        CONVERT(INT, SUBSTRING(acd.VirtualUniqueID, 11, 2), 2) AS Voltage,
        CONVERT(INT, SUBSTRING(acd.VirtualUniqueID, 19, 1), 2) AS SoC,
        CONVERT(INT, acd.VirtualUniqueID, 16) AS DecimalVirtualUniqueID,
        ROW_NUMBER() OVER (PARTITION BY acd.AssetID, acd.Port ORDER BY acd.Date DESC) AS RowNum
    FROM AlarmChargeDetail acd
    WHERE acd.Port = 1
), LatestAlarmChargerPort2 AS (
    SELECT ac.AssetID, ac.Port, ac.Ended, ac.EventCode, ac.EventData,
        ROW_NUMBER() OVER (PARTITION BY ac.AssetID, ac.Port ORDER BY ac.Date DESC) AS RowNum
    FROM AlarmCharger ac
    WHERE ac.Port = 2
), LatestAlarmChargeDetailPort2 AS (
    SELECT acd.AssetID, acd.Port, acd.BatteryModule, acd.VirtualUniqueID, acd.UnitID,
        CONVERT(INT, SUBSTRING(acd.VirtualUniqueID, 7, 4), 2) AS Power,
        CONVERT(INT, SUBSTRING(acd.VirtualUniqueID, 11, 2), 2) AS Voltage,
        CONVERT(INT, SUBSTRING(acd.VirtualUniqueID, 19, 1), 2) AS SoC,
        CONVERT(INT, acd.VirtualUniqueID, 16) AS DecimalVirtualUniqueID,
        ROW_NUMBER() OVER (PARTITION BY acd.AssetID, acd.Port ORDER BY acd.Date DESC) AS RowNum
    FROM AlarmChargeDetail acd
    WHERE acd.Port = 2
)
SELECT as.AssetID, a.Name, a.UnitID, ac1.Ended, ac1.EventCode,
    CASE
        WHEN ac1.Ended IS NULL THEN 'Faulted'
        ELSE
            CASE ac1.EventCode
                WHEN 1792 THEN 'Charging'
                WHEN 1794 THEN 'Equalizing'
                WHEN 1796 THEN 'Idling'
            END
    END AS EndedStatus,
    ac2.EventCode, CONVERT(INT, SUBSTRING(ac2.EventData, 7, 4), 2) AS Power,
    CONVERT(INT, SUBSTRING(ac2.EventData, 11, 2), 2) AS Voltage,
    CONVERT(INT, SUBSTRING(ac2.EventData, 19, 1), 2) AS SoC,
    lacd1.BatteryModule, lacd1.DecimalVirtualUniqueID, u1.LastSeenDate AS LastSeenDate1,
    lacd2.BatteryModule, lacd2.DecimalVirtualUniqueID, u2.LastSeenDate AS LastSeenDate2
FROM AssetSite asite
INNER JOIN Asset a ON a.AssetID = asite.AssetID
LEFT JOIN LatestAlarmCharger ac1 ON ac1.AssetID = a.AssetID AND ac1.Port = 1 AND ac1.RowNum = 1
LEFT JOIN LatestAlarmChargeDetail lacd1 ON lacd1.AssetID = a.AssetID AND lacd1.Port = 1 AND lacd1.RowNum = 1
LEFT JOIN Unit u1 ON u1.ID = lacd1.UnitID
LEFT JOIN AssetChargeDetail acd1 ON acd1.AssetID = a.AssetID AND acd1.Port = 1 AND acd1.RowNum = 1
LEFT JOIN (
    SELECT AssetID, VirtualUniqueID
    FROM AlarmChargeDetail
    WHERE Port = 1
) acdHex1 ON acdHex1.AssetID = a.AssetID AND acdHex1.VirtualUniqueID = lacd1.BatteryModule
LEFT JOIN LatestAlarmChargerPort2 ac2 ON ac2.AssetID = a.AssetID AND ac2.Port = 2 AND ac2.RowNum = 1
LEFT JOIN LatestAlarmChargeDetailPort2 lacd2 ON lacd2.AssetID = a.AssetID AND lacd2.Port = 2 AND lacd2.RowNum = 1
LEFT JOIN Unit u2 ON u2.ID = lacd2.UnitID
LEFT JOIN AssetChargeDetail acd2 ON acd2.AssetID = a.AssetID AND acd2.Port = 2 AND acd2.RowNum = 1
LEFT JOIN (
    SELECT AssetID, VirtualUniqueID
    FROM AlarmChargeDetail
    WHERE Port = 2
) acdHex2 ON acdHex2.AssetID = a.AssetID AND acdHex2.VirtualUniqueID = lacd2.BatteryModule
WHERE asite.SiteID = '${siteID}'
    AND a.System = 6;
`


const d1=`WITH cte1 AS (
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
SELECT Asset.ID AS AssetID, Unit.ID AS UnitID, Unit.LastSeenDate, Asset.Lat, Asset.Lon,
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

const d6=`SELECT
AssetSite.AssetID,
Asset.Name,
Asset.UnitID,
Asset.UnitID2,
CASE
  WHEN AlarmCharger.Ended IS NULL THEN 'Faulted'
  WHEN AlarmCharger.EventCode = 1792 THEN 'Charging'
  WHEN AlarmCharger.EventCode = 1794 THEN 'Equalizing'
  WHEN AlarmCharger.EventCode = 1796 THEN 'Idling'
END AS Status_Port1,
CONVERT(INT, CONVERT(VARBINARY(4), SUBSTRING(AlarmCharger.EventData, 7, 4), 2)) AS Power_Port1,
CONVERT(INT, CONVERT(VARBINARY(2), SUBSTRING(AlarmCharger.EventData, 5, 2), 2)) AS Voltage_Port1,
CONVERT(INT, CONVERT(VARBINARY(1), SUBSTRING(AlarmCharger.EventData, 19, 1), 2)) AS SoC_Port1,
CASE
  WHEN Unit.ID IS NOT NULL THEN Asset.Name
  ELSE CONVERT(VARCHAR(50), AlarmChargeDetail.BatteryModule, 2)
END AS BatteryModule_Port1,
CONVERT(INT, CONVERT(VARBINARY(4), AlarmChargeDetail.VirtualUniqueID, 2)) AS VirtualUniqueID_Port1,
AlarmChargeDetail.UnitID AS UnitID_Port1,
Unit.LastSeenDate AS LastSeenDate_Port1,
CASE
  WHEN AlarmCharger_Ended_Port2 IS NULL THEN 'Faulted'
  WHEN AlarmCharger_EventCode_Port2 = 1792 THEN 'Charging'
  WHEN AlarmCharger_EventCode_Port2 = 1794 THEN 'Equalizing'
  WHEN AlarmCharger_EventCode_Port2 = 1796 THEN 'Idling'
END AS Status_Port2,
CONVERT(INT, CONVERT(VARBINARY(4), SUBSTRING(AlarmCharger_EventData_Port2, 7, 4), 2)) AS Power_Port2,
CONVERT(INT, CONVERT(VARBINARY(2), SUBSTRING(AlarmCharger_EventData_Port2, 5, 2), 2)) AS Voltage_Port2,
CONVERT(INT, CONVERT(VARBINARY(1), SUBSTRING(AlarmCharger_EventData_Port2, 19, 1), 2)) AS SoC_Port2,
CASE
  WHEN Unit_Port2.ID IS NOT NULL THEN Asset_Port2.Name
  ELSE CONVERT(VARCHAR(50), AlarmChargeDetail_Port2.BatteryModule, 2)
END AS BatteryModule_Port2,
CONVERT(INT, CONVERT(VARBINARY(4), AlarmChargeDetail_Port2.VirtualUniqueID, 2)) AS VirtualUniqueID_Port2,
AlarmChargeDetail_Port2.UnitID AS UnitID_Port2,
Unit_Port2.LastSeenDate AS LastSeenDate_Port2
FROM
AssetSite
INNER JOIN Asset ON AssetSite.AssetID = Asset.ID
LEFT JOIN (
  SELECT TOP 1 WITH TIES *
  FROM AlarmCharger
  WHERE Port = 1
  AND AssetID = AssetSite.AssetID
  ORDER BY Date DESC
) AS AlarmCharger ON 1 = 1
LEFT JOIN AlarmChargeDetail ON AlarmCharger.ID = AlarmChargeDetail.AlarmChargerID
LEFT JOIN Unit ON AlarmChargeDetail.UnitID = Unit.ID AND CONVERT(INT, CONVERT(VARBINARY(4), AlarmChargeDetail.VirtualUniqueID, 2)) = Unit.UniqueId
OUTER APPLY (
  SELECT TOP 1 *
  FROM AlarmCharger
  WHERE Port = 2
  AND AssetID = AssetSite.AssetID
  ORDER BY Date DESC
) AS AlarmCharger_Port2
OUTER APPLY (
  SELECT TOP 1 *
  FROM AlarmChargeDetail
  WHERE AlarmChargerID = AlarmCharger_Port2.ID
  ORDER BY Date DESC
) AS AlarmChargeDetail_Port2
OUTER APPLY (
  SELECT TOP 1 *
  FROM Unit
  WHERE ID = AlarmChargeDetail_Port2.UnitID AND CONVERT(INT, CONVERT(VARBINARY(4), AlarmChargeDetail_Port2.VirtualUniqueID, 2)) = Unit.UniqueId
) AS Unit_Port2
WHERE
AssetSite.SiteID = '${siteID}'
AND Asset.System = 6`
    return await pool.request()
            .query(d1)
}
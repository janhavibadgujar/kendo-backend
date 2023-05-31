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

const g=`WITH HourOffsets AS (
    SELECT
      ID,
      CASE
        WHEN Timezone IN ('1') THEN -12.0
        WHEN Timezone IN ('2') THEN -11.0
        WHEN Timezone IN ('3') THEN -10.0
        WHEN Timezone IN ('4') THEN -9.0
        WHEN Timezone IN ('5', '6') THEN -8.0
        WHEN Timezone IN ('7', '8', '9', '10') THEN -7.0
        WHEN Timezone IN ('11', '12', '13', '14', '15') THEN -6.0
        WHEN Timezone IN ('16', '17', '18') THEN -5.0
        WHEN Timezone IN ('19') THEN -4.5
        WHEN Timezone IN ('20', '21', '22', '23') THEN -4.0
        WHEN Timezone IN ('24') THEN -3.5
        WHEN Timezone IN ('25', '26', '27', '28', '29') THEN -3.0
        WHEN Timezone IN ('30') THEN -2.0
        WHEN Timezone IN ('31', '32') THEN -1.0
        WHEN Timezone IN ('33', '34', '35') THEN 0.0
        WHEN Timezone IN ('36', '37', '38', '39', '40') THEN 1.0
        WHEN Timezone IN ('41', '42', '43', '44', '45', '46', '47', '48', '49') THEN 2.0
        WHEN Timezone IN ('50', '51', '52', '53', '54') THEN 3.0
        WHEN Timezone IN ('55') THEN 3.5
        WHEN Timezone IN ('56', '57', '58', '59', '60') THEN 4.0
        WHEN Timezone IN ('61') THEN 4.5
        WHEN Timezone IN ('62', '63', '64') THEN 5.0
        WHEN Timezone IN ('65', '66') THEN 5.5
        WHEN Timezone IN ('67') THEN 5.75
        WHEN Timezone IN ('68', '69') THEN 6.0
        WHEN Timezone IN ('70') THEN 6.5
        WHEN Timezone IN ('71', '72') THEN 7.0
        WHEN Timezone IN ('73', '74', '75', '76', '77') THEN 8.0
        WHEN Timezone IN ('78', '79', '80') THEN 9.0
        WHEN Timezone IN ('81', '82') THEN 9.5
        WHEN Timezone IN ('83', '84', '85', '86', '87') THEN 10.0
        WHEN Timezone IN ('88') THEN 11.0
        WHEN Timezone IN ('89', '90') THEN 12.0
        WHEN Timezone IN ('91') THEN 13.0
        ELSE 0.0
      END AS HourOffset
    FROM Site
    WHERE ID = '${siteID}'
  ), HourSequence AS (
    SELECT TOP 24
      DATEADD(HOUR, -ROW_NUMBER() OVER (ORDER BY (SELECT NULL)), GETUTCDATE()) AS StartHour
    FROM master..spt_values
  ), HourRange AS (
    SELECT
      FORMAT(DATEADD(HOUR, HO.HourOffset, HS.StartHour), 'yyyy-MM-dd HH:00') AS Hour,
      HO.HourOffset
    FROM HourOffsets AS HO
    CROSS JOIN HourSequence AS HS
  )
  SELECT
    HR.Hour,
    COALESCE(COUNT(CASE WHEN AP.InstantaneouskW > 0 THEN 1 END), 0) AS Charger,
    COALESCE(MAX(AP.InstantaneouskW), 0) AS MaxkW
  FROM HourRange AS HR
  LEFT JOIN AlarmPowerUsage AS AP ON
    FORMAT(DATEADD(HOUR, HR.HourOffset, AP.Date), 'yyyy-MM-dd HH:00') = HR.Hour
    AND AP.SiteID = '${siteID}'
    AND AP.Date >= HR.Hour
    AND AP.Date < DATEADD(HOUR, 24, HR.Hour)
  GROUP BY HR.Hour, HR.HourOffset
  ORDER BY HR.Hour;
  `
    return await pool.request()
            .query(g)
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
CONVERT(INT, CONVERT(VARBINARY(4), SUBSTRING(AlarmCharger1.EventData, 7, 4), 2)) / 100 AS Power1,
CONVERT(INT, CONVERT(VARBINARY(2), SUBSTRING(AlarmCharger1.EventData, 5, 2), 2)) / 100 AS Voltage1,
CONVERT(INT, CONVERT(VARBINARY(1), SUBSTRING(AlarmCharger1.EventData, 19, 1), 2)) AS SoC1,
CASE
    WHEN AlarmChargeDetail.BatteryModule = Unit.UniqueID THEN Asset.Name
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
        AlarmCharger
    WHERE
        Port = 1 AND EventCode = 1798
    ) AS AlarmCharger1 ON AssetSite.AssetID = AlarmCharger1.AssetID AND AlarmCharger1.RowNumber = 1
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
const g=`WITH CTE AS (
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
        ac.EventCode,
        ac.AssetID
    FROM AlarmCharger ac
    INNER JOIN CTE ON ac.AssetID = CTE.AssetID
    WHERE ac.Port = 1
    ORDER BY ac.Date DESC
),
CTE4 AS (
    SELECT CONVERT(INT, CONVERT(VARBINARY(4), SUBSTRING(EventData, 7, 4), 2)) / 100 AS Power
    FROM CTE3
),
CTE5 AS (
    SELECT CONVERT(INT, CONVERT(VARBINARY(2), SUBSTRING(EventData, 5, 2), 2)) / 100 AS Voltage
    FROM CTE3
),
CTE6 AS (
    SELECT CONVERT(INT, CONVERT(VARBINARY(1), SUBSTRING(EventData, 19, 1), 2)) AS SoC
    FROM CTE3
),
CTE7 AS (
    SELECT TOP 1 
        BatteryModule,
        CONVERT(INT, VirtualUniqueID, 1) AS VirtualUniqueID,
        UnitID,
        Date,
        ac.AssetID
    FROM AlarmChargeDetail ac
    INNER JOIN CTE ON ac.AssetID = CTE.AssetID
    WHERE Port = 1
    ORDER BY Date DESC
),
CTE10 AS (
    SELECT TOP 1 
        ac.EventData,
        ac.Ended,
        ac.EventCode,
        ac.AssetID
    FROM AlarmCharger ac
    INNER JOIN CTE ON ac.AssetID = CTE.AssetID
    WHERE ac.Port = 2
    ORDER BY ac.Date DESC
),
CTE11 AS (
    SELECT CONVERT(INT, CONVERT(VARBINARY(4), SUBSTRING(EventData, 7, 4), 2)) / 100 AS Power
    FROM CTE10
),
CTE12 AS (
    SELECT CONVERT(INT, CONVERT(VARBINARY(2), SUBSTRING(EventData, 5, 2), 2)) / 100 AS Voltage
    FROM CTE10
),
CTE13 AS (
    SELECT CONVERT(INT, CONVERT(VARBINARY(1), SUBSTRING(EventData, 19, 1), 2)) AS SoC
    FROM CTE10
),
CTE14 AS (
    SELECT TOP 1 
        BatteryModule,
        CONVERT(INT, VirtualUniqueID, 1) AS VirtualUniqueID,
        UnitID,
        Date,
        ac.AssetID
    FROM AlarmChargeDetail ac
    INNER JOIN CTE ON ac.AssetID = CTE.AssetID
    WHERE Port = 2
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
        WHEN CTE3.AssetID IS NOT NULL THEN
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
            WHEN u1.ID = a1.UnitID THEN a1.Name
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
        WHEN CTE10.AssetID IS NOT NULL THEN
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
            WHEN u2.ID = a2.UnitID2 THEN a2.Name
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
LEFT JOIN CTE7 ON 1=1 AND CTE2.ID = CTE7.AssetID
LEFT JOIN CTE10 ON 1=1
LEFT JOIN CTE11 ON 1=1
LEFT JOIN CTE12 ON 1=1
LEFT JOIN CTE13 ON 1=1
LEFT JOIN CTE14 ON 1=1 AND CTE2.ID = CTE14.AssetID;
`
const g1=`WITH CTE AS (
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
        ac.EventCode,
        ac.AssetID
    FROM AlarmCharger ac
    INNER JOIN CTE ON ac.AssetID = CTE.AssetID
    WHERE ac.Port = 1
    ORDER BY ac.Date DESC
),
CTE4 AS (
    SELECT CONVERT(INT, CONVERT(VARBINARY(4), SUBSTRING(EventData, 7, 4), 2)) / 100 AS Power
    FROM CTE3
),
CTE5 AS (
    SELECT CONVERT(INT, CONVERT(VARBINARY(2), SUBSTRING(EventData, 5, 2), 2)) / 100 AS Voltage
    FROM CTE3
),
CTE6 AS (
    SELECT CONVERT(INT, CONVERT(VARBINARY(1), SUBSTRING(EventData, 19, 1), 2)) AS SoC
    FROM CTE3
),
CTE7 AS (
    SELECT TOP 1 
        BatteryModule,
        UnitID,
        Date,
        ac.AssetID
    FROM AlarmChargeDetail ac
    INNER JOIN CTE ON ac.AssetID = CTE.AssetID
    WHERE Port = 1
    ORDER BY Date DESC
),
CTE10 AS (
    SELECT TOP 1 
        ac.EventData,
        ac.Ended,
        ac.EventCode,
        ac.AssetID
    FROM AlarmCharger ac
    INNER JOIN CTE ON ac.AssetID = CTE.AssetID
    WHERE ac.Port = 2
    ORDER BY ac.Date DESC
),
CTE11 AS (
    SELECT CONVERT(INT, CONVERT(VARBINARY(4), SUBSTRING(EventData, 7, 4), 2)) / 100 AS Power
    FROM CTE10
),
CTE12 AS (
    SELECT CONVERT(INT, CONVERT(VARBINARY(2), SUBSTRING(EventData, 5, 2), 2)) / 100 AS Voltage
    FROM CTE10
),
CTE13 AS (
    SELECT CONVERT(INT, CONVERT(VARBINARY(1), SUBSTRING(EventData, 19, 1), 2)) AS SoC
    FROM CTE10
),
CTE14 AS (
    SELECT TOP 1 
        BatteryModule,
        UnitID,
        Date,
        ac.AssetID
    FROM AlarmChargeDetail ac
    INNER JOIN CTE ON ac.AssetID = CTE.AssetID
    WHERE Port = 2
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
        WHEN CTE3.AssetID IS NOT NULL THEN
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
            WHEN EXISTS (
                SELECT 1
                FROM Unit u1
                WHERE u1.UniqueID = ad1.BatteryModule
            ) THEN
                (SELECT a1.Name FROM Asset a1 WHERE a1.UnitID = (SELECT u1.ID FROM Unit u1 WHERE u1.UniqueID = ad1.BatteryModule))
            ELSE STUFF(CONVERT(VARCHAR(50), CONVERT(VARBINARY(8), CAST(ad1.BatteryModule AS BIGINT)), 2), 1, PATINDEX('%[^0]%', CONVERT(VARCHAR(50), CONVERT(VARBINARY(8), CAST(ad1.BatteryModule AS BIGINT)), 2)) - 1, '')
        END
        FROM AlarmChargeDetail ad1
        WHERE ad1.UnitID = CTE2.UnitID AND ad1.Port = 1
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
        WHEN CTE10.AssetID IS NOT NULL THEN
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
            WHEN EXISTS (
                SELECT 1
                FROM Unit u2
                WHERE u2.UniqueID = ad2.BatteryModule
            ) THEN
                (SELECT a2.Name FROM Asset a2 WHERE a2.UnitID2 = (SELECT u2.ID FROM Unit u2 WHERE u2.UniqueID = ad2.BatteryModule))
            ELSE STUFF(CONVERT(VARCHAR(50), CONVERT(VARBINARY(8), CAST(ad2.BatteryModule AS BIGINT)), 2), 1, PATINDEX('%[^0]%', CONVERT(VARCHAR(50), CONVERT(VARBINARY(8), CAST(ad2.BatteryModule AS BIGINT)), 2)) - 1, '')
        END
        FROM AlarmChargeDetail ad2
        WHERE ad2.UnitID = CTE2.UnitID2 AND ad2.Port = 2
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
LEFT JOIN CTE7 ON 1=1 AND CTE2.ID = CTE7.AssetID
LEFT JOIN CTE10 ON 1=1
LEFT JOIN CTE11 ON 1=1
LEFT JOIN CTE12 ON 1=1
LEFT JOIN CTE13 ON 1=1
LEFT JOIN CTE14 ON 1=1 AND CTE2.ID = CTE14.AssetID;`

const g2=`WITH CTE AS (
    SELECT AssetID
    FROM AssetSite
    WHERE SiteID = '${siteID}'
),
CTE2 AS (
    SELECT a.Name, a.UnitID, a.UnitID2, a.ID AS AssetID, a.Lat, a.Lon
    FROM Asset a
    WHERE a.System = 6
        AND a.ID IN (SELECT AssetID FROM CTE)
        AND a.Lat IS NOT NULL
        AND a.Lon IS NOT NULL
),
CTE3 AS (
    SELECT ac.AssetID,
        CASE
            WHEN ac.Ended IS NULL THEN 'Faulted'
            WHEN ac.EventCode = 1792 THEN 'Charging'
            WHEN ac.EventCode = 1794 THEN 'Equalizing'
            WHEN ac.EventCode = 1796 THEN 'Idling'
        END AS [Status],
        ROW_NUMBER() OVER (PARTITION BY ac.AssetID ORDER BY ac.Date DESC) AS RowNum
    FROM AlarmCharger ac
    JOIN CTE ON ac.AssetID = CTE.AssetID
    WHERE ac.Port = 1
),
CTE4 AS (
    SELECT ac.AssetID,
        CONVERT(INT, CONVERT(VARBINARY(4), SUBSTRING(ac.EventData, 7, 4), 2)) / 100 AS Power,
        CONVERT(INT, CONVERT(VARBINARY(2), SUBSTRING(ac.EventData, 5, 2), 2)) / 100 AS Voltage,
        CONVERT(INT, CONVERT(VARBINARY(1), SUBSTRING(ac.EventData, 19, 1), 2)) AS SoC,
        ROW_NUMBER() OVER (PARTITION BY ac.AssetID ORDER BY ac.Date DESC) AS RowNum
    FROM AlarmCharger ac
    JOIN CTE ON ac.AssetID = CTE.AssetID
    WHERE ac.EventCode = 1798
        AND ac.Port = 1
),
CTE5 AS (
    SELECT acd.BatteryModule, acd.UnitID, acd.AssetID,
        CASE
            WHEN EXISTS (
                SELECT 1
                FROM Unit u
                WHERE u.UniqueID = acd.BatteryModule
            ) THEN (SELECT a.Name FROM Asset a WHERE a.ID = acd.AssetID)
            ELSE STUFF(CONVERT(VARCHAR(50), CONVERT(VARBINARY(8), CAST(acd.BatteryModule AS BIGINT)), 2), 1, PATINDEX('%[^0]%', CONVERT(VARCHAR(50), CONVERT(VARBINARY(8), CAST(acd.BatteryModule AS BIGINT)), 2)) - 1, '')
        END AS Name,
        ROW_NUMBER() OVER (PARTITION BY acd.AssetID ORDER BY acd.Date DESC) AS RowNum
    FROM AlarmChargeDetail acd
    LEFT JOIN Unit u ON u.ID = acd.UnitID
    INNER JOIN Asset a ON a.ID = acd.AssetID AND a.UnitID = acd.UnitID
    JOIN CTE ON acd.AssetID = CTE.AssetID
    WHERE acd.Port = 1
),
CTE6 AS (
    SELECT u.LastSeenDate, a.ID AS AssetID,
        ROW_NUMBER() OVER (PARTITION BY a.ID ORDER BY a.ID) AS RowNum
    FROM Unit u
    JOIN Asset a ON u.ID = a.UnitID
    JOIN CTE ON a.ID = CTE.AssetID
),
CTE7 AS (
    SELECT ac.AssetID,
        CASE
            WHEN ac.Ended IS NULL THEN 'Faulted'
            WHEN ac.EventCode = 1792 THEN 'Charging'
            WHEN ac.EventCode = 1794 THEN 'Equalizing'
            WHEN ac.EventCode = 1796 THEN 'Idling'
        END AS [Status],
        ROW_NUMBER() OVER (PARTITION BY ac.AssetID ORDER BY ac.Date DESC) AS RowNum
    FROM AlarmCharger ac
    JOIN CTE ON ac.AssetID = CTE.AssetID
    WHERE ac.Port = 2
),
CTE8 AS (
    SELECT ac.AssetID,
        CONVERT(INT, CONVERT(VARBINARY(4), SUBSTRING(ac.EventData, 7, 4), 2)) / 100 AS Power,
        CONVERT(INT, CONVERT(VARBINARY(2), SUBSTRING(ac.EventData, 5, 2), 2)) / 100 AS Voltage,
        CONVERT(INT, CONVERT(VARBINARY(1), SUBSTRING(ac.EventData, 19, 1), 2)) AS SoC,
        ROW_NUMBER() OVER (PARTITION BY ac.AssetID ORDER BY ac.Date DESC) AS RowNum
    FROM AlarmCharger ac
    JOIN CTE ON ac.AssetID = CTE.AssetID
    WHERE ac.EventCode = 1798
        AND ac.Port = 2
),
CTE9 AS (
    SELECT acd.BatteryModule, acd.UnitID, acd.AssetID,
        CASE
            WHEN EXISTS (
                SELECT 1
                FROM Unit u
                WHERE u.UniqueID = acd.BatteryModule
            ) THEN (SELECT a.Name FROM Asset a WHERE a.ID = acd.AssetID)
            ELSE STUFF(CONVERT(VARCHAR(50), CONVERT(VARBINARY(8), CAST(acd.BatteryModule AS BIGINT)), 2), 1, PATINDEX('%[^0]%', CONVERT(VARCHAR(50), CONVERT(VARBINARY(8), CAST(acd.BatteryModule AS BIGINT)), 2)) - 1, '')
        END AS Name,
        ROW_NUMBER() OVER (PARTITION BY acd.AssetID ORDER BY acd.Date DESC) AS RowNum
    FROM AlarmChargeDetail acd
    LEFT JOIN Unit u ON u.ID = acd.UnitID
    INNER JOIN Asset a ON a.ID = acd.AssetID AND a.UnitID2 = acd.UnitID
    JOIN CTE ON acd.AssetID = CTE.AssetID
    WHERE acd.Port = 2
),
CTE10 AS (
    SELECT u.LastSeenDate, a.ID AS AssetID,
        ROW_NUMBER() OVER (PARTITION BY a.ID ORDER BY a.ID) AS RowNum
    FROM Unit u
    JOIN Asset a ON u.ID = a.UnitID2
    JOIN CTE ON a.ID = CTE.AssetID
)
SELECT CTE2.AssetID, CTE2.Name, CTE2.Lat, CTE2.Lon,
    COALESCE(CTE3.[Status], NULL) AS [Status1],
    CTE4.Power AS Power1, CTE4.Voltage AS Voltage1, CTE4.SoC AS SoC1,
    CTE5.Name AS Paired1,
    CTE6.LastSeenDate AS LastSeen1,
    COALESCE(CTE7.[Status], NULL) AS [Status2],
    CTE8.Power AS Power2, CTE8.Voltage AS Voltage2, CTE8.SoC AS SoC2,
    CTE9.Name AS Paired2,
    CTE10.LastSeenDate AS LastSeen2
FROM CTE2
LEFT JOIN (
    SELECT AssetID, [Status], RowNum
    FROM CTE3
    WHERE RowNum = 1
) AS CTE3 ON CTE3.AssetID = CTE2.AssetID
LEFT JOIN (
    SELECT AssetID, Power, Voltage, SoC, RowNum
    FROM CTE4
    WHERE RowNum = 1
) AS CTE4 ON CTE4.AssetID = CTE2.AssetID
LEFT JOIN (
    SELECT AssetID, Name, RowNum
    FROM CTE5
    WHERE RowNum = 1
) AS CTE5 ON CTE5.AssetID = CTE2.AssetID
LEFT JOIN (
    SELECT AssetID, LastSeenDate, RowNum
    FROM CTE6
    WHERE RowNum = 1
) AS CTE6 ON CTE6.AssetID = CTE2.AssetID
LEFT JOIN (
    SELECT AssetID, [Status], RowNum
    FROM CTE7
    WHERE RowNum = 1
) AS CTE7 ON CTE7.AssetID = CTE2.AssetID
LEFT JOIN (
    SELECT AssetID, Power, Voltage, SoC, RowNum
    FROM CTE8
    WHERE RowNum = 1
) AS CTE8 ON CTE8.AssetID = CTE2.AssetID
LEFT JOIN (
    SELECT AssetID, Name, RowNum
    FROM CTE9
    WHERE RowNum = 1
) AS CTE9 ON CTE9.AssetID = CTE2.AssetID
LEFT JOIN (
    SELECT AssetID, LastSeenDate, RowNum
    FROM CTE10
    WHERE RowNum = 1
) AS CTE10 ON CTE10.AssetID = CTE2.AssetID;
`
    return await pool.request()
            .query(g2)
}

exports.getMapSystem=async(siteID)=>{
const t=`WITH CTE AS (
    SELECT AssetID
    FROM AssetSite
    WHERE SiteID = '${siteID}'
),
CTE2 AS (
    SELECT a.Name, a.UnitID, a.ID AS AssetID, a.Lat, a.Lon
    FROM Asset a
    WHERE a.System IN (7, 11)
        AND a.ID IN (SELECT AssetID FROM CTE)
        AND a.Lat IS NOT NULL
        AND a.Lon IS NOT NULL
),
CTE3 AS (
    SELECT ac.AssetID,
        CASE
            WHEN ac.Ended IS NULL THEN 'Faulted'
            WHEN ac.EventCode = 1792 THEN 'Charging'
            WHEN ac.EventCode = 1794 THEN 'Equalizing'
            WHEN ac.EventCode = 1796 THEN 'Idling'
        END AS [Status],
        ROW_NUMBER() OVER (PARTITION BY ac.AssetID ORDER BY ac.Date DESC) AS RowNum
    FROM AlarmCharger ac
    JOIN CTE ON ac.AssetID = CTE.AssetID
    WHERE ac.Port = 1
),
CTE4 AS (
    SELECT ac.AssetID,
        CONVERT(INT, CONVERT(VARBINARY(4), SUBSTRING(ac.EventData, 7, 4), 2)) / 100 AS Power,
        CONVERT(INT, CONVERT(VARBINARY(2), SUBSTRING(ac.EventData, 5, 2), 2)) / 100 AS Voltage,
        CONVERT(INT, CONVERT(VARBINARY(1), SUBSTRING(ac.EventData, 19, 1), 2)) AS SoC,
        ROW_NUMBER() OVER (PARTITION BY ac.AssetID ORDER BY ac.Date DESC) AS RowNum
    FROM AlarmCharger ac
    JOIN CTE ON ac.AssetID = CTE.AssetID
    WHERE ac.EventCode = 1798
        AND ac.Port = 1
),
CTE5 AS (
    SELECT acd.BatteryModule, acd.VirtualUniqueID, acd.UnitID, acd.AssetID,
        CASE
            WHEN u.ID IS NOT NULL THEN a.Name
            ELSE STUFF(CONVERT(VARCHAR(50), CONVERT(VARBINARY(8), CAST(acd.BatteryModule AS BIGINT)), 2), 1, PATINDEX('%[^0]%', CONVERT(VARCHAR(50), CONVERT(VARBINARY(8), CAST(acd.BatteryModule AS BIGINT)), 2)) - 1, '')
        END AS Name,
        ROW_NUMBER() OVER (PARTITION BY acd.AssetID ORDER BY acd.Date DESC) AS RowNum
    FROM AlarmChargeDetail acd
    LEFT JOIN Unit u ON u.ID = acd.UnitID AND u.UniqueId = CONVERT(VARBINARY(8), acd.VirtualUniqueID, 2)
    INNER JOIN Asset a ON a.ID = acd.AssetID AND a.UnitID = acd.UnitID
    JOIN CTE ON acd.AssetID = CTE.AssetID
    WHERE acd.Port = 1
),
CTE6 AS (
    SELECT u.LastSeenDate, a.ID AS AssetID,
        ROW_NUMBER() OVER (PARTITION BY a.ID ORDER BY a.ID) AS RowNum
    FROM Unit u
    JOIN Asset a ON u.ID = a.UnitID
    JOIN CTE ON a.ID = CTE.AssetID
),
CTE7 AS (
    SELECT ac.AssetID,
        CASE
            WHEN ac.Ended IS NULL THEN 'Faulted'
            WHEN ac.EventCode = 1792 THEN 'Charging'
            WHEN ac.EventCode = 1794 THEN 'Equalizing'
            WHEN ac.EventCode = 1796 THEN 'Idling'
        END AS [Status],
        ROW_NUMBER() OVER (PARTITION BY ac.AssetID ORDER BY ac.Date DESC) AS RowNum
    FROM AlarmCharger ac
    JOIN CTE ON ac.AssetID = CTE.AssetID
    WHERE ac.Port = 2
),
CTE8 AS (
    SELECT ac.AssetID,
        CONVERT(INT, CONVERT(VARBINARY(4), SUBSTRING(ac.EventData, 7, 4), 2)) / 100 AS Power,
        CONVERT(INT, CONVERT(VARBINARY(2), SUBSTRING(ac.EventData, 5, 2), 2)) / 100 AS Voltage,
        CONVERT(INT, CONVERT(VARBINARY(1), SUBSTRING(ac.EventData, 19, 1), 2)) AS SoC,
        ROW_NUMBER() OVER (PARTITION BY ac.AssetID ORDER BY ac.Date DESC) AS RowNum
    FROM AlarmCharger ac
    JOIN CTE ON ac.AssetID = CTE.AssetID
    WHERE ac.EventCode = 1798
        AND ac.Port = 2
),
CTE9 AS (
    SELECT acd.BatteryModule, acd.VirtualUniqueID, acd.UnitID, acd.AssetID,
        CASE
            WHEN u.ID IS NOT NULL THEN a.Name
            ELSE STUFF(CONVERT(VARCHAR(50), CONVERT(VARBINARY(8), CAST(acd.BatteryModule AS BIGINT)), 2), 1, PATINDEX('%[^0]%', CONVERT(VARCHAR(50), CONVERT(VARBINARY(8), CAST(acd.BatteryModule AS BIGINT)), 2)) - 1, '')
        END AS Name,
        ROW_NUMBER() OVER (PARTITION BY acd.AssetID ORDER BY acd.Date DESC) AS RowNum
    FROM AlarmChargeDetail acd
    LEFT JOIN Unit u ON u.ID = acd.UnitID AND u.UniqueId = CONVERT(VARBINARY(8), acd.VirtualUniqueID, 2)
    INNER JOIN Asset a ON a.ID = acd.AssetID AND a.UnitID = acd.UnitID
    JOIN CTE ON acd.AssetID = CTE.AssetID
    WHERE acd.Port = 2
),
CTE10 AS (
    SELECT u.LastSeenDate, a.ID AS AssetID,
        ROW_NUMBER() OVER (PARTITION BY a.ID ORDER BY a.ID) AS RowNum
    FROM Unit u
    JOIN Asset a ON u.ID = a.UnitID
    JOIN CTE ON a.ID = CTE.AssetID
)
SELECT CTE2.AssetID, CTE2.Name, CTE2.Lat, CTE2.Lon,
    COALESCE(CTE3.[Status], NULL) AS [Status1],
    CTE4.Power AS Power1, CTE4.Voltage AS Voltage1, CTE4.SoC AS SoC1,
    CTE5.Name AS Paired1,
    CTE6.LastSeenDate AS LastSeen1,
    COALESCE(CTE7.[Status], NULL) AS [Status2],
    CTE8.Power AS Power2, CTE8.Voltage AS Voltage2, CTE8.SoC AS SoC2,
    CTE9.Name AS Paired2,
    CTE10.LastSeenDate AS LastSeen2
FROM CTE2
LEFT JOIN (
    SELECT AssetID, [Status], RowNum
    FROM CTE3
    WHERE RowNum = 1
) AS CTE3 ON CTE3.AssetID = CTE2.AssetID
LEFT JOIN (
    SELECT AssetID, Power, Voltage, SoC, RowNum
    FROM CTE4
    WHERE RowNum = 1
) AS CTE4 ON CTE4.AssetID = CTE2.AssetID
LEFT JOIN (
    SELECT AssetID, Name, RowNum
    FROM CTE5
    WHERE RowNum = 1
) AS CTE5 ON CTE5.AssetID = CTE2.AssetID
LEFT JOIN (
    SELECT AssetID, LastSeenDate, RowNum
    FROM CTE6
    WHERE RowNum = 1
) AS CTE6 ON CTE6.AssetID = CTE2.AssetID
LEFT JOIN (
    SELECT AssetID, [Status], RowNum
    FROM CTE7
    WHERE RowNum = 1
) AS CTE7 ON CTE7.AssetID = CTE2.AssetID
LEFT JOIN (
    SELECT AssetID, Power, Voltage, SoC, RowNum
    FROM CTE8
    WHERE RowNum = 1
) AS CTE8 ON CTE8.AssetID = CTE2.AssetID
LEFT JOIN (
    SELECT AssetID, Name, RowNum
    FROM CTE9
    WHERE RowNum = 1
) AS CTE9 ON CTE9.AssetID = CTE2.AssetID
LEFT JOIN (
    SELECT AssetID, LastSeenDate, RowNum
    FROM CTE10
    WHERE RowNum = 1
) AS CTE10 ON CTE10.AssetID = CTE2.AssetID;
`

const t1=`WITH CTE AS (
    SELECT AssetID
    FROM AssetSite
    WHERE SiteID = '${siteID}'
),
CTE2 AS (
    SELECT a.Name, a.UnitID, a.ID AS AssetID, a.Lat, a.Lon
    FROM Asset a
    WHERE a.System IN (7, 11)
        AND a.ID IN (SELECT AssetID FROM CTE)
        AND a.Lat IS NOT NULL
        AND a.Lon IS NOT NULL
),
CTE3 AS (
    SELECT ac.AssetID,
        CASE
            WHEN ac.Ended IS NULL THEN 'Faulted'
            WHEN ac.EventCode = 1792 THEN 'Charging'
            WHEN ac.EventCode = 1794 THEN 'Equalizing'
            WHEN ac.EventCode = 1796 THEN 'Idling'
        END AS [Status],
        ROW_NUMBER() OVER (PARTITION BY ac.AssetID ORDER BY ac.Date DESC) AS RowNum
    FROM AlarmCharger ac
    JOIN CTE ON ac.AssetID = CTE.AssetID
    WHERE ac.Port = 1
),
CTE4 AS (
    SELECT ac.AssetID,
        CONVERT(INT, CONVERT(VARBINARY(4), SUBSTRING(ac.EventData, 7, 4), 2)) / 100 AS Power,
        CONVERT(INT, CONVERT(VARBINARY(2), SUBSTRING(ac.EventData, 5, 2), 2)) / 100 AS Voltage,
        CONVERT(INT, CONVERT(VARBINARY(1), SUBSTRING(ac.EventData, 19, 1), 2)) AS SoC,
        ROW_NUMBER() OVER (PARTITION BY ac.AssetID ORDER BY ac.Date DESC) AS RowNum
    FROM AlarmCharger ac
    JOIN CTE ON ac.AssetID = CTE.AssetID
    WHERE ac.EventCode = 1798
        AND ac.Port = 1
),
CTE5 AS (
    SELECT acd.BatteryModule, acd.UnitID, acd.AssetID,
        CASE
            WHEN EXISTS (
                SELECT 1
                FROM Unit u
                WHERE u.UniqueID = acd.BatteryModule
            ) THEN (SELECT a.Name FROM Asset a WHERE a.ID = acd.AssetID)
            ELSE STUFF(CONVERT(VARCHAR(50), CONVERT(VARBINARY(8), CAST(acd.BatteryModule AS BIGINT)), 2), 1, PATINDEX('%[^0]%', CONVERT(VARCHAR(50), CONVERT(VARBINARY(8), CAST(acd.BatteryModule AS BIGINT)), 2)) - 1, '')
        END AS Name,
        ROW_NUMBER() OVER (PARTITION BY acd.AssetID ORDER BY acd.Date DESC) AS RowNum
    FROM AlarmChargeDetail acd
    LEFT JOIN Unit u ON u.ID = acd.UnitID
    INNER JOIN Asset a ON a.ID = acd.AssetID AND a.UnitID = acd.UnitID
    JOIN CTE ON acd.AssetID = CTE.AssetID
    WHERE acd.Port = 1
),
CTE6 AS (
    SELECT u.LastSeenDate, a.ID AS AssetID,
        ROW_NUMBER() OVER (PARTITION BY a.ID ORDER BY a.ID) AS RowNum
    FROM Unit u
    JOIN Asset a ON u.ID = a.UnitID
    JOIN CTE ON a.ID = CTE.AssetID
),
CTE7 AS (
    SELECT ac.AssetID,
        CASE
            WHEN ac.Ended IS NULL THEN 'Faulted'
            WHEN ac.EventCode = 1792 THEN 'Charging'
            WHEN ac.EventCode = 1794 THEN 'Equalizing'
            WHEN ac.EventCode = 1796 THEN 'Idling'
        END AS [Status],
        ROW_NUMBER() OVER (PARTITION BY ac.AssetID ORDER BY ac.Date DESC) AS RowNum
    FROM AlarmCharger ac
    JOIN CTE ON ac.AssetID = CTE.AssetID
    WHERE ac.Port = 2
),
CTE8 AS (
    SELECT ac.AssetID,
        CONVERT(INT, CONVERT(VARBINARY(4), SUBSTRING(ac.EventData, 7, 4), 2)) / 100 AS Power,
        CONVERT(INT, CONVERT(VARBINARY(2), SUBSTRING(ac.EventData, 5, 2), 2)) / 100 AS Voltage,
        CONVERT(INT, CONVERT(VARBINARY(1), SUBSTRING(ac.EventData, 19, 1), 2)) AS SoC,
        ROW_NUMBER() OVER (PARTITION BY ac.AssetID ORDER BY ac.Date DESC) AS RowNum
    FROM AlarmCharger ac
    JOIN CTE ON ac.AssetID = CTE.AssetID
    WHERE ac.EventCode = 1798
        AND ac.Port = 2
),
CTE9 AS (
    SELECT acd.BatteryModule, acd.UnitID, acd.AssetID,
        CASE
            WHEN EXISTS (
                SELECT 1
                FROM Unit u
                WHERE u.UniqueID = acd.BatteryModule
            ) THEN (SELECT a.Name FROM Asset a WHERE a.ID = acd.AssetID)
            ELSE STUFF(CONVERT(VARCHAR(50), CONVERT(VARBINARY(8), CAST(acd.BatteryModule AS BIGINT)), 2), 1, PATINDEX('%[^0]%', CONVERT(VARCHAR(50), CONVERT(VARBINARY(8), CAST(acd.BatteryModule AS BIGINT)), 2)) - 1, '')
        END AS Name,
        ROW_NUMBER() OVER (PARTITION BY acd.AssetID ORDER BY acd.Date DESC) AS RowNum
    FROM AlarmChargeDetail acd
    LEFT JOIN Unit u ON u.ID = acd.UnitID
    INNER JOIN Asset a ON a.ID = acd.AssetID AND a.UnitID = acd.UnitID
    JOIN CTE ON acd.AssetID = CTE.AssetID
    WHERE acd.Port = 2
),
CTE10 AS (
    SELECT u.LastSeenDate, a.ID AS AssetID,
        ROW_NUMBER() OVER (PARTITION BY a.ID ORDER BY a.ID) AS RowNum
    FROM Unit u
    JOIN Asset a ON u.ID = a.UnitID
    JOIN CTE ON a.ID = CTE.AssetID
)
SELECT CTE2.AssetID, CTE2.Name, CTE2.Lat, CTE2.Lon,
    COALESCE(CTE3.[Status], NULL) AS [Status1],
    CTE4.Power AS Power1, CTE4.Voltage AS Voltage1, CTE4.SoC AS SoC1,
    CTE5.Name AS Paired1,
    CTE6.LastSeenDate AS LastSeen1,
    COALESCE(CTE7.[Status], NULL) AS [Status2],
    CTE8.Power AS Power2, CTE8.Voltage AS Voltage2, CTE8.SoC AS SoC2,
    CTE9.Name AS Paired2,
    CTE10.LastSeenDate AS LastSeen2
FROM CTE2
LEFT JOIN (
    SELECT AssetID, [Status], RowNum
    FROM CTE3
    WHERE RowNum = 1
) AS CTE3 ON CTE3.AssetID = CTE2.AssetID
LEFT JOIN (
    SELECT AssetID, Power, Voltage, SoC, RowNum
    FROM CTE4
    WHERE RowNum = 1
) AS CTE4 ON CTE4.AssetID = CTE2.AssetID
LEFT JOIN (
    SELECT AssetID, Name, RowNum
    FROM CTE5
    WHERE RowNum = 1
) AS CTE5 ON CTE5.AssetID = CTE2.AssetID
LEFT JOIN (
    SELECT AssetID, LastSeenDate, RowNum
    FROM CTE6
    WHERE RowNum = 1
) AS CTE6 ON CTE6.AssetID = CTE2.AssetID
LEFT JOIN (
    SELECT AssetID, [Status], RowNum
    FROM CTE7
    WHERE RowNum = 1
) AS CTE7 ON CTE7.AssetID = CTE2.AssetID
LEFT JOIN (
    SELECT AssetID, Power, Voltage, SoC, RowNum
    FROM CTE8
    WHERE RowNum = 1
) AS CTE8 ON CTE8.AssetID = CTE2.AssetID
LEFT JOIN (
    SELECT AssetID, Name, RowNum
    FROM CTE9
    WHERE RowNum = 1
) AS CTE9 ON CTE9.AssetID = CTE2.AssetID
LEFT JOIN (
    SELECT AssetID, LastSeenDate, RowNum
    FROM CTE10
    WHERE RowNum = 1
) AS CTE10 ON CTE10.AssetID = CTE2.AssetID;
`
   return await pool.request()
            .query(t1)
}


exports.getMaintenanceHistory=async(assetIds)=>{
    const assetIdValues = assetIds.map(asset => `CONVERT(uniqueidentifier, '${asset}')`).join(',');

const q=`SELECT
Asset.ID,
Asset.Name,
Asset.AssetTypeID,
Asset.UnderPM,
Asset.OverPM,
CASE WHEN Alarm.EventCode = 1289 THEN Alarm.Date ELSE Alarm.ResetDate END AS MaintenanceCompletedAt,
CASE
    WHEN Alarm.EventCode = 1289 AND Alarm.OperatorID IS NOT NULL THEN (
        SELECT Operator.Name
        FROM Operator
        WHERE Alarm.OperatorID = Operator.ID
    )
    WHEN Alarm.EventCode = 1289 AND Alarm.OperatorID IS NULL THEN (
        SELECT TOP 1 WebUser.Name
        FROM SentCommand
        INNER JOIN WebUser ON SentCommand.SentByUserID = WebUser.ID
        WHERE SentCommand.CommandCode = 1484
        ORDER BY SentCommand.CreateDate DESC
    )
END AS ResetBy,
Asset.Frequency AS [ExpectedPM],
ISNULL(CONVERT(INT, CONVERT(VARBINARY(8), AlarmHMR.HMRData, 2), 0) / 3600, 0) AS [PreviousMaintenance],
ISNULL(CASE WHEN AlarmHMR.EventCode = 1289 THEN CONVERT(INT, CONVERT(VARBINARY(8), AlarmHMR.HMRData, 2), 0) / 3600 END, 0) AS [CurrentMaintenance]
FROM
Asset
LEFT JOIN (
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY AssetID ORDER BY Date DESC) AS RowNum
    FROM Alarm
) AS Alarm ON Asset.ID = Alarm.AssetID AND Alarm.RowNum = 1
LEFT JOIN Operator ON Alarm.OperatorID = Operator.ID
LEFT JOIN (
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY AssetID ORDER BY Date DESC) AS RowNum
    FROM AlarmHMR
) AS AlarmHMR ON Asset.ID = AlarmHMR.AssetID AND AlarmHMR.RowNum = 1
WHERE
Asset.ID IN (${assetIdValues});
`
const q1=`SELECT
Asset.ID,
Asset.Name,
Asset.AssetTypeID,
Asset.UnderPM,
Asset.OverPM,
CASE WHEN Alarm.EventCode = 1289 THEN Alarm.Date ELSE Alarm.ResetDate END AS MaintenanceCompletedAt,
CASE
    WHEN Alarm.EventCode = 1289 AND Alarm.OperatorID IS NOT NULL THEN Operator.Name
    WHEN Alarm.EventCode = 1289 AND Alarm.OperatorID IS NULL THEN WebUser.Name
END AS ResetBy,
Asset.Frequency AS [ExpectedPM],
ISNULL(CONVERT(INT, CONVERT(VARBINARY(8), PrevHMR.HMRData, 2), 0) / 3600, 0) AS [PreviousMaintenance],
ISNULL(CONVERT(INT, CONVERT(VARBINARY(8), CurrentHMR.HMRData, 2), 0) / 3600, 0) AS [CurrentMaintenance]
FROM
Asset
LEFT JOIN (
SELECT
    AssetID,
    EventCode,
    Date,
    ResetDate,
    OperatorID,
    ROW_NUMBER() OVER (PARTITION BY AssetID, EventCode ORDER BY Date DESC) AS RowNum
FROM
    Alarm
WHERE
    EventCode = 1289
) AS Alarm ON Asset.ID = Alarm.AssetID AND Alarm.RowNum = 1
LEFT JOIN Operator ON Alarm.OperatorID = Operator.ID
LEFT JOIN (
SELECT TOP 1 WITH TIES
    SentCommand.SentByUserID,
    WebUser.Name,
    SentCommand.CreateDate
FROM
    SentCommand
INNER JOIN WebUser ON SentCommand.SentByUserID = WebUser.ID
WHERE
    SentCommand.CommandCode = 1484
ORDER BY
    SentCommand.CreateDate DESC
) AS WebUser ON Alarm.EventCode = 1289 AND Alarm.OperatorID IS NULL
LEFT JOIN (
SELECT
    AssetID,
    HMRData,
    ROW_NUMBER() OVER (PARTITION BY AssetID, EventCode ORDER BY Date DESC) AS RowNum
FROM
    AlarmHMR
WHERE
    EventCode = 1289
) AS PrevHMR ON Asset.ID = PrevHMR.AssetID AND PrevHMR.RowNum = 2
LEFT JOIN (
SELECT
    AssetID,
    HMRData,
    ROW_NUMBER() OVER (PARTITION BY AssetID, EventCode ORDER BY Date DESC) AS RowNum
FROM
    AlarmHMR
WHERE
    EventCode = 1289
) AS CurrentHMR ON Asset.ID = CurrentHMR.AssetID AND CurrentHMR.RowNum = 1
WHERE
Asset.ID IN (${assetIdValues});
`
const k=`SELECT
a.ID,
a.AssetTypeID,
a.UnderPM,
a.OverPM,
a.Frequency AS [ExpectedPM],
al.ID AS AID,
CASE
    WHEN al.EventCode = 1289 THEN al.Date
    ELSE al.ResetDate
END AS [MaintenanceCompletedAt],
CASE
    WHEN al.EventCode = 264 THEN o.Name
    WHEN al.EventCode = 1289 AND al.OperatorID IS NOT NULL THEN o2.Name
    WHEN al.EventCode = 1289 AND al.OperatorID IS NULL THEN w.Name
END AS [ResetBy],
ISNULL(CONVERT(INT, CONVERT(VARBINARY(8), h.HMRData, 2), 0) / 3600, 0) AS [CurrentMaintenance],
ISNULL(CONVERT(INT, CONVERT(VARBINARY(8), h2.HMRData, 2), 0) / 3600, 0) AS [PreviousMaintenance]
FROM
Asset a
LEFT JOIN (
SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY AssetID ORDER BY
        CASE
            WHEN EventCode = 264 THEN ResetDate
            WHEN EventCode = 1289 THEN Date
        END DESC
    ) AS rn
FROM
    Alarm
WHERE
    (EventCode = 264 AND ResetDate IS NOT NULL)
    OR (EventCode = 1289)
) al ON a.ID = al.AssetID AND al.rn = 1
LEFT JOIN Operator o ON al.OperatorID = o.ID
LEFT JOIN Operator o2 ON al.OperatorID IS NOT NULL AND al.OperatorID = o2.ID
LEFT JOIN Unit u ON a.UnitID = u.ID
LEFT JOIN SentCommand sc ON u.UniqueID = sc.Destination AND sc.CommandCode = 1484
LEFT JOIN WebUser w ON sc.SentByUserID = w.ID
LEFT JOIN (
SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY AssetID ORDER BY Date DESC) AS rn
FROM
    AlarmHMR
) h ON a.ID = h.AssetID AND h.rn = 1
LEFT JOIN (
SELECT
    a2.AssetID,
    a2.HMRData
FROM
    AlarmHMR a2
    JOIN (
        SELECT
            AssetID,
            MAX(Date) AS MaxDate
        FROM
            AlarmHMR
        GROUP BY
            AssetID
    ) a3 ON a2.AssetID = a3.AssetID AND a2.Date = a3.MaxDate
) h2 ON a.ID = h2.AssetID
WHERE
a.ID IN (${assetIdValues});
`
const y=`WITH CTE AS (
    SELECT
        a.ID,
        a.AssetTypeID,
        a.UnderPM,
        a.OverPM,
        a.Frequency AS [ExpectedPM],
        al.ID AS AID,
        CASE
            WHEN al.EventCode = 1289 THEN al.Date
            ELSE al.ResetDate
        END AS [MaintenanceCompletedAt],
        CASE
            WHEN al.EventCode = 264 THEN o.Name
            WHEN al.EventCode = 1289 AND al.OperatorID IS NOT NULL THEN o2.Name
            WHEN al.EventCode = 1289 AND al.OperatorID IS NULL THEN w.Name
        END AS [ResetBy],
        ISNULL(CONVERT(INT, CONVERT(VARBINARY(8), h.HMRData, 2), 0) / 3600, 0) AS [CurrentMaintenance],
        ISNULL(CONVERT(INT, CONVERT(VARBINARY(8), h2.HMRData, 2), 0) / 3600, 0) AS [PreviousMaintenance],
        ROW_NUMBER() OVER (PARTITION BY a.ID ORDER BY al.Date DESC) AS rn
    FROM
        Asset a
    LEFT JOIN (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY AssetID ORDER BY
                CASE
                    WHEN EventCode = 264 THEN ResetDate
                    WHEN EventCode = 1289 THEN Date
                END DESC
            ) AS rn
        FROM
            Alarm
        WHERE
            (EventCode = 264 AND ResetDate IS NOT NULL)
            OR (EventCode = 1289)
    ) al ON a.ID = al.AssetID AND al.rn = 1
    LEFT JOIN Operator o ON al.OperatorID = o.ID
    LEFT JOIN Operator o2 ON al.OperatorID IS NOT NULL AND al.OperatorID = o2.ID
    LEFT JOIN Unit u ON a.UnitID = u.ID
    LEFT JOIN SentCommand sc ON u.UniqueID = sc.Destination AND sc.CommandCode = 1484
    LEFT JOIN WebUser w ON sc.SentByUserID = w.ID
    LEFT JOIN (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY AssetID ORDER BY Date DESC) AS rn
        FROM
            AlarmHMR
    ) h ON a.ID = h.AssetID AND h.rn = 1
    LEFT JOIN (
        SELECT
            a2.AssetID,
            a2.HMRData
        FROM
            AlarmHMR a2
        JOIN (
            SELECT
                AssetID,
                MAX(Date) AS MaxDate
            FROM
                AlarmHMR
            GROUP BY
                AssetID
        ) a3 ON a2.AssetID = a3.AssetID AND a2.Date = a3.MaxDate
    ) h2 ON a.ID = h2.AssetID
    WHERE
        a.ID IN (${assetIdValues})
)
SELECT
    ID,
    AssetTypeID,
    UnderPM,
    OverPM,
    ExpectedPM,
    AID,
    MaintenanceCompletedAt,
    ResetBy,
    CurrentMaintenance,
    ISNULL((
        SELECT TOP 1 CONVERT(INT, CONVERT(VARBINARY(8), h.HMRData, 2), 0) / 3600
        FROM AlarmHMR h
        WHERE
            h.AssetID = CTE.ID
            AND CAST(h.Date AS DATE) = CAST(CTE.MaintenanceCompletedAt AS DATE)
        ORDER BY h.Date DESC
    ), 0) AS PreviousMaintenance
FROM CTE
WHERE rn = 1;
`


return await pool.request()
            .query(y)

}

exports.getLoginReport=async(assetIds)=>{
    const assetIdValues = assetIds.map(asset => `CONVERT(uniqueidentifier, '${asset}')`).join(',');

const q1=`SELECT A.ID,
A.Name AS AssetName,
A.AssetTypeID,
L.StartTime AS LoginTime,
L.Date AS LogoutTime,
L.OperatorID AS Operator,
L.LoginDuration AS Duration,
L.Lat,
L.Lon,
CASE L.LogOffReason
    WHEN '00' THEN 'Operator logged out'
    WHEN '01' THEN 'Logged off due to operator idling'
    WHEN '02' THEN 'Logged off due to equipment idling'
    WHEN '03' THEN 'Logged off due to impact alarm'
    WHEN '04' THEN 'Logged off due to low fuel alert'
    WHEN '05' THEN 'Logged off due to low fuel alarm'
    WHEN '06' THEN 'Logged off due to low SoC'
    WHEN '07' THEN 'Logged off due to Checklist'
END AS LogOffReason,
O.Name AS OptoInputName,
CASE O.No
    WHEN 1 THEN CONVERT(VARCHAR(5), DATEADD(minute, CONVERT(INT, SUBSTRING(L.Timers, 1, 4), 2), '00:00'), 108)
    WHEN 2 THEN CONVERT(VARCHAR(5), DATEADD(minute, CONVERT(INT, SUBSTRING(L.Timers, 5, 4), 2), '00:00'), 108)
    WHEN 3 THEN CONVERT(VARCHAR(5), DATEADD(minute, CONVERT(INT, SUBSTRING(L.Timers, 9, 4), 2), '00:00'), 108)
    WHEN 4 THEN CONVERT(VARCHAR(5), DATEADD(minute, CONVERT(INT, SUBSTRING(L.Timers, 13, 4), 2), '00:00'), 108)
    WHEN 5 THEN CONVERT(VARCHAR(5), DATEADD(minute, CONVERT(INT, SUBSTRING(L.Timers, 17, 4), 2), '00:00'), 108)
    WHEN 6 THEN CONVERT(VARCHAR(5), DATEADD(minute, CONVERT(INT, SUBSTRING(L.Timers, 21, 4), 2), '00:00'), 108)
    WHEN 7 THEN CONVERT(VARCHAR(5), DATEADD(minute, CONVERT(INT, SUBSTRING(L.Timers, 25, 4), 2), '00:00'), 108)
    WHEN 8 THEN CONVERT(VARCHAR(5), DATEADD(minute, CONVERT(INT, SUBSTRING(L.Timers, 29, 4), 2), '00:00'), 108)
END AS OptoInputTimers
FROM
Asset A
LEFT JOIN (
    SELECT
        AssetID,
        StartTime,
        Date,
        OperatorID,
        LoginDuration,
        Lat,
        Lon,
        LogOffReason,
        Timers,
        ROW_NUMBER() OVER (PARTITION BY AssetID ORDER BY Date DESC) AS rn
    FROM
        AlarmLogOff
) L ON A.ID = L.AssetID AND L.rn = 1
LEFT JOIN OptoInput O ON A.ID = O.AssetID AND O.HourMeter = 'Y'
WHERE
A.ID IN (${assetIdValues});
`

const w=`SELECT
A.ID,
A.Name AS AssetName,
A.AssetTypeID,
L.StartTime AS LoginTime,
L.Date AS LogoutTime,
L.OperatorID AS Operator,
L.LoginDuration AS Duration,
L.Lat,
L.Lon,
CASE L.LogOffReason
    WHEN '00' THEN 'Operator logged out'
    WHEN '01' THEN 'Logged off due to operator idling'
    WHEN '02' THEN 'Logged off due to equipment idling'
    WHEN '03' THEN 'Logged off due to impact alarm'
    WHEN '04' THEN 'Logged off due to low fuel alert'
    WHEN '05' THEN 'Logged off due to low fuel alarm'
    WHEN '06' THEN 'Logged off due to low SoC'
    WHEN '07' THEN 'Logged off due to Checklist'
END AS LogOffReason,
O.Name AS OptoInputName,
CASE O.No
    WHEN 1 THEN CONVERT(VARCHAR(5), DATEADD(MINUTE, CONVERT(INT, SUBSTRING(L.Timers, 1, 4), 2), '00:00'), 108)
    WHEN 2 THEN CONVERT(VARCHAR(5), DATEADD(MINUTE, CONVERT(INT, SUBSTRING(L.Timers, 5, 4), 2), '00:00'), 108)
    WHEN 3 THEN CONVERT(VARCHAR(5), DATEADD(MINUTE, CONVERT(INT, SUBSTRING(L.Timers, 9, 4), 2), '00:00'), 108)
    WHEN 4 THEN CONVERT(VARCHAR(5), DATEADD(MINUTE, CONVERT(INT, SUBSTRING(L.Timers, 13, 4), 2), '00:00'), 108)
    WHEN 5 THEN CONVERT(VARCHAR(5), DATEADD(MINUTE, CONVERT(INT, SUBSTRING(L.Timers, 17, 4), 2), '00:00'), 108)
    WHEN 6 THEN CONVERT(VARCHAR(5), DATEADD(MINUTE, CONVERT(INT, SUBSTRING(L.Timers, 21, 4), 2), '00:00'), 108)
    WHEN 7 THEN CONVERT(VARCHAR(5), DATEADD(MINUTE, CONVERT(INT, SUBSTRING(L.Timers, 25, 4), 2), '00:00'), 108)
    WHEN 8 THEN CONVERT(VARCHAR(5), DATEADD(MINUTE, CONVERT(INT, SUBSTRING(L.Timers, 29, 4), 2), '00:00'), 108)
END AS OptoInputTimers
FROM
Asset A
LEFT JOIN (
SELECT
    AssetID,
    StartTime,
    Date,
    OperatorID,
    LoginDuration,
    Lat,
    Lon,
    LogOffReason,
    Timers,
    ROW_NUMBER() OVER (PARTITION BY AssetID ORDER BY Date DESC) AS rn
FROM
    AlarmLogOff
) L ON A.ID = L.AssetID AND L.rn = 1
LEFT JOIN OptoInput O ON A.ID = O.AssetID AND O.HourMeter = 'Y'
WHERE
A.ID IN (${assetIdValues});
`
const g=`WITH LatestAlarmLogOff AS (
    SELECT
        AssetID,
        StartTime,
        Date,
        OperatorID,
        LoginDuration,
        Lat,
        Lon,
        LogOffReason,
        Timers,
        ROW_NUMBER() OVER (PARTITION BY AssetID ORDER BY Date DESC) AS rn
    FROM
        AlarmLogOff
),
LookupLogOffReason AS (
    SELECT
        LogOffCode,
        LogOffReason
    FROM
        (VALUES
            ('00', 'Operator logged out'),
            ('01', 'Logged off due to operator idling'),
            ('02', 'Logged off due to equipment idling'),
            ('03', 'Logged off due to impact alarm'),
            ('04', 'Logged off due to low fuel alert'),
            ('05', 'Logged off due to low fuel alarm'),
            ('06', 'Logged off due to low SoC'),
            ('07', 'Logged off due to Checklist')
        ) AS L(LogOffCode, LogOffReason)
)
SELECT
    A.ID,
    A.Name AS AssetName,
    A.AssetTypeID,
    L.StartTime AS LoginTime,
    L.Date AS LogoutTime,
    L.OperatorID AS Operator,
    L.LoginDuration AS Duration,
    L.Lat,
    L.Lon,
    LR.LogOffReason AS LogOffReason,
    O.Name AS OptoInputName,
    CONVERT(VARCHAR(5), DATEADD(MINUTE, CONVERT(INT, SUBSTRING(L.Timers, O.No * 4 - 3, 4), 2), '00:00'), 108) AS OptoInputTimers
FROM
    Asset A
LEFT JOIN LatestAlarmLogOff L ON A.ID = L.AssetID AND L.rn = 1
LEFT JOIN OptoInput O ON A.ID = O.AssetID AND O.HourMeter = 'Y'
LEFT JOIN LookupLogOffReason LR ON L.LogOffReason = LR.LogOffCode
WHERE
    A.ID IN (${assetIdValues});
`

const v=`WITH LatestAlarmLogOff AS (
    SELECT
        AssetID,
        StartTime,
        Date,
        OperatorID,
        LoginDuration,
        Lat,
        Lon,
        LogOffReason,
        Timers,
        ROW_NUMBER() OVER (PARTITION BY AssetID ORDER BY Date DESC) AS rn
    FROM
        AlarmLogOff
),
LookupLogOffReason AS (
    SELECT
        LogOffCode,
        LogOffReason
    FROM
        (VALUES
            ('00', 'Operator logged out'),
            ('01', 'Logged off due to operator idling'),
            ('02', 'Logged off due to equipment idling'),
            ('03', 'Logged off due to impact alarm'),
            ('04', 'Logged off due to low fuel alert'),
            ('05', 'Logged off due to low fuel alarm'),
            ('06', 'Logged off due to low SoC'),
            ('07', 'Logged off due to Checklist')
        ) AS L(LogOffCode, LogOffReason)
)
SELECT
    A.ID,
    A.Name AS AssetName,
    A.AssetTypeID,
    L.StartTime AS LoginTime,
    L.Date AS LogoutTime,
    L.OperatorID AS Operator,
    L.LoginDuration AS Duration,
    L.Lat,
    L.Lon,
    LR.LogOffReason AS LogOffReason,
    O.Name AS OptoInputName,
    CONVERT(VARCHAR(2), DATEPART(HOUR, DATEADD(MINUTE, CONVERT(INT, SUBSTRING(L.Timers, O.No * 4 - 3, 4), 2), '00:00')) ) + '.' + CONVERT(VARCHAR(2), DATEPART(MINUTE, DATEADD(MINUTE, CONVERT(INT, SUBSTRING(L.Timers, O.No * 4 - 3, 4), 2), '00:00'))) AS OptoInputTimers
FROM
    Asset A
LEFT JOIN LatestAlarmLogOff L ON A.ID = L.AssetID AND L.rn = 1
LEFT JOIN OptoInput O ON A.ID = O.AssetID AND O.HourMeter = 'Y'
LEFT JOIN LookupLogOffReason LR ON L.LogOffReason = LR.LogOffCode
WHERE
    A.ID IN (${assetIdValues});
`
    return await pool.request()
            .query(g)
}
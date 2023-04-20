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
console.log("HELPER---",siteid)
const values=[1,2,3]
const q=`SELECT a.AssetTypeID, at.Active, at.Name, CONCAT('[', CAST(STRING_AGG(CONCAT('{"AssetTypeID":', QUOTENAME(CAST(a.AssetTypeID AS NVARCHAR(MAX)), '"'), ',"AssetTypeName":', QUOTENAME(CAST(at.Name AS NVARCHAR(MAX)), '"'), ',"ID":', QUOTENAME(CAST(a.ID AS NVARCHAR(MAX)), '"'), ',"System":', QUOTENAME(CAST(a.System AS NVARCHAR(MAX)), '"'), ',"UniqueID":', QUOTENAME(CAST(a.UniqueID AS NVARCHAR(MAX)), '"'), ',"name":', QUOTENAME(CAST(a.Name AS NVARCHAR(MAX)), '"'),'}'), ',') WITHIN GROUP (ORDER BY a.ID) AS NVARCHAR(MAX)), ']') AS Assets
     FROM Asset a
     JOIN AssetType at ON a.AssetTypeID = at.ID
     WHERE a.AssetTypeID IN (SELECT AssetTypeID FROM AssetTypeSite WHERE SiteID = '${siteid}')
     GROUP BY a.AssetTypeID, at.Name, at.Active;`

const q4=`SELECT a.AssetTypeID, a.AssetTypeName,
(SELECT a2.AssetTypeID, a2.AssetTypeName, a2.ID, a2.System, a2.UniqueID, a2.Name
 FROM Asset a2
 INNER JOIN AssetSite ass ON a2.ID = ass.AssetID
 WHERE ass.SiteID = '${siteid}' AND a2.AssetTypeID = a.AssetTypeID
 FOR JSON PATH) AS Assets
FROM Asset a
INNER JOIN AssetSite ass ON a.ID = ass.AssetID
INNER JOIN Unit ut ON a.UnitID = ut.ID
WHERE ass.SiteID = '${siteid}' AND ut.UnitType IN (${values})
GROUP BY a.AssetTypeID,a.AssetTypeName;`

const q6=`SELECT a.AssetTypeID, a.AssetTypeName,
(SELECT a2.AssetTypeID, a2.AssetTypeName, a2.ID, a2.System, a2.UniqueID, a2.Name
 FROM Asset a2
 INNER JOIN AssetSite ass ON a2.ID = ass.AssetID
 WHERE ass.SiteID = '${siteid}' AND a2.AssetTypeID = a.AssetTypeID
 FOR JSON PATH) AS Assets
FROM Asset a
INNER JOIN AssetSite ass ON a.ID = ass.AssetID
WHERE ass.SiteID = '${siteid}'
GROUP BY a.AssetTypeID, a.AssetTypeName;`


const q7=`SELECT a.AssetTypeID, a.AssetTypeName,
(SELECT a2.AssetTypeID, a2.AssetTypeName, a2.ID, a2.System, a2.UniqueID, a2.Name
 FROM Asset a2
 INNER JOIN AssetSite ass ON a2.ID = ass.AssetID
 WHERE ass.SiteID = '${siteid}' AND a2.AssetTypeID = a.AssetTypeID
 FOR JSON PATH) AS Assets
FROM Asset a
INNER JOIN AssetSite ass ON a.ID = ass.AssetID
LEFT JOIN Unit ut ON a.UnitID = ut.ID
WHERE ass.SiteID = '${siteid}' AND (ut.UnitType IN (1,2,3) OR ut.UnitType IS NULL)
GROUP BY a.AssetTypeID, a.AssetTypeName;`


//console.log("QUERY>>>",q5)
    return await pool.request()
    .query(q6)
}

exports.getAssetByAssetId = async (assetIds) => {
    const assetIdValues = assetIds.map(asset => `CONVERT(uniqueidentifier, '${asset}')`).join(',');
    return await pool.request()
    .query(`SELECT ID,Name FROM Asset WHERE ID IN (${assetIdValues})`)
        // .query(`SELECT a1.ID,a1.Name,a1.Active,a1.AssetTypeID,a1.System,a1.UniqueID,a2.Name
        //         FROM Asset a1
        //         INNER JOIN AssetType a2 ON a1.AssetTypeID = a2.ID
        //         WHERE a1.AssetTypeID IN (${assetIdValues})`)
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

exports.getFaultCode=async(siteID)=>{
    const q=`SELECT COUNT(*) AS TotalCountfrom FROM ServiceAndProduct sap 
    INNER JOIN Asset a on a.[System] =sap.ID
    WHERE a.[System] IN (5,6,7);`
    
    return await pool.request()
    .query(q)
}


exports.getUnitCount=async(siteID)=>{
    
}

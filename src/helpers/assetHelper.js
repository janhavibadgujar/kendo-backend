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

exports.getFaultCode=async(siteID)=>{
    const q=`SELECT COUNT(*) as TotalCount 
    FROM Asset a
    JOIN AssetSite as2 on as2.AssetID =a.ID 
    WHERE a.[System] IN (5,6,7) AND as2.SiteID ='${siteID}';`
    
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

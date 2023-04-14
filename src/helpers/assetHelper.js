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
    const q=`SELECT a.AssetTypeID, at.Active, at.Name, CONCAT('[', CAST(STRING_AGG(CONCAT('{"AssetTypeID":', QUOTENAME(CAST(a.AssetTypeID AS NVARCHAR(MAX)), '"'), ',"AssetTypeName":', QUOTENAME(CAST(at.Name AS NVARCHAR(MAX)), '"'), ',"ID":', QUOTENAME(CAST(a.ID AS NVARCHAR(MAX)), '"'), ',"System":', QUOTENAME(CAST(a.System AS NVARCHAR(MAX)), '"'), ',"UniqueID":', QUOTENAME(CAST(a.UniqueID AS NVARCHAR(MAX)), '"'), ',"name":', QUOTENAME(CAST(a.Name AS NVARCHAR(MAX)), '"'),'}'), ',') WITHIN GROUP (ORDER BY a.ID) AS NVARCHAR(MAX)), ']') AS Assets
    FROM Asset a
    JOIN AssetType at ON a.AssetTypeID = at.ID
    WHERE a.AssetTypeID IN (SELECT AssetTypeID FROM AssetTypeSite WHERE SiteID = '${siteid}')
    GROUP BY a.AssetTypeID, at.Name, at.Active;`


    return await pool.request()
    .query(q)
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

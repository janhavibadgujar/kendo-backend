const sql = require('mssql');
const request = new sql.Request();


exports.getById=async(id)=>{
    console.log("Id---",id)
    request.input('param1', sql.VarChar(50), id);
   return request.query('SELECT * FROM Asset WHERE ID = @param1 ')
}

exports.getAll=async()=>{
    return request.query('SELECT * FROM Asset')
}

exports.getAssetBySiteId=async(siteid)=>{
    request.input('param1', sql.VarChar(50), siteid);
 return request.query('SELECT AssetID FROM AssetSite WHERE SiteID = @param1 ')
}

exports.getAssetByAssetId=async(assetIds)=>{
    const assetIdValues = assetIds.map(asset => `CONVERT(uniqueidentifier, '${asset}')`).join(',');
    return request.query(`SELECT ID,Name FROM Asset WHERE ID IN (${assetIdValues})`)
}

exports.getAssetByDepartment=async(departmentIds)=>{
  const departmentIdValues = departmentIds.map(dept => `CONVERT(uniqueidentifier, '${dept}')`).join(',');
    return request.query(`SELECT * FROM AssetSite WHERE SiteID IN (${departmentIdValues})`)
}

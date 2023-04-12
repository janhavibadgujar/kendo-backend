const sql = require('mssql');
const request = new sql.Request();

exports.getDepartmentBySiteId=async(siteIds)=>{
    const siteIdValues = siteIds.map(site => `CONVERT(uniqueidentifier, '${site}')`).join(',');
    return request.query(`SELECT ID,SiteID,Name FROM Site WHERE SiteID IN (${siteIdValues}) AND Level = 3`)
}
const pool = require('../config/dbconfig')


exports.getDepartmentBySiteId = async (siteIds) => {
    const siteIdValues = siteIds.map(site => `CONVERT(uniqueidentifier, '${site}')`).join(',');
    return await pool.request()
    .query(`SELECT ID FROM Site WHERE SiteID IN (${siteIdValues}) AND Level = 2`)
}

exports.getDepartmentByID=async(ids)=>{
    const idValues = ids.map(sites => `CONVERT(uniqueidentifier, '${sites}')`).join(',');
    return await pool.request()
    .query(`SELECT ID,Name,SiteID FROM Site WHERE SiteID IN (${idValues}) AND Level = 3`)
}

exports.getDepartmentByCompanyId=async(companyid)=>{
    request.input('param1', sql.VarChar(50), companyid);
    return await pool.request()
   .query('SELECT * FROM Asset WHERE CompanyID = @param1  AND Level = 3')
}
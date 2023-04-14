const pool = require('../config/dbconfig')


exports.getDepartmentBySiteId = async (siteIds) => {
    const siteIdValues = siteIds.map(site => `CONVERT(uniqueidentifier, '${site}')`).join(',');
    return await pool.request()
    .query(`SELECT ID,SiteID,Name FROM Site WHERE SiteID IN (${siteIdValues}) AND Level = 3`)
}

exports.getDepartmentByCompanyId=async(companyid)=>{
    request.input('param1', sql.VarChar(50), companyid);
    return await pool.request()
   .query('SELECT * FROM Asset WHERE CompanyID = @param1  AND Level = 3')
}
const pool = require('../config/dbconfig')


exports.getDepartmentBySiteId = async (siteIds) => {
    const siteIdValues = siteIds.map(site => `CONVERT(uniqueidentifier, '${site}')`).join(',');
    console.log('sdsds', siteIdValues)
    return await pool.request()
        .query(`SELECT ID,SiteID,Name FROM Site WHERE SiteID IN (${siteIdValues}) AND Level = 3`)
}
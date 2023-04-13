const sql = require('mssql');
const pool = require('../config/dbconfig')

exports.getBySiteId = async (id) => {
    return await pool.request()
        .input('param1', sql.VarChar(50), siteid)
        .query('SELECT OperatorID FROM OperatorSite WHERE SiteID = @param1 ')
}

exports.getOpeartorByOperatorId = async (id) => {
    return await pool.request()
        .query`SELECT * FROM Operator WHERE ID = ${id}`
}
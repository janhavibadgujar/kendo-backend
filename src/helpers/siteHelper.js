const sql = require('mssql');
const pool = require('../config/dbconfig')

exports.getById = async (id) => {
    return await pool.request()
        .input('param1', sql.VarChar(50), id)
        .query('SELECT * FROM Site WHERE ID = @param1 ')
}

exports.getAll = async () => {
    return await pool.request()
        .query('SELECT * FROM Site')
}

exports.getByCompany = async (companyid) => {
    return await pool.request()
        .input('param1', sql.VarChar(50), companyid)
        .query('SELECT * FROM Site WHERE CompanyID = @param1 AND Level = 2')
}
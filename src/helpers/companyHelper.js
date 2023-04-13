const sql = require('mssql');
const pool = require('../config/dbconfig')

exports.getById = async (id) => {
  return await pool.request()
    .input('id', sql.VarChar(50), id)
    .query('SELECT * FROM Company WHERE ID = @id ')
}

exports.getAllCompany = async () => {
  return await pool.request()
    .query('SELECT * FROM Company')
}
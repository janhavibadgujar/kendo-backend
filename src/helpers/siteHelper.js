const sql = require('mssql');
const request = new sql.Request();

exports.getById=async(id)=>{
    request.input('param1', sql.VarChar(50), id);
   return request.query('SELECT * FROM Site WHERE ID = @param1 ')
}

exports.getAll=async()=>{
    return request.query('SELECT * FROM Site')
}

exports.getByCompany=async(companyid)=>{
    request.input('param1', sql.VarChar(50), companyid);
   return request.query('SELECT * FROM Site WHERE CompanyID = @param1 AND Level = 2')
}
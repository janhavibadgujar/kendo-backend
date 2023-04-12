const sql = require('mssql');
const request = new sql.Request();

exports.getById=async(id)=>{
    console.log("In Company Helper---",id)
    request.input('id', sql.VarChar(50), id);
  return  request.query('SELECT * FROM Company WHERE ID = @id ')
}

exports.getAllCompany=async()=>{
    return request.query('SELECT * FROM Company')
}
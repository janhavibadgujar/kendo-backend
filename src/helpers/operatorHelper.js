const sql = require('mssql');
const request = new sql.Request();

exports.getBySiteId=async(id)=>{
    request.input('param1', sql.VarChar(50), siteid);
 return request.query('SELECT OperatorID FROM OperatorSite WHERE SiteID = @param1 ')
}

exports.getOpeartorByOperatorId=async(id)=>{
    return  sql.query`SELECT * FROM Operator WHERE ID = ${id}`
}
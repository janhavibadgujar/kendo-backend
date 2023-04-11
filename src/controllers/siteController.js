const sql = require('mssql');

exports.getById=async(req,res)=>{
    const request = new sql.Request();
    request.input('param1', sql.VarChar(50), req.params.id);
    request.query('SELECT * FROM Site WHERE ID = @param1 ')
    .then((result) => {
      res.send(result.recordset);
    })
    .catch((err) => {
        res.status(400).send({message: `Can't find details for ${req.params.id}`})
    });
}

exports.getAll=async(req,res)=>{
    const request = new sql.Request();
    request.query('SELECT * FROM Site')
    .then((result) => {
      res.send(result.recordset);
    })
    .catch((err) => {
     res.status(400).send({message:"No Data"})
    });
}

exports.getByCompany=async(req,res)=>{
  const request = new sql.Request();
    request.input('param1', sql.VarChar(50), req.params.companyid);
    request.query('SELECT * FROM Site WHERE CompanyID = @param1 ')
    .then((result) => {
      res.send(result.recordset);
    })
    .catch((err) => {
        res.status(400).send({message: `Can't find details for ${req.params.companyid}`})
    });
}
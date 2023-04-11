const sql = require('mssql');

exports.getById=async(req,res)=>{
    const request = new sql.Request();
    console.log("Id---",req.params.id)
    request.input('id', sql.VarChar(50), req.params.id);
    request.query('SELECT * FROM Company WHERE ID = @id ')
    .then((result) => {
      res.send(result.recordset);
    })
    .catch((err) => {
        res.status(400).send({message: `Can't find details for ${req.params.id}`})
    });
}

exports.getAllCompany=async(req,res)=>{
    console.log("In GETALL COMPANY---")
    const request = new sql.Request();
    request.query('SELECT * FROM Company')
    .then((result) => {
      res.send(result.recordset);
    })
    .catch((err) => {
     res.status(400).send({message:"No Data"})
    });
}

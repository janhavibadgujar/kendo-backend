const companyHelper = require("../helpers/companyHelper");

exports.getById=async(req,res)=>{
  var result=[];
  await companyHelper.getById(req.params.id).then((response)=>{
      const data={
        Data:response.recordset,
        Message:'',
        Status:true
      }
      result.push(data)
      res.send(result)
  })
    .catch((err) => {
      res.status(400).send({ message: `Can't find details for ${req.params.id}` })
    });
}

exports.getAllCompany=async(req,res)=>{
  var result=[];
    await companyHelper.getAllCompany().then((response)=>{
      const data={
        Data:response.recordset,
        Message:'',
        Status:true
       }
       result.push(data)
       res.send(result)
    })
    .catch((err) => {
      res.status(400).send({ message: "No Data" })
    });
}

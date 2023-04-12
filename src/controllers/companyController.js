const companyHelper=require("../helpers/companyHelper");

exports.getById=async(req,res)=>{
  await companyHelper.getById(req.params.id).then((response)=>{
    if(response.recordset != null)
    {
      res.send(response.recordset);
    }
  })
  .catch((err) => {
    res.status(400).send({message: `Can't find details for ${req.params.id}`})
  });
}

exports.getAllCompany=async(req,res)=>{
    await companyHelper.getAllCompany().then((response)=>{
      if(response.recordset != null)
      {
        res.send(response.recordset);
      }
    })
    .catch((err) => {
      res.status(400).send({message:"No Data"})
     });
}

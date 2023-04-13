const siteHelper=require("../helpers/siteHelper");

exports.getById=async(req,res)=>{
  var result=[];
    await siteHelper.getById(req.params.id).then((response) => {
      const data={
        Data:response.recordset,
        Message:'',
        Status:true
      }
      result.push(data)
      res.send(result)
    })
    .catch((err) => {
        res.status(400).send({message: `Can't find details for ${req.params.id}`})
    });
}

exports.getAll=async(req,res)=>{
  var result=[];
    await siteHelper.getAll().then((response) => {
        const data={
          Data:response.recordset,
          Message:'',
          Status:true
        }
        result.push(data)
        res.send(result)
    })
    .catch((err) => {
     res.status(400).send({message:"No Data"})
    });
}

exports.getByCompany=async(req,res)=>{
  var result=[];
  await siteHelper.getByCompany(req.params.companyid).then((response) => {
        const data={
          Data:response.recordset,
          Message:'',
          Status:true
        }
        result.push(data)
        res.send(result)
    })
    .catch((err) => {
      console.log("Errr---",err)
        res.status(400).send({message: `Can't find details for ${req.params.companyid}`})
    });
}
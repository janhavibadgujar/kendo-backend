const departmentHelper=require("../helpers/departmentHelper");

exports.getDepartmentBySiteId=async(req,res)=>{
    var result=[];
    await departmentHelper.getDepartmentBySiteId(req.body.siteId).then((response)=>{
            const data={
                Data:response.recordset,
                Message:'',
                Status:true
            }
            result.push(data)
            res.send(result)
        })
        .catch((err) => {
            console.log("Error--",err)
            res.status(400).send({message: `Can't find details for ${req.body.siteId}`})
        });
}

exports.getDepartmentByCompanyId=async(req,res)=>{
    var result=[];
    await departmentHelper.getDepartmentByCompanyId(req.params.companyid).then((response)=>{
        const data={
            Data:response.recordset,
            Message:'',
            Status:true
        }
        result.push(data)
        res.send(result)
    })
    .catch((err) => {
        console.log("Error--",err)
        res.status(400).send({message: `Can't find details for ${req.body.siteId}`})
    });
}
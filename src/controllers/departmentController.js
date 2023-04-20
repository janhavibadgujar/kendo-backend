const departmentHelper = require("../helpers/departmentHelper");

exports.getDepartmentBySiteId=async(req,res)=>{
    var result=[];
    var ids=[];
    await departmentHelper.getDepartmentBySiteId(req.body.SiteId).then(async(response)=>{
        response.recordset.forEach((element)=>{
            ids.push(element.ID)
        })
        await departmentHelper.getDepartmentByID(ids).then((resp)=>{
            //console.log("Response----",resp.recordset)
            const data={
                Data:resp.recordset,
                Message:'',
                Status:true
            }
            result.push(data)
            res.send(result)
        })
            
        })
        .catch((err) => {
            console.log("Error--", err)
            res.status(400).send({ message: `Can't find details for ${req.body.SiteId}` })
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
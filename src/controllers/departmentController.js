const departmentHelper = require("../helpers/departmentHelper");

exports.getDepartmentBySiteId=async(req,res)=>{
    var ids=[];
    await departmentHelper.getDepartmentBySiteId(req.body.SiteId).then(async(response)=>{
        response.recordset.forEach((element)=>{
            ids.push(element.ID)
        })
        await departmentHelper.getDepartmentByID(ids).then((resp)=>{
            const data={
                Data:resp.recordset,
                Message:'',
                Status:true
            }
            res.send(data)
        })
            
        })
        .catch((err) => {
            res.status(400).send({ message: `Can't find details for ${req.body.SiteId}` })
        });
}

exports.getDepartmentByCompanyId=async(req,res)=>{
    await departmentHelper.getDepartmentByCompanyId(req.params.companyid).then((response)=>{
        const data={
            Data:response.recordset,
            Message:'',
            Status:true
        }
        res.send(data)
    })
    .catch((err) => {
        res.status(400).send({message: `Can't find details for ${req.body.siteId}`})
    });
}
const departmentHelper=require("../helpers/departmentHelper");

exports.getDepartmentBySiteId=async(req,res)=>{
    await departmentHelper.getDepartmentBySiteId(req.body.siteId).then((response)=>{
        if(response.recordset != null)
            {
                res.send(response.recordset)
            }
        })
        .catch((err) => {
            console.log("Error--",err)
            res.status(400).send({message: `Can't find details for ${req.body.siteId}`})
        });
}
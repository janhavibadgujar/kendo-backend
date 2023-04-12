const assetHelper=require('../helpers/assetHelper');

exports.getById=async(req,res)=>{
  await assetHelper.getById(req.params.id).then((response)=>{
    if(response.recordset != null)
    {
      res.send(response.recordset);
    }
    
  })
  .catch((err) => {
    res.status(400).send({message: `Can't find details for ${req.params.id}`})
  });
   
}

exports.getAll=async(req,res)=>{
  await assetHelper.getAll().then((response)=>{
    if(response.recordset != null)
    {
      res.send(response.recordset);
    }
  })
  .catch((err) => {
    res.status(400).send({message:"No Data"})
   });
}

exports.getAssetBySiteId=async(req,res)=>{
  var assetIds=[];
  await assetHelper.getAssetBySiteId(req.params.siteid).then(async(response)=>{
   if(response.recordset != null)
   {
      response.recordset.forEach((element)=>{
        assetIds.push(element.AssetID)
      })
      await assetHelper.getAssetByAssetId(assetIds).then(async(assets)=>{
        res.send(assets.recordset);
      })
   }
  })
  .catch((err) => {
    res.status(400).send({message: `Can't find details for ${req.params.siteid}`})
   });
}

exports.getAssetByDepartment=async(req,res)=>{
  var assetIds=[];
  await assetHelper.getAssetByDepartment(req.body.department).then(async(response)=>{
    if(response.recordset != null)
    {
      response.recordset.forEach((element)=>{
        assetIds.push(element.AssetID)
      })
      await assetHelper.getAssetByAssetId(assetIds).then(async(assets)=>{
        res.send(assets.recordset);
      })
    }
  })
  .catch((err) => {
    res.status(400).send({message: `Can't find details`})
   });
}


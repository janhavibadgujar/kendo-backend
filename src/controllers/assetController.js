const assetHelper = require('../helpers/assetHelper');

exports.getById=async(req,res)=>{
  var result=[];
  await assetHelper.getById(req.params.id).then((response)=>{
      const data={
        Data:response.recordset,
        Message:'',
        Status:true
      }
      result.push(data)
      res.send(result);
  })
    .catch((err) => {
      res.status(400).send({ message: `Can't find details for ${req.params.id}` })
    });

}

exports.getAll=async(req,res)=>{
  var result=[];
  await assetHelper.getAll().then((response)=>{
    const data={
      Data:response.recordset,
      Message:'',
      Status:true
    }
    result.push(data)
    res.send(result);
  })
    .catch((err) => {
      res.status(400).send({ message: "No Data" })
    });
}

exports.getAssetBySiteId=async(req,res)=>{
  var assetIds=[];
  var result=[];
  await assetHelper.getAssetBySiteId(req.params.siteid).then(async(response)=>{
      response.recordset.forEach((element)=>{
        assetIds.push(element.AssetID)
      })
      await assetHelper.getAssetByAssetId(assetIds).then(async(assets)=>{
        const data={
          Data:assets.recordset,
          Message:'',
          Status:true
        }
        result.push(data)
        res.send(result);
      })
  })
    .catch((err) => {
      res.status(400).send({ message: `Can't find details for ${req.params.siteid}` })
    });
}

exports.getAssetByDepartment=async(req,res)=>{
  var assetIds=[];
  var result=[];
  await assetHelper.getAssetByDepartment(req.body.department).then(async(response)=>{
    if(response.recordset != null)
    {
      response.recordset.forEach((element)=>{
        assetIds.push(element.AssetID)
      })
      await assetHelper.getAssetByAssetId(assetIds).then(async(assets)=>{
        const data={
          Data:assets.recordset,
          Message:'',
          Status:true
        }
        result.push(data)
        res.send(result);
      })
    }
  })
    .catch((err) => {
      res.status(400).send({ message: `Can't find details` })
    });
}


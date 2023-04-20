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
  var result=[];
  var custom=[];

  await assetHelper.getAssetBySiteId(req.body.SiteID).then(async(response)=>{
      response.recordset.forEach((element)=>{
       const data={
        ID:element.AssetTypeID,
        assets:JSON.parse(element.Assets),
        assetstype:element.AssetTypeName
       }
       result.push(data)
      })
      const details={
        Data:result,
        Message:"",
        Status:true
      }
      custom.push(details)
      res.send(custom)
  })
    .catch((err) => {
      res.status(400).send({ message: `Can't find details for ${req.body.SiteID}` })
    });
}

exports.getAssetByDepartment=async(req,res)=>{
  var assetIds=[];
  var result=[];
  await assetHelper.getAssetByDepartment(req.body.department).then(async(response)=>{
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
      res.status(400).send({ message: `Can't find details` })
    });
}

exports.getChargerMap=async(req,res)=>{
  var result=[];
  await assetHelper.getChargerMap(req.params.SiteID).then((response)=>{
    const data={
      Data:response.recordset,
      Message:'',
      Status:true
    }
    result.push(data)
    res.send(result)
  })
  .catch((err) => {
    res.status(400).send({ message: `Can't find details` })
  });
}

exports.getFaultCode=async(req,res)=>{
  var result=[];
  await assetHelper.getFaultCode(req.params.SiteID).then((response)=>{
    const data={
      Data:response.recordset,
      Message:'',
      Status:true
    }
    result.push(data)
    res.send(result)
  })
  .catch((err) => {
    res.status(400).send({ message: `Can't find details` })
  });
}

exports.getUnitCount=async(req,res)=>{
  var result=[];
  var custom=[];
  var online=0;
  var offline=0;
  await assetHelper.getUnitCount(req.params.SiteID).then((response)=>{
    response.recordset.forEach((element)=>{
      const now = new Date();
      const dateToCheck = new Date(element.LastUpdate);
      const diffInMs = now - dateToCheck;
      if (diffInMs <= 300000) {
        online = online + 1;
      } 
      else 
      {
        offline = offline + 1;
      }
    })
    const data={
      onlineCount:online,
      offlineCount:offline,
    }
    result.push(data)
    const data1={
      Data:result,
      Message:'',
      Status:true
    }
    custom.push(data1)
    res.send(custom)
  })
  .catch((err) => {
    res.status(400).send({ message: `Can't find details` })
  });
}
const assetHelper = require('../helpers/assetHelper');

exports.getById=async(req,res)=>{
  await assetHelper.getById(req.params.id).then((response)=>{
      const data={
        Data:response.recordset,
        Message:'',
        Status:true
      }
      res.send(data);
  })
    .catch((err) => {
      res.status(400).send({ message: `Can't find details for ${req.params.id}` })
    });

}

exports.getAll=async(req,res)=>{
  await assetHelper.getAll().then((response)=>{
    const data={
      Data:response.recordset,
      Message:'',
      Status:true
    }
    res.send(data);
  })
    .catch((err) => {
      res.status(400).send({ message: "No Data" })
    });
}

exports.getAssetBySiteId=async(req,res)=>{
  var result=[];
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
      res.send(details)
  })
    .catch((err) => {
      res.status(400).send({ message: `Can't find details for ${req.body.SiteID}` })
    });
}

exports.getAssetByDepartment=async(req,res)=>{
  var assetIds=[];
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
        res.send(data);
      })
  })
    .catch((err) => {
      res.status(400).send({ message: `Can't find details` })
    });
}

exports.getChargerMap=async(req,res)=>{
  await assetHelper.getChargerMap(req.params.SiteID).then((response)=>{
    const data={
      Data:response.recordset,
      Message:'',
      Status:true
    }
    res.send(data)
  })
  .catch((err) => {
    res.status(400).send({ message: `Can't find details` })
  });
}

exports.getFaultCodeByCharger=async(req,res)=>{
  await assetHelper.getFaultCodeByCharger(req.params.SiteID).then((response)=>{
    const data={
      Data:response.recordset,
      Message:'',
      Status:true
    }
    res.send(data)
  })
  .catch((err) => {
    console.log("Err---",err)
    res.status(400).send({ message: `Can't find details` })
  });
}

exports.getFaultCodeByFaultCode=async(req,res)=>{
  await assetHelper.getFaultCodeByFaultCode(req.params.SiteID).then((response)=>{
    const data={
      Data:response.recordset,
      Message:'',
      Status:true
    }
    res.send(data)
  })
  .catch((err) => {
    console.log("Err---",err)
    res.status(400).send({ message: `Can't find details` })
  });
}

exports.getUnitCount=async(req,res)=>{
  var result=[];
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
    res.send(data1)
  })
  .catch((err) => {
    res.status(400).send({ message: `Can't find details` })
  });
}


exports.getMaintenanceStatusReport=async(req,res)=>{
  var assetDetails=[];
  await assetHelper.getMaintenanceStatusReport(req.body.assetID).then(async(response)=>{
await assetHelper.getMaintenanceStatusDetails(req.body.assetID).then((details)=>{
 details.recordset.forEach((element)=>{
 const np= element.LastPerformed != null ?element.LastPerformed + element.Frequency :null
 const unp =element.CurrentHMR != null? np - element.CurrentHMR : null

 const data={
  AssetName:element.AssetName,
  AssetTypeName:element.AssetTypeName,
  Status:element.Status,
  LastPerformed:element.LastPerformed,
  CurrentHMR:element.CurrentHMR,
  Next_PM:np,
  Until_NextPM:unp,
  Frequency:element.Frequency
 }

 assetDetails.push(data)

 })

const data1={
  Data:{
    details:assetDetails,
    count:response.recordset
  },
  Message:'',
  Status:true
}

  res.send(data1)
})
.catch((err1) => {
  console.log("err in details",err1)
});
  })
  .catch((err) => {
    console.log("err",err)
    res.status(400).send({ message: `Can't find details` })
  });
}

exports.getPowerUsage=async(req,res)=>{
  var currentHours=[];
  var today=new Date();
  const year = today.getFullYear();
  const month = String(today.getMonth() + 1).padStart(2, '0');
  const day = String(today.getDate()).padStart(2, '0');
  const currentDate=`${year}-${month}-${day}`;
  if(currentDate != req.body.date)
  {
    await assetHelper.getPowerUsage(req.body.SiteID,req.body.date).then((response)=>{
      const data={
        Data:response.recordset,
        Message:'',
        Status:true
      }
        res.send(data)
    })
    .catch((err) => {
      console.log("err",err)
      res.status(400).send({ message: `Can't find details` })
    });
  }
  else
  {
    await assetHelper.getPowerUsageCurrent(req.body.SiteID,req.body.date).then((response)=>{
      // currentHours = response.recordset.map(({ Hour, Charger, MaxkW }) => ({
      //   Hour: Hour.split(' ')[1],
      //   Charger,
      //   MaxkW
      // }));
      const data={
        Data:response.recordset,
        Message:'',
        Status:true
      }
        res.send(data)
      })
      .catch((err) => {
        console.log("err",err)
        res.status(400).send({ message: `Can't find details` })
      });
  }
}


exports.getMapDetails=async(req,res)=>{
  await assetHelper.getMapDetails(req.params.SiteID).then(async(response)=>{
    console.log("Length----",response.recordset.length)
    await assetHelper.getMap(req.params.SiteID).then(async(details)=>{

      const result = response.recordset.map(obj => {
        const match = details.recordset.find(item => item.AssetID === obj.AssetID);
        
        if (match) {
            return { ...obj, Status1: match.Status1, Status2: match.Status2 };
        }
        
        return obj;
      });
      var data={
        Data:result,
        Message:'',
        Status:true
      }
      res.send(data);
    })
      
 })
  .catch((err) => {
    console.log("err",err)
    res.status(400).send({ message: `Can't find details` })
  });
}
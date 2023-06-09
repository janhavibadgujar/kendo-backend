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
        Message:'',
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
  await assetHelper.getUnitCount(req.params.SiteID).then((response)=>{
    const data={
      onlineCount:response.recordset[0].OnlineCount,
      offlineCount:response.recordset[0].OfflineCount,
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
      currentHours = response.recordset.map(({ Hour, Charger, MaxkW }) => ({
        Hour: Hour.split(' ')[1],
        Charger,
        MaxkW
        }));
      const data={
        Data:currentHours,
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
    await assetHelper.getMap(req.params.SiteID).then(async(details)=>{

      const result = response.recordset.map(obj => {
        const match = details.recordset.find(item => item.AssetID === obj.AssetID);
        
        if (match) 
        {
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


exports.getMaps=async(req,res)=>{
  var d=[];
 await assetHelper.getMapSystem5(req.params.SiteID).then(async(response)=>{
  //console.log("5 length---",response.recordset.length)
    await assetHelper.getMapSystem6(req.params.SiteID).then(async(resp)=>{
      //console.log("6 length---",resp.recordset.length)
      await assetHelper.getMapSystem(req.params.SiteID).then(async(resp1)=>{
      //  console.log("7,11 length---",resp1.recordset.length)
       d=response.recordset.concat(resp.recordset, resp1.recordset);
       console.log("d length---",d.length)
        const data={
          Data:d,
          Message:'',
          Status:true
        }
          res.send(data)
      })
    })
    
  })
  .catch((err) => {
    console.log("err",err)
    res.status(400).send({ message: `Can't find details` })
  });
}

exports.getMaintenanceHistory=async(req,res)=>{
  await assetHelper.getMaintenanceHistory(req.body.assetID).then((response)=>{
    const result = response.recordset.map((element) => {
      const PMPerformed = element.CurrentMaintenance - element.PreviousMaintenance;
      const hours = PMPerformed - element.ExpectedPM;
      const over = PMPerformed > element.OverPM || PMPerformed < element.UnderPM;
      return {
        ID: element.ID,
        Name: element.Name,
        AssetTypeID: element.AssetTypeID,
        ResetBy: element.ResetBy,
        MaintenanceCompletedAt: element.MaintenanceCompletedAt,
        ExpectedPM: element.ExpectedPM,
        PMPerformed: PMPerformed,
        HoursPM: hours,
        status: over
      };
    });
    
    const data={
      Data:result,
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


exports.getLoginReport=async(req,res)=>{
  var result=[];
  await assetHelper.getLoginReport(req.body.assetID).then((response)=>{
    let arr=response.recordset;
    for (let i = 0; i < arr.length; i++) {
      const obj = arr[i];
      const index = result.findIndex(item => item.ID === obj.ID);
      if (index === -1) {
        const newObj = {
          ID: obj.ID,
          AssetName: obj.AssetName,
          AssetTypeID: obj.AssetTypeID,
          LoginTime: formatDate(obj.LoginTime),
          LogoutTime: formatDate(obj.LogoutTime),
          Operator: obj.Operator,
          Duration: obj.Duration == 0 ?`${obj.Duration} sec`:`${obj.Duration} secs`,
          Lat: obj.Lat,
          Lon: obj.Lon,
          LogOffReason: obj.LogOffReason,
          Timers: {}
        };
        newObj.Timers[obj.OptoInputName] = convertToHours(obj.OptoInputTimers);
        result.push(newObj);
      } else {
        result[index].Timers[obj.OptoInputName] = convertToHours(obj.OptoInputTimers);
      }
    }
    const data={
      Data:result,
      Message:'',
      Sttaus:true
    }
    res.send(data)
  })
  .catch((err) => {
    console.log("err",err)
    res.status(400).send({ message: `Can't find details` })
  });
}




function formatDate(dateString) {
  const date = new Date(dateString);
  const day = String(date.getDate()).padStart(2, '0');
  const month = String(date.getMonth() + 1).padStart(2, '0');
  const year = date.getFullYear();
  const hours = String(date.getHours()).padStart(2, '0');
  const minutes = String(date.getMinutes()).padStart(2, '0');

  return `${day}/${month}/${year}, ${hours}:${minutes}`;
}

function convertToHours(time) {
  const [hours, minutes] = time.split(':');
  const totalHours = (parseInt(hours) + parseInt(minutes) / 60).toFixed(1);
  return totalHours === '0.0' ? '0 hrs' : `${totalHours } hrs`;
}

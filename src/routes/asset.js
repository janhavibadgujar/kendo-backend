const express = require("express");
const router = express.Router();
const assetController=require("../controllers/assetController");

router.get("/getAssetById/:id",assetController.getById);

router.get("/getAllAsset",assetController.getAll);

router.post("/getAssetBySiteId",assetController.getAssetBySiteId);

router.post("/getAssetByDepartment",assetController.getAssetByDepartment);

router.get("/getChargerMap/:SiteID",assetController.getChargerMap);

router.get("/getFaultCodeByCharger/:SiteID",assetController.getFaultCode);

router.get("/getUnitCount/:SiteID",assetController.getUnitCount);

router.post("/getMaintenanceStatusReport",assetController.getMaintenanceStatusReport);

router.post("/getPowerUsage",assetController.getPowerUsage);

module.exports=router
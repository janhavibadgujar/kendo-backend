const express = require("express");
const router = express.Router();
const assetController=require("../controllers/assetController");

router.get("/getAssetById/:id",assetController.getById);

router.get("/getAllAsset",assetController.getAll);

router.post("/getAssetBySiteId",assetController.getAssetBySiteId);

router.post("/getAssetByDepartment",assetController.getAssetByDepartment);

router.get("/getChargerMap",assetController.getChargerMap);

router.get("/getFaultCode",assetController.getFaultCode);

router.get("/getUnitCount",assetController.getUnitCount);


module.exports=router
const express = require("express");
const router = express.Router();
const assetController=require("../controllers/assetController");

router.get("/getAssetById/:id",assetController.getById);

router.get("/getAllAsset",assetController.getAll);

router.get("/getAssetBySiteId/:siteid",assetController.getAssetBySiteId);

router.post("/getAssetByDepartment",assetController.getAssetByDepartment);


module.exports=router
const express = require("express");
const router = express.Router();
const operatorController = require("../controllers/operatorController");

router.get("/getOperatorBySiteId/:siteid", operatorController.getBySiteId)


module.exports = router
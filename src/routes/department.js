const express = require("express");
const router = express.Router();
const departmentController = require("../controllers/departmentController");

router.post("/getDepartmentBySiteId",departmentController.getDepartmentBySiteId);

module.exports = router
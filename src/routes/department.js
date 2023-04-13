const express = require("express");
const router = express.Router();
const departmentController = require("../controllers/departmentController");

router.post("/getDepartmentBySiteId",departmentController.getDepartmentBySiteId);

//router.get("/getDepartmentByCompanyId/:companyid",departmentController.getDepartmentByCompanyId)

module.exports = router
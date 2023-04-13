const express = require("express");
const router = express.Router();
const siteController = require("../controllers/siteController");

router.get("/getSiteById/:id", siteController.getById);

router.get("/getAllSite", siteController.getAll);

router.get("/getSiteByCompany/:companyid", siteController.getByCompany)


module.exports = router
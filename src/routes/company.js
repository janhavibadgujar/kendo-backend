const express = require("express");
const router = express.Router();
const companyController=require('../controllers/companyController');

router.get('/getCompanyById/:id',companyController.getById);

router.get('/getAllCompany', companyController.getAllCompany);

module.exports = router;
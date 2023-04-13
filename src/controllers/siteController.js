const siteHelper = require("../helpers/siteHelper");

exports.getById = async (req, res) => {
  await siteHelper.getById(req.params.id).then((response) => {
    if (response.recordset != null) {
      res.send(response.recordset);
    }
  })
    .catch((err) => {
      res.status(400).send({ message: `Can't find details for ${req.params.id}` })
    });
}

exports.getAll = async (req, res) => {
  await siteHelper.getAll().then((response) => {
    if (response.recordset != null) {
      res.send(response.recordset);
    }
  })
    .catch((err) => {
      res.status(400).send({ message: "No Data" })
    });
}

exports.getByCompany = async (req, res) => {
  await siteHelper.getByCompany(req.params.companyid).then((response) => {
    if (response.recordset != null) {
      res.send(response.recordset);
    }
  })
    .catch((err) => {
      res.status(400).send({ message: `Can't find details for ${req.params.companyid}` })
    });
}
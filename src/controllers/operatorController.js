const operatorHelper = require("../helpers/operatorHelper");

exports.getBySiteId = async (req, res) => {
  var results = [];
  await operatorHelper.getBySiteId(req.params.siteid).then(async (response) => {
    if (response != null) {
      for (let i = 0; i < response.recordset.length; i++) {
        const id = response.recordset[i].AssetID;
        await operatorHelper.getOpeartorByOperatorId(id).then(async (operator) => {
          if (operator != null) {
            results.push(operator.recordset)
          }
        })
      }

      res.send(results)
    }
  })
    .catch((err) => {
      res.status(400).send({ message: `Can't find details for ${req.params.siteid}` })
    });
}
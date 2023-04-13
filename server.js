const express = require('express');
const bodyParser = require("body-parser");
const cors = require("cors");
const app = express();
const config = require('./src/config/dbconfig')

const server = require("http").createServer(app);
const port = 8080;

const routes = [
  "Company",
  "Asset",
  "Site",
  "Operator",
  "Department",
]

app.use(bodyParser.json());
app.use(bodyParser.urlencoded({ extended: true }));

app.use(cors());
app.options("*", cors());
app.use(function (req, res, next) {
  res.header("Access-Control-Allow-Origin", "*");
  res.header("Access-Control-Allow-Methods", "DELETE, PUT, GET, POST, PATCH");
  res.header(
    "Access-Control-Allow-Headers",
    "Origin, X-Requested-With, Content-Type, Accept"
  );
  next();
});

routes.forEach((route) => {
  let routers = require("./src/routes/" + route.toLowerCase() + ".js");
  app.use("/api/" + route, routers);
});

app.use("*", function (req, res) {
  res.status(404).send("Invalid endpoint");
});

server.listen(port, "0.0.0.0", () => {
  console.log(`Server is running on port ${port}.`);
});



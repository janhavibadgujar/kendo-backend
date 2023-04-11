const express = require('express');
const bodyParser = require("body-parser");
const cors = require("cors");
const app = express();
const sql = require('mssql');
const config = {
    user: process.env.DB_USER,
    password: '',
    server: "",
    database: process.env.DB_DATABASE,
    options: {
        encrypt: false ,

      }
  };

const pool = new sql.ConnectionPool(config);

pool.connect().then(() => {
    console.log('Connected to database!');
  }).catch(err => {
    console.log('Error',err);
  });


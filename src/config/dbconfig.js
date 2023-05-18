const sql = require('mssql');

const config = {
    user: process.env.username,
    password: process.env.password,
    server: process.env.server_host,
    database: process.env.dbName,
    options: {
        encrypt: false,
    },
    requestTimeout: 100000000
};

const pool = new sql.ConnectionPool(config);

pool.connect().then(() => {
    console.log('Connected to database!');
}).catch(err => {
    console.log('Error', err);
});

module.exports = pool;
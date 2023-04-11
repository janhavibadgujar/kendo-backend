const config = {
    user: process.env.username,
    password:process.env.password,
    server: process.env.server_host,
    database: process.env.dbName,
    options: {
        encrypt: false ,

      }
  };

 module.exports = config
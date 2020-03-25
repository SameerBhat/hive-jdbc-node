var express = require('express');
var cors = require('cors');
var app= express();
const fs = require('fs');
app.use(cors())


app.get('/api/transcripts/:tableName/:limit', function (req, res) {
    const tableName = req.params.tableName;
    const limit = req.params.limit;

    fs.readFile('./test_response.json',"utf8", function read(err, data) {
      if (err) {
          throw err;
      }
      res.send(data);
  });
    
    

  })

console.log("app is listening at port http://localhost:1212")
app.listen(1212)




// for (var i = 0; i < result.length; i++) {
//     var row = result[i];
//     console.log(row["foo"]);
//   }
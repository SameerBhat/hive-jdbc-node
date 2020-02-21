var express = require('express');
var app= express();


var result = [ { foo: 1, bar: 'a' },
{ foo: 2, bar: 'b' },
{ foo: 3, bar: 'c' },
{ foo: 4, bar: 'd' },
{ foo: 5, bar: 'e' },
{ foo: 6, bar: 'f' },
{ foo: 7, bar: 'g' },
{ foo: 8, bar: 'h' },
{ foo: 9, bar: 'i' },
{ foo: 10, bar: 'j' } ];



app.get('/api/transcripts/:tableName/:limit', function (req, res) {
    const tableName = req.params.tableName;
    const limit = req.params.limit;
    
    res.send(JSON.stringify(result));

  })

console.log("app is listening at port http://localhost:1212")
app.listen(1212)




// for (var i = 0; i < result.length; i++) {
//     var row = result[i];
//     console.log(row["foo"]);
//   }
var JDBC = require("jdbc");
var jinst = require("jdbc/lib/jinst");
var cors = require('cors');
var express = require('express');
var app = express();
var bodyParser = require('body-parser');
app.use(cors());
app.use(bodyParser.json());


//create a jvm and specify the jars required in the classpath and other jvm parameters
if (!jinst.isJvmCreated()) {
  jinst.addOption("-Xrs");
  jinst.setupClasspath([
    "./lib/hive-jdbc-1.2.0-mapr-1611-standalone.jar",
    "./lib/hadoop-common-2.7.0-mapr-1707-javadoc.jar",
    "./lib/mysql-connector-java-5.1.23-bin.jar"
  ]);
}


var conf = {
  url: "jdbc:mysql://localhost/workspace_dev?user=root&password=root",
  drivername: "org.apache.hive.jdbc.HiveDriver",
  properties: {}
};

var hive = new JDBC(conf);

//initialize the connection

hive.initialize(function (err) {
  if (err) {
    console.log(err);
    console.log("error from lime 35")
  }
});

hive.reserve(function (err, connObj) {
  if (connObj) {
    //console.log("Connection : " + connObj.uuid);
    var conn = connObj.conn;

    app.get('/api/transcripts/:tableName/:fnum', function (req, res) {
      const tableName = req.params.tableName;
      const fnum = req.params.fnum;

      getItemsFromDB(conn, tableName, fnum, function (data) {
        //  console.log(data);
          res.send(JSON.stringify(data));



        }, function (error) {

          if (error != null) {
            console.log(error);
          }

        }

      );


    });



    app.post('/api/transcripts/:tableName/:fnum', function (req, res) {
      //const tableName = 'label_'+req.params.tableName;
      const tableName = 'label_dom_nap_raw_trans_data';
      const fnum = req.params.fnum;
     // console.log(tableName);
      //console.log(fnum);
      req.body.forEach(data => {
        console.log(data);

        var newdata = [];

        // data.forEach(element => {

        //   var item = element ? element.replace(/[^\w\s]/gi, ' ') : null;

        //   newdata.push(item);
        // });



        insertItemsToDB(conn, tableName, data, function (data) {
            console.log(data);
           // res.send(JSON.stringify(data))
  
          }, function (error) {
  
            if (error != null) {
              console.log(error);
            }
  
          }
  
        );


      });
      
    });


    console.log("app is listening at port http://localhost:1212")
    app.listen(1212)


  } else {
    console.log("couldnt create connection")
  }
});







function getItemsFromDB(conn, tableName, fnum, callbackFunction, errorFunction) {
  conn.createStatement(function (err, statement) {
    if (err) {
      errorFunction(err);
    } else {
      // console.log("Executing query.");
      statement.executeQuery("SELECT * FROM " + tableName + " WHERE fnum='" + fnum+"'", function (
        err,
        resultset
      ) {
        if (err) {
          console.log(err);
          errorFunction(err);
        } else {
          // console.log("Query Output :");
          resultset.toObjArray(function (err, result) {
            if (result != null) {

              if (result.length > 0) {

                callbackFunction(result);
                // console.log("foo :" + util.inspect(result));


                // for (var i = 0; i < result.length; i++) {
                //   var row = result[i];
                //   console.log(row["foo"]);
                // }
              } else {
                callbackFunction([]);
              }
            } else {
              callbackFunction([]);
            }
            //errorFunction(null, resultset);
          });
        }
      });
    }
  });





}



function insertItemsToDB(conn, tableName, data, callbackFunction, errorFunction) {

  
  conn.createStatement(function (err, statement) {
    if (err) {
      errorFunction(err);
    } else {
      // console.log("Executing query.");
      statement.executeUpdate(`INSERT INTO ${tableName} values ('${data[0]}','${data[1]}','${data[2]}','${data[3]}','${data[4]}','${data[5]}','${data[6]}','${data[7]}','${data[8]}','${data[9]}','${data[10]}','${data[11]}','${data[12]}','${data[13]}','${data[14]}','${data[15]}','${data[16]}','${data[17]}','${data[18]}','${data[19]}','${data[20]}','${data[21]}','${data[22]}','${data[23]}');`, function (
        err,
        resultset
      ) {
        if (err) {
          console.log(err);
          errorFunction(err);
        } else {
          
           //console.log(resultset);

           if(resultset == 1){
            callbackFunction('ok')
           }else{
            errorFunction('error');
           }

          //  if(resultset == true)
          // resultset.toObjArray(function (err, result) {
          //   if (result != null) {

             
          //   } else {
          //     callbackFunction([]);
          //   }
           
          // });
        }
      });
    }
  });





}
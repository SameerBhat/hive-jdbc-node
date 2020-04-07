var JDBC = require("jdbc");
var jinst = require("jdbc/lib/jinst");
var cors = require('cors');
var express = require('express');
var app = express();
var bodyParser = require('body-parser');
var fs = require('fs');
 
app.use(cors());
app.use(bodyParser.json());

const firstRowTitles = ["fnum","line","time","speaker","paragraph","annotators_name","promise1","pwrd1","promise_phrase1","promise2","pwrd2","promise_phrase2","promise_comment","TOPIC1","twrd1","pharse1","TOPIC2","twrd2","pharse2","sentiment_phrase1","sentiment_phrase2","cpn_name","agent","customer"];


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



    app.get('/api/transcripts/:tableName/:fnum', function (req, res) {
      const tableName = req.params.tableName;
      const fnum = req.params.fnum;

      getItemsFromDB(tableName, fnum, function (data) {
          res.send(JSON.stringify(data));
        }, function (error) {

          if (error != null) {
            res.status(404)        // HTTP status 404: NotFound
            .send(JSON.stringify(error));
            console.log(error);
          }

        }

      );


    });



    app.post('/api/transcripts/:tableName/:fnum', function (req, res) {

      const tableName = req.params.tableName.replace('_raw_', '_labeled_');
      const fnum = req.params.fnum;
      var csvDataArray = req.body;
      const csvFileLocation = "/tmp/test.csv";


      var csvStringArray = [];

      csvStringArray.push(firstRowTitles.join(","));

      for (let i = 0; i < csvDataArray.length; i++) {
        const row = csvDataArray[i];
        const rowString = row.join(",");
        csvStringArray.push(rowString);
      }

      var csv = csvStringArray.join("\n");
      fs.writeFile(csvFileLocation, csv, function(err) {
        if(err) {
            return console.log(err);
        }
        console.log("The file was saved!");
    });

      

         insertItemsToDB(tableName, csvFileLocation, function (data) {

          

          if(data == "ok"){
          
            res.send({status: "success", message: "All records inserted successfuly"})
            

          }else{
            console.log("there was an error in inserting row number "+rowIndex);
            res.send({status: "error", message: "Error saving some records at row "+rowIndex})
          }

          
          }, function (error) {
            if (error != null) {
              res.send({status: "error", message: "Something went wrong while saving data"});
              console.log(error);
            }
          }
  
        );

      
    });


    console.log("app is listening at port http://localhost:1212")
    app.listen(1212)

    







function getItemsFromDB( tableName, fnum, callbackFunction, errorFunction) {


  hive.initialize(function (err) {
    if (err) {
      console.log(err);
      console.log("error from lime 35")
    }
  });
  
  hive.reserve(function (err, connObj) {
    if (connObj) {
      var conn = connObj.conn;

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
    
    
    
} else {
  console.log("couldnt create connection")
}
});
  


}



function insertItemsToDB(tableName, csvFileLocation, callbackFunction, errorFunction) {


  hive.initialize(function (err) {
    if (err) {
      console.log(err);
      console.log("error from lime 200");
      errorFunction(err);
    }
  });
  
  hive.reserve(function (err, connObj) {
    if (connObj) {
      var conn = connObj.conn;

      conn.createStatement(function (err, statement) {
        if (err) {
          errorFunction(err);
        } else {
          // console.log("Executing query.");
          statement.executeUpdate(`LOAD DATA LOCAL INPATH '${csvFileLocation}' INTO TABLE ${tableName};`, function (
            err,
            resultset
          ) {
            if (err) {
              console.log(err);
              errorFunction(err);
            } else {
               if(resultset == 1){
                callbackFunction("ok")
               }else{
                errorFunction('error');
               }
    
             
            }
          });
        }
      });
} else {
  console.log("couldnt create connection")
}
});
 
  
  





}
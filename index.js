var JDBC = require("jdbc");
var jinst = require("jdbc/lib/jinst");
var cors = require('cors');
var express = require('express');
var app = express();
var bodyParser = require('body-parser');
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

hive.initialize(function (err) {
  if (err) {
    console.log(err);
    console.log("error from lime 35")
  }
});

hive.reserve(function (err, connObj) {
  if (connObj) {
    var conn = connObj.conn;

    app.get('/api/transcripts/:tableName/:fnum', function (req, res) {
      const tableName = req.params.tableName;
      const fnum = req.params.fnum;

      getItemsFromDB(conn, tableName, fnum, function (data) {
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

      const totalRowsLength = req.body.length | 0;
      var receivedRowsLength = 0;
      var sqlqueries = [];


      req.body.forEach((rowArray, rowIndex) => {
     
        var newdata = [];
        const currentRowTitles = [];
     
       
        rowArray.forEach((cellItem, index) => {
          if(cellItem == null || cellItem == ''){}else {
            const item = cellItem.replace(/'/g,"\\'") ;
            currentRowTitles.push(firstRowTitles[index]);
            newdata.push(`'${item}'`);
          }
        });

     
        const columns = currentRowTitles.join(",");
        const values = newdata.join(",");
        sqlqueries.push(`INSERT INTO ${tableName} (${columns}) values  (${values});`);

     
   
      });



         insertItemsToDB(conn, tableName, sqlqueries, function (data) {

          if(data == "ok"){
           
            console.log(`total rows : ${totalRowsLength}, Inserted rows: ${receivedRowsLength}`);
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



function insertItemsToDB(conn, tableName, sqlqueries, callbackFunction, errorFunction) {

  
  conn.createStatement(function (err, statement) {
    if (err) {
      errorFunction(err);
    } else {
      // console.log("Executing query.");
      statement.executeUpdate(sqlqueries.join(" "), function (
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
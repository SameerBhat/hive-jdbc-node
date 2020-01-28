var JDBC = require("jdbc");
var jinst = require("jdbc/lib/jinst");
var asyncjs = require("async");
var util = require("util");

//create a jvm and specify the jars required in the classpath and other jvm parameters
if (!jinst.isJvmCreated()) {
  jinst.addOption("-Xrs");
  jinst.setupClasspath([
    "./lib/hive-jdbc-1.2.0-mapr-1611-standalone.jar",
    "./lib/hadoop-common-2.7.0-mapr-1707-javadoc.jar"
  ]);
}

//read the input arguments
//jdbc:hive2://sandbox.hortonworks.com:2181/;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2
var server = process.argv[2];

var port = process.argv[3];

var schema = process.argv[4];

var username = process.argv[5];

var password = process.argv[6];

//specify the hive connection parameters

// var conf = {
//   url:
//     "jdbc:hive2://" +
//     server +
//     ":" +
//     port +
//     "/" +
//     schema +
//     ";user=" +
//     username +
//     ";password=" +
//     password,
//   drivername: "org.apache.hive.jdbc.HiveDriver",
//   properties: {}
// };



var conf = {
    url:
      "jdbc:hive2://" +server +":" +port +"/" +schema +";user=" +username +";password=" +password,
    drivername: "org.apache.hive.jdbc.HiveDriver",
    properties: {}
  };

var hive = new JDBC(conf);

//initialize the connection

hive.initialize(function(err) {
  if (err) {
    console.log(err);
  }
});

// create the connection

hive.reserve(function(err, connObj) {
  if (connObj) {
    console.log("Connection : " + connObj.uuid);
    var conn = connObj.conn;

    asyncjs.series([
      //set hive paramters if required. A sample property is set below
      function(callback) {
        conn.createStatement(function(err, statement) {
          if (err) {
            callback(err);
          } else {
            statement.execute("SET hive.metastore.sasl.enabled=false", function(
              err,
              resultset
            ) {
              if (err) {
                callback(err);
              } else {
                console.log("Seccessfully set the properties");
                callback(null, resultset);
              }
            });
          }
        });
      },
      // calling a select query in the session below.
      function(callback) {
        conn.createStatement(function(err, statement) {
          if (err) {
            callback(err);
          } else {
            console.log("Executing query.");
            statement.executeQuery("SELECT * FROM pokes LIMIT 10", function(
              err,
              resultset
            ) {
              if (err) {
                console.log(err);
                callback(err);
              } else {
                console.log("Query Output :");
                resultset.toObjArray(function(err, result) {
                  if (result.length > 0) {
                    console.log("foo :" + util.inspect(result));

                    // Above statement inspects the result object. Useful for debugging.
                    // Ex. output
                    //foo :[ { 'pokes.foo': 1, 'pokes.bar': 'a' },
                    //   { 'pokes.foo': 2, 'pokes.bar': 'b' } ]

                    for (var i = 0; i < result.length; i++) {
                      var row = result[i];
                      // Column names in the retured objects from
                      // hive are of the form <tablename>.<columnname>.
                      // Below output uses this format for printing
                      // the column output.
                      console.log(row["pokes.foo"]);
                    }
                  }
                  callback(null, resultset);
                });
              }
            });
          }
        });
      }
    ]);
  }
});

function throwUserError(message) {
    DataStudioApp.createCommunityConnector()
      .newUserError()
      .setText(message)
      .throwException();
  }


function myOtherScript(promptResponse) {
  return true;
};
function getId() {

  /** @constructor */
  function UUID() {}
  
  /**
  V3
   * The simplest function to get an UUID string.
   * @returns {string} A version 4 UUID string.
   */
  UUID.generate = function() {
    var rand = UUID._gri, hex = UUID._ha;
    return  hex(rand(32), 8)          // time_low
          + "-"
          + hex(rand(16), 4)          // time_mid
          + "-"
          + hex(0x4000 | rand(12), 4) // time_hi_and_version
          + "-"
          + hex(0x8000 | rand(14), 4) // clock_seq_hi_and_reserved clock_seq_low
          + "-"
          + hex(rand(48), 12);        // node
  };
  
  /**
   * Returns an unsigned x-bit random integer.
   * @param {int} x A positive integer ranging from 0 to 53, inclusive.
   * @returns {int} An unsigned x-bit random integer (0 <= f(x) < 2^x).
   */
  UUID._gri = function(x) { // _getRandomInt
    if (x <   0) return NaN;
    if (x <= 30) return (0 | Math.random() * (1 <<      x));
    if (x <= 53) return (0 | Math.random() * (1 <<     30))
                      + (0 | Math.random() * (1 << x - 30)) * (1 << 30);
    return NaN;
  };
  
  /**
   * Converts an integer to a zero-filled hexadecimal string.
   * @param {int} num
   * @param {int} length
   * @returns {string}
   */
  UUID._ha = function(num, length) {  // _hexAligner
    var str = num.toString(16), i = length - str.length, z = "0";
    for (; i > 0; i >>>= 1, z += z) { if (i & 1) { str = z + str; } }
    return str;
  };
  
  /**
   * Returns result to Google Sheet
  */
  var newId = UUID.generate();
  return newId;
}




function yyyymmdd(epoch_string) {
    var epoch_ms = Number(epoch_string)
    var date = new Date(epoch_ms);
    var y = date.getFullYear();
    var m = date.getMonth() + 1;
    var d = date.getDate();
    var h = date.getHours();
    var mm = m < 10 ? '0' + m : m;
    var dd = d < 10 ? '0' + d : d;
    return '' + y + mm + dd + h;
}

// Initialize new Community Connector
var cc = DataStudioApp.createCommunityConnector();



function getFields(request, token, rtype) {

  
  // Define fields
  var fields = cc.getFields();
  
  
  
  // Define field types
  var types = cc.FieldType;
  
  // Define aggregation types for fields
  var aggregations = cc.AggregationType;
  
  // Defone account URL variable using configParams Input
  var account_url = request.configParams.account_url;
 var myguid = getId();
  // Piece together query request URL
  var url = 'https://' + account_url + '/queries/v1/query-request?requestId=' + myguid;
  

  // Example query: select C_NAME, C_ACCTBAL from snowflake_sample_data.tpch_sf1.CUSTOMER limit 12;' };
  var payload = { sqlText: request.configParams.query };
  
  // Options for second API call to retrieve data
  var options = { 
     'method': 'post',
     'muteHttpExceptions': true,
     'headers': {'Authorization': token,
                 'Accept': 'application/snowflake'
                   },
     'contentType': 'application/json',
     'payload': JSON.stringify(payload)
     };
  
  
  try{
    // Fetch response
    var response = UrlFetchApp.fetch(url, options);
    
    // Parse response into JSON
    var parsedResponse = JSON.parse(response.getContentText())
    
    
    
    var Qresult = parsedResponse['success'];
    var Qmsg = parsedResponse['message'];
    
    if(rtype == 'run') 
    {
      if(Qresult == false) 
      {
        return 'QUERY FAILED: ' + Qmsg;
      }
      else
      {
        return 'QUERY PASS:';
      }
    } 
    
    if(rtype == 'get'){
      // Extract columns from parsed response
      var columns = parsedResponse['data']['rowtype'];
      
      
      
      // For each column that was returned, define a new field with the appropriate data type
      columns.forEach(function(column) {
        var field;
        
        
        // Switch statement to categorize each data type into GDS buckets
        switch (column.type.toLowerCase()) {
          case 'boolean':
            field = fields
            .newDimension()
            .setId(column.name)
            .setType(types.BOOLEAN);
            break;
          case 'fixed':
            field = fields
            .newMetric()
            .setId(column.name)
            .setType(types.NUMBER);
            break;
          case 'variant':
          case 'varchar':
          case 'string':
          case 'real':
          case 'text':
            field = fields
            .newDimension()
            .setId(column.name)
            .setType(types.TEXT);
            break;
          case 'date':
            field = fields
            .newDimension()
            .setId(column.name)
            .setType(types.YEAR_MONTH_DAY);
            break;
          case 'timestamp_ltz':
          case 'timestamp_ntz':
          case 'timestamp':
            field = fields
            .newDimension()
            .setId(column.name)
            .setType(types.YEAR_MONTH_DAY_HOUR);
            break;
          default:
            return;
        }
        
        
      });
      

  // Return fields
  return fields;
    }
  }
  catch (err) {
     // throwUserError(err);
      return err;
    }
    
    
}

function getToken(request) {
 

  var params = request.configParams;
  // params = conn;
  
  
  
  // Define connection variables using configParams input
  var account_url = params.account_url;
  var username = params.username;
  var password = params.password;
  

  
  // Define query context using configParams input
  var database_name = params.database;
  var schema_name = params.schema;
  var warehouse_name = params.warehouse;
  var role_name = params.role;
  
  
  
  
  
  // Piece together URL
  var token_url = "https://" + account_url + "/session/v1/login-request?databaseName=" + database_name + "&schemaName=" + schema_name + "&warehouse=" + warehouse_name;
  
  if(role_name)
  {
    token_url = token_url + "&roleName=" + role_name;
  }
  // Define account name
  var account_name = account_url.split(".")[0];
  
  // Credential payload for first API call
  var payload = {
   data:
      { 'LOGIN_NAME': username,
        'PASSWORD': password,
        'ACCOUNT_NAME': account_name,
       'CLIENT_APP_ID' : 'GoogleDataStudio'
      }
   };
  
  // Options for first API call to retrieve token
  var options = {
   'method' : 'post',
   'contentType': "application/json",
   'muteHttpExceptions': true,
   'payload' : JSON.stringify(payload)
  };
  
  // Fetch response
  var response = UrlFetchApp.fetch(token_url, options);
  
  // Parse response into JSON
  var response_json = JSON.parse(response.getContentText());

  // Extract token from response
  var token = response_json['data']['token'];
  
  var fullToken = 'Snowflake Token="' + token + '"';
 
  
  return fullToken;
}

function getQueryResults(parsedResponse) {

  var rows = [];
  
  // Extract row data and column info from response
  var rowset = parsedResponse['data']['rowset'];
  var rowtype = parsedResponse['data']['rowtype'];
  var returncount = parsedResponse.data.returned;
  var totalcount = parsedResponse.data.total;
  var rowsetcount = rowset.length;

  //IF Result is in Chucks
  if (rowsetcount != returncount)
  {
  

    var chunks = parsedResponse['data']['chunks'];
     var dto = parsedResponse['data']


    var headerst = "{";
    for (let key in dto.chunkHeaders) {
     headerst = headerst + "\"" + key + "\" : \"" + dto.chunkHeaders[key] + "\",";
      
    }
    headerst= headerst + " \"xyz\" : 2 }";
 
    
headerst = JSON.parse(headerst);

    options = {
       headerst
    };    
    

    var chuckurl ;
    var resresponse ;
// SERIAL FETCHING
/*        
    for (i = 0; i < chunks.length; i++) {
      chuckurl = chunks[i].url;
      if(i==0)
      {
        resresponse = UrlFetchApp.fetch(chuckurl, options);
      }
      else
      {
        resresponse += ',' + UrlFetchApp.fetch(chuckurl, options);
      }
    }
 */   
    
 

//----- START PARALLEL CHUNKS
var chuckurls = [];
    for (i = 0; i < chunks.length; i++) {
      chuckurls[i] = chunks[i].url;
    }
    

    
    
    function createReq() {
      return Object.keys(chuckurls).map(function(e, i) {
        return { 
         'headers': headerst,
          "url": chuckurls[e]
         };
      });
    }   

   var z  = createReq();

    
resresponse = UrlFetchApp.fetchAll(z);


//----- END PARALLEL CHUNKS  

    
    var fulloutput = '{\"rowset\": [' + resresponse.toString() + ']}' ;
    
    tempresult = JSON.parse(fulloutput)
    
    rowset = tempresult['rowset'];
  }
  
  
  // For each column retrieved, add its name into an array
  var columns = rowtype.map(function(info) {
      return info['name'];
    });
    
  // For each row retrieved, map each value to its column key (i.e. combine [val0, val1, ...] and [col0, col1, ...] into { col0: val0, col1: val1, ... })
  rowset.forEach(function(row) {
    
    // Create empty dictionary
    var newRow = {};
    
    // Map each value to its column key
    row.forEach(function(data, index) {
       var column = columns[index];
       newRow[column] = data;
    });
    
    // Add the row to the rows array
    rows.push(newRow);
  });

  return rows;
}

function queryResultsToRows(schema, queryResults) {

  // For each item in queryResults, assign it the proper data type
  return queryResults.map(function(data) {
    
    var values = [];
    
    schema.forEach(function(field) {
      var value = data[field.name];
      
      // Snowflake returned all values as strings, need to convert data type
      switch (field.dataType.toLowerCase()) {
        case 'fixed':
          values.push(parseFloat(value));
          break;
        case 'variant':
        case 'text':
          values.push(string(value));
          break;
        case 'timestamp_ltz':
        //case 'real':
        case 'timestamp_ntz':
        case 'timestamp':
          var ts = yyyymmdd(value);
          values.push(ts);
          break;
        case 'boolean':
          values.push(value.toLowerCase() === 'true');
          break;
        default:
          values.push(value);
          break;
      }
    });
    return {values: values};
  });

  return rows.map(function(row) {
    return row.map(function(field, index) {
      switch (schema[index].dataType.toLowerCase()) {
        case 'fixed':
          return parseFloat(field);
        case 'boolean':
          return (field === 'true');
        case 'variant':
        case 'text':
           return string(field);
        case 'timestamp_ltz':
        case 'timestamp_ntz':
        case 'timestamp':
           var ts = yyyymmdd(field);
           return ts;
        default:
          return field;
      }
    });
  });
}

function getDataFromSnowflake(request, token) {
  //request = req;

  var requestedFieldIds = request.fields.map(function(field) {
    return field.name;
  });
  
  var fields = getFields(request, token, 'get');
  var requestedFields = fields.forIds(requestedFieldIds);
  
  // Define schema by building requested fields
 var schema = requestedFields.build();

  // Define account name variable using configParams input
  var account_url = request.configParams.account_url;


  
  // Piece together query request URL - NEED TO PARAMETERIZE REQUEST ID
   var myguid = getId();
  var url = 'https://' + account_url + '/queries/v1/query-request?requestId=' + myguid;
  
  // Grab query from configParams input
  var payload = { sqlText: request.configParams.query };
  
  // Options for second API call to retrieve data
  var new_options = { 
     'method': 'post',
     'muteHttpExceptions': true,
     'headers': {'Authorization': token,
                 'Accept': 'application/snowflake'
                   },
     'contentType': 'application/json',
     'payload': JSON.stringify(payload)
     };
  
  // Fetch response  
  var response = UrlFetchApp.fetch(url, new_options);
  

  
  // Parse response into JSON
  var parsedResponse = JSON.parse(response.getContentText());

  // Pass parsed response to getQueryResults function to extract query results
  var queryResults = getQueryResults(parsedResponse);

  // Pass query results to queryResultToRows to extract rows
  var rows = queryResultsToRows(schema, queryResults);
  
  // Return schema and rows
  return {
    schema: schema,
    rows: rows
  };
}
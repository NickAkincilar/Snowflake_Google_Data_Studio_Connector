
/*
V4
 * This checks whether the current user is an admin user of the connector.
 *
 * @returns {boolean} Returns true if the current authenticated user at the time
 * of function execution is an admin user of the connector. If the function is
 * omitted or if it returns false, then the current user will not be considered
 * an admin user of the connector.
 */
function isAdminUser() {
    return true;
  }
  
  /**
   * Returns the authentication method required by the connector to authorize the
   * third-party service.
   *
   * @returns {Object} `AuthType` used by the connector.
   */


  function getAuthType() {
    var response = {type: 'NONE'};
   
    return response;
  }




  /**
   * Returns the user configurable options for the connector.
   *
   * @param {Object} request Config request parameters.
   * @returns {Object} Connector configuration to be displayed to the user.
   */
  function getConfig(request) {
    var cc = DataStudioApp.createCommunityConnector();
    
    var config = cc.getConfig();

    config.newInfo()
      .setId('instructions')
      .setText('Enter your account URL, username, password, warehouse, database, and schema name.');
    
    config.newTextInput()
      .setId('account_url')
      .setName('Enter your Snowflake account URL')
      .setHelpText('e.g. account_name.snowflakecomputing.com')
      .setPlaceholder('<account_name>.snowflakecomputing.com');
    
    config.newTextInput()
      .setId('username')
      .setName('Username')
      .setHelpText('e.g. JDOE')
    .setPlaceholder('e.g. JOE');
    
    config.newTextInput()
      .setId('password')
      .setName('Password')
      .setHelpText('e.g. passwd')
      .setPlaceholder('passwd'); 
   
    config.newTextInput()
      .setId('role')
      .setName('Role Name (optional)')
      .setHelpText('e.g. SYSADMIN ')
      .setPlaceholder('e.g. SYSADMIN');
    
    config.newTextInput()
      .setId('warehouse')
      .setName('Warehouse name (optional)')
      .setHelpText('e.g. compute_wh')
      .setPlaceholder('e.g.compute_wh');
    
    config.newTextInput()
      .setId('database')
      .setName('Database name (optional)')
      .setHelpText('e.g. my_db')
      .setPlaceholder('e.g. snowflake_sample_data');
    
     config.newTextInput()
      .setId('schema')
      .setName('Schema name (optional)')
      .setHelpText('e.g. my_schema')
      .setPlaceholder('e.g. tpch_sf100');
     
    config.newTextInput()
      .setId('query')
      .setName('Enter your SQL query (use dbname.schema.table if they database & schema is not filled in)')
      .setHelpText('e.g. select * from customer limit 100')
      .setPlaceholder('e.g. select * from trips');
    
    return config.build();
  }
  
  /**
   * Throws User-facing errors.
   *
   * @param  {string} message Error message.
   */
  function throwUserError(message) {
    DataStudioApp.createCommunityConnector()
      .newUserError()
      .setText(message)
      .throwException();
  }
  
  /**
   * Validate config object and throw error if anything wrong.
   *
   * @param {Object} configParams Config object supplied by user.
   */
  function validateConfig(configParams) {
  
    configParams = configParams || {};
    
    if (!configParams.account_url) {
      throwUserError('Account URL is empty.');
    }
    if (!configParams.username) {
      throwUserError('Username is empty.');
    }
    if (!configParams.password) {
      throwUserError('Password is empty.');
    }
    /* 
    if (!configParams.warehouse) {
      throwUserError('Warehouse name is empty.');
    }  
     if (!configParams.database) {
      throwUserError('Database Name is empty.');
    }
    if (!configParams.schema) {
      throwUserError('Schema Name is empty.');
    }
    */
    
    if (!configParams.query) {
      throwUserError('Query is empty.');
    }
  }
  
  /**
   * Returns the schema for the given request.
   *
   * @param {Object} request Schema request parameters.
   * @returns {Object} Schema for the given request.
   */
  function getSchema(request) {
    
    // Validate config param input
    validateConfig(request.configParams);
      
    try {
      var token = getToken(request);
      
      if(token == 'Snowflake Token=\"undefined\"'){
        
        throw 'Login Error! Cancel & Edit the connection to with different credentials!!! ';
      }
      
      
      var qstat = getFields(request, token, 'run');
      
      if(qstat.substring(0, 12) == 'QUERY FAILED')
      {
      throw  qstat;
      }
      var fields = getFields(request, token,'get').build(); 
      return {schema: fields};
      
    } catch (err) {
      throwUserError(err);
    }
    
  
    
  }
  
  /**
   * Returns the data for the given request.
   *
   * @returns {Object} data for the given request.
   */
  function getData(request) {
 
    try {
      var token = getToken(request);

      var data = getDataFromSnowflake(request, token);

      return data;
    } catch (err) {
      throwUserError(err.message);
    }
  }
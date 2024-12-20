-- This stored proc is created outside of the native application, and can perform connection creation/update in a single stored proc call
-- instead of the multi-step process inside the UI
-- Caveats:
-- 1. You must be ACCOUNTADMIN, as well as the owner of the Omnata Sync native app, or have the OMNATA_ADMINISTRATOR application role.
-- 2. The only connectivity options currently supported are 'direct' and 'privatelink' (not 'ngrok')
-- 3. Connection methods that leverage OAuth are not supported
-- 4. You'll need to know the appropriate connection parameters/secrets for your selected plugin and connection method, and provide them in the right format
-- Instructions:
-- 1. Create this stored proc somewhere in your Snowflake account
-- 2. Execute the proc like so:
-- call CONFIGURE_OMNATA_CONNECTION(
--   PLUGIN_FQN => 'MONITORIAL__MSSQL',
--   CONNECTION_NAME => 'My Production SQL Server',
--   CONNECTION_SLUG => 'mssql-prod',
--   CONNECTIVITY_OPTION => 'privatelink',
--   CONNECTION_METHOD => 'SQL Server Authentication',
--   CONNECTION_PARAMETERS => { 'username': 'sa', 'server_host': 'my.privatelink.nlb.address.com', 'server_port': 1433},
--   CONNECTION_SECRETS => { 'password': 'MyPassword123'},
--   OTHER_ENVIRONMENTS_EXIST => false,
--   IS_PRODUCTION_ENVIRONMENT => true
-- );
-- 3. You can re-run the proc to update settings, but use this sparingly as internally it creates a new external access integration then cuts over to it after testing.

CREATE OR REPLACE PROCEDURE CONFIGURE_OMNATA_CONNECTION(
    PLUGIN_FQN VARCHAR,
    CONNECTION_NAME VARCHAR,
    CONNECTION_SLUG VARCHAR,
    CONNECTIVITY_OPTION VARCHAR,
    CONNECTION_METHOD VARCHAR,
    CONNECTION_PARAMETERS OBJECT,
    CONNECTION_SECRETS OBJECT,
    OTHER_ENVIRONMENTS_EXIST BOOLEAN,
    IS_PRODUCTION_ENVIRONMENT BOOLEAN
)
RETURNS VARCHAR
LANGUAGE PYTHON
PACKAGES=('snowflake-snowpark-python', 'snowflake-telemetry-python')
RUNTIME_VERSION=3.11
HANDLER='run'
EXECUTE AS CALLER
AS $$
from snowflake.snowpark import Session
from typing import Dict
import json

def run(session: Session,
    plugin_fqn: str,
    connection_name: str,
    connection_slug: str,
    connectivity_option: str,
    connection_method: str,
    connection_parameters: Dict,
    connection_secrets: Dict,
    other_environments_exist: bool,
    is_production_environment: bool):
  plugin_database_raw = session.sql("""
    select DATABASE from OMNATA_SYNC_ENGINE.DATA_VIEWS.PLUGIN where PLUGIN_FQN=?""",
    [plugin_fqn]
  ).collect()
  if len(plugin_database_raw)==0:
    raise ValueError(f"Plugin with FQN {plugin_fqn} not found in Sync Engine")
  # we accept a simpler version of the connection_parameters and connection_secrets
  # we iterate over the items, and any non-dict values are converted to a dict with the 'value' key set as a string
  for key, value in connection_parameters.items():
    if not isinstance(value, dict):
      connection_parameters[key] = {"value": str(value)}
  for key, value in connection_secrets.items():
    if not isinstance(value, dict):
      connection_secrets[key] = {"value": str(value)}
  plugin_database = plugin_database_raw[0]['DATABASE']
  existing_connections_raw = session.sql("""
    select CONNECTION_ID from OMNATA_SYNC_ENGINE.DATA_VIEWS.CONNECTION where CONNECTION_SLUG=?""",
    [connection_slug]
  ).collect()
  if len(existing_connections_raw)>0:
    existing_connection_id = existing_connections_raw[0]['CONNECTION_ID']
    begin_connection_edit_result_raw = session.sql("""call OMNATA_SYNC_ENGINE.API.BEGIN_CONNECTION_EDIT(?,?,?,?,?)""",
      [
        connection_name,
        connection_slug,
        existing_connection_id,
        False,
        False
      ]
    ).collect()
    begin_connection_edit_result = json.loads(begin_connection_edit_result_raw[0][0])
    if begin_connection_edit_result['success'] == False:
      raise ValueError(begin_connection_edit_result['error'])
    connection_in_progress = begin_connection_edit_result['data']
  else:
    begin_connection_creation_result_raw = session.sql("""call OMNATA_SYNC_ENGINE.API.BEGIN_CONNECTION_CREATION(?,?,?,?,?,?,?,?)""",
      [
        plugin_fqn,
        connection_name,
        connection_slug,
        connectivity_option,
        connection_method,
        False,
        other_environments_exist,
        is_production_environment
      ]
    ).collect()
    begin_connection_creation_result = json.loads(begin_connection_creation_result_raw[0][0])
    if begin_connection_creation_result['success'] == False:
      raise ValueError(begin_connection_creation_result['error'])
    connection_in_progress = begin_connection_creation_result['data']
  session.sql(f"""
    CREATE OR REPLACE EXTERNAL ACCESS INTEGRATION IDENTIFIER(?)
    ALLOWED_NETWORK_RULES = ({plugin_database}.DATA.{connection_in_progress['NETWORK_RULE_NAME']})
    ALLOWED_AUTHENTICATION_SECRETS = ({plugin_database}.DATA.{connection_in_progress['OTHER_SECRETS_NAME']})
    ENABLED = true""",
    [
      connection_in_progress['EXTERNAL_ACCESS_INTEGRATION_NAME']
    ]
  ).collect()
  session.sql(f"""
    GRANT USAGE ON INTEGRATION IDENTIFIER(?)
    TO APPLICATION IDENTIFIER(?)""",
    [
      connection_in_progress['EXTERNAL_ACCESS_INTEGRATION_NAME'],
      plugin_database
    ]
  ).collect()
  # fetch the network addresses
  proc = f"{plugin_database}.PLUGIN.NETWORK_ADDRESSES"
  network_addresses_result_raw = session.sql("""call IDENTIFIER(?)(?,parse_json(?))""",
    [
      proc,
      connection_method,
      json.dumps(connection_parameters)
    ]
  ).collect()
  network_addresses_result = json.loads(network_addresses_result_raw[0][0])
  if network_addresses_result['success'] == False:
    raise ValueError(network_addresses_result['error'])
  network_addresses = network_addresses_result['data']
  
  complete_connection_creation_result_raw = session.sql("""call OMNATA_SYNC_ENGINE.API.COMPLETE_CONNECTION_CREATION(
      ?,PARSE_JSON(?),PARSE_JSON(?),PARSE_JSON(?))""",
    [
      connection_in_progress['CONNECTION_IN_PROGRESS_ID'],
      json.dumps(network_addresses),
      json.dumps(connection_parameters),
      json.dumps(connection_secrets)
    ]
  ).collect()
  complete_connection_creation_result = json.loads(complete_connection_creation_result_raw[0][0])
  if complete_connection_creation_result['success'] == False:
    raise ValueError(complete_connection_creation_result['error'])
  
  return "SUCCESS"
$$;





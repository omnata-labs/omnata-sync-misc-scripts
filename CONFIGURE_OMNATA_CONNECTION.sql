-- This stored proc is created outside of the native application, and can perform connection creation/update in a single stored proc call
-- instead of the multi-step process inside the UI
-- Caveats:
-- 1. You must be ACCOUNTADMIN, as well as the owner of the Omnata Sync native app, or have the OMNATA_ADMINISTRATOR application role.
-- 2. The only connectivity options currently supported are 'direct' and 'privatelink' (not 'ngrok')
-- 3. Connection methods that leverage OAuth are not supported
-- 4. You'll need to know the appropriate connection parameters/secrets for your selected plugin and connection method, and provide them in the right format
-- 5. The plugin application must be using the v2 connection flow (manifest version 2 / app specifications). This proc no longer
--    creates external access integrations directly; the plugin application creates them and this proc approves the resulting
--    application specification on your behalf. The caller must therefore be able to ALTER APPLICATION <plugin app> APPROVE SPECIFICATION
--    (ACCOUNTADMIN can).
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
    is_production_environment: bool):
  plugin_database_raw = session.sql("""
    select DATABASE from OMNATA_SYNC_ENGINE.DATA_VIEWS.PLUGIN where PLUGIN_FQN=?""",
    [plugin_fqn]
  ).collect()
  if len(plugin_database_raw)==0:
    raise ValueError(f"Plugin with FQN {plugin_fqn} not found in Sync Engine")
  plugin_database = plugin_database_raw[0]['DATABASE']
  # we accept a simpler version of the connection_parameters and connection_secrets
  # we iterate over the items, and any non-dict values are converted to a dict with the 'value' key set as a string
  for key, value in connection_parameters.items():
    if not isinstance(value, dict):
      connection_parameters[key] = {"value": str(value)}
  for key, value in connection_secrets.items():
    if not isinstance(value, dict):
      connection_secrets[key] = {"value": str(value)}

  # Step 1: BEGIN the connection creation or edit.
  # In the v2 flow, BEGIN_CONNECTION_CREATION/EDIT no longer accepts connection parameters or
  # creates the integration objects itself. Instead it generates the object names, creates a
  # CONNECTION_IN_PROGRESS record, and asks the plugin application to create the external access
  # integration, network rules, generic secret and the associated application *specification*.
  existing_connections_raw = session.sql("""
    select CONNECTION_ID from OMNATA_SYNC_ENGINE.DATA_VIEWS.CONNECTION where CONNECTION_SLUG=?""",
    [connection_slug]
  ).collect()
  if len(existing_connections_raw)>0:
    existing_connection_id = existing_connections_raw[0]['CONNECTION_ID']
    begin_result_raw = session.sql("""call OMNATA_SYNC_ENGINE.API.BEGIN_CONNECTION_EDIT(?,?,?,?,?,?)""",
      [
        connection_name,
        connection_slug,
        existing_connection_id,
        False, # new_security_integration (OAuth not supported by this proc)
        False, # new_oauth_secret (OAuth not supported by this proc)
        is_production_environment
      ]
    ).collect()
    begin_result = json.loads(begin_result_raw[0][0])
    if begin_result['success'] == False:
      raise ValueError(begin_result['error'])
    connection_in_progress = begin_result['data']
  else:
    begin_result_raw = session.sql("""call OMNATA_SYNC_ENGINE.API.BEGIN_CONNECTION_CREATION(?,?,?,?,?,?,?)""",
      [
        plugin_fqn,
        connection_name,
        connection_slug,
        connectivity_option,
        connection_method,
        False, # connection_method_uses_oauth (OAuth not supported by this proc)
        is_production_environment
      ]
    ).collect()
    begin_result = json.loads(begin_result_raw[0][0])
    if begin_result['success'] == False:
      raise ValueError(begin_result['error'])
    connection_in_progress = begin_result['data']

  connection_in_progress_id = connection_in_progress['CONNECTION_IN_PROGRESS_ID']
  # The BEGIN result carries the plugin database so we can approve the spec on the plugin app
  # without an extra lookup. Fall back to the lookup above if it's not present.
  plugin_database = connection_in_progress.get('PLUGIN_DATABASE') or plugin_database
  eai_spec_name = connection_in_progress.get('EAI_SPEC_NAME')

  # Step 2: push the connection parameters/secrets into the plugin objects.
  # UPDATE_CONNECTION_OBJECTS calls the plugin's NETWORK_ADDRESSES proc to resolve the network
  # rule addresses from the supplied parameters, then re-runs SET_CONNECTION_OBJECTS to populate
  # the network rule and merge in the secrets. OAuth values are not supported here, so we pass an
  # empty object.
  update_result_raw = session.sql("""call OMNATA_SYNC_ENGINE.API.UPDATE_CONNECTION_OBJECTS(
      ?,PARSE_JSON(?)::object,PARSE_JSON(?)::object,PARSE_JSON('{}')::object)""",
    [
      connection_in_progress_id,
      json.dumps(connection_parameters),
      json.dumps(connection_secrets)
    ]
  ).collect()
  update_result = json.loads(update_result_raw[0][0])
  if update_result['success'] == False:
    raise ValueError(update_result['error'])

  # Step 3: approve the external access integration application specification on the plugin app.
  # The plugin created (a new revision of) the spec during BEGIN/UPDATE; until it is approved the
  # integration exists but external network access is blocked. This replaces the old step of
  # creating the external access integration and granting usage to the plugin.
  if eai_spec_name:
    _approve_application_specification(
      session=session,
      plugin_database=plugin_database,
      spec_name=eai_spec_name,
      expected_type='EXTERNAL_ACCESS'
    )

  # Step 4: COMPLETE. Enables the new EAI, rebinds the plugin APIs (CONFIGURE_APIS), runs a
  # connection test, then converts the CONNECTION_IN_PROGRESS into a CONNECTION (and, for edits,
  # disables the previous EAI).
  complete_result_raw = session.sql("""call OMNATA_SYNC_ENGINE.API.COMPLETE_CONNECTION_CREATION(?)""",
    [
      connection_in_progress_id
    ]
  ).collect()
  complete_result = json.loads(complete_result_raw[0][0])
  if complete_result['success'] == False:
    raise ValueError(complete_result['error'])

  return "SUCCESS"


def _approve_application_specification(session: Session,
    plugin_database: str,
    spec_name: str,
    expected_type: str):
  """Finds and approves an application specification by name on the plugin app.
  Snowflake sometimes assigns an auto-incremented integer name to a spec rather than the
  string identifier we generated, so when an exact name match fails we fall back to the sole
  specification of the expected type (mirrors the lookup logic in CHECK_SPECIFICATION_STATUS)."""
  rows = session.sql(f"SHOW SPECIFICATIONS IN APPLICATION {plugin_database}").collect()

  def _col(row, name: str):
    return row[name] if name in row else None

  matched_row = None
  for row in rows:
    row_name = _col(row, 'name')
    if row_name and str(row_name).upper() == spec_name.upper():
      matched_row = row
      break
  if matched_row is None:
    type_matches = [
      r for r in rows
      if str(_col(r, 'type') or '').upper() == expected_type.upper()
    ]
    if len(type_matches) == 1:
      matched_row = type_matches[0]
  if matched_row is None:
    available = [_col(r, 'name') for r in rows]
    raise ValueError(
      f"Could not find specification {spec_name} on application {plugin_database}. "
      f"Available specifications: {available}"
    )
  actual_name = str(_col(matched_row, 'name'))
  sequence_number = _col(matched_row, 'sequence_number')
  session.sql(
    f"ALTER APPLICATION {plugin_database} APPROVE SPECIFICATION {actual_name} "
    f"SEQUENCE_NUMBER = {sequence_number}"
  ).collect()
$$;

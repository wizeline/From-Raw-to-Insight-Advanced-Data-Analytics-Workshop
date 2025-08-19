USE DATABASE SNOWFLAKE_LEARNING_DB;

/* Give access to public repo */

CREATE OR REPLACE API INTEGRATION git_api_integration_public
  API_PROVIDER = git_https_api
  API_ALLOWED_PREFIXES = ('https://github.com/wizeline/')
  ENABLED = TRUE;

/* Allow outbound to dbt Hub + GitHub tarballs (network rule) */

CREATE OR REPLACE NETWORK RULE dbt_packages_rule
  MODE = EGRESS
  TYPE = HOST_PORT
  -- Minimal allowlist recommended by Snowflake docs for dbt deps:
  VALUE_LIST = (
    'hub.getdbt.com',
    'codeload.github.com'
  );

/* Create the External Access Integration */

CREATE OR REPLACE EXTERNAL ACCESS INTEGRATION dbt_deps_eai
  ALLOWED_NETWORK_RULES = (dbt_packages_rule)
  ENABLED = TRUE
  COMMENT = 'EAI for dbt deps';

/* Grant usage to the role that runs your Workspace/commands */

GRANT USAGE ON INTEGRATION dbt_deps_eai TO ROLE ACCOUNTADMIN; 

/* Gain access to data share RAW database */

CREATE DATABASE shared_snowflake_raw FROM SHARE <your_account_locator>.workshop_share;
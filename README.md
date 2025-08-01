# Salesforce Writer
=============

The component takes files from the `in/tables` directory and inserts, upserts, updates, or deletes the corresponding records in Salesforce. The input table
must have a header with field names that exist in Salesforce. The ID column is used to identify which record will
be updated. When inserting records, all required fields must be filled in.

**Important Notes:**
- The process may fail for some records due to reasons such as missing required fields or exceeding string length limits
- Failed records are stored in output tables with detailed error information (see [Output](#output) section)
- There is no way to roll back this transaction, so it is essential to carefully check the logs after each run
- It is recommended to include a column with external IDs, allowing you to perform upserts later
- External IDs also help prevent duplicated records when running inserts multiple times


**Table of contents:**

- [Prerequisites](#prerequisites)
- [Configuration](#configuration)
  - [Authorization configuration](#authorization-configuration)
  - [Row configuration](#row-configuration)
- [Data Formatting (Null values, Multipick values)](#data-formatting)
- [Sample Configuration](#sample-configuration)
- [Output](#output)
- [Development](#development)
- [Integration](#integration)

Prerequisites
=============
To get your Salesforce credentials:

1. Log into your Salesforce platform
2. Click on your profile icon in the top right corner
3. Select "Settings"
4. In the Quick Find box, enter "Reset"
5. Select "Reset My Security Token"
6. Click "Reset Security Token"
7. The new security token will be sent to the email address specified in your Salesforce personal settings

**Important Notes:**
- Security token is required when accessing Salesforce via API from an IP address that's not in your company's trusted IP range
- You cannot reset your security token if:
  - Login IP ranges are set for your user profile
  - You have the API Only user permission
  - You have the Multi-Factor Authentication for API Logins user permission (in this case, use the code generated by an authenticator app)
- If you don't see the option to reset your security token, contact your Salesforce admin

For more information follow [this guide from Salesforce](https://help.salesforce.com/s/articleView?id=sf.user_security_token.htm&type=5)

Configuration
=============

Authorization configuration
---------------------------

- User Name (#username) - [REQ] Your username. When exporting data from a sandbox, don't forget to add `.sandboxname` at the end
- Password (#password) - [REQ] Your password
- Security Token (#security_token) - [REQ] Your security token. ***Note:** The security token is different for sandbox environments*
- Sandbox (sandbox) - [REQ] Set to `true` when pushing data to a sandbox
- Use Proxy (use_proxy) - [OPT] Section for configuring an HTTPS proxy
  - Proxy Server (proxy_server) - [REQ if Use Proxy is selected] HTTPS Proxy Server Address
  - Proxy Port (port) - [REQ] Proxy Server Port
  - Proxy Username (username) - [OPT] Proxy Server Username
  - Proxy Password (#password) - [OPT] Proxy Server Password
  - Use HTTP proxy for HTTPS (use_http_proxy_for_https) - [OPT] A hidden configuration option for an HTTP proxy that also handles HTTPS

Row configuration
---------------

- Object (object) - [REQ] The name of the object on which you want to perform the operation
- Upsert Field (upsertField) - [REQ for upsert] Required when performing an upsert operation. If you do not have an external ID, you must either create one, or use separate insert and update operations to achieve the same result
- Operation (operation) - [REQ] Specifies the operation to perform. Supported operations: Insert, Upsert, Update, Delete
- Serial Mode (serialMode) - [OPT] Set to `true` if you want to run the import in serial mode
- Assignment Rule ID (assignment_rule_id) - [OPT] The ID of the Lead [Assignment Rule](https://help.salesforce.com/s/articleView?id=sf.customize_leadrules.htm&language=en_US&type=5) you want to apply after import
- Replace String (replaceString) - [OPT] A string that replaces dots in column names, allowing the column to be used as a reference to another record via an external ID

**Important Notes:**
- When inserting, you cannot specify the ID field
- When updating, the ID field must be present in the CSV file
- When deleting, keep in mind that Salesforce's recycle bin has a storage limit. If more records are deleted than the bin can hold, they will be hard deleted
- When deleting, the CSV file must contain only the ID field

Data Formatting (Null values, Multipick values)
===============

Input data must be formatted according to Salesforce field requirements:

**Empty Values:**
- To send a null value to Salesforce, use `#N/A` in the cell

**Multipick List Fields:**
- Separate multiple values with semicolons
- Example: `Value1;Value2;Value3;`
- Include the trailing semicolon for proper parsing
- For empty cells, use `#N/A` similarly to example above

Output
======

The component processes all input records and outputs tables containing failed records along with the reason
for failure. The table names are constructed as `{OBJECT_NAME}_{LOAD_TYPE}_unsuccessful`, e.g., `Contact_upsert_unsuccessful`

Sample Configuration
=============

```json
{
  "parameters": {
    "#username": "your_username",
    "#password": "your_password",
    "#security_token": "your_security_token",
    "sandbox": false,
    "proxy": {
      "use_proxy": true,
      "proxy_server": "proxy_server_ip e.g. 44.67.126.78",
      "username": "user",
      "#password": "your_password",
      "port": "8080"
    },
    "rows": [
      {
        "object": "Contact",
        "operation": "upsert",
        "upsertField": "ExternalId__c",
        "serialMode": true,
        "fail_on_error": true
      }
    ]
  },
  "action": "run"
}
```

Development
-----------

If required, change local data folder (the `CUSTOM_FOLDER` placeholder) path to your custom path in
the `docker-compose.yml` file:

```yaml
volumes:
  - ./:/code
  - ./CUSTOM_FOLDER:/data
```

Clone this repository, init the workspace and run the component with following command:

```bash
docker-compose build
docker-compose run --rm dev
```

Run the test suite and lint check using this command:

```bash
docker-compose run --rm test
```

Integration
===========

For information about deployment and integration with KBC, please refer to the
[deployment section of developers documentation](https://developers.keboola.com/extend/component/deployment/)




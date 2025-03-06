# Salesforce Writer

The component takes files from the `in/tables` directory and inserts, upserts, updates, or deletes the corresponding records in Salesforce. The input table
must have a header with field names that exist in Salesforce. The ID column is used to identify which record will
be updated. When inserting records, all required fields must be filled in.

The process may fail for some records due to reasons such as missing required fields or exceeding string lenght limits. Any record that fails will
not be inserted, updated, upserted, or deleted. If a failure occurs, the failed rows—along with additional details— will be stored in the
result table: </br>`{OBJECT_NAME}_{LOAD_TYPE}_unsuccessful` (e.g., `Contact_upsert_unsuccessful`)

Everything else will be processed successfully. There is no way to roll back this transaction, so it is essential to carefully
check the logs after each run. 

**Best practices:**
- It is a good practice to include a column with external IDs, allowing you to perform upserts later.
- External IDs also help prevent duplicated records when running inserts multiple times.

**Fail on error option**
If needed, you can set the `fail on error` parameter to `true`. This will cause the job to fail entirely if one or more
records fail to be inserted, updated, upserted, or deleted.

***Note:** The component processes all input records and outputs tables containing failed records along with the reason
for failure. The table names are constructed as `{OBJECT_NAME}_{LOAD_TYPE}_unsuccessful`, e.g., `Contact_upsert_unsuccessful`.*

**Table of contents:**

[TOC]

# Configuration

## Authorization

- **User Name** - (REQ) Your username. When exporting data from a sandbox, don't forget to add `.sandboxname` at the end.
- **Password** - (REQ) Your password.
- **Security Token** - (REQ) Your security token. ***Note:** The security token is different for sandbox environments.*
- **sandbox** - (REQ) Set to `true` when pushing data to a sandbox.
- **Use Proxy** - (OPT) Section for configuring an HTTPS proxy.
  - **Proxy Server** - `STRING` (REQ if "Use Proxy: is selected) HTTPS Proxy Server Address.
  - **Proxy Port** - `STRING` Proxy Server Port.
  - **Proxy Username** - `STRING` Proxy Server Username.
  - **Proxy Password** - `STRING` Proxy Server Password.
  - **Use HTTP proxy for HTTPS** - `BOOL` A hidden configuration option for an HTTP proxy that also handles HTTPS.
- **Print Failed to log** - `BOOL` If set to `true`, the component will log all failed operations into the job log.

### Proxy Configuration Example

```
"proxy": {
  "use_proxy": true,
  "proxy_server": "144.49.99.170",
  "username": "user",
  "#password": "pwdpwd",
  "port": "8080"
}
```

With this configuration, a proxy server with the address `https://user:pwdpwd@144.49.99.170:8080` will be used.

If you enable the **Use HTTP proxy for HTTPS** option (`true`), the proxy `http://user:pwdpwd@144.49.99.170:8080` will be used instead.


## Row Configuration

* object - (REQ) The name of the object on which you want to perform the operation.
* upsertField - Required when performing an upsert operation. If you do not have an external ID, you must either create one, or use separate insert and update operations to achieve the same result.
* operation - (REQ) Specifies the operation to perform. Supported operations: Insert, Upsert, Update, Delete.
* serialMode - Set to `true` if you want to run the import in serial mode.
* Assignment rule ID - The ID of the Lead [Assignment Rule](https://help.salesforce.com/s/articleView?id=sf.customize_leadrules.htm&language=en_US&type=5) you want to apply after import. [How to find it](https://help.salesforce.com/s/articleView?id=000381858&type=1).
* replaceString - A string that replaces dots in column names, allowing the column to be used as a reference to another record via an external ID.
* fail_on_error - If set to `true`, the job will fail if one or more errors occur during execution.
    * If unchecked, the component will continue running despite failures.
    * In both cases, an output table containing unsuccessful records will be saved.
    * The table name format: `{OBJECT_NAME}_{LOAD_TYPE}_unsuccessful`. E.g., `Contact_upsert_unsuccessful`.

**Important notes:**
- When inserting, you cannot specify the ID field.
- When upserting, the `upsertField` parameter is required.
- When updating, the `ID` field must be present in the CSV file.
- When deleting, keep in mind that Salesforce's recycle bin has a storage limit. If more records are deleted than the bin can hold, they
  will be hard deleted. In addition, the CSV file must contain only the ID field.




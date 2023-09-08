# Salesforce writer

The component takes files from in/tables directory and insert/upsert/update/delete appropriate records. The input table
must have a header with field names, which has to exist in Salesforce. ID column is used to identify record which will
be updated. When inserting records all required fields has to be filled in.

The process can fail on any records (due to missing required field or too large string) and this specific record will
not be inserted/updated/upserted/deleted. In case of failure, the failed rows with additional detail will be stored in
result table  `{OBJECT_NAME}_{LOAD_TYPE}_unsuccessful` e.g. `Contact_upsert_unsuccessful`

Everything else will finish with success. There is no way how to rollback this transaction, so you have to carefully
check the log each time. It is also great idea to include a column with external IDs and based on them do upsert later.
External IDs will also save you from duplicated records when running insert several times.

If you need you can set the fail on error parameter true, this will cause the job of a component to fail if 1 or more
records fail to be inserted/updated/upserted/deleted.

**NOTE** The component processes all records on the input and outputs tables containing the failed records with reason
of failure. The table names are constructed as `{OBJECT_NAME}_{LOAD_TYPE}_unsuccessful`
e.g. `Contact_upsert_unsuccessful`

**Table of contents:**

[TOC]

# Configuration

## Authorization

- **User Name** - (REQ) your user name, when exporting data from sandbox don't forget to add .sandboxname at the end
- **Password** - (REQ) your password
- **Security Token** - (REQ) your security token, don't forget it is different for sandbox
- **sandbox** - (REQ) true when you want to push data to sandbox
- **Use Proxy** - (OPT) Section where you can configure https proxy
  - **Proxy Server** - `STRING` (REQ if Use Proxy is selected) HTTPS Proxy Server Address
  - **Proxy Port** - `STRING` Proxy Server Port
  - **Proxy Username** - `STRING` Proxy Server Username
  - **Proxy Password** - `STRING` Proxy Server Password
  - **Use HTTP proxy for HTTPS** - `BOOL` This is a hidden configuration option for a type of HTTP proxy that also handles HTTPS.
- **Print Failed to log** - `BOOL` If set to true, the component will log all failed operations into the job's log.

### Proxy configuration example:

```
"proxy": {
  "use_proxy": true,
  "proxy_server": "144.49.99.170",
  "username": "user",
  "#password": "pwdpwd",
  "port": "8080"
}
```

With this configuration, a proxy server with address `https://user:pwdpwd@144.49.99.170:8080` will be used.

If you set the **Use HTTP proxy for HTTPS** option to `true`, `http://user:pwdpwd@144.49.99.170:8080` will be used.



## Row configuration

* object - (REQ) name of object you wish to perform the operation on
* upsertField - required when the operation is upsert, in case you have no external ID, you must set it up, or use insert and update operations separately to perform the upsert.
* operation - (REQ) specify the operation you wish to do. Insert/Upsert/Update/Delete are supported.
* serialMode - true if you wish to run the import in serial mode.
* Assignment rule ID - ID of Lead [Assignment rule](https://help.salesforce.com/s/articleView?id=sf.customize_leadrules.htm&language=en_US&type=5) you want to run after import. [How to find it](https://help.salesforce.com/s/articleView?id=000381858&type=1).
* replaceString - string to be replaced in column name for dot, so you can use that column as reference to other record
  via external id
* fail_on_error - if you want the job to fail on any error, set this to true and the job will fail if more than 0 errors
  occur during the execution. When unchecked, the component will continue on failure. In both cases an output table with
  unsuccessful records is saved. The table name will be constructed as `{OBJECT_NAME}_{LOAD_TYPE}_unsuccessful`
  e.g. `Contact_upsert_unsuccessful`

- when inserting you cannot specify ID field
- when upserting the upsertField parameter is required
- when updating the ID field in CSV file is required
- when deleting, keep in mind that Salesforce's recycle bin can take less records than you are trying to delete, so they
  will be hard deleted. Also the CSV file must contain only ID field




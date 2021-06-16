# Salesforce writer

The component takes files from in/tables directory and insert/upsert/update/delete appropriate records.
The input table must have a header with field names, which has to exists in Salesforce. 
ID column is used to identify record which will be updated. When inserting records all required fields has to be filled in.

The process can fail on any records (due to missing required field or too large string) and this specific record will not be inserted/update. 
You can specify whether you want the writer to output the errors to a table.
Everything else will finish with success. There is no way how to rollback this transaction, 
so you have to carefully check the log each time. It is also great idea to include a 
column with external IDs and based on them do upsert later. External IDs will also save you from duplicated records when running insert several times.
**Table of contents:**  
  
[TOC]

# Configuration

## Authorization

- **User Name** - (REQ) your user name, when exporting data from sandbox don't forget to add .sandboxname at the end
- **Password** - (REQ) your password
- **Security Token** - (REQ) your security token, don't forget it is different for sandbox
- **sandbox** - (REQ) true when you want to export data from sandbox

## Row configuration
* Object - (REQ) name of object you wish to perform the operation on 
* upsertField - required when the operation is upsert
* operation - (REQ) specify the operation you wish to do. Insert/Upsert/Update/Delete are supported. 
* serialMode - true if you wish to run the import in serial mode. 
* replaceString - string to be replaced in column name for dot, so you can use that column as reference to other record via external id
* output_errors - if you wish to output errors that occurred into an output table

- when inserting you cannot specify ID field
- when upserting the upsertField parameter is required
- when updating the ID field in CSV file is required
- when deleting, keep in mind that Salesforce's recycle bin can take less records than you are trying to delete, so they will be hard deleted. Also the CSV file must contain only ID field




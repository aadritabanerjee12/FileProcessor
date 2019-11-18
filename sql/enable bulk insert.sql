/*
	Create a database Master Key to store credentials
*/
CREATE MASTER KEY ENCRYPTION BY PASSWORD = 'k3\GkGqGu3Tj}=@~';
GO

/*
	Create a crendential to store SAS key.
*/
--DROP  DATABASE SCOPED CREDENTIAL [Storage-Credentials]
CREATE DATABASE SCOPED CREDENTIAL [Storage-Credentials]
WITH IDENTITY = 'SHARED ACCESS SIGNATURE',
SECRET = '<key>'; -- without leading "?"
GO

SELECT * FROM sys.[database_scoped_credentials]
GO


/*
	Create external data source
*/
--DROP EXTERNAL DATA SOURCE [Azure-Storage]
CREATE EXTERNAL DATA SOURCE [Azure-Storage]
WITH 
( 
	TYPE = BLOB_STORAGE,
 	LOCATION = 'https://fileprocessorstorageacc.blob.core.windows.net/import',
 	CREDENTIAL= [Storage-Credentials]
);

SELECT * FROM sys.[external_data_sources]
GO
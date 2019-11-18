# FileProcessor
This is a blob triggered Azure Function that loads csv data to SQL Azure. Each time a file will be saved into the Azure Blob Store’s “import” folder, within a couple of seconds, if the format is the expected one, data will be available in Azure SQL
This function bulk inserts data to a sql azure database. 

# Project architecture
1. Azure function Project : 
2. SQL Azure database : table for storing TOU and LP data. Also contains a stored 
3. Azure blob storage container : blob named 'import'

Blob storage
============================
Created a storage account in Azure and created a blob container named 'import' within that storage account

Azure function
============================
A blob triggered azure function that is invoked when a file is uploaded in the azure storage container
[FunctionName("FileProcessFn")]
  public static void Run([BlobTrigger("import/{name}", Connection = "AzureWebJobsStorage")]Stream blob, string name, TraceWriter log)
  {
      log.Info($"Blob trigger function processed blob: {name}, size: {blob.Length} bytes");

      string azureSQLConnectionString = Environment.GetEnvironmentVariable("AzureSQLConnStr");

      if (!name.EndsWith(".csv"))
      {
          log.Info($"Blob '{name}' doesn't have the .csv extension. Skipping processing.");
          return;
      }

      log.Info($"Blob '{name}' found. Uploading to Azure SQL");

      SqlConnection conn = null;
      try
      {
          conn = new SqlConnection(azureSQLConnectionString);
          conn.Execute("EXEC dbo.BulkLoadFromAzure @sourceFileName", new { @sourceFileName = name }, commandTimeout: 180);
          log.Info($"Blob '{name}' uploaded");
      }
      catch (SqlException se)
      {
          log.Info($"Exception Trapped: {se.Message}");
      }
      finally
      {
          conn?.Close();
      }
  }

Azue SQL database
============================
A connection from Azure SQL Server is set up to the external source (here, the storage account)

CREATE MASTER KEY ENCRYPTION BY PASSWORD = '<password>';
GO

/*
	Create a crendential to store SAS key.
*/
CREATE DATABASE SCOPED CREDENTIAL [Storage-Credentials]
WITH IDENTITY = 'SHARED ACCESS SIGNATURE',
SECRET = '<key>'; 
GO

/*
	Create external data source
*/
CREATE EXTERNAL DATA SOURCE [Azure-Storage]
WITH 
( 
	TYPE = BLOB_STORAGE,
 	LOCATION = '<blob container URL>',
 	CREDENTIAL= [Storage-Credentials]
);

Azure BULK INSERT is used in a stored procedure to load the csv file data to azure sql db table. The filename is sent as an input parameter to the stored procedure
SET @BulkInsSQL = N'
			 BULK INSERT tblLoadProfile_Staging
			 FROM ''' + @sourceFileName + '''
			 WITH ( DATA_SOURCE = ''' + @ExtlDS + ''',
			 Format=''CSV'',
			 FIELDTERMINATOR = '','',
			 FIRSTROW=2
			 );
			 ';

			EXEC (@BulkInsSQL);


			INSERT INTO [tblLoadProfileHeader]([FileName],[LoadedOn])
			SELECT @sourceFileName, GETDATE()

			SET @fid = (SELECT IDENT_CURRENT('tblLoadProfileHeader'))

			INSERT INTO dbo.[tblLoadProfile]
			(
				[FileId],
				[MeterPoint Code],
				[Serial Number],
				[Plant Code],
				[Date/Time],
				[Data Type],
				[Data Value],
				[Units],
				[Status]
			)
			SELECT @fid,
			[MeterPoint Code],
			[Serial Number],
			[Plant Code],
			CONVERT(DATETIME,[Date/Time],103),
			[Data Type],
			[Data Value],
			[Units],
			ISNULL([Status],'')
			FROM [tblLoadProfile_Staging]
      
Data is first loaded into a staging table. If data loading is successful, then an entry is made in a header table and then the data from staging is moved to the main table

The azure function is hosted in azure

# Assumptions
1) File format will be same
2) File name will follow the same pattern as provided
3) Blob storage container name should be ‘import’


# Scope for improvement
1) File validation, data validation(e.g. wrong data type and unit combination, null values etc.) should be handled while loading the data. This will minimize the risk of data related exception in the API
2) while loading data, if an error occurs, email should be sent out to specified people alerting them of the error. Erroneous files are to be moved to an error folder. On successful data load, the file should be archived
3) Azure data factory could be used for data loading from csv files, instead of Stored procedure


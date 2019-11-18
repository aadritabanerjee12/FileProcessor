----------------------------------------------------------
-----------------TABLE CREATION FOR LP--------------------
----------------------------------------------------------


DROP TABLE IF EXISTS dbo.[tblLoadProfile]
GO

DROP TABLE IF EXISTS dbo.[tblLoadProfileHeader]
GO

DROP TABLE IF EXISTS dbo.[tblLoadProfile_Staging]
GO
	
CREATE TABLE dbo.[tblLoadProfile_Staging]
(
	[MeterPoint Code] varchar(100) NULL,
	[Serial Number] varchar(100) NULL,
	[Plant Code] varchar(100) NULL,
	[Date/Time] varchar(100) NULL,
	[Data Type] varchar(100) NULL,
	[Data Value] varchar(100) NULL,
	[Units] varchar(100) NULL,
	[Status] varchar(100) NULL
)
GO

CREATE TABLE dbo.[tblLoadProfileHeader]
(
	[ID] INT IDENTITY NOT NULL PRIMARY KEY,
	[FileName] NVARCHAR(128) UNIQUE NOT NULL,
	[LoadedOn] DATETIME2 
) 
GO


CREATE TABLE dbo.[tblLoadProfile]
(
	[ID] INT IDENTITY NOT NULL PRIMARY KEY,
	[FileId] INT NOT NULL REFERENCES dbo.[tblLoadProfileHeader](Id),
	[MeterPoint Code] varchar(100) NOT NULL,
	[Serial Number] varchar(100) NOT NULL,
	[Plant Code] varchar(100),
	[Date/Time] datetime NOT NULL,
	[Date] date NOT NULL,
	[Data Type] varchar(50) NOT NULL,
	[Data Value] numeric(20,2) NOT NULL,
	[Units] varchar(10) NOT NULL,
	[Status] varchar(100) NOT NULL
)
GO

----------------------------------------------------------
-----------------TABLE CREATION FOR ToU-------------------
----------------------------------------------------------

DROP TABLE IF EXISTS dbo.[tblToU]
GO

DROP TABLE IF EXISTS dbo.[tblToUHeader]
GO

DROP TABLE IF EXISTS dbo.[tblToU_Staging]
GO
	
CREATE TABLE dbo.[tblToU_Staging]
(
	[MeterCode] varchar(100) NULL,
	[Serial] varchar(100) NULL,
	[PlantCode] varchar(100) NULL,
	[DateTime] varchar(100) NULL,
	[Quality] varchar(100) NULL,
	[Stream] varchar(100) NULL,
	[DataType] varchar(100) NOT NULL,
	[Energy] varchar(100) NULL,
	[Units] varchar(100) NULL
	
)
GO

CREATE TABLE dbo.[tblToUHeader]
(
	[ID] INT IDENTITY NOT NULL PRIMARY KEY,
	[FileName] NVARCHAR(128) UNIQUE NOT NULL,
	[LoadedOn] DATETIME2 
) 
GO


CREATE TABLE dbo.[tblToU]
(
	[ID] INT IDENTITY NOT NULL PRIMARY KEY,
	[FileId] INT NOT NULL REFERENCES dbo.[tblToUHeader](Id),
	[MeterCode] varchar(100) NOT NULL,
	[Serial] varchar(100) NOT NULL,
	[PlantCode] varchar(100),
	[DateTime] datetime NOT NULL,
	[Date] date NOT NULL,
	[Quality] varchar(50) NOT NULL,
	[Stream] varchar(100) NOT NULL,
	[DataType] varchar(50) NOT NULL,
	[Energy] numeric(20,2) NULL,
	[Units] varchar(10) NULL
)
GO


----------------------------------------------------------
-----------------------BULK INSERT------------------------
----------------------------------------------------------

CREATE OR ALTER PROCEDURE [dbo].[BulkLoadFromAzure]
@sourceFileName NVARCHAR(100)
AS
	DECLARE @fid INT,@BulkInsSQL NVARCHAR(MAX),@ExtlDS varchar(50)
	DECLARE @fileName NVARCHAR(MAX) = REPLACE(@sourceFileName, '.csv', '');
	SELECT @ExtlDS = 'Azure-Storage'
	
	IF CHARINDEX('LP',@sourceFileName) > 0
		BEGIN 

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

			TRUNCATE TABLE [tblLoadProfile_Staging]

		END
	ELSE
		BEGIN

			SET @BulkInsSQL = N'
			 BULK INSERT tblToU_Staging
			 FROM ''' + @sourceFileName + '''
			 WITH ( DATA_SOURCE = ''' + @ExtlDS + ''',
			 Format=''CSV'',
			 FIELDTERMINATOR = '','',
			 FIRSTROW=2
			 );
			 ';

			EXEC (@BulkInsSQL);


			INSERT INTO [tblToUHeader]([FileName],[LoadedOn])
			SELECT @sourceFileName, GETDATE()

			SET @fid = (SELECT IDENT_CURRENT('tblToUHeader'))

			INSERT INTO dbo.[tblToU]
			(
				[FileId],
				[MeterCode],
				[Serial],
				[PlantCode],
				[DateTime],
				[Quality],
				[Stream],
				[DataType],
				[Energy],
				[Units]
			)
			SELECT @fid,
				[MeterCode],
				[Serial],
				[PlantCode],
				CONVERT(DATETIME,[DateTime],103),
				[Quality],
				[Stream],
				[DataType],
				[Energy],
				[Units]
			FROM [tblToU_Staging]

			TRUNCATE TABLE [tblToU_Staging]

		END
	
		
GO
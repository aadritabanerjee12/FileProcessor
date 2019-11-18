using System;
using System.IO;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Host;
using Microsoft.Extensions.Logging;
using Microsoft.WindowsAzure.Storage.Blob;
using System.Configuration;
using System.Data;
using System.Data.SqlClient;
using Dapper;

namespace FileProcessor
{
    public static class FileProcessFn
    {
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

        
    }
}

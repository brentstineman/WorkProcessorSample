using System;
using System.IO;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Host;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Auth;
using Microsoft.WindowsAzure.Storage.Blob;
using Microsoft.Azure.WebJobs.ServiceBus;

namespace PreprocessorFn
{
    
    public static class Preprocessor
    {
        public static string STORAGE_KEY = Environment.GetEnvironmentVariable("StorageKey");
        public static string STORAGE_ACCOUNT_NAME = Environment.GetEnvironmentVariable("StorageAccountName");
        public static StorageCredentials credentials = new StorageCredentials(
             STORAGE_ACCOUNT_NAME,
             STORAGE_KEY
        );
        public static string MASTER_DATABASE_BLOB = Environment.GetEnvironmentVariable("MasterBlob");
        public static string TARGET_DATABASE_BLOB = Environment.GetEnvironmentVariable("TargetBlob");
        public static string MASTER_FILE = Environment.GetEnvironmentVariable("MasterFile");
        public static string TARGET_FILE = Environment.GetEnvironmentVariable("TargetFile");


        [FunctionName("Preprocessor")]
        public static  void Run([QueueTrigger("jobqueue", Connection = "StorageConnectionString")] string myQueueItem, 
                                     [EventHub("preprocesshub", Connection = "sb://dnadata.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=53l6aB3jaiyHL/lPCPu2CF99ytfM0EgL4fbVC1HKhV8=")]out string returnValue,
                                     TraceWriter log)
        {
            // Extract ob Details message from job queue
            log.Info($"C# Queue trigger function Processed job\n : {myQueueItem}");

            // Extract offenders list
            ReadFromBlob(MASTER_DATABASE_BLOB, MASTER_FILE, log);

            // Extract suspect list
            ReadFromBlob(TARGET_DATABASE_BLOB,TARGET_FILE,log);


            // Batch to Event Hub
            returnValue = "testvalue";

        }

        public static async void ReadFromBlob(string containerName, string fileName, TraceWriter log)
        {
            CloudStorageAccount account = new CloudStorageAccount(credentials, true);
            CloudBlobClient client = new CloudBlobClient(account.BlobStorageUri, account.Credentials);
            CloudBlobContainer container = client.GetContainerReference(containerName);

            CloudBlob blob = container.GetBlobReference(fileName);
            using (var stream = await blob.OpenReadAsync())
            {
                using (StreamReader reader = new StreamReader(stream))
                {
                    while (!reader.EndOfStream)
                    {
                        log.Info(reader.ReadLine());
                    }
                }
            }
        }
    }
}

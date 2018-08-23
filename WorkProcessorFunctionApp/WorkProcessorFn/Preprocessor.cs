using System;
using System.IO;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Host;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Auth;
using Microsoft.WindowsAzure.Storage.Blob;
using System.Threading.Tasks;
using System.Collections.Generic;
using Microsoft.Azure.EventHubs;
using System.Text;

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
        public static int CHUNK_SIZE = Convert.ToInt32(Environment.GetEnvironmentVariable("ChunkSize"));
        public static string EVENT_HUB_CONNECTION_STRING = Environment.GetEnvironmentVariable("EventHubConnectionString");
        public static string EVENT_HUB_NAME = Environment.GetEnvironmentVariable("EventHubName");
        public static dynamic connectionStringBuilder = new EventHubsConnectionStringBuilder(EVENT_HUB_CONNECTION_STRING)
        {
            EntityPath = EVENT_HUB_NAME
        };
        public static EventHubClient eventHubClient = EventHubClient.CreateFromConnectionString(connectionStringBuilder.ToString());


        [FunctionName("Preprocessor")]
        public static async Task Run([QueueTrigger("jobqueue", Connection = "StorageConnectionString")] string myQueueItem, 
                                             TraceWriter log)
        {
            // Extract job Details message from job queue
            log.Info($"C# Queue trigger function Processed job\n : {myQueueItem}");
            try
            {
                // Extract master list
                String masterList = ReadFromBlob(MASTER_DATABASE_BLOB, MASTER_FILE, log).Result;
                

                int count = 1; int batchNr = 1;
                List<String> batchedData = new List<String>();
                log.Info("Batching data...");
                foreach (var item in masterList)
                { 
                    if(count < CHUNK_SIZE)
                    {
                        batchedData.Add(item.ToString());
                        count++;
                    }
                    else
                    {
                        
                        count = 1; 
                        await SendMessagesToEventHub(batchedData,batchNr,log);
                        batchNr += 1;
                        batchedData = new List<String>();
                    }
                }
                // Extract target list
                //String targetList = ReadFromBlob(TARGET_DATABASE_BLOB, TARGET_FILE, log).Result;

            }catch(Exception e)
            {
                log.Error("Failed while reading from blob...", e);
            }
           
 
        }

        private static async Task SendMessagesToEventHub(List<String> messages,int batchNumber,TraceWriter log)
        {
            log.Info($"Sending batch: {batchNumber}");
            await eventHubClient.SendAsync(new EventData(Encoding.UTF8.GetBytes(messages.ToString())));
            await eventHubClient.CloseAsync();
            await Task.Delay(10);
         }

        public static async Task<String> ReadFromBlob(string containerName, string fileName, TraceWriter log)
        {
            CloudStorageAccount account = new CloudStorageAccount(credentials, true);
            CloudBlobClient BlobClient = new CloudBlobClient(account.BlobStorageUri, account.Credentials);
            var container = BlobClient.GetContainerReference(containerName);
            var blob = container.GetBlockBlobReference(fileName);

            using (var stream = new MemoryStream(blob.StreamWriteSizeInBytes))
            {
                // get the image that was uploaded by the client
                return await blob.DownloadTextAsync();
                
            }
            
        }
    }
}

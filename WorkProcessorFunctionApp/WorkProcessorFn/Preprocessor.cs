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
using System.Text.RegularExpressions;
using Newtonsoft.Json;

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
        public static string EVENT_HUB_CONNECTION_STRING = Environment.GetEnvironmentVariable("WorkItemEH-ConnectionString");
        public static string EVENT_HUB_NAME = Environment.GetEnvironmentVariable("WorkItemEH-Name");
        public static dynamic connectionStringBuilder = new EventHubsConnectionStringBuilder(EVENT_HUB_CONNECTION_STRING)
        {
            EntityPath = EVENT_HUB_NAME
        };
        public static EventHubClient eventHubClient = EventHubClient.CreateFromConnectionString(connectionStringBuilder.ToString());
        public static Boolean isEndOfFile = false;

        [FunctionName("Preprocessor")]
        public static async Task Run([QueueTrigger("jobqueue", Connection = "StorageConnectionString")] string myQueueItem, 
                                             TraceWriter log)
        {
            // Extract job Details message from job queue
            log.Info($"C# Queue trigger function Processed job\n : {myQueueItem}");
            try
            {
                isEndOfFile = false;
                IEnumerable<List<String>> batchdata;
                CloudStorageAccount account = new CloudStorageAccount(credentials, true);
                CloudBlobClient BlobClient = new CloudBlobClient(account.BlobStorageUri, account.Credentials);
                var container = BlobClient.GetContainerReference(MASTER_DATABASE_BLOB);
                var blob = container.GetBlockBlobReference(MASTER_FILE);
                var stream = blob.OpenReadAsync();
                StreamReader reader = new StreamReader(stream.Result);
                int batchNr = 1;
                do
                {
                    // Extract master list
                    List<String> newBatch = new List<String>();
                    batchdata = ReadFromBlob(blob,reader,1,newBatch, log);
                    log.Info($"Sending batch {batchNr}");
                    foreach (List<String> item in batchdata)
                    {
                        log.Info($"Sending {item.Count} items");
                        //log.Info("Items sent...");
                        await SendMessagesToEventHub(item, batchNr, log);
                        batchNr += 1;
                    }
                } while (!isEndOfFile);
                log.Info($"Sent all messages");

                await eventHubClient.CloseAsync();

                // Extract target lists
                //String targetList = ReadFromBlob(TARGET_DATABASE_BLOB, TARGET_FILE, log).Result;

            }
            catch(Exception e)
            {
                log.Error("Failed while reading from blob...", e);
            }
           
 
        }

        private static async Task SendMessagesToEventHub(List<String> messages,int batchNumber,TraceWriter log)
        {
            log.Info($"Sending batch: {batchNumber}");
            await eventHubClient.SendAsync(new EventData(Encoding.UTF8.GetBytes(messages.ToString())));
            //await eventHubClient.CloseAsync();
            await Task.Delay(10);
         }

        public static  IEnumerable<List<String>> ReadFromBlob(dynamic blob, TextReader reader, int count, List<String> batch,TraceWriter log)
        {

            //using (StreamReader reader = new StreamReader(stream.Result))
            //{
                String line;
                while ((line = reader.ReadLine()) != null)
                {
                    if (count <= CHUNK_SIZE)
                    {
                        batch.Add(line);
                        count++;
                    }
                    else
                    {
                        yield return batch;
                    }
                }
                isEndOfFile = true;
            
           // }

        }
    }
}

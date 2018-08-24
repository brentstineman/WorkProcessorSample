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
using WorkProcessorFn;

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
        public static StreamReader reader;

        [FunctionName("Preprocessor")]
        public static async Task Run([QueueTrigger("jobqueue", Connection = "StorageConnectionString")] string myQueueItem, 
                                             TraceWriter log)
        {
            // Extract job Details message from job queue
            log.Info($"C# Queue trigger function Processed job\n : {myQueueItem}");
            try
            {
                var watch = System.Diagnostics.Stopwatch.StartNew();
                log.Info("Starting batching...");
                isEndOfFile = false;

                // Read master database
                CloudStorageAccount account = new CloudStorageAccount(credentials, true);
                CloudBlobClient BlobClient = new CloudBlobClient(account.BlobStorageUri, account.Credentials);
                var container = BlobClient.GetContainerReference(MASTER_DATABASE_BLOB);
                var blob = container.GetBlockBlobReference(MASTER_FILE);
                var stream = blob.OpenReadAsync();
                reader = new StreamReader(stream.Result);
                int batchNr = 1;
                do
                {
                    List<OffenderObject> newBatch = new List<OffenderObject>();
                    IEnumerator<String> en = reader.Lines().GetEnumerator();
                    for (int i = 0; i < CHUNK_SIZE; i++)
                    {
                        if (en.MoveNext())
                        {
                            String line = en.Current;
                            OffenderObject offenderObj = JsonConvert.DeserializeObject<OffenderObject>(line);
                            newBatch.Add(offenderObj);
                        }
                    }

                    // Creating event data object
                    WorkItemObject workItemObject = new WorkItemObject()
                    {
                        SchemaVersion = "1.0",
                        JobDetails = Guid.NewGuid().ToString(),
                        SuspectDetails = new SuspectObject()
                        {
                            Specimen_ID = "Specimen1"
                        },
                        OffenderList = newBatch
                    };

                    WorkItemObject workItemObject2 = new WorkItemObject()
                    {
                        SchemaVersion = "1.0",
                        JobDetails = Guid.NewGuid().ToString(),
                        SuspectDetails = new SuspectObject()
                        {
                            Specimen_ID = "Specimen2"
                        },
                        OffenderList = newBatch
                    };

                    List<string> finalBatch = new List<string>();
                    finalBatch.Add(workItemObject.ToString());
                    finalBatch.Add(workItemObject2.ToString());

                    await SendMessagesToEventHub(finalBatch, batchNr, log);
                    batchNr += 1;
                } while (!isEndOfFile);

                await eventHubClient.CloseAsync();
                watch.Stop();
                var elapsedMs = watch.ElapsedMilliseconds;
                log.Info($"Time elapsed: {elapsedMs.ToString()}");
            }
            catch(Exception e)
            {
                log.Error("Failed while sending pre processed data", e);
            }
           
 
        }

        private static async Task SendMessagesToEventHub(List<String> messages,int batchNumber,TraceWriter log)
        {
            await eventHubClient.SendAsync(new EventData(Encoding.UTF8.GetBytes(messages.ToString())));
            await Task.Delay(10);
         }

    
        public static IEnumerable<string> Lines(this TextReader reader)
        {
            string line;
            while ((line = reader.ReadLine()) != null)
            {
                yield return line;
            }
            isEndOfFile = true;
        }

    }
}

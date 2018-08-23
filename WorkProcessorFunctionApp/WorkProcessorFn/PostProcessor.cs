using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Host;
using Microsoft.Azure.WebJobs.ServiceBus;
using Microsoft.Extensions.Logging;
using Microsoft.Azure.WebJobs.Extensions.CosmosDB;
using System;
using Newtonsoft.Json;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;

namespace WorkProcessorFn
{
    public static class PostProcessor
    {
        public static int threshold = Convert.ToInt32(Environment.GetEnvironmentVariable("RESULT_THRESHOLD"));

        [FunctionName("PostProcessor")]
        public static async void Run([EventHubTrigger("%ResultStreamEventHubName%", Connection = "EventHubConnectionAppSetting")]string[] eventHubMessages,
            [CosmosDB(
                databaseName: "resultDataDB",
                collectionName: "resultCollection",
                ConnectionStringSetting = "CosmosDBConnection")]
                IAsyncCollector<OffenderResult> finalResult,
            ILogger log)
        {
            // Final result object
            List<OffenderResult> finalResultObject = new List<OffenderResult>();

            // Each message needs to compare thresholds
            foreach (var message in eventHubMessages)
            {
                log.LogInformation($"C# Event Hub trigger function processed a message: {message}");
                ResultStreamObject resultObject = JsonConvert.DeserializeObject<ResultStreamObject>(message);

                // For every Offender Result item check the threshold
                foreach (var offenderResult in resultObject.ResultList)
                {
                    if((offenderResult.PI_freq1 > threshold) || 
                        (offenderResult.PI_freq2 > threshold) ||
                        (offenderResult.PI_freq3 > threshold) ||
                        (offenderResult.SIB_freq1 > threshold) ||
                        (offenderResult.SIB_freq2 > threshold) ||
                        (offenderResult.SIB_freq3 > threshold) ||
                        (offenderResult.SIB_Local > threshold))
                    {
                        finalResultObject.Add(offenderResult);
                        await finalResult.AddAsync(offenderResult);
                    }
                }
            }

            var outputString = JsonConvert.SerializeObject(finalResultObject);
            try
            {
                await finalResult.FlushAsync();
            }
            catch (Exception ex)
            {
                log.LogError(ex.Message);
                //await Task.Delay(5000);
                // do something else with it so its not lost
            }

        }
    }
}

using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Host;
using Microsoft.Azure.WebJobs.ServiceBus;
using Microsoft.Extensions.Logging;
using System;

namespace WorkProcessorFn
{
    public static class PostProcessor
    {
        // Getting the hubName from Environment does not work!
        //public static string hubName = Environment.GetEnvironmentVariable("ResultStreamEventHubName").ToString();
        [FunctionName("PostProcessor")]
        public static void Run([EventHubTrigger("resultstreamhub", Connection = "EventHubConnectionAppSetting")]string[] eventHubMessages, ILogger log)
        {
            // Each message needs to compare thresholds
            foreach (var message in eventHubMessages)
            {
                log.LogInformation($"C# Event Hub trigger function processed a message: {message}");
                
            }
        }
    }
}

using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Host;
using Microsoft.Azure.WebJobs.ServiceBus;
using Microsoft.Extensions.Logging;
using System;
using Newtonsoft.Json;
using System.Collections.Generic;
using System.IO;

namespace WorkProcessorFn
{
    public static class PostProcessor
    {
        // Getting the hubName from Environment does not work!
        //public static string hubName = Environment.GetEnvironmentVariable("ResultStreamEventHubName").ToString();

        public static int threshold = Convert.ToInt32(Environment.GetEnvironmentVariable("RESULT_THRESHOLD"));

        [FunctionName("PostProcessor")]
        public static void Run([EventHubTrigger("resultstreamhub", Connection = "EventHubConnectionAppSetting")]string[] eventHubMessages,
            [Blob("final-result/{name}", FileAccess.Write, Connection = "StorageConnectionAppSetting")] Stream finalResult,
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
                    }
                }
            }

            var outputString = JsonConvert.SerializeObject(finalResultObject);

            // Put final result in blob storage
            using (var stream = GenerateStreamFromString(outputString))
            {
                finalResult = stream;
            }

        }

        public static Stream GenerateStreamFromString(string s)
        {
            var stream = new MemoryStream();
            var writer = new StreamWriter(stream);
            writer.Write(s);
            writer.Flush();
            stream.Position = 0;
            return stream;
        }
    }
}

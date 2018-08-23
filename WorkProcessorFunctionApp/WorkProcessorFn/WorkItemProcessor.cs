using Microsoft.Azure.EventHubs;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Host;
using Microsoft.Azure.WebJobs.ServiceBus;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Security.Cryptography;
using System.Text;

namespace WorkProcessorFn
{
    public static class WorkItemProcessor
    {
        [FunctionName("WorkItemProcessor")]
        public static void Run([EventHubTrigger("%WorkItemEH-Name%", Connection = "WorkItemEH-ConnectionString")] EventData[] myEventHubMessages,
            [EventHub("%ResultsEH-Name%", Connection = "ResultsEH-ConnectionString")] IAsyncCollector<EventData> outputEventHubMessages,
            ILogger log)
        {
            MD5 md5 = MD5.Create();

            // process messages
            foreach (EventData message in myEventHubMessages)
            {
                string messagePayload = Encoding.UTF8.GetString(message.Body.Array);

                // process each message
                WorkItemObject myEvent = JsonConvert.DeserializeObject<WorkItemObject>(messagePayload);

                try
                {
                    ResultStreamObject result = new ResultStreamObject()
                    {
                        SchemaVersion = myEvent.SchemaVersion,
                        JobDetails = myEvent.JobDetails,
                        // modify the results here
                        ResultList = new List<OffenderResult>()
                    };

                    // simulated caluclation on payload
                    Random rNum = new Random();
                    
                    foreach (var offender in myEvent.OffenderList)
                    {
                        OffenderResult newResult = new OffenderResult()
                        {
                            Forensic_ID = myEvent.SuspectDetails.Specimen_ID,
                            Offender_ID = offender.Specimen_ID,
                            PI_freq1 = rNum.Next(),
                            PI_freq2 = rNum.Next(),
                            PI_freq3 = rNum.Next(),
                            SIB_freq1 = rNum.Next(),
                            SIB_freq2 = rNum.Next(),
                            SIB_freq3 = rNum.Next(),
                            SIB_Local = rNum.Next()
                        };
                        result.ResultList.Add(newResult);
                    }
                    // craft outbound event
                    EventData outputEvent = new EventData(Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(result)));

                    outputEventHubMessages.AddAsync(outputEvent).Wait();
                }
                catch (Exception ex)
                {
                    log.LogError(ex.Message);
                    // do something else with it so its not lost
                }
            }

            // flush the event hub output, we're doing this and not letting the system so we can do error handling
            // note that output batch must not exceed maximum Event Hub batch size
            try
            {
                outputEventHubMessages.FlushAsync().Wait();
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

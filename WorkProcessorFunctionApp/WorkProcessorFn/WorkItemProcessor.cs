using Microsoft.Azure.EventHubs;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Host;
using Microsoft.Azure.WebJobs.ServiceBus;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using System;
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
            // process messages
            foreach (EventData message in myEventHubMessages)
            {
                string messagePayload = Encoding.UTF8.GetString(message.Body.Array);

                // process each message
                //var myEvent = JsonConvert.DeserializeObject(messagePayload);

                try
                {
                    // modify message here
                    outputEventHubMessages.AddAsync(message).Wait();
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

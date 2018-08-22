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
        public static async void Run([EventHubTrigger("samples-workitems", Connection = "")] EventData[] myEventHubMessages,
            [EventHub("%EventHub%", Connection = "connectionEventHub")] IAsyncCollector<EventData> outputEventHubMessages,
            ILogger log)
        {
            // process messages
            foreach (EventData message in myEventHubMessages)
            {
                string messagePaylog = Encoding.UTF8.GetString(message.Body.Array);
                if (!messagePaylog.StartsWith("Message"))
                {
                    // process each message
                    var myEvent = JsonConvert.DeserializeObject(messagePaylog);

                    try
                    {
                        // modify message here
                        await outputEventHubMessages.AddAsync(message);
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
                    await outputEventHubMessages.FlushAsync();
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
}

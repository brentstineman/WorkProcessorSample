using Microsoft.Azure.EventHubs;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Host;
using Microsoft.Azure.WebJobs.ServiceBus;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using System.Text;

namespace WorkProcessorFn
{
    public static class WorkItemProcessor
    {
        [FunctionName("WorkItemProcessor")]
        [return: EventHub("outputEventHubMessage", Connection = "EventHubConnectionAppSetting")]
        public static void Run([EventHubTrigger("samples-workitems", Connection = "")] EventData[] myEventHubMessages, ILogger log)
        {
            // process messages
            foreach (EventData message in myEventHubMessages)
            {
                string messagePaylog = Encoding.UTF8.GetString(message.Body.Array);
                if (!messagePaylog.StartsWith("Message"))
                {
                    // process each message
                    var myEvent = JsonConvert.DeserializeObject(messagePaylog);

                    // do something with it
                }

            }

            // gather all results

            // write to output binding
        }
    }
}

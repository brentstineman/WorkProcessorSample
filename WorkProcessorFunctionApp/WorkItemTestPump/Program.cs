using System;
using System.Configuration;
using System.Text;
using Microsoft.Azure.EventHubs;

namespace WorkItemTestPump
{
    class Program
    {

        private static EventHubClient eventHubClient;

        static void Main(string[] args)
        {
            var connectionStringBuilder = new EventHubsConnectionStringBuilder("<connectionstring>")
            {
                EntityPath = "<hubname>"
            };
            eventHubClient = EventHubClient.CreateFromConnectionString(connectionStringBuilder.ToString());

            int i = 0;
            while(true)
            {
                try
                {
                    var message = $"Message {i++}";
                    Console.WriteLine($"Sending message: {message}");
                    eventHubClient.SendAsync(new EventData(Encoding.UTF8.GetBytes(message))).Wait();
                }
                catch (Exception exception)
                {
                    Console.WriteLine($"{DateTime.Now} > Exception: {exception.Message}");
                }

            }

            eventHubClient.CloseAsync().Wait();

        }

    }
}

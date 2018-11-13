using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.WindowsAzure.Storage.Queue;
using System;

namespace BackgroundQueueProcessor
{
    class Program
    {
        static void Main(string[] args)
        {
            //connect to storage
            //grab a message
            //do the business
            //handle bad messages

            var host = new HostBuilder()
                    .ConfigureLogging(l => l.AddConsole())
                    .ConfigureAppConfiguration(c => c.AddEnvironmentVariables())
                    .ConfigureServices(s=> s.AddHostedService<QueueWorker>())
                    .Build();

            host.Run();
        }
    }

    public class QueueHandler
    {
        [QueueHandler("workerqueue")]
        public void HandleMessage(CloudQueueMessage message)
        {

        }
    }
}

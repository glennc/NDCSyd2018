using System;
using System.Collections.Generic;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Queue;

namespace BackgroundQueueProcessor
{
    internal class QueueWorker : BackgroundService
    {
        private IConfiguration _config;
        private ILogger<QueueWorker> _logger;
        private IServiceProvider _sp;

        public QueueWorker(IConfiguration config,
                            ILogger<QueueWorker> logger,
                            IServiceProvider sp)
        {
            _config = config;
            _logger = logger;
            _sp = sp;
        }
        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {

            var storageAccount = CloudStorageAccount.Parse(_config["connectionStrings:Storage"]);

            // Create the queue client.
            CloudQueueClient queueClient = storageAccount.CreateCloudQueueClient();

            BindQueueMethods(queueClient);

            if (!int.TryParse(_config["LoopDelay"], out int loopDelay))
            {
                loopDelay = 1000;
            }

            while (!stoppingToken.IsCancellationRequested)
            {
                foreach (var binding in _queueBindings)
                {
                    var message = await binding.Queue.GetMessageAsync();

                    //If there are no messages then sleep for a bit and wait.
                    //One common pattern here is to do exponential sleep time
                    //up to a max. If overnight there is no work then you want
                    //to sleep as much as possible, for example.
                    if (message == null)
                    {
                        await Task.Delay(loopDelay);
                        continue;
                    }

                    if (message.DequeueCount > 3)
                    {
                        //TODO: here you would transfer the message to table/blob storage or some other queue
                        //for later recovery and analysis. Kind of a manual dead letter queue.
                        //This would handle bad messages.
                        _logger.LogCritical("Giving up on processing {messageId} : {messageContent}", message.Id, message.AsString);
                        await binding.Queue.DeleteMessageAsync(message);
                        continue;
                    }

                    try
                    {
                        _logger.LogInformation("Processing data {data}", message.AsString);

                        using (var scope = _sp.CreateScope())
                        {
                            var handler = ActivatorUtilities.CreateInstance(scope.ServiceProvider,
                                                                            binding.Method.DeclaringType);

                            binding.Method.Invoke(handler, new object[] { message });
                        }

                            await binding.Queue.DeleteMessageAsync(message);
                        _logger.LogInformation("Processing data complete.");
                    }
                    catch (Exception ex)
                    {
                        _logger.LogError(ex, "unknown error processing message {messageId}", message.Id);
                    }
                }
            }
        }


        internal class QueueBinding
        {
            public string QueueName { get; set; }
            public MethodInfo Method { get; set; }
            public CloudQueue Queue { get; set; }
        }

        List<QueueBinding> _queueBindings = new List<QueueBinding>();

        private void BindQueueMethods(CloudQueueClient queueClient)
        {
            foreach (var type in Assembly.GetExecutingAssembly().GetTypes())
            {
                foreach (var method in type.GetMethods())
                {
                    var attr = method.GetCustomAttribute<QueueHandlerAttribute>();
                    if (attr != null)
                    {
                        _queueBindings.Add(new QueueBinding
                        {
                            QueueName = attr.QueueName,
                            Method = method,
                            Queue = queueClient.GetQueueReference(attr.QueueName)
                        });
                    }
                }
            }
        }
    }
}
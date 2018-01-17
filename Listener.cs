using System;
using System.Text;
using System.Threading;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using RabbitMQ.Client.Logging;
using Steeltoe.CloudFoundry.Connector.Rabbit;
using Steeltoe.Extensions.Configuration;

class Worker
{

    public static void Main()
    {
        RabbitMqConsoleEventListener loggingEventSource = new RabbitMqConsoleEventListener();

        var msgsReceived = 0;

        // Set default interval for heartbeats
        ushort heartbeatInterval = 20;
        string heartbeatIntervalStr = Environment.GetEnvironmentVariable("HEARTBEAT_INTERVAL_SEC");
        if (heartbeatIntervalStr == null)
        {
            Console.WriteLine("HEARTBEAT_INTERVAL_SEC environment variable not defined, using default.");
        }
        else
        {
            heartbeatInterval = Convert.ToUInt16(heartbeatIntervalStr);
        }

        Console.WriteLine("Setting heartbeat interval to {0} s", heartbeatInterval);

        var factory = new ConnectionFactory() { HostName = "localhost" };
        if (Environment.GetEnvironmentVariable("VCAP_SERVICES") != null) {
            // Running on PCF
            IServiceCollection services = new ServiceCollection();
            var config = new ConfigurationBuilder()
                .AddEnvironmentVariables()
                .AddCloudFoundry()
                .Build();
            services.AddRabbitConnection(config);
            factory = services.BuildServiceProvider().GetService<ConnectionFactory>();
        }

        // No need to explicitly set this value, default is already true
        // factory.AutomaticRecoveryEnabled = true;

        // Since we have a durable queue anyways, there should be no need to recreate it on a connection failure.
        // Otherwise this currently can result in exceptions (if the durable queue home node is down), that
        // hangs the RMQ client.
        factory.TopologyRecoveryEnabled = false;

        // Reduce the heartbeat interval so that bad connections are detected sooner than the default of 60s
        factory.RequestedHeartbeat = heartbeatInterval;

        using (var connection = factory.CreateConnection())
        using (var channel = connection.CreateModel())
        {
            channel.QueueDeclare(queue: "task_queue", durable: true, exclusive: false, autoDelete: false, arguments: null);

            Console.WriteLine(" [*] Waiting for messages.");

            var consumer = new EventingBasicConsumer(channel);
            consumer.Received += (model, ea) =>
            {
                msgsReceived++;
                Console.WriteLine("Received {0} messages", msgsReceived);

            };
            consumer.Shutdown += (model, ea) =>
            {
                Console.WriteLine("*** Entering Shutdown ***");
            };

            channel.BasicConsume(queue: "task_queue", autoAck: true, consumer: consumer);
            channel.CallbackException += (sender, e) => Console.WriteLine(e.Exception);
            channel.ModelShutdown += (sender, e) => Console.WriteLine($"Channel closed: {e.Cause?.ToString()}");

            while(true) {
                Thread.Sleep(1000);
            }
        }
    }
}

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

    private static ConnectionFactory factory;
    private static IConnection connection;
    private static IModel channel;
    private static EventingBasicConsumer consumer;
    private static bool topologyRecoveryEnabled=true;
    private static int msgsReceived = 0;

    private const string taskQueueName = "task_queue";

    public static void Main()
    {
        RabbitMqConsoleEventListener loggingEventSource = new RabbitMqConsoleEventListener();

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

        int port = 5672;
        string portStr = Environment.GetEnvironmentVariable("RABBITMQ_NODE_PORT");
        if (portStr != null) {
            port = Convert.ToInt32(portStr);
        }

        factory = new ConnectionFactory() { HostName = "localhost", Port = port };
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
        String topologyRecoveryEnabledStr = Environment.GetEnvironmentVariable("TOPOLOGY_RECOVERY_ENABLED");
        if (topologyRecoveryEnabledStr != null)
        {
            if (topologyRecoveryEnabledStr.ToUpper().Equals("FALSE"))
            {
                Console.WriteLine("Disabling topology recovery.");
                factory.TopologyRecoveryEnabled = false;
                topologyRecoveryEnabled = false;
            }
        }

        // Reduce the heartbeat interval so that bad connections are detected sooner than the default of 60s
        factory.RequestedHeartbeat = heartbeatInterval;
        using (connection = factory.CreateConnection()) {

            // An autorecovery occurred
            connection.RecoverySucceeded += (sender, e) =>
            {
                // If topology recovery is not enabled then we'll need to restart the consumer
                if (!topologyRecoveryEnabled)
                {
                    CreateConsumer(taskQueueName);
                }
            };

            using (channel = connection.CreateModel())
            {
                channel.QueueDeclare(queue: taskQueueName, durable: true, exclusive: false, autoDelete: false, arguments: null);

                Console.WriteLine(" [*] Waiting for messages.");

                CreateConsumer(taskQueueName);
                channel.CallbackException += (sender, e) => Console.WriteLine(e.Exception);
                channel.ModelShutdown += (sender, e) => Console.WriteLine($"Channel closed: {e.Cause?.ToString()}");

                while (true)
                {
                    Thread.Sleep(1000);
                }
            }

        };
    }


    protected static void CreateConsumer(string queueName) {

        consumer = new EventingBasicConsumer(channel);

        consumer.Received += (model, ea) =>
        {
            msgsReceived++;
            Console.WriteLine("Received {0} messages", msgsReceived);
        };

        consumer.Shutdown += (model, ea) =>
        {
            Console.WriteLine("*** Entering Shutdown ***");
        };

        consumer.ConsumerCancelled += (model, ea) =>
        {
            Console.WriteLine("*** Received ConsumerCancelled ***");
        };

        channel.BasicConsume(queue: queueName, autoAck: true, consumer: consumer);
    }
}

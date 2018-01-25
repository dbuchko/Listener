using System;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using RabbitMQ.Client.Logging;

class Worker
{
    private static ConnectionFactory factory;
    private static IConnection connection;
    private static IModel channel;
    private static EventingBasicConsumer consumer;
    private static bool enableTopologyRecovery = true;
    private static int msgsReceived = 0;

    private const string taskQueueName = "task_queue";

    public static void Main()
    {
        RabbitMqConsoleEventListener loggingEventSource = new RabbitMqConsoleEventListener();

        // Set default interval for heartbeats
        // Reduce the heartbeat interval so that bad connections are detected sooner than the default of 60s
        ushort heartbeatInterval = 20;
        if (ushort.TryParse(Environment.GetEnvironmentVariable("HEARTBEAT_INTERVAL_SEC"), out heartbeatInterval))
        {
            Console.WriteLine("Setting heartbeat interval from HEARTBEAT_INTERVAL_SEC");
        }
        else
        {
            Console.WriteLine("HEARTBEAT_INTERVAL_SEC environment variable not defined, using default.");
            heartbeatInterval = 20;
        }
        Console.WriteLine("Setting heartbeat interval to {0} s", heartbeatInterval);

        int port = 5672;
        if (int.TryParse(Environment.GetEnvironmentVariable("RABBITMQ_NODE_PORT"), out port))
        {
            Console.WriteLine("Setting port from RABBITMQ_NODE_PORT");
        }
        else
        {
            port = 5672;
        }
        Console.WriteLine("Port: {0}", port);

        factory = new ConnectionFactory()
        {
            HostName = "shostakovich",
            UserName = "guest",
            Password = "guest",
            Port = port,
            AutomaticRecoveryEnabled = true,
            RequestedHeartbeat = heartbeatInterval
        };

        // Since we have a durable queue anyways, there should be no need to recreate it on a connection failure.
        // Otherwise this currently can result in exceptions (if the durable queue home node is down), that
        // hangs the RMQ client.
        if (bool.TryParse(Environment.GetEnvironmentVariable("TOPOLOGY_RECOVERY_ENABLED"), out enableTopologyRecovery))
        {
            Console.WriteLine("Setting topology recovery from TOPOLOGY_RECOVERY_ENABLED");
        }
        else
        {
            enableTopologyRecovery = true;
        }
        factory.TopologyRecoveryEnabled = enableTopologyRecovery;
        Console.WriteLine("Topology recovery enabled: {0}", factory.TopologyRecoveryEnabled);

        using (connection = factory.CreateConnection())
        {
            // An autorecovery occurred
            connection.RecoverySucceeded += (sender, e) =>
            {
                // If topology recovery is not enabled then we'll need to restart the consumer
                if (consumer.IsRunning == false)
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

                Console.WriteLine("Hit enter key to exit!");
                Console.ReadLine();
            }
        }
    }

    private static void CreateConsumer(string queueName)
    {
        if (consumer != null)
        {
            Console.WriteLine("Cancelling consumer with tag: {0}", consumer.ConsumerTag);
            consumer.Model.BasicCancel(consumer.ConsumerTag);
            consumer = null;
        }

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
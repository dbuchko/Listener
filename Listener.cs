using System;
using System.Threading;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using RabbitMQ.Client.Logging;

class Worker
{
    private static ConnectionFactory factory;
    private static IConnection connection;
    private static IModel channel;
    private static EventingBasicConsumer consumer;
    private static readonly ReaderWriterLockSlim consumerLock = new ReaderWriterLockSlim();
    private static readonly ManualResetEventSlim consumerRegistered = new ManualResetEventSlim();
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

        int port = 5670;
        if (int.TryParse(Environment.GetEnvironmentVariable("RABBITMQ_NODE_PORT"), out port))
        {
            Console.WriteLine("Setting port from RABBITMQ_NODE_PORT");
        }
        else
        {
            port = 5670;
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
                /*
                 * Note: if consumer.IsRunning is false here, it could just be because the Registered event
                 * has not yet fired for the recovered consumer
                 */
                Console.WriteLine("RecoverySucceeded, now waiting on Registered event - ThreadId: {0}",
                    Thread.CurrentThread.ManagedThreadId);
                if (consumerRegistered.Wait(factory.ContinuationTimeout))
                {
                    Console.WriteLine("RecoverySucceeded, consumer restarted - Consumer tag: {0} IsRunning: {1}",
                        consumer.ConsumerTag, consumer.IsRunning);
                }
                else
                {
                    Console.WriteLine("RecoverySucceeded, consumer did not restart! - Consumer tag: {0}",
                        consumer.ConsumerTag);
                    CreateConsumer(taskQueueName);
                }
            };

            using (channel = connection.CreateModel())
            {
                channel.CallbackException += (sender, e) => Console.WriteLine(e.Exception);
                channel.ModelShutdown += (sender, e) => Console.WriteLine($"Channel closed: {e.Cause?.ToString()}");

                channel.QueueDeclare(queue: taskQueueName,
                    durable: true, exclusive: false,
                    autoDelete: false, arguments: null);

                Console.WriteLine("Creating initial consumer and waiting for Registered...");
                CreateConsumer(taskQueueName);
                if (consumerRegistered.Wait(factory.ContinuationTimeout))
                {
                    Console.WriteLine("Initial consumer started - Consumer tag: {0} IsRunning: {1}",
                        consumer.ConsumerTag, consumer.IsRunning);
                    while (true)
                    {
                        consumerLock.EnterReadLock();
                        try
                        {
                            Console.WriteLine("Current consumer tag: {0} IsRunning: {1} ThreadId: {2}",
                                consumer.ConsumerTag, consumer.IsRunning,
                                Thread.CurrentThread.ManagedThreadId);
                        }
                        finally
                        {
                            consumerLock.ExitReadLock();
                        }
                        Thread.Sleep(TimeSpan.FromSeconds(5));
                    }
                }
                else
                {
                    Console.WriteLine("Initial consumer did not start!");
                }
            }
        }
    }

    private static void CreateConsumer(string queueName)
    {
        consumerLock.EnterWriteLock();
        try
        {
            if (consumer != null)
            {
                Console.WriteLine("Cancelling consumer with tag: {0} ThreadId: {1}",
                    consumer.ConsumerTag, Thread.CurrentThread.ManagedThreadId);
                consumer.Model.BasicCancel(consumer.ConsumerTag);
                consumer = null;
            }
            consumer = new EventingBasicConsumer(channel);
            consumerRegistered.Reset();
        }
        finally
        {
            consumerLock.ExitWriteLock();
        }

        consumer.Received += (model, ea) =>
        {
            msgsReceived++;
            Console.WriteLine("Received {0} messages - Consumer tag: {1} ThreadId: {2}",
                msgsReceived, ea.ConsumerTag, Thread.CurrentThread.ManagedThreadId);
        };

        consumer.Shutdown += (model, ea) =>
        {
            Console.WriteLine("Shutdown - Cause: {0} ThreadId: {1}",
                ea.Cause, Thread.CurrentThread.ManagedThreadId);
            consumerRegistered.Reset();
        };

        consumer.ConsumerCancelled += (model, ea) =>
        {
            Console.WriteLine("ConsumerCancelled - Consumer tag: {0} ThreadId: {1}",
                ea.ConsumerTag, Thread.CurrentThread.ManagedThreadId);
            consumerRegistered.Reset();
        };

        consumer.Registered += (model, ea) =>
        {
            Console.WriteLine("Registered - Consumer tag: {0} IsRunning: {1} ThreadId: {2}",
                ea.ConsumerTag, consumer.IsRunning, Thread.CurrentThread.ManagedThreadId);
            consumerRegistered.Set();
        };

        consumer.Unregistered += (model, ea) =>
        {
            Console.WriteLine("Unregistered - Consumer tag: {0} IsRunning: {1} ThreadId: {2}",
                ea.ConsumerTag, consumer.IsRunning, Thread.CurrentThread.ManagedThreadId);
            consumerRegistered.Reset();
        };

        Console.WriteLine("BasicConsume - ThreadId: {0}",
            Thread.CurrentThread.ManagedThreadId);
        channel.BasicConsume(queue: queueName, autoAck: true, consumer: consumer);
    }
}
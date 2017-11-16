using System;
using System.Text;
using System.Threading;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using Steeltoe.CloudFoundry.Connector.Rabbit;
using Steeltoe.Extensions.Configuration;

class Worker
{
    public static void Main()
    {
        var msgsReceived = 0;

        // Set default interval to publish messages
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

        Console.WriteLine("Setting heartbeat interval to {0} ms", heartbeatInterval);

        IServiceCollection services = new ServiceCollection();
        var config = new ConfigurationBuilder()
            .AddEnvironmentVariables()
            .AddCloudFoundry()
            .Build();
        services.AddRabbitConnection(config);
        var factory = services.BuildServiceProvider().GetService<ConnectionFactory>();

        // No need to explicitly set this value, default is already true
        // factory.AutomaticRecoveryEnabled = true;

        // Reduce the heartbeat interval so that bad connections are detected sooner than the default of 60s
        factory.RequestedHeartbeat = heartbeatInterval;
        using (var connection = factory.CreateConnection())
        using (var channel = connection.CreateModel())
        {
            channel.QueueDeclare(queue: "task_queue", durable: true, exclusive: false, autoDelete: false, arguments: null);

            channel.BasicQos(prefetchSize: 0, prefetchCount: 1, global: false);

            Console.WriteLine(" [*] Waiting for messages.");

            var consumer = new EventingBasicConsumer(channel);
            consumer.Received += (model, ea) =>
            {
//                var body = ea.Body;
//                var message = Encoding.UTF8.GetString(body);
                msgsReceived++;
                Console.WriteLine("Received {0} messages", msgsReceived);

//                channel.BasicAck(deliveryTag: ea.DeliveryTag, multiple: false);
            };
            channel.BasicConsume(queue: "task_queue", autoAck: true, consumer: consumer);

            while(true) {
                Thread.Sleep(1000);
            }
        }
    }
}
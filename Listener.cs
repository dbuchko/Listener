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
        IServiceCollection services = new ServiceCollection();
        var config = new ConfigurationBuilder()
            .AddEnvironmentVariables()
            .AddCloudFoundry()
            .Build();
        services.AddRabbitConnection(config);
        var factory = services.BuildServiceProvider().GetService<ConnectionFactory>();

        // No need to explicitly set this value, as defaults are as listed
        // factory.AutomaticRecoveryEnabled = true;

        // Reduce the heartbeat interval so that bad connections are detected sooner than the default of 60s
        factory.RequestedHeartbeat = 20;
        using (var connection = factory.CreateConnection())
        using (var channel = connection.CreateModel())
        {
            channel.QueueDeclare(queue: "task_queue", durable: true, exclusive: false, autoDelete: false, arguments: null);

            channel.BasicQos(prefetchSize: 0, prefetchCount: 1, global: false);

            Console.WriteLine(" [*] Waiting for messages.");

            var consumer = new EventingBasicConsumer(channel);
            consumer.Received += (model, ea) =>
            {
                var body = ea.Body;
                var message = Encoding.UTF8.GetString(body);
                Console.WriteLine(" [x] Received {0}", message);

                // Simulate some work here
                Thread.Sleep(50);

                channel.BasicAck(deliveryTag: ea.DeliveryTag, multiple: false);
            };
            channel.BasicConsume(queue: "task_queue", autoAck: false, consumer: consumer);

            while(true) {
                Thread.Sleep(1000);
            }
        }
    }
}
using Confluent.Kafka;

namespace NotificationService
{
    public interface IKafkaConsumer
    {
        Task ConsumeAsync(string topic, CancellationToken cancellationToken);
    }

    public class KafkaConsumer(ILogger<KafkaConsumer> logger, IConfiguration configuration) : IKafkaConsumer
    {
        public async Task ConsumeAsync(string topic, CancellationToken cancellationToken)
        {
            var bootstrapServers = configuration["Kafka:BootstrapServers"] ?? "localhost:9092";
            var groupId = configuration["Kafka:GroupId"] ?? "notification-service-group";

            var config = new ConsumerConfig
            {
                BootstrapServers = bootstrapServers,
                GroupId = groupId,
                AutoOffsetReset = AutoOffsetReset.Earliest,
                EnableAutoCommit = true
            };

            try
            {
                using (var consumer = new ConsumerBuilder<string, string>(config)
                    .SetKeyDeserializer(Deserializers.Utf8)
                    .SetValueDeserializer(Deserializers.Utf8)
                    .Build())
                {
                    consumer.Subscribe(topic);
                    logger.LogInformation("Subscribed to Kafka topic: {topic}", topic);

                    while (!cancellationToken.IsCancellationRequested)
                    {
                        try
                        {
                            var consumeResult = consumer.Consume(cancellationToken);

                            if (consumeResult.IsPartitionEOF)
                            {
                                continue;
                            }

                            logger.LogInformation(
                                "Received message from {topic}[{partition}] at offset {offset}: {message}",
                                consumeResult.Topic,
                                consumeResult.Partition,
                                consumeResult.Offset,
                                consumeResult.Message.Value);

                            // Process your message here
                            await ProcessMessageAsync(consumeResult.Message.Value, cancellationToken);
                        }
                        catch (ConsumeException ex)
                        {
                            logger.LogError(ex, "Error consuming message from Kafka");
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                logger.LogError(ex, "Error in Kafka consumer");
                throw;
            }
        }

        private async Task ProcessMessageAsync(string message, CancellationToken cancellationToken)
        {
            // TODO: Implement your business logic here
            // This method is called for each message consumed from Kafka
            await Task.CompletedTask;
        }
    }
}

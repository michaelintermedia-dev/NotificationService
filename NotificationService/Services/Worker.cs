namespace NotificationService.Services
{
    public class Worker(ILogger<Worker> logger, IKafkaConsumer kafkaConsumer) : BackgroundService
    {
        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            logger.LogInformation("Worker service starting");
            
            try
            {
                var topics = new[] { "user.registered", "audio.analyze.completed", "user.deregistered" };
                
                await Task.WhenAll(topics.Select(topic =>
                    Task.Run(async () =>
                    {
                        try
                        {
                            await kafkaConsumer.ConsumeAsync(topic, stoppingToken);
                        }
                        catch (OperationCanceledException)
                        {
                            logger.LogInformation("Consumer for topic {topic} stopped", topic);
                        }
                        catch (Exception ex)
                        {
                            logger.LogError(ex, "Consumer for topic {topic} encountered an error", topic);
                            throw;
                        }
                    }, stoppingToken)));
            }
            catch (OperationCanceledException)
            {
                logger.LogInformation("Worker service stopped");
            }
            catch (Exception ex)
            {
                logger.LogError(ex, "Worker service encountered an error");
                throw;
            }
        }
    }
}

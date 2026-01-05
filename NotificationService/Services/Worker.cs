namespace NotificationService.Services
{
    public class Worker(ILogger<Worker> logger, IKafkaConsumer kafkaConsumer) : BackgroundService
    {
        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            logger.LogInformation("Worker service starting");
            
            try
            {
                // Consume from your Kafka topic
                await kafkaConsumer.ConsumeAsync("audio.analyze.completed", stoppingToken);
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

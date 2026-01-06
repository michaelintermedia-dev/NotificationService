using Microsoft.Extensions.DependencyInjection;
using NotificationService.Models;
using NotificationService.Services.MessageHandlers;
using System.Windows.Input;

namespace NotificationService.Services
{
    public class Worker(ILogger<Worker> logger, IKafkaConsumer kafkaConsumer, TopicConfiguration topicConfiguration) : BackgroundService
    {
        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            logger.LogInformation("Worker service starting");
            
            try
            {

                await Task.WhenAll(topicConfiguration.Topics.Select(topic =>
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

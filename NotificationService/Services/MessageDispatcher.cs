using System.Text.Json;
using NotificationService.Models;

namespace NotificationService.Services
{
    public interface IMessageDispatcher
    {
        Task DispatchAsync(string topic, string message, CancellationToken cancellationToken);
    }

    public class MessageDispatcher : IMessageDispatcher
    {
        private readonly ILogger<MessageDispatcher> logger;
        private readonly IFcmService fcmService;

        public MessageDispatcher(ILogger<MessageDispatcher> logger, IFcmService fcmService)
        {
            this.logger = logger;
            this.fcmService = fcmService;
        }

        public async Task DispatchAsync(string topic, string message, CancellationToken cancellationToken)
        {
            try
            {
                switch (topic)
                {
                    case "user.registered":
                        await HandleUserRegisteredAsync(message, cancellationToken);
                        break;
                    case "audio.analyze.completed":
                        await HandleAudioAnalysisCompletedAsync(message, cancellationToken);
                        break;
                    case "user.deregistered":
                        await HandleUserDeregisteredAsync(message, cancellationToken);
                        break;
                    default:
                        logger.LogWarning("Unknown topic: {topic}", topic);
                        break;
                }
            }
            catch (Exception ex)
            {
                logger.LogError(ex, "Error dispatching message from topic: {topic}", topic);
                throw;
            }
        }

        private async Task HandleUserRegisteredAsync(string message, CancellationToken cancellationToken)
        {
            try
            {
                var @event = JsonSerializer.Deserialize<UserRegisteredEvent>(message);
                if (@event == null)
                {
                    logger.LogWarning("Failed to deserialize UserRegisteredEvent");
                    return;
                }

                if (@event.DeviceTokens.Count == 0)
                {
                    logger.LogWarning("User registered event has no device tokens for user: {userId}", @event.UserId);
                    return;
                }

                var notificationData = new Dictionary<string, string>
                {
                    { "event_type", "user_registered" },
                    { "user_id", @event.UserId }
                };

                await fcmService.SendMulticastAsync(
                    @event.DeviceTokens,
                    "Welcome",
                    "Your account has been registered successfully",
                    notificationData,
                    cancellationToken);

                logger.LogInformation(
                    "User registration notification sent to user: {userId} on {deviceCount} devices",
                    @event.UserId,
                    @event.DeviceTokens.Count);
            }
            catch (Exception ex)
            {
                logger.LogError(ex, "Error handling user registered event");
                throw;
            }
        }

        private async Task HandleAudioAnalysisCompletedAsync(string message, CancellationToken cancellationToken)
        {
            try
            {
                var @event = JsonSerializer.Deserialize<AudioAnalysisCompletedEvent>(message);
                if (@event == null)
                {
                    logger.LogWarning("Failed to deserialize AudioAnalysisCompletedEvent");
                    return;
                }

                if (@event.DeviceTokens.Count == 0)
                {
                    logger.LogWarning("Audio analysis event has no device tokens for user: {userId}", @event.UserId);
                    return;
                }

                var notificationData = new Dictionary<string, string>
                {
                    { "event_type", "audio_analysis_completed" },
                    { "audio_id", @event.AudioId },
                    { "user_id", @event.UserId }
                };

                await fcmService.SendMulticastAsync(
                    @event.DeviceTokens,
                    "Audio Analysis Complete",
                    $"Your audio analysis is ready: {@event.AnalysisResult}",
                    notificationData,
                    cancellationToken);

                logger.LogInformation(
                    "Audio analysis notification sent to user: {userId} on {deviceCount} devices",
                    @event.UserId,
                    @event.DeviceTokens.Count);
            }
            catch (Exception ex)
            {
                logger.LogError(ex, "Error handling audio analysis completed event");
                throw;
            }
        }

        private async Task HandleUserDeregisteredAsync(string message, CancellationToken cancellationToken)
        {
            try
            {
                var userId = JsonSerializer.Deserialize<JsonElement>(message)
                    .GetProperty("userId")
                    .GetString();

                if (string.IsNullOrEmpty(userId))
                {
                    logger.LogWarning("Failed to extract userId from user deregistered event");
                    return;
                }

                logger.LogInformation("User deregistered: {userId}", userId);
                await Task.CompletedTask;
            }
            catch (Exception ex)
            {
                logger.LogError(ex, "Error handling user deregistered event");
                throw;
            }
        }
    }
}

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
                var options = new JsonSerializerOptions { PropertyNameCaseInsensitive = true };
                
                using var doc = JsonDocument.Parse(message);
                var root = doc.RootElement;
                
                if (!root.TryGetProperty("userId", out var userIdElement) || userIdElement.ValueKind == JsonValueKind.Null)
                {
                    logger.LogWarning("User deregistered event has no userId");
                    return;
                }

                var userId = userIdElement.GetString();
                if (string.IsNullOrEmpty(userId))
                {
                    logger.LogWarning("User deregistered event has empty userId");
                    return;
                }

                var deviceTokens = new List<string>();
                if (root.TryGetProperty("deviceTokens", out var tokensElement) && tokensElement.ValueKind == JsonValueKind.Array)
                {
                    foreach (var token in tokensElement.EnumerateArray())
                    {
                        if (token.ValueKind == JsonValueKind.String)
                        {
                            var tokenString = token.GetString();
                            if (!string.IsNullOrEmpty(tokenString))
                            {
                                deviceTokens.Add(tokenString);
                            }
                        }
                    }
                }

                if (deviceTokens.Count > 0)
                {
                    var notificationData = new Dictionary<string, string>
                    {
                        { "event_type", "user_deregistered" },
                        { "user_id", userId }
                    };

                    await fcmService.SendMulticastAsync(
                        deviceTokens,
                        "Account Deregistered",
                        "Your account has been deregistered",
                        notificationData,
                        cancellationToken);

                    logger.LogInformation(
                        "User deregistration notification sent to user: {userId} on {deviceCount} devices",
                        userId,
                        deviceTokens.Count);
                }
                else
                {
                    logger.LogInformation("User deregistered: {userId} (no active devices)", userId);
                }

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

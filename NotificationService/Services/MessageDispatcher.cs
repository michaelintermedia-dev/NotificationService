using Microsoft.Extensions.DependencyInjection;
using NotificationService.Models;
using NotificationService.Services.MessageHandlers;
using System.Reflection;
using System.Text.Json;

namespace NotificationService.Services
{
    public interface IMessageDispatcher
    {
        Task DispatchAsync(string topic, string message, CancellationToken cancellationToken);
    }

    public class MessageDispatcher : IMessageDispatcher
    {
        private readonly ILogger<MessageDispatcher> _logger;
        private readonly IFcmService _fcmService;
        private readonly IServiceProvider _serviceProvider;

        public MessageDispatcher(ILogger<MessageDispatcher> logger, IFcmService fcmService, IServiceProvider serviceProvider)
        {
            _logger = logger;
            _fcmService = fcmService;
            _serviceProvider = serviceProvider;
        }

        public async Task DispatchAsync(string topic, string message, CancellationToken cancellationToken)
        {


            try
            {
                //var type1 = typeof(IMessageHandler);

                //var handlerType = Assembly.GetExecutingAssembly()
                //                .GetTypes()
                //                .Where(type => typeof(IMessageHandler).IsAssignableFrom(type))
                //                .Where(type => !type.IsAbstract && !type.IsGenericType)
                //                .Where(type => ((IMessageHandler)type).TopicName == topic)
                //                .FirstOrDefault();

                var instance = _serviceProvider.GetRequiredKeyedService<IMessageHandler>(topic);


                if (instance == null)
                {
                    _logger.LogWarning("No handler found for topic: {topic}", topic);
                    return;
                }

                //var instance = (IMessageHandler)_serviceProvider.GetRequiredService(handlerType);


                await instance.HandleMessageAsync(message, cancellationToken);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error dispatching message from topic: {topic}", topic);
                throw;
            }




            //try
            //{
            //    switch (topic)
            //    {
            //        case "user.registered":
            //            await HandleUserRegisteredAsync(message, cancellationToken);
            //            break;
            //        case "audio.analyze.completed":
            //            await HandleAudioAnalysisCompletedAsync(message, cancellationToken);
            //            break;
            //        case "user.deregistered":
            //            await HandleUserDeregisteredAsync(message, cancellationToken);
            //            break;
            //        default:
            //            _logger.LogWarning("Unknown topic: {topic}", topic);
            //            break;
            //    }
            //}
            //catch (Exception ex)
            //{
            //    _logger.LogError(ex, "Error dispatching message from topic: {topic}", topic);
            //    throw;
            //}
        }

        //private async Task HandleUserRegisteredAsync(string message, CancellationToken cancellationToken)
        //{
        //    try
        //    {
        //        var @event = JsonSerializer.Deserialize<UserRegisteredEvent>(message);
        //        if (@event == null)
        //        {
        //            _logger.LogWarning("Failed to deserialize UserRegisteredEvent");
        //            return;
        //        }

        //        if (@event.DeviceTokens.Count == 0)
        //        {
        //            _logger.LogWarning("User registered event has no device tokens for user: {userId}", @event.UserId);
        //            return;
        //        }

        //        var notificationData = new Dictionary<string, string>
        //        {
        //            { "event_type", "user_registered" },
        //            { "user_id", @event.UserId }
        //        };

        //        await _fcmService.SendMulticastAsync(
        //            @event.DeviceTokens,
        //            "Welcome",
        //            "Your account has been registered successfully",
        //            notificationData,
        //            cancellationToken);

        //        _logger.LogInformation(
        //            "User registration notification sent to user: {userId} on {deviceCount} devices",
        //            @event.UserId,
        //            @event.DeviceTokens.Count);
        //    }
        //    catch (Exception ex)
        //    {
        //        _logger.LogError(ex, "Error handling user registered event");
        //        throw;
        //    }
        //}

        //private async Task HandleAudioAnalysisCompletedAsync(string message, CancellationToken cancellationToken)
        //{
        //    try
        //    {
        //        var @event = JsonSerializer.Deserialize<AudioAnalysisCompletedEvent>(message);
        //        if (@event == null)
        //        {
        //            _logger.LogWarning("Failed to deserialize AudioAnalysisCompletedEvent");
        //            return;
        //        }

        //        if (@event.DeviceTokens.Count == 0)
        //        {
        //            _logger.LogWarning("Audio analysis event has no device tokens for user: {userId}", @event.UserId);
        //            return;
        //        }

        //        var notificationData = new Dictionary<string, string>
        //        {
        //            { "event_type", "audio_analysis_completed" },
        //            { "audio_id", @event.AudioId },
        //            { "user_id", @event.UserId }
        //        };

        //        await _fcmService.SendMulticastAsync(
        //            @event.DeviceTokens,
        //            "Audio Analysis Complete",
        //            $"Your audio analysis is ready: {@event.AnalysisResult}",
        //            notificationData,
        //            cancellationToken);

        //        _logger.LogInformation(
        //            "Audio analysis notification sent to user: {userId} on {deviceCount} devices",
        //            @event.UserId,
        //            @event.DeviceTokens.Count);
        //    }
        //    catch (Exception ex)
        //    {
        //        _logger.LogError(ex, "Error handling audio analysis completed event");
        //        throw;
        //    }
        //}

        //private async Task HandleUserDeregisteredAsync(string message, CancellationToken cancellationToken)
        //{
        //    try
        //    {
        //        var options = new JsonSerializerOptions { PropertyNameCaseInsensitive = true };

        //        using var doc = JsonDocument.Parse(message);
        //        var root = doc.RootElement;

        //        if (!root.TryGetProperty("userId", out var userIdElement) || userIdElement.ValueKind == JsonValueKind.Null)
        //        {
        //            _logger.LogWarning("User deregistered event has no userId");
        //            return;
        //        }

        //        var userId = userIdElement.GetString();
        //        if (string.IsNullOrEmpty(userId))
        //        {
        //            _logger.LogWarning("User deregistered event has empty userId");
        //            return;
        //        }

        //        var deviceTokens = new List<string>();
        //        if (root.TryGetProperty("deviceTokens", out var tokensElement) && tokensElement.ValueKind == JsonValueKind.Array)
        //        {
        //            foreach (var token in tokensElement.EnumerateArray())
        //            {
        //                if (token.ValueKind == JsonValueKind.String)
        //                {
        //                    var tokenString = token.GetString();
        //                    if (!string.IsNullOrEmpty(tokenString))
        //                    {
        //                        deviceTokens.Add(tokenString);
        //                    }
        //                }
        //            }
        //        }

        //        if (deviceTokens.Count > 0)
        //        {
        //            var notificationData = new Dictionary<string, string>
        //            {
        //                { "event_type", "user_deregistered" },
        //                { "user_id", userId }
        //            };

        //            await _fcmService.SendMulticastAsync(
        //                deviceTokens,
        //                "Account Deregistered",
        //                "Your account has been deregistered",
        //                notificationData,
        //                cancellationToken);

        //            _logger.LogInformation(
        //                "User deregistration notification sent to user: {userId} on {deviceCount} devices",
        //                userId,
        //                deviceTokens.Count);
        //        }
        //        else
        //        {
        //            _logger.LogInformation("User deregistered: {userId} (no active devices)", userId);
        //        }

        //        await Task.CompletedTask;
        //    }
        //    catch (Exception ex)
        //    {
        //        _logger.LogError(ex, "Error handling user deregistered event");
        //        throw;
        //    }
        //}
    }
}

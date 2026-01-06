using Microsoft.Extensions.Logging;
using NotificationService.Models;
using System;
using System.Collections.Generic;
using System.Text;
using System.Text.Json;

namespace NotificationService.Services.MessageHandlers
{
    public class UserRegisteredMessagaHandler : IMessageHandler
    {
        private readonly ILogger<UserRegisteredMessagaHandler> _logger;
        private readonly IFcmService _fcmService;

        public UserRegisteredMessagaHandler(ILogger<UserRegisteredMessagaHandler> logger, IFcmService fcmService)
        {
            _logger = logger;
            _fcmService = fcmService;
        }

        public async Task HandleMessageAsync(string message, CancellationToken cancellationToken)
        {
            try
            {
                var @event = JsonSerializer.Deserialize<UserRegisteredEvent>(message, new JsonSerializerOptions { PropertyNameCaseInsensitive = true });
                if (@event == null)
                {
                    _logger.LogWarning("Failed to deserialize UserRegisteredEvent");
                    return;
                }

                if (@event.DeviceTokens.Count == 0)
                {
                    _logger.LogWarning("User registered event has no device tokens for user: {userId}", @event.UserId);
                    return;
                }

                var notificationData = new Dictionary<string, string>
                {
                    { "event_type", "user_registered" },
                    { "user_id", @event.UserId }
                };

                await _fcmService.SendMulticastAsync(
                    @event.DeviceTokens,
                    "Welcome",
                    "Your account has been registered successfully",
                    notificationData,
                    cancellationToken);

                _logger.LogInformation(
                    "User registration notification sent to user: {userId} on {deviceCount} devices",
                    @event.UserId,
                    @event.DeviceTokens.Count);
            }
            catch (Exception ex)
            {
                _logger.LogError(null, "Error handling user registered event");
                throw;
            }
        }
    }
}

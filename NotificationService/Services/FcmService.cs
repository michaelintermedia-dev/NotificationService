using FirebaseAdmin;
using FirebaseAdmin.Messaging;
using Google.Apis.Auth.OAuth2;

namespace NotificationService.Services
{
    public interface IFcmService
    {
        Task SendNotificationAsync(string deviceToken, string title, string body, Dictionary<string, string>? data = null, CancellationToken cancellationToken = default);
        Task SendMulticastAsync(List<string> deviceTokens, string title, string body, Dictionary<string, string>? data = null, CancellationToken cancellationToken = default);
    }
    public class FcmService : IFcmService
    {
        private readonly ILogger<FcmService> logger;
        private readonly IConfiguration configuration;
        private FirebaseMessaging messagingClient;

        public FcmService(ILogger<FcmService> logger, IConfiguration configuration)
        {
            this.logger = logger;
            this.configuration = configuration;
            InitializeFirebase();
        }

        private void InitializeFirebase()
        {
            try
            {
                if (FirebaseApp.DefaultInstance == null)
                {
                    var credentialsPath = configuration["Firebase:CredentialsPath"] ?? "firebase-credentials.json";
                    FirebaseApp.Create(new AppOptions
                    {
                        Credential = GoogleCredential.FromFile(credentialsPath)
                    });
                }

                messagingClient = FirebaseMessaging.DefaultInstance;
                logger.LogInformation("Firebase initialized successfully");
            }
            catch (Exception ex)
            {
                logger.LogError(ex, "Failed to initialize Firebase");
                throw;
            }
        }

        public async Task SendNotificationAsync(string deviceToken, string title, string body, Dictionary<string, string>? data = null, CancellationToken cancellationToken = default)
        {
            try
            {
                var message = new Message
                {
                    Token = deviceToken,
                    Notification = new Notification
                    {
                        Title = title,
                        Body = body
                    },
                    Data = data ?? new Dictionary<string, string>()
                };

                var messageId = await messagingClient.SendAsync(message);
                logger.LogInformation("Notification sent successfully. MessageId: {messageId}", messageId);
            }
            catch (Exception ex)
            {
                logger.LogError(ex, "Failed to send notification to device token: {deviceToken}", deviceToken);
                throw;
            }
        }

        public async Task SendMulticastAsync(List<string> deviceTokens, string title, string body, Dictionary<string, string>? data = null, CancellationToken cancellationToken = default)
        {
            try
            {
                if (deviceTokens.Count == 0)
                {
                    logger.LogWarning("No device tokens provided for multicast notification");
                    return;
                }

                var multicastMessage = new MulticastMessage
                {
                    Tokens = deviceTokens,
                    Notification = new Notification
                    {
                        Title = title,
                        Body = body
                    },
                    Data = data ?? new Dictionary<string, string>()
                };

                var response = await messagingClient.SendMulticastAsync(multicastMessage);
                logger.LogInformation(
                    "Multicast notification sent. Successful: {successful}, Failed: {failed}",
                    response.SuccessCount,
                    response.FailureCount);

                if (response.FailureCount > 0)
                {
                    logger.LogWarning("Some notifications failed to send. Failed count: {failureCount}", response.FailureCount);
                }
            }
            catch (Exception ex)
            {
                logger.LogError(ex, "Failed to send multicast notification");
                throw;
            }
        }
    }
}

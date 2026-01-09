using NotificationService.Models;
using NotificationService.Services;
using NotificationService.Services.MessageHandlers;

var builder = Host.CreateApplicationBuilder(args);

builder.Services.AddSingleton<IFcmService, FcmService>();
builder.Services.AddSingleton<IMessageDispatcher, MessageDispatcher>();
builder.Services.AddSingleton<IKafkaConsumer, KafkaConsumer>();

var messageHandlersConfig = new MessageHandlresConfig(new Dictionary<string, Type>
{
    { "test.topic", typeof(TestHandler) },
    { "audio.analyze.completed", typeof(AudioAnalysisCompletedMessagaHandler) },
    { "user.registered", typeof(UserRegisteredMessagaHandler) },
    { "user.deregistered", typeof(UserDeregisteredMessagaHandler) }
});

foreach (var kvp in messageHandlersConfig.messageHandlers)
{
    builder.Services.AddKeyedSingleton(typeof(IMessageHandler), kvp.Key, kvp.Value);
}

builder.Services.AddSingleton(new TopicConfiguration(messageHandlersConfig.messageHandlers.Keys.ToArray()));

builder.Services.AddHostedService<Worker>();

var host = builder.Build();
host.Run();
using Microsoft.Extensions.DependencyInjection;
using NotificationService.Models;
using NotificationService.Services;
using NotificationService.Services.MessageHandlers;

var builder = Host.CreateApplicationBuilder(args);

// Register Kafka consumer service
builder.Services.AddSingleton<IFcmService, FcmService>();
builder.Services.AddSingleton<IMessageDispatcher, MessageDispatcher>();
builder.Services.AddSingleton<IKafkaConsumer, KafkaConsumer>();

var messageHandlers = new Dictionary<string, Type>
{
    { "audio.analyze.completed", typeof(AudioAnalysisCompletedMessagaHandler) },
    { "user.registered", typeof(UserRegisteredMessagaHandler) },
    { "user.deregistered", typeof(UserDeregisteredMessagaHandler) }
};

foreach (var kvp in messageHandlers)
{
    builder.Services.AddKeyedSingleton(typeof(IMessageHandler), kvp.Key, kvp.Value);
}

builder.Services.AddSingleton(new TopicConfiguration(messageHandlers.Keys.ToArray()));

builder.Services.AddHostedService<Worker>();

var host = builder.Build();
host.Run();
using Microsoft.Extensions.DependencyInjection;
using NotificationService.Services;
using NotificationService.Services.MessageHandlers;

var builder = Host.CreateApplicationBuilder(args);

// Register Kafka consumer service
builder.Services.AddSingleton<IFcmService, FcmService>();
builder.Services.AddSingleton<IMessageDispatcher, MessageDispatcher>();
builder.Services.AddSingleton<IKafkaConsumer, KafkaConsumer>();
builder.Services.AddKeyedSingleton<IMessageHandler, AudioAnalysisCompletedMessagaHandler>("audio.analyze.completed");
builder.Services.AddKeyedSingleton<IMessageHandler, UserRegisteredMessagaHandler>("user.registered");
builder.Services.AddKeyedSingleton<IMessageHandler, UserDeregisteredMessagaHandler>("user.deregistered");
builder.Services.AddHostedService<Worker>();

var host = builder.Build();
host.Run();

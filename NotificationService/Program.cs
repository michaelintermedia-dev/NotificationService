using NotificationService.Services;

var builder = Host.CreateApplicationBuilder(args);

// Register Kafka consumer service
builder.Services.AddSingleton<IFcmService, FcmService>();
builder.Services.AddSingleton<IMessageDispatcher, MessageDispatcher>();
builder.Services.AddSingleton<IKafkaConsumer, KafkaConsumer>();
builder.Services.AddHostedService<Worker>();

var host = builder.Build();
host.Run();

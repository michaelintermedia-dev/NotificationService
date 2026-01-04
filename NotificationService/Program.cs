using NotificationService;

var builder = Host.CreateApplicationBuilder(args);

// Register Kafka consumer service
builder.Services.AddSingleton<IKafkaConsumer, KafkaConsumer>();
builder.Services.AddHostedService<Worker>();

var host = builder.Build();
host.Run();

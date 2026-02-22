using DemoApp.Services.Customers.Worker;
using SurianMing.Utilities.Kafka;

var builder = Host.CreateApplicationBuilder(args);
builder.Services.AddHostedService<Worker>();
builder.Services.AddEventHandling();

var host = builder.Build();
host.Run();

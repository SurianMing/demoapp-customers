namespace SurianMing.Utilities.Kafka;

internal class KafkaConsumerProvider(
    IOptionsMonitor<KafkaServerOptions> kafkaServerOptionsProvider
)
{
    private readonly KafkaServerOptions _kafkaServerSettings = kafkaServerOptionsProvider.CurrentValue!;

    internal IConsumer<Ignore, string> GetConsumer(string? groupName = null)
    {
        var consumerBuilder = new ConsumerBuilder<Ignore, string>(
            new ConsumerConfig
            {
                BootstrapServers = $"{_kafkaServerSettings.KafkaServerIP}:{_kafkaServerSettings.KafkaPort}",
                GroupId = string.IsNullOrEmpty(groupName) ? Guid.NewGuid().ToString() : groupName,
                MetadataMaxAgeMs = 5000,
                EnableAutoOffsetStore = true,
                EnableAutoCommit = true,
                AutoCommitIntervalMs = 100,
                ApiVersionRequest = false
            });

        return consumerBuilder.Build();
    }
}
using Confluent.Kafka.Admin;

namespace SurianMing.Utilities.Kafka;

internal class KafkaTopicRegistrationManager
{
    private readonly KafkaServerOptions _kafkaServerSettings;
    private readonly ILogger<KafkaTopicRegistrationManager> _logger;

    public KafkaTopicRegistrationManager(
        IOptionsMonitor<KafkaServerOptions> kafkaServerOptionsProvider,
        ILogger<KafkaTopicRegistrationManager> logger
    ) => (_kafkaServerSettings, _logger)
        = (kafkaServerOptionsProvider.CurrentValue, logger);

    internal async Task<bool> CreateTopic(string topicName, short replicationFactor = 1)
    {
        using var adminClient = new AdminClientBuilder(
            new AdminClientConfig
            {
                BootstrapServers = $"{_kafkaServerSettings.KafkaServerIp}:{_kafkaServerSettings.KafkaPort}"
            }
        ).Build();

        try
        {
            if (adminClient.GetMetadata(topicName, TimeSpan.FromSeconds(2))
                .Topics.Count == 0)
            {
                await adminClient.CreateTopicsAsync(
                [
                    new() { Name = topicName, ReplicationFactor = replicationFactor }
                ], new() { });

            }

            return true;
        }
        catch (CreateTopicsException ex)
        {
            _logger.LogError(ex, "Unable to add topic {topicName} to kafka - handler cannot be initialised.", topicName);
            return false;
        }
    }

    internal async Task<bool> RemoveTopic(string topicName)
    {
        using var adminClient = new AdminClientBuilder(
            new AdminClientConfig
            {
                BootstrapServers = $"{_kafkaServerSettings.KafkaServerIp}:{_kafkaServerSettings.KafkaPort}"
            }
        ).Build();

        try
        {
            await adminClient.DeleteTopicsAsync(
                [
                    topicName
                ],
                new DeleteTopicsOptions { OperationTimeout = TimeSpan.FromSeconds(2) }
            );

            return true;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Unable to remove topic {topicName}.", topicName);
            return false;
        }
    }
}
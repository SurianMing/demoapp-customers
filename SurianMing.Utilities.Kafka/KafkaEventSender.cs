using System.Text.Json;

namespace SurianMing.Utilities.Kafka;

internal class KafkaEventSender(
    IOptionsMonitor<KafkaServerOptions> kafkaServerOptionsMonitor
) : IKafkaEventSender
{
    private readonly KafkaServerOptions _kafkaServerOptions = kafkaServerOptionsMonitor.CurrentValue!;

    public async Task<bool> SendEvent<TEvent>(TEvent eventToSend)
        where TEvent : KafkaEvent
    {
        using var producer = GetProducer();
        var deliveryResult = await producer.ProduceAsync(
            eventToSend.Topic,
            new Message<Null, string> { Value = JsonSerializer.Serialize(eventToSend) }
        );

        return deliveryResult.Status == PersistenceStatus.Persisted;
    }

    public async Task<bool> SendEvent<TEvent>(TEvent eventToSend, Guid identifier)
        where TEvent : KafkaEvent
    {
        using var producer = GetProducer();
        var deliveryResult = await producer.ProduceAsync(
            eventToSend.Topic,
            new Message<Null, string>
            {
                Value = JsonSerializer.Serialize(eventToSend),
                Headers = new Headers
                {
                    new("CorrelationId", identifier.ToByteArray())
                }
            }
        );

        return deliveryResult.Status == PersistenceStatus.Persisted;
    }

    private IProducer<Null, string> GetProducer()
    {
        var producerBuilder = new ProducerBuilder<Null, string>(
            new ProducerConfig
            {
                BootstrapServers = $"{_kafkaServerOptions.KafkaServerIP}:{_kafkaServerOptions.KafkaPort}",
                ApiVersionRequest = false
            });

        return producerBuilder.Build();
    }
}
namespace SurianMing.Utilities.Kafka;

internal class KafkaEventManager(
    IEnumerable<IKafkaEventConsumer> kafkaEventConsumers,
    ILogger<KafkaEventManager> logger
) : IKafkaEventManager, IDisposable
{
    private readonly List<IKafkaEventConsumer> _kafkaEventConsumers = [.. kafkaEventConsumers];
    private readonly ILogger<KafkaEventManager> _logger = logger;

    public void InitialiseKafkaEventHandlers()
    {
        _kafkaEventConsumers.ForEach(eventConsumer => eventConsumer.InitialiseEventConsumer());
    }

    ~KafkaEventManager()
    {
        Dispose(false);
    }

    public void Dispose()
    {
        Dispose(true);

        GC.SuppressFinalize(this);
    }

    protected virtual void Dispose(bool disposing)
    {
        _logger.LogInformation("Inside Dispose");
        if (disposing)
        {
            foreach(var consumer in _kafkaEventConsumers)
            {
                consumer.Dispose();
            }
        }
    }
}
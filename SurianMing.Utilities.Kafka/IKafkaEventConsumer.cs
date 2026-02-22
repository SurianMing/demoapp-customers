namespace SurianMing.Utilities.Kafka;

internal interface IKafkaEventConsumer : IDisposable
{
    void InitialiseEventConsumer();
}

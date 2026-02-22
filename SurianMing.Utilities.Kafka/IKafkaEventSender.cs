namespace SurianMing.Utilities.Kafka;

internal interface IKafkaEventSender
{
    Task<bool> SendEvent<TEvent>(TEvent eventToSend)
        where TEvent : KafkaEvent;
    Task<bool> SendEvent<TEvent>(TEvent eventToSend, Guid identifier)
        where TEvent : KafkaEvent;
}
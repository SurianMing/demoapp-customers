namespace SurianMing.Utilities.Kafka;

/// <summary>
/// Manager for all kafka event handler processes defined by KafkaEventHandlers.
/// </summary>
public interface IKafkaEventManager : IDisposable
{
    /// <summary>
    /// This method will find all classes derived from KafkaEventHandler<>
    /// and initialise a kafka consumer to subscribe to their specified topics. When matching messages are
    /// consumed, they will be passed to the relevant handler for processing.
    /// </summary>
    void InitialiseKafkaEventHandlers();
}
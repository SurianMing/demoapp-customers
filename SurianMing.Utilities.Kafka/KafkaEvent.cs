namespace SurianMing.Utilities.Kafka;

/// <summary>
/// A base class for all kafka events that will be processed by this Utility.
/// </summary>
public class KafkaEvent
{
    /// <summary>
    /// A value that can be used to logically join together multiple kafka messages if necessary.
    /// </summary>
    public Guid CorrelationId { get; set; }
    /// <summary>
    /// The time at which the kafka message was generated.
    /// </summary>
    public DateTime Timestamp { get; set; } = DateTime.UtcNow;
    /// <summary>
    /// The full topic that the kafka message relates to.
    /// </summary>
    public string? Topic { get; set; }
}
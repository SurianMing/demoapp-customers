using System.Text.Json;

namespace SurianMing.Utilities.Kafka;
using ApplicationSettings;

// internal interface IKafkaEventHandler
// {
//     string TopicToMatch { get; }
//     IsolationMode IsolationMode { get; }
//     bool UseRegexPatternMatchingForTopic { get; }
//     Task<KafkaEventResult> Handle(string topic, string eventData, Guid trackingId);
// }

public class KafkaApplication
{
    private List<IKafkaHandler> _kafkaHandlers = [];

    internal KafkaApplication() { }

    KafkaHandlerBuilder MapEventHandler(
        string topicToMatch,
        Delegate handler
    )
    {

    }
}

public class KafkaHandlerBuilder
{

}

internal interface IKafkaHandler { }

internal class KafkaHandler<TEvent>(
    KafkaHandlerConfiguration<TEvent> _configuration,
    IServiceScopeFactory _serviceScopeFactory
)
{

}

public interface IKafkaEventHandler
{
    string TopicToMatch { get; }
    IsolationMode IsolationMode { get; }
    bool UseRegexPatternMatchingForTopic { get; }
    Task<KafkaEventResult> Handle<TEvent>(string topic, TEvent eventData, Guid trackingId);
}

/// <summary>
/// The base class for implementing Kafka event handlers. Any derived class will be automatically picked up by
/// the call to the InitialiseKafkaEventHandlers method of the IKafkaEventManager ready to be processed when
/// a message with a matching topic is received.
/// </summary>
/// <typeparam name="T">The type that the kafka message's Message->Value should be deserializable into.</typeparam>
public abstract class KafkaEventHandler<T>(
    IOptionsMonitor<SmingApplicationSettings> smingApplicationSettingsOptionsMonitor
) : IKafkaEventHandler
    where T : KafkaEvent
{
    private static readonly JsonSerializerOptions _deserializeOptions = new ()
    {
        PropertyNameCaseInsensitive = true
    };

    /// <summary>
    /// The topic (or regex pattern for the topic's - if UseRegexPatternMatchingForTopic is true) that will be matched
    /// and lead to events being passed to this Handler for processing.
    /// </summary>
    public abstract string TopicToMatch { get; }
    /// <summary>
    /// The rule to follow when multiple instances of the parent process is running concurrently.
    /// Either ALL running instances should process each event, or the event should be processed by one
    /// running instance of this process.
    /// </summary>
    public abstract IsolationMode IsolationMode { get; }
    /// <summary>
    /// A bool indicating whether pattern matching should be used when matching against incoming message/event topics.
    /// </summary>
    public abstract bool UseRegexPatternMatchingForTopic { get; }

    /// <summary>
    /// This method will be called when an event is received with a topic matching that required by this class.
    /// </summary>
    /// <param name="topic">The full topic of the event that triggered this method to be called.</param>
    /// <param name="eventData">A strongly typed object constructed from the Message->Value of the event that
    /// triggered this method to be called.</param>
    /// <param name="trackingId">A tracking id that can be used in logging to track a process from start to finish.</param>
    /// <returns>An indicator of whether the Handle process has been successful.</returns>
    public abstract Task<KafkaEventResult> Handle(string topic, T eventData, Guid trackingId);

    async Task<KafkaEventResult> IKafkaEventHandler.Handle(string topic, string eventData, Guid trackingId)
    {
        T? eventDto = JsonSerializer.Deserialize<T>(eventData, _deserializeOptions);

        return eventDto is not null
            ? await Handle(topic, eventDto, trackingId)
            : throw new Exception("Couldn't deserialize message into T");
    }
}
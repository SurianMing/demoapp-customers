using System.Text.Json;

namespace SurianMing.Utilities.Kafka;

internal interface IKafkaRequestManager<TRequest, TResponse>
    where TRequest : KafkaEvent
    where TResponse : KafkaEvent
{
    Task<TResponse?> SendRequestAsync(TRequest request, CancellationToken cancellationToken = default);
}

internal class KafkaRequestManager<TRequest, TResponse> : IKafkaRequestManager<TRequest, TResponse>
    where TRequest : KafkaEvent
    where TResponse : KafkaEvent
{
    private static readonly JsonSerializerOptions _deserializeOptions = new ()
    {
        PropertyNameCaseInsensitive = true
    };
    private readonly IConsumer<Ignore, string> _consumer;
    private readonly IKafkaEventSender _kafkaEventSender;
    private readonly string _requestTopic;
    private readonly ILogger<KafkaEventManager> _logger;

    public KafkaRequestManager(
        KafkaConsumerProvider consumerProvider,
        KafkaTopicRegistrationManager topicRegistrationManager,
        IKafkaEventSender kafkaEventSender,
        ILogger<KafkaEventManager> logger
    )
    {
        _kafkaEventSender = kafkaEventSender;
        _logger = logger;

        _requestTopic = GetRequestTopic();
        var topicToConsume = GetResponseTopic();
        topicRegistrationManager.CreateTopic(topicToConsume).Wait();
        _consumer = consumerProvider.GetConsumer();
        _consumer.Subscribe(topicToConsume);
    }

    public async Task<TResponse?> SendRequestAsync(TRequest request, CancellationToken cancellationToken = default)
    {
        var uniqueEventReference = Guid.NewGuid();
        request.Topic = _requestTopic;
        TResponse? result = null;

        await Task.WhenAll(
            Task.Run(() =>
            {
                _logger.LogInformation("Starting consumer for kafka request.");

                while (result is null && !cancellationToken.IsCancellationRequested)
                {
                    try
                    {
                        var cr = _consumer.Consume(cancellationToken);
                        var correlationIdHeader = cr.Message.Headers.First(x => x.Key == "CorrelationId");
                        var correlationId = new Guid(correlationIdHeader.GetValueBytes());

                        if (correlationId == uniqueEventReference)
                        {
                            //Raise the event..
                            _logger.LogTrace("Response received fo kafka request - returning to request.");
                            result = JsonSerializer.Deserialize<TResponse>(cr.Message.Value, _deserializeOptions);
                        }
                    }
                    catch (OperationCanceledException)
                    {
                        _logger.LogError("Request cancelled.");
                    }
                }
            }, cancellationToken),
            _kafkaEventSender.SendEvent(request)
        );

        return result;
    }

    internal static string GetRequestTopic()
        => $"request_{typeof(TRequest).Name}_{typeof(TResponse).Name}";

    internal static string GetResponseTopic()
        => $"response_{typeof(TRequest).Name}_{typeof(TResponse).Name}";
}
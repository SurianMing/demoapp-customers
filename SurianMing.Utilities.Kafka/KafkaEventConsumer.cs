namespace SurianMing.Utilities.Kafka;
using ApplicationSettings;

internal class KafkaEventConsumer<THandler>(
    IServiceScopeFactory _serviceScopeFactory,
    KafkaTopicRegistrationManager _topicRegistrationManager,
    KafkaConsumerProvider _kafkaConsumerProvider,
    IOptionsMonitor<SmingApplicationSettings> _smingApplicationSettingsOptionsMonitor,
    ILogger<KafkaEventConsumer<THandler>> _logger
) : IKafkaEventConsumer, IDisposable
    where THandler : IKafkaEventHandler
{
    private readonly SmingApplicationSettings _smingApplicationSettings =
        _smingApplicationSettingsOptionsMonitor.CurrentValue
            ?? throw new InvalidOperationException(
                "Attempt to initialise Kafka event streaming without available SmingApplicationSettings"
            );
    private readonly CancellationTokenSource _cancellationTokenSource = new();
    private string _topicToConsume = null!;

    internal void ConfigureEventConsumer(
        string topicToMatch,
        IsolationMode isolationMode,
        bool useRegexPatternMatchingForTopic
    )
    {
        _topicToConsume = topicToMatch;

        var clientGroupId = isolationMode switch
        {
            IsolationMode.PerServiceInstance => Guid.NewGuid().ToString(),
            IsolationMode.PerServiceType => _smingApplicationSettings.ApplicationId,
            _ => throw new NotSupportedException($"Isolation level {isolationMode} not currently supported.")
        };

        if (useRegexPatternMatchingForTopic)
        {
            _topicToConsume = $"^{_topicToConsume}";
        }
        else
        {
            if (!_topicRegistrationManager.CreateTopic(_topicToConsume).Result)
            {
                throw new Exception("Couldn't register topic - oopsie!");
            }
        }

        _consumer = kafkaConsumerProvider.GetConsumer(clientGroupId);
    }

    public void InitialiseEventConsumer()
    {
        var cancellationToken = _cancellationTokenSource.Token;

        using var scope = _serviceScopeFactory.CreateScope();

        var handler = scope.ServiceProvider.GetRequiredService<THandler>();
        _topicToConsume = handler.TopicToMatch;

        var clientGroupId = handler.IsolationMode switch
        {
            IsolationMode.PerServiceInstance => Guid.NewGuid().ToString(),
            IsolationMode.PerServiceType => smingApplicationSettings.ApplicationId,
            _ => throw new NotSupportedException($"Isolation level {handler.IsolationMode} not currently supported.")
        };

        if (handler.UseRegexPatternMatchingForTopic)
        {
            _topicToConsume = $"^{_topicToConsume}";
        }
        else
        {
            if (!topicRegistrationManager.CreateTopic(_topicToConsume).Result)
            {
                throw new Exception("Couldn't register topic - oopsie!");
            }
        }

        _consumer = kafkaConsumerProvider.GetConsumer(clientGroupId);

        Task.Run(() => MetadataRefresh(_consumer.Handle));

        _consumer.Subscribe(_topicToConsume);
        var consumerTask = Task.Run(() =>
        {
            try
            {
                _logger.LogInformation("Starting consumer on topic {topicToConsume}.",
                    _topicToConsume);

                while (true)
                {
                    _cancellationToken.ThrowIfCancellationRequested();

                    try
                    {
                        var cr = _consumer.Consume(_cancellationToken); //Will block until canceled or a event occurs
                        if (cr.Topic != "__consumer_offsets")
                        {
                            var trackingId = Guid.NewGuid();
                            _logger.LogTrace("Message in from {topicToConsume}, raising the event", _topicToConsume);

                            Task.Run(async () =>
                            {
                                try
                                {
                                    using var scope = _serviceScopeFactory.CreateScope();
                                    var handler = scope.ServiceProvider.GetRequiredService<THandler>();
                                    var result = await handler.Handle(_topicToConsume, cr.Message.Value, trackingId);
                                    if (result == KafkaEventResult.Complete)
                                    {
                                        _logger.LogInformation("Successfully consumed message.");
                                    }
                                    else
                                    {
                                        _logger.LogWarning(
                                            "Consumer failed to complete processing for message from topic {topicToConsume} with value {messageValue}.",
                                            _topicToConsume, cr.Message.Value
                                        );
                                    }
                                }
                                catch (Exception ex)
                                {
                                    _logger.LogError(ex, "Exception occurred whilst processing message.");
                                }
                            });
                        }
                    }
                    catch (ConsumeException e)
                    {
                        //We can get this when we consume from a queue not yet created.
                        //The first message sent to that queue will then create the message
                        if (!e.Message.Contains("Broker: Unknown topic or partition"))
                            _logger.LogWarning("Subscription to topic '{topicToConsume}' has raised an exception, but will continue until stopped.",
                                _topicToConsume);
                    }
                    catch { }
                }
            }
            catch (OperationCanceledException)
            {
                // Close and Release all the resources held by this consumer
                _logger.LogError("Subscription to topic '{topicToConsume}' has been stopped.",
                    _topicToConsume);
                _consumer.Close();
                _consumer.Dispose();
            }
        }, _cancellationToken);
    }

    private static void MetadataRefresh(Handle handle)
    {
        using var client = new DependentAdminClientBuilder(handle).Build();
        while (true)
        {
            Thread.Sleep(5000);
            Console.WriteLine("Refreshing Metadata...");
            client.GetMetadata(TimeSpan.FromMilliseconds(5000));
        }
    }

    ~KafkaEventConsumer()
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
        _logger.LogInformation("Disposing kafka consumer for {topicToConsume}",
            _topicToConsume);
        _cancellationTokenSource.Cancel();
    }
}
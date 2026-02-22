using System.Reflection;

namespace SurianMing.Utilities.Kafka;

public static class ServiceCollectionExtensions
{
    public static IServiceCollection AddEventHandling(
        this IServiceCollection services
    ) => services.AddEventHandling(
        Assembly.GetCallingAssembly());

    public static IServiceCollection AddEventHandling<TEventForTargeting>(
        this IServiceCollection services
    ) => services.AddEventHandling(
        Assembly.GetAssembly(typeof(TEventForTargeting))!);

    /// <summary>
    /// Extension method to set up the IOC requirements of the kafka event handling utility.
    /// </summary>
    /// <param name="services">The service collection to which we should add the required dependencies.</param>
    /// <param name="settingsSectionName">The name of the settings section to be passed to the SmingOptions utility
    /// in order to fetch the kafka cluster details.</param>
    /// <returns>The service collection with the required dependencies for kafka handling added.</returns>
    public static IServiceCollection AddEventHandling(
        this IServiceCollection services,
        Assembly eventImplementationAssembly
    )
    {
        services.AddOptions<KafkaServerOptions>("Kafka");
        var kafkaEventHandlerTypes = eventImplementationAssembly
            .GetTypes()
            .Where(type =>
                typeof(IKafkaEventHandler).IsAssignableFrom(type)
                && type is { IsAbstract:false, IsInterface: false }
            )
            .ToList();

        kafkaEventHandlerTypes.ForEach(eventHandlerType =>
        {
            services.AddScoped(eventHandlerType);

            var baseConsumerType = typeof(KafkaEventConsumer<>);
            var typedConsumer = baseConsumerType.MakeGenericType(eventHandlerType);
            services.AddSingleton(typeof(IKafkaEventConsumer), typedConsumer);
        });

        services.AddSingleton<KafkaTopicRegistrationManager>();
        services.AddSingleton<KafkaConsumerProvider>();
        services.AddSingleton<IKafkaEventSender, KafkaEventSender>();
        services.AddSingleton(typeof(IKafkaRequestManager<,>), typeof(KafkaRequestManager<,>));
        services.AddSingleton<IKafkaEventManager, KafkaEventManager>();

        return services;
    }

    public static IServiceCollection AddEventHandler<TEventHandler, TEvent>(
        this IServiceCollection services
    ) where TEventHandler : KafkaEventHandler<TEvent> where TEvent : KafkaEvent
    {
        services.AddScoped<TEventHandler>();
        services.AddSingleton<IKafkaEventConsumer, KafkaEventConsumer<TEventHandler>>();

        return services;
    }
}
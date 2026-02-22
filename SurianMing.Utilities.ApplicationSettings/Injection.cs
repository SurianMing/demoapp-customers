using Microsoft.Extensions.DependencyInjection;

namespace SurianMing.Utilities.ApplicationSettings;

public static class Injection
{
    public static IServiceCollection InitialiseApplicationSettings(
        this IServiceCollection services
    )
    {
        services.AddOptions<SmingApplicationSettings>("SmingApplication");

        return services;
    }
}
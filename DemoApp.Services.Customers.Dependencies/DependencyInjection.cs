using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Configuration;

namespace DemoApp.Services.Customers.Dependencies;
using Databases.Customers;
using Databases.Customers.Context;

public static class DependencyInjection
{
    public static IServiceCollection InitialiseDependencies(
        this IServiceCollection services,
        IConfiguration configuration
    )
    {
        services.AddScoped<ICustomerData, CustomerData>();

        var customersDbConnString = configuration.GetConnectionString("CustomersDatabase")
            ?? throw new InvalidOperationException(
                "Attempt to connect to customers database requires connection string with name 'CustomersDatabase'"
            );

        services.AddDbContext<CustomerContext>(options =>
        {
            options.UseSqlServer(customersDbConnString);
        });

        return services;
    }
}
namespace DemoApp.Services.Customers.Dependencies.Databases.Customers.Mappers;
using Context.Models;

internal static class CustomerMapper
{
    internal static CustomerDto ToDto(
        this Customer entity
    ) => new(
        entity.CustomerId,
        entity.FirstName,
        entity.Surname
    );
}
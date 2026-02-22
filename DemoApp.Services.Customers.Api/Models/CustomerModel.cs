namespace DemoApp.Services.Customers.Api.Models;

public record CustomerModel(
    Guid CustomerIdentifier,
    string FirstName,
    string Surname
);

internal static class CustomerModelExtensions
{
    internal static CustomerModel ToModel(
        this CustomerDto customerDto
    ) => new(
        customerDto.CustomerIdentifier,
        customerDto.FirstName,
        customerDto.Surname
    );
}
namespace DemoApp.Services.Customers.Domain.Definition.Dtos;

public record CustomerDto(
    Guid CustomerIdentifier,
    string FirstName,
    string Surname
);
namespace DemoApp.Services.Customers.Domain.Definition.Services;
using Dtos;

public interface ICustomerService
{
    Task<Guid> CreateCustomer(
        string firstName,
        string surname
    );
    Task<CustomerDto[]> GetAllCustomers();
    Task<CustomerDto> GetCustomerByIdentifier(
        Guid customerIdentifier
    );
}
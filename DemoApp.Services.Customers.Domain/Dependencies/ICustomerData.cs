namespace DemoApp.Services.Customers.Domain.Dependencies;
using Definition.Dtos;

public interface ICustomerData
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
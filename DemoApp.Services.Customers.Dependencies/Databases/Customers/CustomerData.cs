using Microsoft.EntityFrameworkCore;

namespace DemoApp.Services.Customers.Dependencies.Databases.Customers;
using Context;
using Context.Models;
using Mappers;

internal class CustomerData(
    CustomerContext _customerContext
) : ICustomerData
{
    public async Task<Guid> CreateCustomer(
        string firstName,
        string surname
    )
    {
        var newEntity = new Customer
        {
            FirstName = firstName,
            Surname = surname
        };

        _customerContext.Add(newEntity);

        await _customerContext.SaveChangesAsync();
        return newEntity.CustomerId;
    }

    public async Task<CustomerDto[]> GetAllCustomers()
        => await _customerContext.Customers
            .Select(entity => entity.ToDto())
            .ToArrayAsync();

    public async Task<CustomerDto> GetCustomerByIdentifier(
        Guid customerIdentifier
    )
    {
        var customerEntity = await _customerContext
            .Customers
            .FirstOrDefaultAsync(
                entity => entity.CustomerId == customerIdentifier
            )
            ?? throw new Exception("Not good");

        return customerEntity.ToDto();
    }
}
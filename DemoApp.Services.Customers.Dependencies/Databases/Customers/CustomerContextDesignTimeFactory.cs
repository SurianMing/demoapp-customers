using Microsoft.EntityFrameworkCore.Design;

namespace DemoApp.Services.Customers.Dependencies.Databases.Customers;
using Context;
using Microsoft.EntityFrameworkCore;

public class CustomerContextFactory : IDesignTimeDbContextFactory<CustomerContext>
{
    public CustomerContext CreateDbContext(string[] args)
    {
        var optionsBuilder = new DbContextOptionsBuilder<CustomerContext>()
            .UseSqlServer("Server=127.0.0.1;Database=Customer;User Id=sa;Password=LetMeIn!2;TrustServerCertificate=True;");

        return new CustomerContext(optionsBuilder.Options);
    }
}
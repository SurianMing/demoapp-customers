using System.Reflection;
using Microsoft.EntityFrameworkCore;

namespace DemoApp.Services.Customers.Dependencies.Databases.Customers.Context;
using Models;

public class CustomerContext(
    DbContextOptions<CustomerContext> options
) : DbContext(options)
{
    public DbSet<Customer> Customers { get; set; }
    public DbSet<CustomerAddress> CustomerAddresses { get; set; }

    protected override void OnModelCreating(ModelBuilder modelBuilder)
        => modelBuilder.ApplyConfigurationsFromAssembly(
            Assembly.GetExecutingAssembly()
        );
}
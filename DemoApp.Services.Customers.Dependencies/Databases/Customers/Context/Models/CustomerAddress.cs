using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata.Builders;

namespace DemoApp.Services.Customers.Dependencies.Databases.Customers.Context.Models;

public class CustomerAddress
{
    public Guid CustomerAddressId { get; set; }
    public required Guid CustomerId { get; set; }
    public required string AddressLine1 { get; set; }
    public string? AddressLine2 { get; set; }
    public required string TownCity { get; set; }
    public required string County { get; set; }
    public required string PostCode { get; set; }
    public required string Country { get; set; }

    public Customer Customer { get; set; } = null!;
}

internal class CustomerAddressEntityTypeConfiguration : IEntityTypeConfiguration<CustomerAddress>
{
    public void Configure(EntityTypeBuilder<CustomerAddress> builder)
    {
        builder.ToTable("CustomerAddress");

        builder.HasKey(entity => entity.CustomerAddressId);

        builder.HasOne(entity => entity.Customer)
            .WithMany(foreign => foreign.Addresses)
            .HasForeignKey(entity => entity.CustomerId);

        builder.Property(entity => entity.AddressLine1)
            .HasMaxLength(100);
        builder.Property(entity => entity.AddressLine2)
            .HasMaxLength(100);
        builder.Property(entity => entity.TownCity)
            .HasMaxLength(100);
        builder.Property(entity => entity.County)
            .HasMaxLength(100);
        builder.Property(entity => entity.PostCode)
            .HasMaxLength(15);
        builder.Property(entity => entity.Country)
            .HasMaxLength(100);
    }
}
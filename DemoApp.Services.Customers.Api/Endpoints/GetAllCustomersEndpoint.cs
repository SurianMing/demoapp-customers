namespace DemoApp.Services.Customers.Api.Endpoints;
using Models;

public class GetAllCustomersEndpoint : IMinimalEndpoint
{
    public void MapEndpoint(WebApplication app) =>
        app.MapGet(
            "customer",
            async (
                [FromServices] ICustomerService customerService
            ) =>
            {
                var allCustomers = await customerService.GetAllCustomers();

                var response = allCustomers
                    .Select(customer => customer.ToModel());

                return Results.Ok(
                    response
                );
            }
        )
        .WithGroupName("Customers")
        .WithName("GetAllCustomers")
        .Produces<CustomerModel[]>();
}

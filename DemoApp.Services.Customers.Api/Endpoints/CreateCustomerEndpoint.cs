namespace DemoApp.Services.Customers.Api.Endpoints;
using Models;

public class CreateCustomerEndpoint : IMinimalEndpoint
{
    public void MapEndpoint(WebApplication app) =>
        app.MapPost(
            "customer",
            async (
                [FromBody] CreateCustomerModel newCustomer,
                [FromServices] ICustomerService customerService,
                HttpContext httpContext,
                LinkGenerator linkGenerator
            ) =>
            {
                var newCustomerIdentifier = await customerService.CreateCustomer(
                    newCustomer.FirstName,
                    newCustomer.Surname
                );

                var getCustomerLink = linkGenerator.GetUriByName(
                    httpContext,
                    "GetCustomerByIdentifier",
                    new { customerIdentifier = newCustomerIdentifier.ToString() }
                );

                return Results.Created(
                    getCustomerLink,
                    newCustomerIdentifier
                );
            }
        )
        // .WithGroupName("Customers")
        .WithName("CreateCustomer")
        .Produces<Guid>();
}

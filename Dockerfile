#See https://aka.ms/customizecontainer to learn how to customize your debug container and how Visual Studio uses this Dockerfile to build your images for faster debugging.

FROM mcr.microsoft.com/dotnet/aspnet:10.0 AS base
WORKDIR /app
EXPOSE 8080
EXPOSE 443

FROM mcr.microsoft.com/dotnet/sdk:10.0 AS build
WORKDIR /src
COPY . .

RUN curl -L https://raw.githubusercontent.com/Microsoft/artifacts-credprovider/master/helpers/installcredprovider.sh  | sh

ARG FEED_ACCESSTOKEN
ENV VSS_NUGET_EXTERNAL_FEED_ENDPOINTS="{\"endpointCredentials\": [{\"endpoint\":\"https://pkgs.dev.azure.com/surian-ming/_packaging/surian-ming/nuget/v3/index.json\", \"username\":\"docker\", \"password\":\"${FEED_ACCESSTOKEN}\"}]}"
RUN echo $VSS_NUGET_EXTERNAL_FEED_ENDPOINTS
RUN ls
RUN dotnet restore "DemoApp.Services.Customers.Api/DemoApp.Services.Customers.Api.csproj"

WORKDIR "/src/DemoApp.Services.Customers.Api"
RUN dotnet build "DemoApp.Services.Customers.Api.csproj" -c Release -o /app/build

FROM build AS publish
RUN dotnet publish "DemoApp.Services.Customers.Api.csproj" -c Release -o /app/publish /p:UseAppHost=false

FROM base AS final
WORKDIR /app
COPY --from=publish /app/publish .
ENTRYPOINT ["dotnet", "DemoApp.Services.Customers.Api.dll"]
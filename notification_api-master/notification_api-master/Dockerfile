#See https://aka.ms/containerfastmode to understand how Visual Studio uses this Dockerfile to build your images for faster debugging.

FROM mcr.microsoft.com/dotnet/aspnet:5.0 AS base
WORKDIR /app
EXPOSE 80
EXPOSE 443

FROM mcr.microsoft.com/dotnet/sdk:5.0 AS build
WORKDIR /src
COPY ["notification_api.csproj", "."]
COPY ["nuget.config", "."]
RUN echo -n | openssl s_client -connect $(dotnet nuget list source --configfile nuget.config --format short | sed -e 's|^[^/]*//||' -e 's|/.*$||') | openssl x509 > /usr/local/share/ca-certificates/packageSources.crt
RUN update-ca-certificates
RUN dotnet restore "./notification_api.csproj"
COPY . .
WORKDIR "/src/."
RUN dotnet build "notification_api.csproj" -c Release -o /app/build

FROM build AS publish
RUN dotnet publish "notification_api.csproj" -c Release -o /app/publish

FROM base AS final
WORKDIR /app
COPY --from=publish /app/publish .
ENTRYPOINT ["dotnet", "notification_api.dll"]
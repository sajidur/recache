# syntax=docker/dockerfile:1

FROM mcr.microsoft.com/dotnet/sdk:10.0 AS build
WORKDIR /src

# Restore first, on its own layer, so dependency downloads are
# cached across builds that only change source files.
COPY recache/recache.csproj recache/
RUN dotnet restore recache/recache.csproj

COPY recache/ recache/
RUN dotnet publish recache/recache.csproj \
    -c Release \
    -o /app/publish \
    --no-restore

FROM mcr.microsoft.com/dotnet/runtime:10.0 AS final
WORKDIR /app

COPY --from=build /app/publish .

EXPOSE 6380

# Runs as the non-root "app" user built into the base image rather
# than root — port 6380 doesn't need elevated privileges to bind.
USER app

ENTRYPOINT ["dotnet", "recache.dll"]

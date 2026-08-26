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

# /app is owned by root at this point (everything above ran as
# root). Create the log directory and hand it to the non-root user
# now, before switching — otherwise file logging fails with a
# permission error at startup.
RUN mkdir -p /app/logs && chown -R $APP_UID:$APP_UID /app/logs

EXPOSE 6380

# Runs as the non-root user built into the base image (identified
# by $APP_UID, provided automatically) rather than root — port 6380
# doesn't need elevated privileges to bind.
USER $APP_UID

ENTRYPOINT ["dotnet", "recache.dll"]

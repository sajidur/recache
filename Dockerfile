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
# root). Create the log and data directories and hand them to the
# non-root user now, before switching — otherwise file logging, the
# AOF and the cluster's nodes.conf fail with permission errors.
RUN mkdir -p /app/logs /data && chown -R $APP_UID:$APP_UID /app/logs /data

# Everything that must survive a container restart lives in /data:
# the append-only file and, in cluster mode, nodes.conf (this node's
# identity and vote history). Mount a volume there.
ENV Recache__AofPath=/data/appendonly.aof \
    Recache__ClusterConfigFile=/data/nodes.conf

VOLUME ["/data"]

# 6380: clients. 16380: cluster bus (node-to-node; only needed with
# Recache__ClusterEnabled=true, and should not be exposed publicly).
EXPOSE 6380 16380

# Runs as the non-root user built into the base image (identified
# by $APP_UID, provided automatically) rather than root — port 6380
# doesn't need elevated privileges to bind.
USER $APP_UID

ENTRYPOINT ["dotnet", "recache.dll"]

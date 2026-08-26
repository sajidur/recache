# Recache

A lightweight, Redis-compatible in-memory data store built for Windows developers who want the Redis development experience without the complexity of setting up Redis through WSL, Docker, or other workarounds.

It speaks RESP2 over TCP, so existing Redis clients (`redis-cli`, StackExchange.Redis, RedisInsight, etc.) can talk to it directly.

```
                         ┌──────────────────────┐
                         │      redis-cli        │
                         │ StackExchange.Redis  │
                         │     Other clients     │
                         └──────────┬───────────┘
                                    │
                             TCP / RESP2
                                    │
                    ┌───────────────▼────────────────┐
                    │       Recache Server            │
                    │                                 │
                    │  Connection Manager             │
                    │       │                         │
                    │       ▼                         │
                    │  RESP Reader                    │
                    │       │                         │
                    │       ▼                         │
                    │  Command Dispatcher             │
                    │       │                         │
                    │  ┌────┴────────────────────┐   │
                    │  │                         │   │
                    │  ▼                         ▼   │
                    │ String Commands        Hash/List│
                    │ GET/SET/DEL             Commands│
                    │                                 │
                    │       │                         │
                    │       ▼                         │
                    │     Cache Engine                │
                    │       │                         │
                    │  ┌────┼─────┬──────┐            │
                    │  ▼    ▼     ▼      ▼            │
                    │ TTL  LRU/LFU Memory Persistence │
                    │                                 │
                    │ Metrics / Health / Logging      │
                    └─────────────────────────────────┘
```

## Features

- **Data types**: String, Hash, List, Set, Sorted Set, and JSON (RedisJSON-compatible `JSON.*` commands with JSONPath support)
- **Expiration**: `TTL`, `PTTL`, `EXPIRE`, `PEXPIRE`, `PERSIST`
- **Auth**: password-based `AUTH`, matching real Redis's `NOAUTH`/`WRONGPASS` semantics
- **RedisInsight-compatible**: `SCAN`/`HSCAN`/`SSCAN`/`ZSCAN`, `INFO`, `CONFIG GET`, `CLIENT`, `MODULE LIST`
- Runs as a normal console app or as a Windows Service

## Running it

```
cd recache
cp appsettings.example.yaml appsettings.yaml   # then set a real password
dotnet run
```

## Installing as a Windows Service

```
.\scripts\install-service.ps1
```

(run from an elevated PowerShell prompt). See `installer/` for building a distributable installer via Inno Setup.

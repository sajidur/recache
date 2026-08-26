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

---

## For users

**1. Download** the latest installer from [Releases](https://github.com/sajidur/recache/releases/latest) — `RecacheSetup.exe`.

**2. Run it.** It's self-signed, not signed by a commercial CA, so Windows SmartScreen and/or your antivirus may flag it as coming from an unrecognized publisher — click "More info" → "Run anyway" if prompted. The installer needs administrator rights (UAC prompt) because it registers a Windows Service.

That's it — Recache installs to `Program Files\Recache`, registers itself as the **Recache** Windows Service, starts automatically, and will keep running in the background (including across reboots) from then on.

**3. Set a real password.** The installer ships with a placeholder password. Edit `appsettings.yaml` in the install folder (`Password: "..."` under `Recache:`), then restart the service so it picks up the change:

```powershell
notepad "C:\Program Files\Recache\appsettings.yaml"
Restart-Service Recache
```

**4. Connect** with any Redis client, pointed at `localhost:6380` with that password — `redis-cli -p 6380 -a yourpassword`, RedisInsight, StackExchange.Redis, etc.

**Managing the service** (PowerShell, run elevated for start/stop):
```powershell
Get-Service Recache          # check status
Restart-Service Recache      # apply a config change
Stop-Service Recache
Start-Service Recache
```

**To uninstall**, use "Apps & Features" / "Programs and Features" in Windows — it cleanly stops and removes the service along with the files.

---

## For developers

**Prerequisites**: [.NET 10 SDK](https://dotnet.microsoft.com/download).

**Clone and run:**
```powershell
git clone https://github.com/sajidur/recache.git
cd recache\recache
copy appsettings.example.yaml appsettings.yaml   # then set a real password
dotnet run
```

This starts an ordinary console app listening on `localhost:6380` — no service registration involved, `Ctrl+C` to stop.

### Project layout

```
recache/
  Commands/        RESP command dispatch (CommandDispatcher.cs)
  Storage/          Data types: CacheStore, RedisHash/List/Set/SortedSet/Json, JSONPath engine
  Protocol/         RESP2 wire format (reader/writer/value)
  Server/           TCP accept loop (RecacheServer, a BackgroundService) and per-connection handling
  Security/         AUTH / password validation, per-connection session state
  Configuration/    RecacheOptions bound from appsettings.yaml
scripts/            install-service.ps1 / uninstall-service.ps1 — for local dev/test service installs
installer/          Inno Setup script (recache.iss) that builds the distributable installer
```

### Building a Release build

```powershell
cd recache
dotnet publish -c Release -o publish
```

### Installing as a Windows Service (for local testing)

From an **elevated** PowerShell prompt:
```powershell
.\scripts\install-service.ps1     # publishes + registers + starts the service
.\scripts\uninstall-service.ps1   # stops + removes it
```

### Building the distributable installer

Requires [Inno Setup 6](https://jrsoftware.org/isinfo.php).

```powershell
cd recache
dotnet publish -c Release -o publish
& "C:\Program Files (x86)\Inno Setup 6\ISCC.exe" ..\installer\recache.iss
```

Produces `installer\Output\RecacheSetup.exe`. It's unsigned by default — if you have a code-signing certificate, sign both `recache\publish\recache.exe` and the compiled installer with `signtool.exe` before distributing; without a certificate from a recognized CA, expect SmartScreen/AV false positives on other machines (self-signing only helps machines that explicitly import your certificate).

### License

Apache License 2.0 — see [LICENSE](LICENSE).

using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using recache.Clustering;
using recache.Commands;
using recache.Configuration;
using recache.Security;
using recache.Server;
using recache.Storage;
using Serilog;
using System.Runtime;

// A cache holds millions of long-lived objects; a blocking full GC over
// them stalls every client for hundreds of milliseconds or more. This
// mode keeps full collections in the background (concurrent) unless
// memory is genuinely short.
GCSettings.LatencyMode = GCLatencyMode.SustainedLowLatency;

// Linux: run socket continuations directly on the epoll threads instead
// of handing each completion to the thread pool — measured: single-
// client latency drops from ~0.19 ms to ~0.06 ms (Redis parity). Safe
// because command handlers never block. The runtime reads this only
// from the environment, once, when the first socket is created — so it
// is set here, before anything opens one. (No effect on Windows, which
// uses IOCP.) An explicit setting from the environment wins.
if (!OperatingSystem.IsWindows() &&
    Environment.GetEnvironmentVariable("DOTNET_SYSTEM_NET_SOCKETS_INLINE_COMPLETIONS") is null)
{
    Environment.SetEnvironmentVariable("DOTNET_SYSTEM_NET_SOCKETS_INLINE_COMPLETIONS", "1");
}

var builder =
    Host.CreateApplicationBuilder(new HostApplicationBuilderSettings
    {
        Args = args,

        // The Service Control Manager launches Windows Services
        // with C:\Windows\System32 as the working directory, so the
        // default (CWD-relative) content root would never find the
        // config file. Anchor it to the executable's own folder.
        ContentRootPath = AppContext.BaseDirectory,
    });

// Optional: containers commonly configure entirely via environment
// variables (Recache__Password, Recache__RequireAuth, etc. — the
// Generic Host wires those up automatically) rather than a mounted
// or baked-in config file.
builder.Configuration.AddYamlFile(
    "appsettings.yaml",
    optional: true,
    reloadOnChange: true);

builder.Configuration.AddYamlFile(
    $"appsettings.{builder.Environment.EnvironmentName}.yaml",
    optional: true,
    reloadOnChange: true);

// The Generic Host registered environment variables and the command
// line before the YAML files above, which let a checked-out
// appsettings.yaml silently override them — `Recache__Port=6381`
// (or `--Recache:Port=6381`) did nothing for a second local cluster
// node. Re-adding both restores the usual precedence: command line >
// environment > YAML > built-in defaults.
builder.Configuration.AddEnvironmentVariables();
builder.Configuration.AddCommandLine(args);

builder.Services.Configure<RecacheOptions>(
    builder.Configuration.GetSection("Recache"));

builder.Services.AddSingleton(sp =>
{
    var options =
        sp.GetRequiredService<
            IOptions<RecacheOptions>>()
        .Value;

    return options;
});

// Logging needs to be set up before the host is built. Read
// straight from configuration here rather than waiting for the
// bound RecacheOptions singleton, which isn't available yet.
var recacheSection = builder.Configuration.GetSection("Recache");

var enableFileLogging =
    recacheSection.GetValue<bool?>("EnableFileLogging") ?? true;

var logRetentionDays =
    recacheSection.GetValue<int?>("LogRetentionDays") ?? 7;

var logDirectory =
    recacheSection.GetValue<string>("LogDirectory") ?? "logs";

var resolvedLogDirectory =
    Path.IsPathRooted(logDirectory)
        ? logDirectory
        : Path.Combine(builder.Environment.ContentRootPath, logDirectory);

// Millisecond timestamps: failure detection, elections and handovers
// happen on a sub-second scale, and reconstructing one from logs
// across several nodes needs that resolution.
const string consoleTemplate =
    "[{Timestamp:HH:mm:ss.fff} {Level:u3}] {Message:lj}{NewLine}{Exception}";

var loggerConfiguration =
    new LoggerConfiguration()
        .MinimumLevel.Information()
        .Enrich.FromLogContext()
        .WriteTo.Console(outputTemplate: consoleTemplate);

if (enableFileLogging)
{
    Directory.CreateDirectory(resolvedLogDirectory);

    loggerConfiguration = loggerConfiguration.WriteTo.File(
        Path.Combine(resolvedLogDirectory, "recache-.log"),
        outputTemplate:
            "{Timestamp:yyyy-MM-dd HH:mm:ss.fff zzz} [{Level:u3}] {Message:lj}{NewLine}{Exception}",
        rollingInterval: RollingInterval.Day,

        // One file per day, so this is a straightforward day-count
        // retention policy — files beyond this count are deleted
        // automatically as new ones are created.
        retainedFileCountLimit: logRetentionDays,

        // Several connections can be logging concurrently; `shared`
        // lets more than one process/instance safely append to the
        // same file if that ever applies, and avoids exclusive-lock
        // surprises in general.
        shared: true);
}

Log.Logger = loggerConfiguration.CreateLogger();

builder.Logging.ClearProviders();
builder.Logging.AddSerilog(Log.Logger, dispose: true);

builder.Services.AddSingleton<
    AuthenticationManager>();

// Everything stateful (store, replication stream, AOF, replication,
// cluster bus) is built and wired together by RecacheNode — several
// of those components depend on each other in both directions.
builder.Services.AddSingleton(sp =>
    new RecacheNode(
        sp.GetRequiredService<RecacheOptions>(),
        sp.GetRequiredService<AuthenticationManager>(),
        sp.GetRequiredService<ILoggerFactory>(),
        builder.Environment.ContentRootPath));

builder.Services.AddSingleton(sp =>
    sp.GetRequiredService<RecacheNode>().Dispatcher);

builder.Services.AddSingleton<ICacheStore>(sp =>
    sp.GetRequiredService<RecacheNode>().Store);

builder.Services.AddHostedService<
    RecacheServer>();

// Runs under the Windows Service Control Manager when launched that
// way (via `sc start`/the Services console); falls back to a plain
// console app when run interactively (`dotnet run`, double-click).
builder.Services.AddWindowsService(options =>
{
    options.ServiceName = "Recache";
});

try
{
    var host =
        builder.Build();

    var logger =
        host.Services.GetRequiredService<ILogger<Program>>();

    var startupOptions =
        host.Services.GetRequiredService<RecacheOptions>();

    var lifetime =
        host.Services.GetRequiredService<IHostApplicationLifetime>();

    host.Services.GetRequiredService<RecacheNode>()
        .Dispatcher.ShutdownRequested = lifetime.StopApplication;

    if (startupOptions.RequireAuth &&
        string.Equals(
            startupOptions.Password,
            "CHANGE_THIS",
            StringComparison.Ordinal))
    {
        logger.LogWarning(
            "Recache:Password is still set to the default " +
            "placeholder 'CHANGE_THIS'. Set a real password in " +
            "appsettings.yaml (or override it via configuration) " +
            "before exposing this server.");
    }
    else if (startupOptions.RequireAuth &&
        string.IsNullOrEmpty(startupOptions.Password))
    {
        logger.LogWarning(
            "Recache:RequireAuth is true but no password is set, " +
            "so AUTH is effectively disabled. Set Recache:Password " +
            "(or the Recache__Password environment variable) to " +
            "actually require authentication.");
    }

    if (enableFileLogging)
    {
        logger.LogInformation(
            "File logging enabled: {LogDirectory} " +
            "(retaining {RetentionDays} day(s))",
            resolvedLogDirectory,
            logRetentionDays);
    }

    // host.RunAsync() drives the IHostedService lifecycle and fires
    // ApplicationStarted once RecacheServer.StartAsync returns —
    // that's the signal WindowsServiceLifetime needs to report
    // "running" back to the Service Control Manager.
    await host.RunAsync();
}
catch (Exception ex)
{
    Log.Fatal(ex, "Recache terminated unexpectedly");
    throw;
}
finally
{
    Log.CloseAndFlush();
}

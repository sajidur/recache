using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using recache.Commands;
using recache.Configuration;
using recache.Security;
using recache.Server;
using recache.Storage;
using Serilog;

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

var loggerConfiguration =
    new LoggerConfiguration()
        .MinimumLevel.Information()
        .Enrich.FromLogContext()
        .WriteTo.Console();

if (enableFileLogging)
{
    Directory.CreateDirectory(resolvedLogDirectory);

    loggerConfiguration = loggerConfiguration.WriteTo.File(
        Path.Combine(resolvedLogDirectory, "recache-.log"),
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

builder.Services.AddSingleton<ICacheStore,
    CacheStore>();

builder.Services.AddSingleton<
    AuthenticationManager>();

builder.Services.AddSingleton<
    CommandDispatcher>();

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

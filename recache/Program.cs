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

// host.RunAsync() drives the IHostedService lifecycle and fires
// ApplicationStarted once RecacheServer.StartAsync returns — that's
// the signal WindowsServiceLifetime needs to report "running" back
// to the Service Control Manager.
await host.RunAsync();

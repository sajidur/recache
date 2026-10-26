using System.Diagnostics;
using System.Net;
using System.Net.Sockets;
using System.Text;

namespace recache.Tests.Infrastructure;

// A real recache server process — so tests can crash it (hard kill,
// no cleanup, exactly like a power cut or OOM kill), restart it on
// the same data directory, and talk to it over TCP like any client.
public sealed class ServerProcess : IAsyncDisposable
{
    private static readonly object CopyLock = new();
    private static string? s_binaryDirectory;
    private static readonly Random PortRandom = new();

    private readonly Dictionary<string, string> _settings;
    private readonly StringBuilder _output = new();
    private Process? _process;

    static ServerProcess()
    {
        // Redirected stdout/stderr pipes are synchronous on Windows, so
        // each one parks a thread-pool thread in a blocking read. With
        // a dozen servers that exhausts the pool's minimum and stalls
        // the test's own I/O for ~500 ms at a time (the pool's thread
        // injection delay) — latency that would be blamed on the server.
        ThreadPool.GetMinThreads(out var worker, out var io);
        ThreadPool.SetMinThreads(Math.Max(worker, 128), Math.Max(io, 128));
    }

    private ServerProcess(string name, int port, string dataDirectory, Dictionary<string, string> settings)
    {
        Name = name;
        Port = port;
        DataDirectory = dataDirectory;
        _settings = settings;
    }

    public string Name { get; }

    public int Port { get; }

    public int BusPort => Port + 10000;

    public string DataDirectory { get; }

    public string AofPath => Path.Combine(DataDirectory, "appendonly.aof");

    public bool IsRunning => _process is { HasExited: false };

    public string Log
    {
        get
        {
            lock (_output)
                return _output.ToString();
        }
    }

    public static async Task<ServerProcess> StartAsync(
        string name,
        IDictionary<string, string>? settings = null,
        int? port = null)
    {
        var dataDirectory = Path.Combine(
            Path.GetTempPath(),
            "recache-tests",
            $"{name}-{Guid.NewGuid():N}");

        Directory.CreateDirectory(dataDirectory);

        var server = new ServerProcess(
            name,
            port ?? AllocatePortPair(),
            dataDirectory,
            new Dictionary<string, string>(settings ?? new Dictionary<string, string>()));

        await server.StartProcessAsync();
        return server;
    }

    public Task<TestClient> ConnectAsync()
        => TestClient.ConnectAsync(Port);

    // Starts the process again on the same port and data directory.
    public Task RestartAsync() => StartProcessAsync();

    // Hard kill: no shutdown hooks, no final flush, no handover.
    public void Kill()
    {
        if (_process is { HasExited: false } process)
        {
            process.Kill(entireProcessTree: true);
            process.WaitForExit(10_000);
        }
    }

    // Graceful stop through SHUTDOWN (the same path as a service stop).
    public async Task ShutdownAsync(TimeSpan? timeout = null)
    {
        if (_process is not { HasExited: false } process)
            return;

        try
        {
            await using var client = await ConnectAsync();
            await client.SendAsync("SHUTDOWN");
        }
        catch (Exception ex) when (ex is IOException or SocketException)
        {
        }

        using var cts = new CancellationTokenSource(timeout ?? TimeSpan.FromSeconds(30));

        try
        {
            await process.WaitForExitAsync(cts.Token);
        }
        catch (OperationCanceledException)
        {
            throw new TimeoutException($"{Name} did not shut down in time.\n{Log}");
        }
    }

    private async Task StartProcessAsync()
    {
        var arguments = new List<string>
        {
            Path.Combine(BinaryDirectory, "recache.dll"),
            $"--Recache:Host=127.0.0.1",
            $"--Recache:Port={Port}",
            "--Recache:RequireAuth=false",
            "--Recache:EnableFileLogging=false",
            $"--Recache:AofPath={AofPath}",
            $"--Recache:ClusterConfigFile={Path.Combine(DataDirectory, "nodes.conf")}",
        };

        foreach (var (key, value) in _settings)
            arguments.Add($"--Recache:{key}={value}");

        var startInfo = new ProcessStartInfo("dotnet")
        {
            RedirectStandardOutput = true,
            RedirectStandardError = true,
            UseShellExecute = false,
            CreateNoWindow = true,
            WorkingDirectory = DataDirectory,
        };

        foreach (var argument in arguments)
            startInfo.ArgumentList.Add(argument);

        var process = new Process { StartInfo = startInfo, EnableRaisingEvents = true };

        process.OutputDataReceived += (_, e) => Append(e.Data);
        process.ErrorDataReceived += (_, e) => Append(e.Data);

        process.Start();
        process.BeginOutputReadLine();
        process.BeginErrorReadLine();

        _process = process;

        await WaitUntilReadyAsync(TimeSpan.FromSeconds(30));
    }

    private void Append(string? line)
    {
        if (line is null)
            return;

        lock (_output)
            _output.AppendLine(line);
    }

    private async Task WaitUntilReadyAsync(TimeSpan timeout)
    {
        var deadline = DateTime.UtcNow + timeout;

        while (DateTime.UtcNow < deadline)
        {
            if (_process!.HasExited)
                throw new InvalidOperationException($"{Name} exited during startup.\n{Log}");

            try
            {
                await using var client = await ConnectAsync();
                var reply = await client.SendAsync("PING");

                if (reply.AsString() == "PONG")
                    return;
            }
            catch (Exception ex) when (ex is SocketException or IOException or OperationCanceledException)
            {
            }

            await Task.Delay(100);
        }

        throw new TimeoutException($"{Name} did not become ready.\n{Log}");
    }

    // Server binaries copied once per test run to a folder with no
    // appsettings.yaml, so a developer's local config can't leak in.
    private static string BinaryDirectory
    {
        get
        {
            lock (CopyLock)
            {
                if (s_binaryDirectory is not null)
                    return s_binaryDirectory;

                var testBin = AppContext.BaseDirectory.TrimEnd(Path.DirectorySeparatorChar);
                var configuration = new DirectoryInfo(testBin).Parent!.Name;
                var repoRoot = Path.GetFullPath(Path.Combine(testBin, "..", "..", "..", "..", ".."));
                var source = Path.Combine(repoRoot, "recache", "bin", configuration, "net10.0");

                if (!File.Exists(Path.Combine(source, "recache.dll")))
                    throw new FileNotFoundException($"Server build not found in {source}");

                var target = Path.Combine(
                    Path.GetTempPath(),
                    "recache-tests",
                    $"bin-{Environment.ProcessId}");

                CopyDirectory(source, target);
                s_binaryDirectory = target;

                AppDomain.CurrentDomain.ProcessExit += (_, _) =>
                {
                    try
                    {
                        Directory.Delete(target, recursive: true);
                    }
                    catch (Exception ex) when (ex is IOException or UnauthorizedAccessException)
                    {
                    }
                };

                return target;
            }
        }
    }

    private static void CopyDirectory(string source, string target)
    {
        Directory.CreateDirectory(target);

        foreach (var file in Directory.GetFiles(source))
        {
            var name = Path.GetFileName(file);

            if (name.StartsWith("appsettings", StringComparison.OrdinalIgnoreCase))
                continue;

            File.Copy(file, Path.Combine(target, name), overwrite: true);
        }

        foreach (var directory in Directory.GetDirectories(source))
        {
            var name = Path.GetFileName(directory);

            if (name is "logs" or "data")
                continue;

            CopyDirectory(directory, Path.Combine(target, name));
        }
    }

    // A client port whose cluster-bus port (port + 10000) is free too.
    private static int AllocatePortPair()
    {
        lock (PortRandom)
        {
            for (var attempt = 0; attempt < 200; attempt++)
            {
                var port = PortRandom.Next(20000, 29000);

                if (IsFree(port) && IsFree(port + 10000))
                    return port;
            }
        }

        throw new InvalidOperationException("No free port pair found.");
    }

    private static bool IsFree(int port)
    {
        try
        {
            var listener = new TcpListener(IPAddress.Loopback, port);
            listener.Start();
            listener.Stop();
            return true;
        }
        catch (SocketException)
        {
            return false;
        }
    }

    public ValueTask DisposeAsync()
    {
        Kill();

        try
        {
            Directory.Delete(DataDirectory, recursive: true);
        }
        catch (Exception ex) when (ex is IOException or UnauthorizedAccessException)
        {
        }

        return ValueTask.CompletedTask;
    }
}

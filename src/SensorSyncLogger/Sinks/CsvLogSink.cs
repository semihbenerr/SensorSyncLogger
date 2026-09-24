using System.Buffers;
using System.Globalization;
using System.Text;
using SensorSyncLogger.Writing;

namespace SensorSyncLogger.Sinks;

/// <summary>Options of <see cref="CsvLogSink"/>.</summary>
public sealed class CsvLogSinkOptions
{
    /// <summary>Field separator. Defaults to <c>','</c>; use <c>';'</c> for Excel in locales with a decimal comma.</summary>
    public char Delimiter { get; set; } = ',';

    /// <summary>Append to an existing file instead of overwriting it (the header is written only for new/empty files).</summary>
    public bool Append { get; set; }

    /// <summary>Adds an <c>elapsed_s</c> column (seconds since session start) — convenient for plotting. Defaults to <c>true</c>.</summary>
    public bool IncludeElapsedSeconds { get; set; } = true;

    /// <summary>Adds the unit to aligned column headers, e.g. <c>LoadCell [N]</c>. Defaults to <c>true</c>.</summary>
    public bool IncludeUnitsInHeader { get; set; } = true;

    /// <summary>Forces data to the physical disk on every flush (slower, survives power loss). Defaults to <c>false</c>.</summary>
    public bool FlushToDisk { get; set; }
}

/// <summary>
/// Streams rows to a CSV file (RFC 4180 quoting, invariant culture, UTF-8 without BOM).
/// Memory use is independent of the file size: each batch is formatted into a pooled buffer and written once.
/// </summary>
/// <remarks>
/// Raw mode columns: <c>timestamp_utc, [elapsed_s,] channel, value</c>.
/// Aligned mode columns: <c>timestamp_utc, [elapsed_s,] &lt;channel 1&gt;, &lt;channel 2&gt;, …</c>.
/// Timestamps are ISO 8601 with 100 ns precision; <see cref="double.NaN"/> values are written as empty fields.
/// </remarks>
public sealed class CsvLogSink : ILogSink
{
    private const int FileBufferSize = 64 * 1024;

    private readonly CsvLogSinkOptions _options;
    private readonly ArrayBufferWriter<char> _buffer = new(FileBufferSize);
    private FileStream? _stream;
    private StreamWriter? _writer;
    private LogSchema? _schema;
    private string[] _channelFields = Array.Empty<string>();
    private bool _disposed;

    /// <summary>Creates the sink. The file is created when the session starts.</summary>
    public CsvLogSink(string filePath, CsvLogSinkOptions? options = null)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(filePath);
        FilePath = Path.GetFullPath(filePath);
        _options = options ?? new CsvLogSinkOptions();
        if (_options.Delimiter is '"' or '\r' or '\n')
            throw new ArgumentException("The delimiter cannot be a quote or a line break.", nameof(options));
    }

    /// <summary>Full path of the CSV file.</summary>
    public string FilePath { get; }

    /// <inheritdoc />
    public string Name => $"CSV {Path.GetFileName(FilePath)}";

    /// <inheritdoc />
    public ValueTask OpenAsync(LogSchema schema, CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        if (_writer is not null)
            throw new InvalidOperationException("The sink is already open.");

        _schema = schema ?? throw new ArgumentNullException(nameof(schema));
        _channelFields = schema.Channels.Select(c => Escape(c.Name)).ToArray();

        string? directory = Path.GetDirectoryName(FilePath);
        if (!string.IsNullOrEmpty(directory))
            Directory.CreateDirectory(directory);

        _stream = new FileStream(FilePath, _options.Append ? FileMode.Append : FileMode.Create, FileAccess.Write,
            FileShare.Read, FileBufferSize, FileOptions.SequentialScan);
        _writer = new StreamWriter(_stream, new UTF8Encoding(encoderShouldEmitUTF8Identifier: false), FileBufferSize);

        if (_stream.Length == 0)
        {
            WriteHeader(schema);
            _writer.Flush();
        }

        return ValueTask.CompletedTask;
    }

    /// <inheritdoc />
    public async ValueTask WriteBatchAsync(IReadOnlyList<LogRow> rows, CancellationToken cancellationToken = default)
    {
        var writer = _writer ?? throw new InvalidOperationException("The sink is not open.");
        var schema = _schema!;

        _buffer.Clear();
        for (int i = 0; i < rows.Count; i++)
            FormatRow(rows[i], schema);

        // One write per batch keeps partial lines out of the file if formatting fails halfway.
        await writer.WriteAsync(_buffer.WrittenMemory, cancellationToken).ConfigureAwait(false);
        await writer.FlushAsync(cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public async ValueTask FlushAsync(CancellationToken cancellationToken = default)
    {
        if (_writer is null)
            return;
        await _writer.FlushAsync(cancellationToken).ConfigureAwait(false);
        if (_options.FlushToDisk)
            _stream!.Flush(flushToDisk: true);
    }

    /// <inheritdoc />
    public async ValueTask DisposeAsync()
    {
        if (_disposed)
            return;
        _disposed = true;
        if (_writer is not null)
        {
            await _writer.FlushAsync().ConfigureAwait(false);
            if (_options.FlushToDisk)
                _stream!.Flush(flushToDisk: true);
            await _writer.DisposeAsync().ConfigureAwait(false);
        }

        _writer = null;
        _stream = null;
    }

    private void WriteHeader(LogSchema schema)
    {
        var sb = new StringBuilder("timestamp_utc");
        if (_options.IncludeElapsedSeconds)
            sb.Append(_options.Delimiter).Append("elapsed_s");

        if (schema.Mode == LogMode.Raw)
        {
            sb.Append(_options.Delimiter).Append("channel").Append(_options.Delimiter).Append("value");
        }
        else
        {
            foreach (var channel in schema.Channels)
            {
                string title = _options.IncludeUnitsInHeader && !string.IsNullOrWhiteSpace(channel.Unit)
                    ? $"{channel.Name} [{channel.Unit}]"
                    : channel.Name;
                sb.Append(_options.Delimiter).Append(Escape(title));
            }
        }

        _writer!.WriteLine(sb.ToString());
    }

    private void FormatRow(in LogRow row, LogSchema schema)
    {
        AppendTimestamp(row.Timestamp);
        if (_options.IncludeElapsedSeconds)
        {
            Append(_options.Delimiter);
            AppendNumber((row.Timestamp - schema.SessionStartUtc).TotalSeconds, "0.0######");
        }

        if (row.Values is { } values)
        {
            for (int c = 0; c < values.Length; c++)
            {
                Append(_options.Delimiter);
                AppendNumber(values[c]);
            }
        }
        else
        {
            Append(_options.Delimiter);
            Append(row.ChannelIndex < _channelFields.Length ? _channelFields[row.ChannelIndex] : row.ChannelIndex.ToString(CultureInfo.InvariantCulture));
            Append(_options.Delimiter);
            AppendNumber(row.Value);
        }

        Append('\n');
    }

    private void AppendTimestamp(DateTime timestamp)
    {
        var span = _buffer.GetSpan(32);
        timestamp.TryFormat(span, out int written, "yyyy-MM-dd'T'HH:mm:ss.fffffff'Z'", CultureInfo.InvariantCulture);
        _buffer.Advance(written);
    }

    private void AppendNumber(double value, string? format = null)
    {
        if (!double.IsFinite(value))
            return; // NaN / infinity → empty field
        var span = _buffer.GetSpan(32);
        value.TryFormat(span, out int written, format, CultureInfo.InvariantCulture);
        _buffer.Advance(written);
    }

    private void Append(char c)
    {
        _buffer.GetSpan(1)[0] = c;
        _buffer.Advance(1);
    }

    private void Append(string text)
    {
        text.AsSpan().CopyTo(_buffer.GetSpan(text.Length));
        _buffer.Advance(text.Length);
    }

    private string Escape(string field)
    {
        bool needsQuotes = field.IndexOfAny(new[] { _options.Delimiter, '"', '\r', '\n' }) >= 0;
        return needsQuotes ? "\"" + field.Replace("\"", "\"\"", StringComparison.Ordinal) + "\"" : field;
    }
}

using System.Globalization;
using Microsoft.Data.Sqlite;
using SensorSyncLogger.Writing;

namespace SensorSyncLogger.Sinks;

/// <summary>Options of <see cref="SqliteLogSink"/>.</summary>
public sealed class SqliteLogSinkOptions
{
    /// <summary>Table for raw rows. Defaults to <c>samples</c>.</summary>
    public string RawTableName { get; set; } = "samples";

    /// <summary>Table for aligned rows (one column per channel). Defaults to <c>aligned_samples</c>.</summary>
    public string AlignedTableName { get; set; } = "aligned_samples";

    /// <summary>Creates an index on (session_id, ts_unix_us) for fast time-range queries. Defaults to <c>true</c>.</summary>
    public bool CreateIndexes { get; set; } = true;

    /// <summary>
    /// Uses <c>PRAGMA synchronous=FULL</c> instead of <c>NORMAL</c>. NORMAL (with WAL) never corrupts the database and
    /// survives application crashes; FULL additionally survives power loss at the cost of write speed.
    /// </summary>
    public bool SynchronousFull { get; set; }

    /// <summary>How long to wait when another connection holds the write lock. Defaults to 5 s.</summary>
    public TimeSpan BusyTimeout { get; set; } = TimeSpan.FromSeconds(5);
}

/// <summary>
/// Writes rows to a SQLite database with <see cref="Microsoft.Data.Sqlite"/>: every batch is inserted inside one
/// transaction with a prepared, parameterised command (tens of thousands of rows per second on ordinary disks).
/// </summary>
/// <remarks>
/// <para>Tables (created if missing, so several sessions can share one file):</para>
/// <list type="bullet">
/// <item><c>sessions(session_id, started_utc, mode, align_interval_ms, machine_name)</c></item>
/// <item><c>channels(session_id, channel_index, name, unit, sample_rate_hz)</c></item>
/// <item>raw mode: <c>samples(session_id, ts_unix_us, channel_index, value)</c> and the readable view <c>samples_view</c></item>
/// <item>aligned mode: <c>aligned_samples(session_id, ts_unix_us, "&lt;channel&gt;"…)</c> — a REAL column per channel,
/// added automatically when a later session introduces new channels</item>
/// </list>
/// <para>Timestamps are stored as integer microseconds since the Unix epoch (UTC); <see cref="double.NaN"/> is stored as NULL.
/// The database uses WAL journaling, so it can be read while logging is in progress.</para>
/// </remarks>
public sealed class SqliteLogSink : ILogSink
{
    private static readonly string[] ReservedAlignedColumns = { "session_id", "ts_unix_us" };

    private readonly SqliteLogSinkOptions _options;
    private SqliteConnection? _connection;
    private SqliteCommand? _insert;
    private SqliteParameter[] _valueParameters = Array.Empty<SqliteParameter>();
    private SqliteParameter? _timestampParameter;
    private SqliteParameter? _channelParameter;
    private SqliteParameter? _valueParameter;
    private bool _disposed;

    /// <summary>Creates the sink. The database is opened when the session starts.</summary>
    public SqliteLogSink(string databasePath, SqliteLogSinkOptions? options = null)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(databasePath);
        DatabasePath = Path.GetFullPath(databasePath);
        _options = options ?? new SqliteLogSinkOptions();
        ArgumentException.ThrowIfNullOrWhiteSpace(_options.RawTableName, nameof(options));
        ArgumentException.ThrowIfNullOrWhiteSpace(_options.AlignedTableName, nameof(options));
    }

    /// <summary>Full path of the database file.</summary>
    public string DatabasePath { get; }

    /// <summary>Id of this session in the <c>sessions</c> table (after <see cref="OpenAsync"/>).</summary>
    public long SessionId { get; private set; }

    /// <inheritdoc />
    public string Name => $"SQLite {Path.GetFileName(DatabasePath)}";

    /// <inheritdoc />
    public async ValueTask OpenAsync(LogSchema schema, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(schema);
        ObjectDisposedException.ThrowIf(_disposed, this);
        if (_connection is not null)
            throw new InvalidOperationException("The sink is already open.");

        if (schema.Mode == LogMode.Aligned)
        {
            var clash = schema.Channels.FirstOrDefault(c => ReservedAlignedColumns.Contains(c.Name, StringComparer.OrdinalIgnoreCase));
            if (clash is not null)
                throw new ArgumentException($"Channel name '{clash.Name}' is reserved in the aligned table.", nameof(schema));
        }

        string? directory = Path.GetDirectoryName(DatabasePath);
        if (!string.IsNullOrEmpty(directory))
            Directory.CreateDirectory(directory);

        var connectionString = new SqliteConnectionStringBuilder
        {
            DataSource = DatabasePath,
            Mode = SqliteOpenMode.ReadWriteCreate,
            Pooling = false, // release the file as soon as the sink is disposed
            DefaultTimeout = (int)Math.Ceiling(_options.BusyTimeout.TotalSeconds),
        }.ToString();

        var connection = new SqliteConnection(connectionString);
        try
        {
            await connection.OpenAsync(cancellationToken).ConfigureAwait(false);
            Execute(connection, null,
                "PRAGMA journal_mode=WAL;" +
                $"PRAGMA synchronous={(_options.SynchronousFull ? "FULL" : "NORMAL")};" +
                $"PRAGMA busy_timeout={(long)_options.BusyTimeout.TotalMilliseconds};");

            using (var tx = connection.BeginTransaction())
            {
                CreateCommonTables(connection, tx);
                SessionId = InsertSession(connection, tx, schema);
                InsertChannels(connection, tx, schema);
                if (schema.Mode == LogMode.Raw)
                    CreateRawTable(connection, tx);
                else
                    CreateAlignedTable(connection, tx, schema);
                tx.Commit();
            }

            _connection = connection;
            _insert = schema.Mode == LogMode.Raw ? PrepareRawInsert(connection) : PrepareAlignedInsert(connection, schema);
        }
        catch
        {
            await connection.DisposeAsync().ConfigureAwait(false);
            throw;
        }
    }

    /// <inheritdoc />
    public ValueTask WriteBatchAsync(IReadOnlyList<LogRow> rows, CancellationToken cancellationToken = default)
    {
        var connection = _connection ?? throw new InvalidOperationException("The sink is not open.");
        var insert = _insert!;

        using var tx = connection.BeginTransaction();
        insert.Transaction = tx;
        for (int i = 0; i < rows.Count; i++)
        {
            if ((i & 1023) == 0)
                cancellationToken.ThrowIfCancellationRequested();

            var row = rows[i];
            _timestampParameter!.Value = ToUnixMicroseconds(row.Timestamp);
            if (row.Values is { } values)
            {
                for (int c = 0; c < _valueParameters.Length; c++)
                    _valueParameters[c].Value = c < values.Length ? ToDb(values[c]) : DBNull.Value;
            }
            else
            {
                _channelParameter!.Value = row.ChannelIndex;
                _valueParameter!.Value = ToDb(row.Value);
            }

            insert.ExecuteNonQuery();
        }

        tx.Commit();
        insert.Transaction = null;
        return ValueTask.CompletedTask;
    }

    /// <inheritdoc />
    public ValueTask FlushAsync(CancellationToken cancellationToken = default)
    {
        // Every batch is committed; a passive checkpoint moves WAL content into the main file without blocking readers.
        if (_connection is not null)
            Execute(_connection, null, "PRAGMA wal_checkpoint(PASSIVE);");
        return ValueTask.CompletedTask;
    }

    /// <inheritdoc />
    public async ValueTask DisposeAsync()
    {
        if (_disposed)
            return;
        _disposed = true;

        if (_insert is not null)
            await _insert.DisposeAsync().ConfigureAwait(false);
        if (_connection is not null)
        {
            try
            {
                Execute(_connection, null, "PRAGMA wal_checkpoint(TRUNCATE);");
            }
            catch (SqliteException)
            {
                // Another connection may still be reading; the WAL is merged by the next checkpoint.
            }

            await _connection.DisposeAsync().ConfigureAwait(false);
        }

        _insert = null;
        _connection = null;
    }

    private static long ToUnixMicroseconds(DateTime timestamp) =>
        (DateTime.SpecifyKind(timestamp, DateTimeKind.Utc).Ticks - DateTime.UnixEpoch.Ticks) / 10;

    private static object ToDb(double value) => double.IsFinite(value) ? value : DBNull.Value;

    private static string Quote(string identifier) => "\"" + identifier.Replace("\"", "\"\"", StringComparison.Ordinal) + "\"";

    private static void Execute(SqliteConnection connection, SqliteTransaction? tx, string sql)
    {
        using var cmd = connection.CreateCommand();
        cmd.Transaction = tx;
        cmd.CommandText = sql;
        cmd.ExecuteNonQuery();
    }

    private static void CreateCommonTables(SqliteConnection connection, SqliteTransaction tx) => Execute(connection, tx, """
        CREATE TABLE IF NOT EXISTS sessions (
            session_id        INTEGER PRIMARY KEY AUTOINCREMENT,
            started_utc       TEXT NOT NULL,
            mode              TEXT NOT NULL,
            align_interval_ms REAL,
            machine_name      TEXT
        );
        CREATE TABLE IF NOT EXISTS channels (
            session_id     INTEGER NOT NULL REFERENCES sessions(session_id),
            channel_index  INTEGER NOT NULL,
            name           TEXT NOT NULL,
            unit           TEXT,
            sample_rate_hz REAL,
            PRIMARY KEY (session_id, channel_index)
        );
        """);

    private static long InsertSession(SqliteConnection connection, SqliteTransaction tx, LogSchema schema)
    {
        using var cmd = connection.CreateCommand();
        cmd.Transaction = tx;
        cmd.CommandText = """
            INSERT INTO sessions (started_utc, mode, align_interval_ms, machine_name) VALUES ($start, $mode, $interval, $machine);
            SELECT last_insert_rowid();
            """;
        cmd.Parameters.AddWithValue("$start", schema.SessionStartUtc.ToString("O", CultureInfo.InvariantCulture));
        cmd.Parameters.AddWithValue("$mode", schema.Mode.ToString());
        cmd.Parameters.AddWithValue("$interval", schema.AlignInterval is { } i ? i.TotalMilliseconds : DBNull.Value);
        cmd.Parameters.AddWithValue("$machine", Environment.MachineName);
        return Convert.ToInt64(cmd.ExecuteScalar(), CultureInfo.InvariantCulture);
    }

    private void InsertChannels(SqliteConnection connection, SqliteTransaction tx, LogSchema schema)
    {
        using var cmd = connection.CreateCommand();
        cmd.Transaction = tx;
        cmd.CommandText = "INSERT INTO channels (session_id, channel_index, name, unit, sample_rate_hz) VALUES ($s, $i, $n, $u, $r);";
        var s = cmd.Parameters.Add("$s", SqliteType.Integer);
        var i = cmd.Parameters.Add("$i", SqliteType.Integer);
        var n = cmd.Parameters.Add("$n", SqliteType.Text);
        var u = cmd.Parameters.Add("$u", SqliteType.Text);
        var r = cmd.Parameters.Add("$r", SqliteType.Real);
        for (int index = 0; index < schema.Channels.Count; index++)
        {
            var channel = schema.Channels[index];
            s.Value = SessionId;
            i.Value = index;
            n.Value = channel.Name;
            u.Value = (object?)channel.Unit ?? DBNull.Value;
            r.Value = channel.SampleRateHz;
            cmd.ExecuteNonQuery();
        }
    }

    private void CreateRawTable(SqliteConnection connection, SqliteTransaction tx)
    {
        string table = Quote(_options.RawTableName);
        Execute(connection, tx, $"""
            CREATE TABLE IF NOT EXISTS {table} (
                session_id    INTEGER NOT NULL,
                ts_unix_us    INTEGER NOT NULL,
                channel_index INTEGER NOT NULL,
                value         REAL
            );
            CREATE VIEW IF NOT EXISTS {Quote(_options.RawTableName + "_view")} AS
                SELECT s.session_id,
                       strftime('%Y-%m-%dT%H:%M:%f', s.ts_unix_us / 1000000.0, 'unixepoch') || 'Z' AS timestamp_utc,
                       c.name AS channel, c.unit AS unit, s.value AS value
                FROM {table} s
                JOIN channels c ON c.session_id = s.session_id AND c.channel_index = s.channel_index;
            """);
        if (_options.CreateIndexes)
            Execute(connection, tx, $"CREATE INDEX IF NOT EXISTS {Quote("ix_" + _options.RawTableName + "_session_ts")} ON {table} (session_id, ts_unix_us);");
    }

    private void CreateAlignedTable(SqliteConnection connection, SqliteTransaction tx, LogSchema schema)
    {
        string table = Quote(_options.AlignedTableName);
        Execute(connection, tx, $"CREATE TABLE IF NOT EXISTS {table} (session_id INTEGER NOT NULL, ts_unix_us INTEGER NOT NULL);");

        var existing = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        using (var info = connection.CreateCommand())
        {
            info.Transaction = tx;
            info.CommandText = $"PRAGMA table_info({table});";
            using var reader = info.ExecuteReader();
            while (reader.Read())
                existing.Add(reader.GetString(1));
        }

        foreach (var channel in schema.Channels.Where(c => !existing.Contains(c.Name)))
            Execute(connection, tx, $"ALTER TABLE {table} ADD COLUMN {Quote(channel.Name)} REAL;");

        if (_options.CreateIndexes)
            Execute(connection, tx, $"CREATE INDEX IF NOT EXISTS {Quote("ix_" + _options.AlignedTableName + "_session_ts")} ON {table} (session_id, ts_unix_us);");
    }

    private SqliteCommand PrepareRawInsert(SqliteConnection connection)
    {
        var cmd = connection.CreateCommand();
        cmd.CommandText = $"INSERT INTO {Quote(_options.RawTableName)} (session_id, ts_unix_us, channel_index, value) VALUES ($s, $t, $c, $v);";
        cmd.Parameters.Add("$s", SqliteType.Integer).Value = SessionId;
        _timestampParameter = cmd.Parameters.Add("$t", SqliteType.Integer);
        _channelParameter = cmd.Parameters.Add("$c", SqliteType.Integer);
        _valueParameter = cmd.Parameters.Add("$v", SqliteType.Real);
        cmd.Prepare();
        return cmd;
    }

    private SqliteCommand PrepareAlignedInsert(SqliteConnection connection, LogSchema schema)
    {
        var cmd = connection.CreateCommand();
        string columns = string.Join(", ", schema.Channels.Select(c => Quote(c.Name)));
        string values = string.Join(", ", schema.Channels.Select((_, i) => "$p" + i.ToString(CultureInfo.InvariantCulture)));
        cmd.CommandText = $"INSERT INTO {Quote(_options.AlignedTableName)} (session_id, ts_unix_us, {columns}) VALUES ($s, $t, {values});";
        cmd.Parameters.Add("$s", SqliteType.Integer).Value = SessionId;
        _timestampParameter = cmd.Parameters.Add("$t", SqliteType.Integer);
        _valueParameters = schema.Channels
            .Select((_, i) => cmd.Parameters.Add("$p" + i.ToString(CultureInfo.InvariantCulture), SqliteType.Real))
            .ToArray();
        cmd.Prepare();
        return cmd;
    }
}

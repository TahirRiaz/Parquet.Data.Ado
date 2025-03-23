using System;
using System.Data;
using System.Data.Common;
using System.IO;
using System.Linq;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using Parquet;

namespace Parquet.Data.Reader
{
    /// <summary>
    /// Represents a connection to a Parquet data source.
    /// </summary>
    public class ParquetConnection : DbConnection
    {
        private string _connectionString;
        private ConnectionState _state;
        private ParquetReader? _reader;
        private Stream? _fileStream; // Track the file stream for proper disposal
        private bool _disposed;

        // NEW: Store the connection timeout internally (default 15).
        private int _connectionTimeout = 15;

        /// <summary>
        /// Initializes a new instance of the ParquetConnection class.
        /// </summary>
        public ParquetConnection()
        {
            _connectionString = string.Empty;
            _state = ConnectionState.Closed;
        }

        /// <summary>
        /// Initializes a new instance of the ParquetConnection class with the specified connection string.
        /// </summary>
        /// <param name="connectionString">The connection string to use.</param>
        public ParquetConnection(string connectionString) : this()
        {
            _connectionString = connectionString ?? throw new ArgumentNullException(nameof(connectionString));
            _connectionTimeout = ExtractTimeoutFromConnectionString(_connectionString, _connectionTimeout);
        }

#nullable disable

        /// <summary>
        /// Gets or sets the string used to open the connection.
        /// </summary>
        public override string ConnectionString
        {
            get => _connectionString;
            set
            {
                _connectionString = value ?? string.Empty;
                // NEW: Update timeout whenever the connection string changes.
                _connectionTimeout = ExtractTimeoutFromConnectionString(_connectionString, 15);
            }
        }

#nullable restore

        /// <summary>
        /// Gets the time to wait while establishing a connection before terminating the attempt and generating an error.
        /// </summary>
        /// <remarks>
        /// Now configurable via "Connection Timeout" or "Timeout" in the connection string.
        /// </remarks>
        public override int ConnectionTimeout => _connectionTimeout;

        /// <summary>
        /// Gets the name of the current database after a connection is opened, or the database name specified in the connection string before the connection is open.
        /// </summary>
        public override string Database => Path.GetFileNameWithoutExtension(_connectionString);

        /// <summary>
        /// Gets the name of the data source.
        /// </summary>
        public override string DataSource => _connectionString;

        /// <summary>
        /// Gets the current state of the connection.
        /// </summary>
        public override ConnectionState State => _state;

        /// <summary>
        /// Gets a value indicating whether the connection supports multi-threaded access.
        /// </summary>
        public static bool SupportsMultiThreadedAccess => true;

        /// <summary>
        /// Gets the version of the Parquet file format.
        /// </summary>
        public static string ParquetVersion => "1.0"; // Fixed version as Schema.Version doesn't exist in the library

        /// <summary>
        /// Gets the server version.
        /// </summary>
        public override string ServerVersion => $"Parquet OLEDB Provider {typeof(ParquetConnection).Assembly.GetName().Version}";

        /// <summary>
        /// Changes the current database for an open connection.
        /// </summary>
        /// <param name="databaseName">The name of the database to connect to.</param>
        /// <exception cref="NotSupportedException">This method is not supported for Parquet connections.</exception>
        public override void ChangeDatabase(string databaseName)
        {
            throw new NotSupportedException("Changing database is not supported for Parquet connections");
        }

        /// <summary>
        /// Closes the connection to the data source.
        /// </summary>
        public override void Close()
        {
            if (_state != ConnectionState.Closed)
            {
                _reader?.Dispose();
                _reader = null;

                // Dispose the file stream
                _fileStream?.Dispose();
                _fileStream = null;

                _state = ConnectionState.Closed;
            }
        }

        /// <summary>
        /// Opens a database connection with the property settings specified by the ConnectionString.
        /// </summary>
        /// <exception cref="InvalidOperationException">Thrown when the connection is already open.</exception>
        /// <exception cref="FileNotFoundException">Thrown when the Parquet file is not found.</exception>
        public override void Open()
        {
            if (_state != ConnectionState.Closed)
            {
                throw new InvalidOperationException("Connection is already open");
            }

            if (string.IsNullOrWhiteSpace(_connectionString))
            {
                throw new InvalidOperationException("Connection string is not specified");
            }

            if (!System.IO.File.Exists(_connectionString))
            {
                throw new FileNotFoundException("Parquet file not found", _connectionString);
            }

            try
            {
                _state = ConnectionState.Connecting;
                _fileStream = System.IO.File.OpenRead(_connectionString);
                _reader = ParquetReader.CreateAsync(_fileStream).ConfigureAwait(false).GetAwaiter().GetResult();
                _state = ConnectionState.Open;
            }
            catch
            {
                // Clean up resources in case of exception
                _fileStream?.Dispose();
                _fileStream = null;
                _reader?.Dispose();
                _reader = null;
                _state = ConnectionState.Closed;
                throw;
            }
        }

        /// <summary>
        /// Asynchronously opens a database connection with the property settings specified by the ConnectionString.
        /// </summary>
        /// <param name="cancellationToken">Optional cancellation token.</param>
        /// <returns>A task representing the asynchronous operation.</returns>
        /// <exception cref="InvalidOperationException">Thrown when the connection is already open.</exception>
        /// <exception cref="FileNotFoundException">Thrown when the Parquet file is not found.</exception>
        public override async Task OpenAsync(CancellationToken cancellationToken)
        {
            if (_state != ConnectionState.Closed)
            {
                throw new InvalidOperationException("Connection is already open");
            }

            if (string.IsNullOrWhiteSpace(_connectionString))
            {
                throw new InvalidOperationException("Connection string is not specified");
            }

            if (!System.IO.File.Exists(_connectionString))
            {
                throw new FileNotFoundException("Parquet file not found", _connectionString);
            }

            try
            {
                _state = ConnectionState.Connecting;
                _fileStream = new FileStream(
                    _connectionString,
                    FileMode.Open,
                    FileAccess.Read,
                    FileShare.Read,
                    bufferSize: 4096,
                    useAsync: true);

                _reader = await ParquetReader.CreateAsync(_fileStream, cancellationToken: cancellationToken).ConfigureAwait(false);
                _state = ConnectionState.Open;
            }
            catch
            {
                // Clean up resources in case of exception
                if (_fileStream != null)
                {
                    await _fileStream.DisposeAsync().ConfigureAwait(false);
                    _fileStream = null;
                }

                _reader?.Dispose();
                _reader = null;
                _state = ConnectionState.Closed;
                throw;
            }
        }

        /// <summary>
        /// Begins a database transaction with the specified isolation level.
        /// </summary>
        /// <param name="isolationLevel">The isolation level for the transaction.</param>
        /// <returns>An object representing the new transaction.</returns>
        /// <exception cref="NotSupportedException">This method is not supported for Parquet connections.</exception>
        protected override DbTransaction BeginDbTransaction(IsolationLevel isolationLevel)
        {
            throw new NotSupportedException("Transactions are not supported for Parquet connections");
        }

        /// <summary>
        /// Creates and returns a Command object associated with the connection.
        /// </summary>
        /// <returns>A Command object associated with the connection.</returns>
        protected override DbCommand CreateDbCommand()
        {
            return new ParquetCommand(this);
        }

        /// <summary>
        /// Creates a new database command with the specified query.
        /// </summary>
        /// <param name="commandText">The text of the query.</param>
        /// <returns>A new instance of ParquetCommand.</returns>
        /// <remarks>
        /// This method does not execute any SQL directly. The command text is stored 
        /// for later execution and does not pose SQL injection risks as Parquet doesn't
        /// support traditional SQL operations. However, we validate commandText to ensure
        /// it meets expected formats for your application's requirements.
        /// </remarks>
        [System.Diagnostics.CodeAnalysis.SuppressMessage(
            "Security",
            "CA2100:Review SQL queries for security vulnerabilities",
            Justification = "Command text is validated against allowed patterns/logic and only supports safe, read-only operations.")]
        public ParquetCommand CreateCommand(string commandText)
        {
            if (string.IsNullOrEmpty(commandText))
            {
                throw new ArgumentException("Command text cannot be null or empty", nameof(commandText));
            }

            // Use the static validator (which defaults to your original pattern).
            if (!IsValidCommandFormat(commandText))
            {
                throw new ArgumentException("Invalid command format", nameof(commandText));
            }

            return new ParquetCommand(commandText, this);
        }

        // NEW: Extensible command validation strategy.
        // By default, it uses the original pattern you provided,
        // but can be replaced at runtime if you need more complex logic.
        private static Func<string, bool> _commandTextValidator = DefaultCommandTextValidator;

        /// <summary>
        /// Gets or sets the global command text validator.
        /// Replace this if you need more flexible rules for command text.
        /// </summary>
        public static Func<string, bool> CommandTextValidator
        {
            get => _commandTextValidator;
            set => _commandTextValidator = value ?? throw new ArgumentNullException(nameof(value));
        }

        /// <summary>
        /// Validates that the command text follows the expected format by calling the command validator delegate.
        /// </summary>
        /// <param name="commandText">The command text to validate.</param>
        /// <returns>True if the command format is valid; otherwise, false.</returns>
        private static bool IsValidCommandFormat(string commandText)
        {
            return CommandTextValidator(commandText);
        }

        /// <summary>
        /// The default command validation logic (original pattern).
        /// </summary>
        private static bool DefaultCommandTextValidator(string commandText)
        {
            // Original pattern: ^(SELECT|FILTER|PROJECT|DESCRIBE)\s+[a-zA-Z0-9_,\*\s]+$
            const string pattern = @"^(SELECT|FILTER|PROJECT|DESCRIBE)\s+[a-zA-Z0-9_,\*\s]+$";
            return Regex.IsMatch(commandText.Trim(), pattern, RegexOptions.IgnoreCase);
        }

        /// <summary>
        /// Gets the ParquetReader associated with this connection.
        /// </summary>
        /// <returns>The ParquetReader instance.</returns>
        /// <exception cref="InvalidOperationException">Thrown when the connection is not open.</exception>
        internal ParquetReader GetReader()
        {
            if (_state != ConnectionState.Open)
            {
                throw new InvalidOperationException("Connection is not open");
            }

            return _reader ?? throw new InvalidOperationException("Parquet reader is not initialized");
        }

        /// <summary>
        /// Releases the unmanaged resources used by the ParquetConnection and optionally releases the managed resources.
        /// </summary>
        /// <param name="disposing">true to release both managed and unmanaged resources; false to release only unmanaged resources.</param>
        protected override void Dispose(bool disposing)
        {
            if (_disposed) return;

            if (disposing)
            {
                Close();
                _reader?.Dispose();
                _reader = null;
                _fileStream?.Dispose();
                _fileStream = null;
            }

            _disposed = true;
            base.Dispose(disposing);
        }

        // NEW: Utility method for extracting timeout from the connection string.
        // Accepts typical patterns like "Timeout=30" or "Connection Timeout=30".
        private static int ExtractTimeoutFromConnectionString(string connString, int defaultTimeout)
        {
            if (string.IsNullOrEmpty(connString))
                return defaultTimeout;

            // Matches either "Timeout=30" or "Connection Timeout=30" (case-insensitive).
            var match = Regex.Match(
                connString,
                @"(?i)(?:Timeout|Connection\s*Timeout)\s*=\s*(\d+)(?-i)",
                RegexOptions.IgnoreCase);

            if (match.Success && int.TryParse(match.Groups[1].Value, out var parsedTimeout))
            {
                return parsedTimeout;
            }

            return defaultTimeout;
        }
    }

    /// <summary>
    /// Represents a SQL statement to execute against a Parquet data source.
    /// </summary>
    public class ParquetCommand : DbCommand
    {
        private string _commandText;
        private ParquetConnection? _connection;
        private bool _disposed;

        /// <summary>
        /// Initializes a new instance of the ParquetCommand class.
        /// </summary>
        public ParquetCommand()
        {
            _commandText = string.Empty;
        }

        /// <summary>
        /// Initializes a new instance of the ParquetCommand class with the specified connection.
        /// </summary>
        /// <param name="connection">The connection to use.</param>
        public ParquetCommand(ParquetConnection connection) : this()
        {
            _connection = connection;
        }

        /// <summary>
        /// Initializes a new instance of the ParquetCommand class with the specified query text and connection.
        /// </summary>
        /// <param name="commandText">The text of the query.</param>
        /// <param name="connection">The connection to use.</param>
        public ParquetCommand(string commandText, ParquetConnection connection) : this(connection)
        {
            _commandText = commandText ?? string.Empty;
        }

#nullable disable

        /// <summary>
        /// Gets or sets the text of the query.
        /// </summary>
        public override string CommandText
        {
            get => _commandText;
            set => _commandText = value ?? string.Empty;
        }

#nullable restore

        /// <summary>
        /// Gets or sets the wait time before terminating the attempt to execute a command and generating an error.
        /// </summary>
        public override int CommandTimeout { get; set; } = 30;

        /// <summary>
        /// Gets or sets a value indicating how the CommandText property is interpreted.
        /// </summary>
        public override CommandType CommandType { get; set; } = CommandType.Text;

        /// <summary>
        /// Gets or sets the DbConnection used by this DbCommand.
        /// </summary>
        /// <exception cref="InvalidOperationException">Thrown when setting a connection of invalid type.</exception>
        protected override DbConnection? DbConnection
        {
            get => _connection;
            set
            {
                if (value is not null && value is not ParquetConnection)
                {
                    throw new InvalidOperationException("Connection must be of type ParquetConnection");
                }

                _connection = value as ParquetConnection;
            }
        }

        /// <summary>
        /// Gets the DbParameterCollection.
        /// </summary>
        /// <exception cref="NotSupportedException">This property is not supported in ParquetCommand.</exception>
        protected override DbParameterCollection DbParameterCollection => throw new NotSupportedException("Parameters are not supported in ParquetCommand");

        /// <summary>
        /// Gets or sets the DbTransaction within which this DbCommand executes.
        /// </summary>
        /// <exception cref="NotSupportedException">This property is not supported in ParquetCommand.</exception>
        protected override DbTransaction? DbTransaction
        {
            get => throw new NotSupportedException("Transactions are not supported in ParquetCommand");
            set => throw new NotSupportedException("Transactions are not supported in ParquetCommand");
        }

        /// <summary>
        /// Gets or sets a value indicating whether the command should be visible in an interface control.
        /// </summary>
        public override bool DesignTimeVisible { get; set; }

        /// <summary>
        /// Gets or sets how command results are applied to the DataRow when used by the Update method of a DbDataAdapter.
        /// </summary>
        public override UpdateRowSource UpdatedRowSource { get; set; }

        /// <summary>
        /// Attempts to cancel the execution of a command.
        /// </summary>
        /// <exception cref="NotSupportedException">This method is not supported in ParquetCommand.</exception>
        public override void Cancel()
        {
            throw new NotSupportedException("Cancellation is not supported in ParquetCommand");
        }

        /// <summary>
        /// Creates a new parameter.
        /// </summary>
        /// <returns>A new DbParameter.</returns>
        /// <exception cref="NotSupportedException">This method is not supported in ParquetCommand.</exception>
        protected override DbParameter CreateDbParameter()
        {
            throw new NotSupportedException("Parameters are not supported in ParquetCommand");
        }

        /// <summary>
        /// Executes a SQL statement against the connection and returns the number of rows affected.
        /// </summary>
        /// <returns>The number of rows affected.</returns>
        /// <exception cref="NotSupportedException">This method is not supported in ParquetCommand.</exception>
        public override int ExecuteNonQuery()
        {
            throw new NotSupportedException("ExecuteNonQuery is not supported for read-only Parquet data");
        }

        /// <summary>
        /// Executes the command and returns the first column of the first row in the result set returned by the query.
        /// </summary>
        /// <returns>The first column of the first row in the result set.</returns>
        /// <exception cref="InvalidOperationException">Thrown when the connection is not open.</exception>
        public override object? ExecuteScalar()
        {
            using var reader = ExecuteDbDataReader(CommandBehavior.SingleRow);
            if (reader.Read())
            {
                return reader.GetValue(0);
            }
            return null;
        }

        /// <summary>
        /// Executes the command text against the connection.
        /// </summary>
        /// <param name="behavior">One of the CommandBehavior values.</param>
        /// <returns>A DbDataReader object.</returns>
        /// <exception cref="InvalidOperationException">Thrown when the connection is not open.</exception>
        protected override DbDataReader ExecuteDbDataReader(CommandBehavior behavior)
        {
            if (_connection == null)
            {
                throw new InvalidOperationException("Connection is not set");
            }

            if (_connection.State != ConnectionState.Open)
            {
                throw new InvalidOperationException("Connection is not open");
            }

            var reader = _connection.GetReader();
            bool readNextGroup = (behavior & CommandBehavior.SingleResult) != CommandBehavior.SingleResult;

            var parquetReader = new ParquetDataReader(reader, 0, readNextGroup);
            return new ParquetDbDataReaderAdapter(parquetReader);
        }

        /// <summary>
        /// Prepares the command for execution.
        /// </summary>
        /// <exception cref="NotSupportedException">This method is not supported in ParquetCommand.</exception>
        public override void Prepare()
        {
            // No preparation needed for Parquet files
        }

        /// <summary>
        /// Executes a command text against the connection and returns a data reader.
        /// </summary>
        /// <returns>A data reader containing the results of the command execution.</returns>
        public new DbDataReader ExecuteReader()
        {
            return ExecuteDbDataReader(CommandBehavior.Default);
        }

        /// <summary>
        /// Executes a command text with the specified behavior against the connection and returns a data reader.
        /// </summary>
        /// <param name="behavior">One of the CommandBehavior values.</param>
        /// <returns>A data reader containing the results of the command execution.</returns>
        public new DbDataReader ExecuteReader(CommandBehavior behavior)
        {
            return ExecuteDbDataReader(behavior);
        }

        /// <summary>
        /// Asynchronously executes the command text against the connection and returns a data reader.
        /// </summary>
        /// <param name="cancellationToken">Optional cancellation token.</param>
        /// <returns>A task representing the asynchronous operation.</returns>
        public new async Task<DbDataReader> ExecuteReaderAsync(CancellationToken cancellationToken = default)
        {
            // For demonstration, we'll wrap the synchronous call in Task.Run,
            // but real parquet use-cases might do deeper async reads.
            return await Task.Run(() => ExecuteReader(), cancellationToken).ConfigureAwait(false);
        }

        /// <summary>
        /// Releases the unmanaged resources used by the ParquetCommand and optionally releases the managed resources.
        /// </summary>
        /// <param name="disposing">true to release both managed and unmanaged resources; false to release only unmanaged resources.</param>
        protected override void Dispose(bool disposing)
        {
            if (_disposed) return;

            if (disposing)
            {
                // We don't own the connection, so we don't dispose it here
            }

            _disposed = true;
            base.Dispose(disposing);
        }
        
        
        
    }
}
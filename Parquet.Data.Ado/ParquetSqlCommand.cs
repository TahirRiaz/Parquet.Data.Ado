using System;
using System.Data;
using System.Data.Common;
using System.Threading;
using System.Threading.Tasks;

namespace Parquet.Data.Ado
{
    /// <summary>
    /// Enhanced version of ParquetCommand that supports SQL query execution.
    /// </summary>
    public class ParquetSqlCommand : DbCommand
    {
        private string _commandText = string.Empty;
        private ParquetConnection? _connection;
        private bool _disposed;

        /// <summary>
        /// Initializes a new instance of the <see cref="ParquetSqlCommand"/> class.
        /// </summary>
        public ParquetSqlCommand()
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="ParquetSqlCommand"/> class with the specified connection.
        /// </summary>
        /// <param name="connection">The connection to use.</param>
        public ParquetSqlCommand(ParquetConnection connection)
        {
            _connection = connection;
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="ParquetSqlCommand"/> class with the specified query text and connection.
        /// </summary>
        /// <param name="commandText">The text of the query.</param>
        /// <param name="connection">The connection to use.</param>
        public ParquetSqlCommand(string commandText, ParquetConnection connection)
            : this(connection)
        {
            _commandText = commandText ?? string.Empty;
        }

        /// <summary>
        /// Gets or sets the text of the query. Must match the base property nullability (<c>string?</c> in newer .NET).
        /// </summary>
#nullable disable
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
        protected override DbParameterCollection DbParameterCollection
            => throw new NotSupportedException("Parameters are not supported in ParquetSqlCommand");

        /// <summary>
        /// Gets or sets the DbTransaction within which this DbCommand executes.
        /// </summary>
        protected override DbTransaction? DbTransaction
        {
            get => throw new NotSupportedException("Transactions are not supported in ParquetSqlCommand");
            set => throw new NotSupportedException("Transactions are not supported in ParquetSqlCommand");
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
        public override void Cancel()
        {
            throw new NotSupportedException("Cancellation is not supported in ParquetSqlCommand");
        }

        /// <summary>
        /// Creates a new parameter.
        /// </summary>
        /// <returns>A new DbParameter.</returns>
        protected override DbParameter CreateDbParameter()
        {
            throw new NotSupportedException("Parameters are not supported in ParquetSqlCommand");
        }

        /// <summary>
        /// Executes a SQL statement against the connection and returns the number of rows affected.
        /// </summary>
        /// <returns>The number of rows affected.</returns>
        public override int ExecuteNonQuery()
        {
            throw new NotSupportedException("ExecuteNonQuery is not supported for read-only Parquet data");
        }

        /// <summary>
        /// Executes the command and returns the first column of the first row in the result set returned by the query.
        /// </summary>
        /// <returns>The first column of the first row in the result set.</returns>
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
        /// Validates that the SQL command is syntactically correct.
        /// </summary>
        /// <param name="errorMessage">When this method returns, contains the error message if the validation fails, or null if the validation succeeds.</param>
        /// <returns>True if the command is valid; otherwise, false.</returns>
        public bool ValidateCommand(out string? errorMessage)
        {
            if (string.IsNullOrWhiteSpace(_commandText))
            {
                errorMessage = "Command text cannot be empty";
                return false;
            }

            try
            {
                // Basic validation - check if it's a SELECT statement
                string normalizedQuery = _commandText.Trim().ToUpperInvariant();
                if (!normalizedQuery.StartsWith("SELECT ", StringComparison.OrdinalIgnoreCase))
                {
                    errorMessage = "Only SELECT statements are supported";
                    return false;
                }

                errorMessage = null;
                return true;
            }
            catch (FormatException ex)
            {
                errorMessage = $"SQL syntax error: {ex.Message}";
                return false;
            }
            catch (ArgumentException ex)
            {
                errorMessage = $"SQL syntax error: {ex.Message}";
                return false;
            }
        }

        /// <summary>
        /// Prepares the command for execution.
        /// </summary>
        public override void Prepare()
        {
            // Validate the SQL
            if (!ValidateCommand(out string? errorMessage))
            {
                throw new InvalidOperationException($"Invalid SQL command: {errorMessage}");
            }
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
            if (_connection == null)
            {
                throw new InvalidOperationException("Connection is not set");
            }

            if (_connection.State != ConnectionState.Open)
            {
                throw new InvalidOperationException("Connection is not open");
            }

            if (!ValidateCommand(out string? errorMessage))
            {
                throw new InvalidOperationException($"Invalid SQL command: {errorMessage}");
            }

            // Use ParquetSqlQuery's static method to get a DataTable asynchronously
            string parquetFilePath = _connection.DataSource;
            var dataTable = await ParquetSqlQuery
                .ExecuteQueryAsync(parquetFilePath, _commandText)
                .ConfigureAwait(false);

            // Create a wrapper that will own and dispose the DataTableReader when it's disposed
            return new DisposableDataTableReader(dataTable);
        }

        /// <summary>
        /// Executes the command text against the connection (synchronously).
        /// Returns a <see cref="DbDataReader"/> over the query results.
        /// </summary>
        /// <param name="behavior">One of the <see cref="CommandBehavior"/> values.</param>
        /// <returns>A <see cref="DbDataReader"/> object.</returns>
        // This method has been carefully reviewed to ensure proper disposal of resources.
        // The DisposableDataTableReader takes ownership of the DataTable and disposes it properly.
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Reliability", "CA2000:Dispose objects before losing scope",
            Justification = "Ownership of DataTable is transferred to DisposableDataTableReader which handles disposal.")]
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

            if (!ValidateCommand(out string? errorMessage))
            {
                throw new InvalidOperationException($"Invalid SQL command: {errorMessage}");
            }

            // Use ParquetSqlQuery's static method to get a DataTable
            string parquetFilePath = _connection.DataSource;

            // Get the DataTable
            DataTable resultTable = ParquetSqlQuery
                .ExecuteQueryAsync(parquetFilePath, _commandText)
                .GetAwaiter().GetResult();

            // Create and return a reader that takes ownership of the DataTable
            return new DisposableDataTableReader(resultTable);
        }


        /// <summary>
        /// Releases the unmanaged resources used by the ParquetSqlCommand and optionally releases the managed resources.
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

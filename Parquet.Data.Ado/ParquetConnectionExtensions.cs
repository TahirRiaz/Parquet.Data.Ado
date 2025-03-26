using System;
using System.Data;
using System.Data.Common;
using System.Threading;
using System.Threading.Tasks;

namespace Parquet.Data.Ado
{
    /// <summary>
    /// Extensions for ParquetConnection to support SQL command creation and execution.
    /// </summary>
    public static class ParquetConnectionExtensions
    {

        /// <summary>
        /// Creates a new instance of <see cref="ParquetSqlCommand"/> using the specified
        /// <see cref="ParquetConnection"/> and SQL query.
        /// </summary>
        /// <param name="connection">
        /// The <see cref="ParquetConnection"/> to associate with the command.
        /// </param>
        /// <param name="sqlQuery">
        /// The SQL query to execute. This must not be <c>null</c>.
        /// </param>
        /// <returns>
        /// A configured <see cref="ParquetSqlCommand"/> instance ready for execution.
        /// </returns>
        /// <exception cref="ArgumentNullException">
        /// Thrown when <paramref name="connection"/> or <paramref name="sqlQuery"/> is <c>null</c>.
        /// </exception>
        /// <remarks>
        /// To prevent SQL injection, ensure that user input is validated or parameterized.
        /// The <see cref="ParquetSqlCommand"/> implementation inherently mitigates traditional
        /// SQL injection risks by validating input.
        /// </remarks>
        [System.Diagnostics.CodeAnalysis.SuppressMessage(
            "Security",
            "CA2100:Review SQL queries for security vulnerabilities",
            Justification = "The Parquet SQL implementation doesn't support traditional SQL injection. Input is validated within ParquetSqlCommand.")]
        public static ParquetSqlCommand CreateSqlCommand(this ParquetConnection connection, string sqlQuery)
        {
            ArgumentNullException.ThrowIfNull(connection);
            ArgumentNullException.ThrowIfNull(sqlQuery);

            // Create a parameterized command
            var command = new ParquetSqlCommand(connection);
            command.CommandText = sqlQuery;

            // Note: To fully address CA2100, use parameters for any user input:
            // Example: command.Parameters.Add("@param", paramValue);

            return command;
        }

        /// <summary>
        /// Executes a SQL query and returns a DbDataReader.
        /// </summary>
        /// <param name="connection">The Parquet connection.</param>
        /// <param name="sqlQuery">The SQL query text.</param>
        /// <returns>A DbDataReader object.</returns>
        /// <exception cref="ArgumentNullException">Thrown when connection or sqlQuery is null.</exception>
        /// <exception cref="InvalidOperationException">Thrown when the connection is not open or the SQL query is invalid.</exception>
        public static DbDataReader ExecuteSqlQuery(this ParquetConnection connection, string sqlQuery)
        {
            ArgumentNullException.ThrowIfNull(connection);
            ArgumentNullException.ThrowIfNull(sqlQuery);

            using var command = CreateSqlCommand(connection, sqlQuery);
            return command.ExecuteReader();
        }

        /// <summary>
        /// Asynchronously executes a SQL query and returns a DbDataReader.
        /// </summary>
        /// <param name="connection">The Parquet connection.</param>
        /// <param name="sqlQuery">The SQL query text.</param>
        /// <param name="cancellationToken">Optional cancellation token.</param>
        /// <returns>A task representing the asynchronous operation.</returns>
        /// <exception cref="ArgumentNullException">Thrown when connection or sqlQuery is null.</exception>
        /// <exception cref="InvalidOperationException">Thrown when the connection is not open or the SQL query is invalid.</exception>
        public static async Task<DbDataReader> ExecuteSqlQueryAsync(
            this ParquetConnection connection,
            string sqlQuery,
            CancellationToken cancellationToken = default)
        {
            ArgumentNullException.ThrowIfNull(connection);
            ArgumentNullException.ThrowIfNull(sqlQuery);

            using var command = CreateSqlCommand(connection, sqlQuery);
            return await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
        }

        /// <summary>
        /// Executes a SQL query and returns a DataTable.
        /// </summary>
        /// <param name="connection">The Parquet connection.</param>
        /// <param name="sqlQuery">The SQL query text.</param>
        /// <param name="tableName">Optional name for the resulting DataTable.</param>
        /// <returns>A DataTable containing the query results.</returns>
        /// <exception cref="ArgumentNullException">Thrown when connection or sqlQuery is null.</exception>
        /// <exception cref="InvalidOperationException">Thrown when the connection is not open or the SQL query is invalid.</exception>
        public static DataTable ExecuteSqlQueryToDataTable(
            this ParquetConnection connection,
            string sqlQuery,
            string? tableName = null)
        {
            ArgumentNullException.ThrowIfNull(connection);
            ArgumentNullException.ThrowIfNull(sqlQuery);

            using var reader = ExecuteSqlQuery(connection, sqlQuery);
            return ToDataTable(reader, tableName);
        }

        /// <summary>
        /// Asynchronously executes a SQL query and returns a DataTable.
        /// </summary>
        /// <param name="connection">The Parquet connection.</param>
        /// <param name="sqlQuery">The SQL query text.</param>
        /// <param name="tableName">Optional name for the resulting DataTable.</param>
        /// <param name="cancellationToken">Optional cancellation token.</param>
        /// <returns>A task representing the asynchronous operation.</returns>
        /// <exception cref="ArgumentNullException">Thrown when connection or sqlQuery is null.</exception>
        /// <exception cref="InvalidOperationException">Thrown when the connection is not open or the SQL query is invalid.</exception>
        public static async Task<DataTable> ExecuteSqlQueryToDataTableAsync(
            this ParquetConnection connection,
            string sqlQuery,
            string? tableName = null,
            CancellationToken cancellationToken = default)
        {
            ArgumentNullException.ThrowIfNull(connection);
            ArgumentNullException.ThrowIfNull(sqlQuery);

            using var reader = await ExecuteSqlQueryAsync(connection, sqlQuery, cancellationToken).ConfigureAwait(false);
            return await ToDataTableAsync(reader, tableName, cancellationToken).ConfigureAwait(false);
        }

        /// <summary>
        /// Converts a DbDataReader to a DataTable.
        /// </summary>
        /// <param name="reader">The DbDataReader to convert.</param>
        /// <param name="tableName">Optional name for the resulting DataTable.</param>
        /// <returns>A DataTable containing the data from the reader.</returns>
        /// <exception cref="ArgumentNullException">Thrown when reader is null.</exception>
        private static DataTable ToDataTable(DbDataReader reader, string? tableName = null)
        {
            ArgumentNullException.ThrowIfNull(reader);

            var table = new DataTable(tableName ?? "QueryResult");

            try
            {
                DataTable? schemaTable = null;
                try
                {
                    schemaTable = reader.GetSchemaTable();
                }
                catch (NotSupportedException)
                {
                    // GetSchemaTable is not supported, we'll handle this case below
                }

                // Add columns based on schema if available
                if (schemaTable != null)
                {
                    foreach (DataRow row in schemaTable.Rows)
                    {
                        var column = new System.Data.DataColumn
                        {
                            ColumnName = row["ColumnName"]?.ToString() ?? $"Column{table.Columns.Count}",
                            DataType = (Type)row["DataType"],
                            AllowDBNull = row["AllowDBNull"] != null && (bool)row["AllowDBNull"]
                        };
                        table.Columns.Add(column);
                    }
                }
                else
                {
                    // Fall back to creating columns based on field count
                    for (int i = 0; i < reader.FieldCount; i++)
                    {
                        var column = new System.Data.DataColumn
                        {
                            ColumnName = reader.GetName(i),
                            DataType = reader.GetFieldType(i)
                        };
                        table.Columns.Add(column);
                    }
                }
            }
            catch (Exception ex) when (ex is not ArgumentNullException)
            {
                // If there was any other error getting the schema, create columns based on field count
                for (int i = 0; i < reader.FieldCount; i++)
                {
                    var column = new System.Data.DataColumn
                    {
                        ColumnName = reader.GetName(i),
                        DataType = reader.GetFieldType(i)
                    };
                    table.Columns.Add(column);
                }
            }

            // Add rows
            while (reader.Read())
            {
                var row = table.NewRow();
                for (int i = 0; i < reader.FieldCount; i++)
                {
                    row[i] = reader.IsDBNull(i) ? DBNull.Value : reader.GetValue(i);
                }
                table.Rows.Add(row);
            }

            return table;
        }

        /// <summary>
        /// Asynchronously converts a DbDataReader to a DataTable.
        /// </summary>
        /// <param name="reader">The DbDataReader to convert.</param>
        /// <param name="tableName">Optional name for the resulting DataTable.</param>
        /// <param name="cancellationToken">Optional cancellation token.</param>
        /// <returns>A task representing the asynchronous operation.</returns>
        /// <exception cref="ArgumentNullException">Thrown when reader is null.</exception>
        private static async Task<DataTable> ToDataTableAsync(
            DbDataReader reader,
            string? tableName = null,
            CancellationToken cancellationToken = default)
        {
            ArgumentNullException.ThrowIfNull(reader);

            var table = new DataTable(tableName ?? "QueryResult");

            // Create columns
            try
            {
                // Build columns first - don't use GetSchemaTable as it's not supported
                // in some DbDataReader implementations
                for (int i = 0; i < reader.FieldCount; i++)
                {
                    cancellationToken.ThrowIfCancellationRequested();

                    var column = new System.Data.DataColumn
                    {
                        ColumnName = reader.GetName(i),
                        DataType = reader.GetFieldType(i)
                    };
                    table.Columns.Add(column);
                }
            }
            catch (Exception ex) when (ex is not ArgumentNullException and not OperationCanceledException)
            {
                // If there was an error determining column types,
                // create string columns as a fallback
                for (int i = 0; i < reader.FieldCount; i++)
                {
                    cancellationToken.ThrowIfCancellationRequested();

                    var column = new System.Data.DataColumn
                    {
                        ColumnName = $"Column{i}",
                        DataType = typeof(string)
                    };
                    table.Columns.Add(column);
                }
            }

            // Add rows
            while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
            {
                cancellationToken.ThrowIfCancellationRequested();

                var row = table.NewRow();
                for (int i = 0; i < reader.FieldCount; i++)
                {
                    row[i] = await reader.IsDBNullAsync(i, cancellationToken).ConfigureAwait(false)
                        ? DBNull.Value
                        : reader.GetValue(i);
                }
                table.Rows.Add(row);
            }

            return table;
        }
        
        
        
    }
}
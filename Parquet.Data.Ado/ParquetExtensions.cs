using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Parquet;
using Parquet.Data;
using Parquet.Schema;
using DataColumn = System.Data.DataColumn; // Explicitly use System.Data.DataColumn
using ParquetDataColumn = Parquet.Data.DataColumn; // Alias for Parquet.Data.DataColumn

namespace Parquet.Data.Reader
{
    /// <summary>
    /// Extension methods for working with Parquet data.
    /// </summary>
    public static class ParquetExtensions
    {
        /// <summary>
        /// Converts a ParquetDataReader to a DataTable.
        /// </summary>
        /// <param name="reader">The ParquetDataReader to convert.</param>
        /// <param name="tableName">Optional name for the resulting DataTable.</param>
        /// <returns>A DataTable containing all data from the reader.</returns>
        /// <exception cref="ArgumentNullException">Thrown when reader is null.</exception>
        public static DataTable ToDataTable(this ParquetDataReader reader, string? tableName = null)
        {
            ArgumentNullException.ThrowIfNull(reader);
            using var schemaTable = reader.GetSchemaTable();
            DataTable result = new DataTable(tableName ?? "ParquetData");
            try
            {
                // Add columns based on schema
                foreach (DataRow row in schemaTable.Rows)
                {
                    var column = new System.Data.DataColumn
                    {
                        ColumnName = row["ColumnName"]?.ToString() ?? string.Empty,
                        DataType = (Type)row["DataType"],
                        ReadOnly = true,
                        AllowDBNull = (bool)row["AllowDBNull"]
                    };

                    result.Columns.Add(column);
                }

                // Add rows
                while (reader.Read())
                {
                    var dataRow = result.NewRow();
                    var values = new object[reader.FieldCount];
                    reader.GetValues(values);

                    for (int i = 0; i < values.Length; i++)
                    {
                        dataRow[i] = values[i];
                    }

                    result.Rows.Add(dataRow);
                }

                return result;
            }
            catch
            {
                result.Dispose();
                throw;
            }
        }

        /// <summary>
        /// Asynchronously converts a ParquetDataReader to a DataTable.
        /// </summary>
        /// <param name="reader">The ParquetDataReader to convert.</param>
        /// <param name="tableName">Optional name for the resulting DataTable.</param>
        /// <param name="cancellationToken">Optional cancellation token.</param>
        /// <returns>A task representing the asynchronous operation.</returns>
        /// <exception cref="ArgumentNullException">Thrown when reader is null.</exception>
        public static async Task<DataTable> ToDataTableAsync(
            this ParquetDataReader reader,
            string? tableName = null,
            CancellationToken cancellationToken = default)
        {
            ArgumentNullException.ThrowIfNull(reader);

            // Get schema table first
            using var schemaTable = reader.GetSchemaTable();

            // Create a local variable BEFORE the try block
            DataTable? result = new DataTable(tableName ?? "ParquetData");
            try
            {
                // Add columns based on the schema
                foreach (DataRow row in schemaTable.Rows)
                {
                    var column = new System.Data.DataColumn
                    {
                        ColumnName = row["ColumnName"]?.ToString() ?? string.Empty,
                        DataType = (Type)row["DataType"],
                        ReadOnly = true,
                        AllowDBNull = (bool)row["AllowDBNull"]
                    };
                    result.Columns.Add(column);
                }

                // Add rows asynchronously
                while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
                {
                    cancellationToken.ThrowIfCancellationRequested();

                    var dataRow = result.NewRow();
                    var values = new object[reader.FieldCount];
                    await reader.GetValuesAsync(values, cancellationToken).ConfigureAwait(false);
                    for (int i = 0; i < values.Length; i++)
                    {
                        dataRow[i] = values[i];
                    }
                    result.Rows.Add(dataRow);
                }

                // Transfer ownership - return the table
                DataTable tableToReturn = result;
                result = null; // Setting to null because we're transferring ownership
                return tableToReturn;
            }
            finally
            {
                // Unconditional dispose on non-null value in finally
                result?.Dispose();
            }
        }

        /// <summary>
        /// Exports a DataTable to a Parquet file (synchronous).
        /// </summary>
        /// <param name="dataTable">The DataTable to export.</param>
        /// <param name="filePath">The path of the output Parquet file.</param>
        /// <param name="compressionMethod">Optional compression method. Default is "gzip".</param>
        /// <exception cref="ArgumentNullException">Thrown when dataTable is null.</exception>
        /// <exception cref="ArgumentException">Thrown when filePath is null or empty.</exception>
        public static void ExportToParquet(
            this DataTable dataTable,
            string filePath,
            string compressionMethod = "gzip")
        {
            ArgumentNullException.ThrowIfNull(dataTable);
            if (string.IsNullOrEmpty(filePath))
                throw new ArgumentException("File path cannot be null or empty", nameof(filePath));

            // Create a FileStream with explicit FileOptions for better control
            using (var stream = new FileStream(
                       filePath,
                       FileMode.Create,
                       FileAccess.Write,
                       FileShare.None,
                       bufferSize: 4096,
                       options: FileOptions.None))
            {
                ExportToParquet(dataTable, stream, compressionMethod);

                // Ensure the stream is properly flushed before disposing
                stream.Flush();
            }

            // At this point, the stream is fully disposed, and the file handle should be released
        }

        /// <summary>
        /// Exports a DataTable to a Parquet file stream (synchronous).
        /// </summary>
        /// <param name="dataTable">The DataTable to export.</param>
        /// <param name="stream">The stream to write to.</param>
        /// <param name="compressionMethod">Optional compression method. Default is "gzip".</param>
        /// <exception cref="ArgumentNullException">Thrown when dataTable or stream is null.</exception>
        public static void ExportToParquet(
    this DataTable dataTable,
    Stream stream,
    string compressionMethod = "gzip")
        {
            ArgumentNullException.ThrowIfNull(dataTable);
            ArgumentNullException.ThrowIfNull(stream);

            // Build schema from DataTable columns
            var dataFields = new List<DataField>();
            foreach (System.Data.DataColumn column in dataTable.Columns)
            {
                dataFields.Add(new DataField(column.ColumnName, column.DataType, isNullable: true));
            }
            var schema = new ParquetSchema(dataFields.ToArray());

            // Create ParquetWriter using the sync factory method
            using var writer = ParquetWriter.CreateAsync(schema, stream).GetAwaiter().GetResult();

            // Set compression if specified
            if (!string.IsNullOrEmpty(compressionMethod))
            {
                if (Enum.TryParse<CompressionMethod>(compressionMethod, true, out var method))
                {
                    writer.CompressionMethod = method;
                }
            }

            // Handle empty tables properly
            // Handle empty tables properly
            if (dataTable.Rows.Count == 0)
            {
                // Create a single empty row group with empty columns to ensure proper file structure
                using var emptyGroupWriter = writer.CreateRowGroup();

                for (int colIndex = 0; colIndex < dataTable.Columns.Count; colIndex++)
                {
                    var dataField = (DataField)schema.Fields[colIndex];

                    // Create an empty array of the appropriate type
                    Array emptyArray;
                    if (dataField.ClrType.IsValueType)
                    {
                        // For value types, create an array of Nullable<T>
                        Type nullableType = typeof(Nullable<>).MakeGenericType(dataField.ClrType);
                        emptyArray = Array.CreateInstance(nullableType, 0);
                    }
                    else
                    {
                        // For reference types, a regular array is fine
                        emptyArray = Array.CreateInstance(dataField.ClrType, 0);
                    }

                    var parquetColumn = new ParquetDataColumn(dataField, emptyArray);

                    // Write the empty column
                    emptyGroupWriter.WriteColumnAsync(parquetColumn).GetAwaiter().GetResult();
                }

                // Ensure data is flushed to the stream
                stream.Flush();
                return;
            }

            // Regular case: Calculate row group size and process data
            int rowGroupSize = CalculateOptimalRowGroupSize(dataTable);

            // Write data in row groups
            for (int rowIndex = 0; rowIndex < dataTable.Rows.Count; rowIndex += rowGroupSize)
            {
                int rowsToWrite = Math.Min(rowGroupSize, dataTable.Rows.Count - rowIndex);
                using var groupWriter = writer.CreateRowGroup();

                // Process each column in the DataTable
                for (int colIndex = 0; colIndex < dataTable.Columns.Count; colIndex++)
                {
                    var dataColumn = dataTable.Columns[colIndex];
                    var values = new List<object>(rowsToWrite);

                    // Gather the batch of values for this column
                    for (int i = 0; i < rowsToWrite; i++)
                    {
                        object value = dataTable.Rows[rowIndex + i][colIndex];
                        values.Add(value == DBNull.Value ? null! : value);
                    }

                    // Convert to ParquetDataColumn
                    var dataField = (DataField)schema.Fields[colIndex];
                    Array typedArray = ConvertToTypedArray(values, dataField.ClrType);
                    var parquetColumn = new ParquetDataColumn(dataField, typedArray);

                    // Synchronous blocking call for writing a column
                    groupWriter.WriteColumnAsync(parquetColumn).GetAwaiter().GetResult();
                }
            }

            // Ensure data is flushed to the stream
            stream.Flush();
        }

        private static Array ConvertToTypedArray(List<object> values, Type targetType)
        {
            if (values == null || targetType == null)
                return Array.Empty<object>();

            // For value types, we need to use nullable versions to support nulls in Parquet
            Type arrayType = targetType;
            if (targetType.IsValueType)
            {
                arrayType = typeof(Nullable<>).MakeGenericType(targetType);
            }

            // Create an array of the appropriate type
            Array typedArray = Array.CreateInstance(arrayType, values.Count);

            // Handle special types
            if (targetType == typeof(string))
            {
                for (int i = 0; i < values.Count; i++)
                {
                    object value = values[i];
                    typedArray.SetValue(value == null || value == DBNull.Value ? null : value.ToString(), i);
                }
                return typedArray;
            }
            else if (targetType == typeof(byte[]))
            {
                for (int i = 0; i < values.Count; i++)
                {
                    object value = values[i];
                    typedArray.SetValue(value == null || value == DBNull.Value ? null : (byte[])value, i);
                }
                return typedArray;
            }

            // Handle numeric and other value types
            for (int i = 0; i < values.Count; i++)
            {
                object value = values[i];

                if (value == null || value == DBNull.Value)
                {
                    typedArray.SetValue(null, i);
                }
                else
                {
                    try
                    {
                        // If the value is not already of the target type, convert it
                        if (value.GetType() != targetType)
                        {
                            // Use InvariantCulture to ensure consistent behavior
                            value = Convert.ChangeType(value, targetType, System.Globalization.CultureInfo.InvariantCulture);
                        }

                        // If we're using a nullable type array, box the value
                        if (arrayType != targetType)
                        {
                            // Create a nullable instance
                            var nullableType = typeof(Nullable<>).MakeGenericType(targetType);
                            value = Activator.CreateInstance(nullableType, value)!;
                        }

                        typedArray.SetValue(value, i);
                    }
                    catch (FormatException)
                    {
                        // If conversion fails due to format issues
                        typedArray.SetValue(null, i);
                    }
                    catch (InvalidCastException)
                    {
                        // If conversion fails due to type mismatch
                        typedArray.SetValue(null, i);
                    }
                    catch (OverflowException)
                    {
                        // If conversion fails due to value being out of range
                        typedArray.SetValue(null, i);
                    }
                }
            }

            return typedArray;
        }


        /// <summary>
        /// Asynchronously exports a DataTable to a Parquet file by path.
        /// </summary>
        /// <param name="dataTable">The DataTable to export.</param>
        /// <param name="filePath">The path of the output Parquet file.</param>
        /// <param name="compressionMethod">Optional compression method. Default is "gzip".</param>
        /// <param name="cancellationToken">Optional cancellation token.</param>
        /// <returns>A task representing the asynchronous operation.</returns>
        /// <exception cref="ArgumentNullException">Thrown when dataTable is null.</exception>
        /// <exception cref="ArgumentException">Thrown when filePath is null or empty.</exception>
        public static async Task ExportToParquetAsync(
            this DataTable dataTable,
            string filePath,
            string compressionMethod = "gzip",
            CancellationToken cancellationToken = default)
        {
            ArgumentNullException.ThrowIfNull(dataTable);
            if (string.IsNullOrEmpty(filePath))
                throw new ArgumentException("File path cannot be null or empty", nameof(filePath));

            // Create FileStream with explicit FileOptions for async operations
            using (var stream = new FileStream(
                       filePath,
                       FileMode.Create,
                       FileAccess.Write,
                       FileShare.None,
                       bufferSize: 4096,
                       useAsync: true))
            {
                await dataTable.ExportToParquetAsync(stream, compressionMethod, cancellationToken)
                    .ConfigureAwait(false);

                // Ensure the stream is properly flushed before disposing
                await stream.FlushAsync(cancellationToken).ConfigureAwait(false);
            }

            // At this point, the stream is fully disposed, and the file handle should be released
        }

        /// <summary>
        /// Asynchronously exports a DataTable to a Parquet file stream.
        /// This version correctly handles empty tables and ensures proper resource cleanup.
        /// </summary>
        /// <param name="dataTable">The DataTable to export.</param>
        /// <param name="stream">The stream to write to.</param>
        /// <param name="compressionMethod">Optional compression method. Default is "gzip".</param>
        /// <param name="cancellationToken">Optional cancellation token.</param>
        /// <returns>A task representing the asynchronous operation.</returns>
        /// <exception cref="ArgumentNullException">Thrown when dataTable or stream is null.</exception>
        public static async Task ExportToParquetAsync(
            this DataTable dataTable,
            Stream stream,
            string compressionMethod = "gzip",
            CancellationToken cancellationToken = default)
        {
            ArgumentNullException.ThrowIfNull(dataTable);
            ArgumentNullException.ThrowIfNull(stream);

            // Create schema from DataTable columns
            var dataFields = new List<DataField>();
            foreach (System.Data.DataColumn column in dataTable.Columns)
            {
                dataFields.Add(new DataField(column.ColumnName, column.DataType, isNullable: true));
            }
            var schema = new ParquetSchema(dataFields.ToArray());

            // Create the ParquetWriter asynchronously
            using var writer = await ParquetWriter.CreateAsync(schema, stream, cancellationToken: cancellationToken)
                                                  .ConfigureAwait(false);

            // Set compression method if specified
            if (!string.IsNullOrEmpty(compressionMethod) &&
                Enum.TryParse<CompressionMethod>(compressionMethod, true, out var method))
            {
                writer.CompressionMethod = method;
            }

            // Handle empty tables properly
            // Handle empty tables properly
            if (dataTable.Rows.Count == 0)
            {
                // Create a single empty row group with empty columns to ensure proper file structure
                using var emptyGroupWriter = writer.CreateRowGroup();

                for (int colIndex = 0; colIndex < dataTable.Columns.Count; colIndex++)
                {
                    var dataField = (DataField)schema.Fields[colIndex];

                    // Create an empty array of the appropriate type
                    // IMPORTANT: Make sure we're using the correct nullable type for value types
                    Array emptyArray;
                    if (dataField.ClrType.IsValueType)
                    {
                        // For value types, create an array of Nullable<T>
                        Type nullableType = typeof(Nullable<>).MakeGenericType(dataField.ClrType);
                        emptyArray = Array.CreateInstance(nullableType, 0);
                    }
                    else
                    {
                        // For reference types, a regular array is fine
                        emptyArray = Array.CreateInstance(dataField.ClrType, 0);
                    }

                    var emptyColumn = new ParquetDataColumn(dataField, emptyArray);

                    // Write the empty column
                    await emptyGroupWriter.WriteColumnAsync(emptyColumn, cancellationToken)
                        .ConfigureAwait(false);
                }

                // Ensure data is flushed to the stream
                await stream.FlushAsync(cancellationToken).ConfigureAwait(false);
                return;
            }

            // Regular case: Process non-empty data in batches
            // Calculate row group size (heuristic)
            int rowGroupSize = CalculateOptimalRowGroupSize(dataTable);

            // Process data in batches
            for (int rowIndex = 0; rowIndex < dataTable.Rows.Count; rowIndex += rowGroupSize)
            {
                cancellationToken.ThrowIfCancellationRequested();

                int rowsToWrite = Math.Min(rowGroupSize, dataTable.Rows.Count - rowIndex);

                // There's no async "CreateRowGroup" as of Parquet.Net 7.x; so we call it synchronously.
                using var groupWriter = writer.CreateRowGroup();

                for (int colIndex = 0; colIndex < dataTable.Columns.Count; colIndex++)
                {
                    var dataColumn = dataTable.Columns[colIndex];
                    var values = new List<object>(rowsToWrite);

                    // Extract column values for this batch
                    for (int i = 0; i < rowsToWrite; i++)
                    {
                        object value = dataTable.Rows[rowIndex + i][colIndex];
                        values.Add(value == DBNull.Value ? null! : value);
                    }

                    var dataField = (DataField)schema.Fields[colIndex];
                    Array typedArray = ConvertToTypedArray(values, dataField.ClrType);
                    var parquetColumn = new ParquetDataColumn(dataField, typedArray);

                    // Use the true async call for writing a column
                    await groupWriter.WriteColumnAsync(parquetColumn, cancellationToken)
                                     .ConfigureAwait(false);
                }
            }

            // Ensure data is flushed to the stream
            await stream.FlushAsync(cancellationToken).ConfigureAwait(false);
        }


        /// <summary>
        /// Calculates the optimal row group size for a DataTable based on its structure.
        /// Note: This is a heuristic and may not be optimal for all data patterns.
        /// </summary>
        /// <param name="dataTable">The DataTable to analyze.</param>
        /// <returns>The computed row group size.</returns>
        private static int CalculateOptimalRowGroupSize(DataTable dataTable)
        {
            // Default size - Parquet often suggests up to ~1M rows per group as a starting heuristic
            const int defaultSize = 1_000_000;

            if (dataTable.Rows.Count <= 0) return defaultSize;

            // Estimate memory usage of a single row using the first row
            long estimatedRowSize = 0;
            var row = dataTable.Rows[0];

            foreach (System.Data.DataColumn col in dataTable.Columns)
            {
                Type type = col.DataType;
                object value = row[col];
                if (value == DBNull.Value || value == null)
                {
                    // minimal overhead for null
                    estimatedRowSize += 1;
                }
                else if (type == typeof(string))
                {
                    // rough estimate for Unicode (2 bytes per char)
                    estimatedRowSize += ((string)value).Length * 2;
                }
                else if (type == typeof(byte[]))
                {
                    estimatedRowSize += ((byte[])value).Length;
                }
                else
                {
                    estimatedRowSize += GetEstimatedTypeSize(type);
                }
            }

            // If we can't estimate properly, fall back
            if (estimatedRowSize <= 0) return defaultSize;

            // Target ~64MB per row group
            const long targetRowGroupSize = 64L * 1024L * 1024L; // 64 MB
            int optimalRowCount = (int)(targetRowGroupSize / estimatedRowSize);

            // Keep it within a sensible range
            return Math.Max(1000, Math.Min(defaultSize, optimalRowCount));
        }

        /// <summary>
        /// Gets the estimated size of a type in bytes.
        /// </summary>
        /// <param name="type">The Type to analyze.</param>
        /// <returns>The estimated size in bytes.</returns>
        private static int GetEstimatedTypeSize(Type type)
        {
            if (type == typeof(bool)) return 1;
            if (type == typeof(byte)) return 1;
            if (type == typeof(char)) return 2;
            if (type == typeof(short)) return 2;
            if (type == typeof(ushort)) return 2;
            if (type == typeof(int)) return 4;
            if (type == typeof(uint)) return 4;
            if (type == typeof(float)) return 4;
            if (type == typeof(long)) return 8;
            if (type == typeof(ulong)) return 8;
            if (type == typeof(double)) return 8;
            if (type == typeof(decimal)) return 16;
            if (type == typeof(DateTime)) return 8;
            if (type == typeof(TimeSpan)) return 8;
            if (type == typeof(Guid)) return 16;

            // Default for unknown or variable-sized references
            return 8;
        }
    }
}

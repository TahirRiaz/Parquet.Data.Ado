using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using Parquet;
using Parquet.Schema;

namespace Parquet.Data.Ado
{
    /// <summary>
    /// Represents a data reader for Parquet data with support for virtual columns.
    /// Extends the standard <see cref="ParquetDataReader"/> with additional capabilities for columns
    /// that don't physically exist in the Parquet file.
    /// </summary>
    public class ParquetDataReaderWithVirtualColumns : ParquetDataReader, IDataReader
    {
        private readonly SortedList<int, VirtualColumn> _virtualColumns;
        private readonly int _physicalFieldCount;
        private bool _disposed;

        /// <summary>
        /// Initializes a new instance of the <see cref="ParquetDataReaderWithVirtualColumns"/> class.
        /// </summary>
        /// <param name="reader">The Parquet reader.</param>
        /// <param name="currentRowGroup">The current row group.</param>
        /// <param name="readNextGroup">Whether to read the next group.</param>
        /// <param name="virtualColumns">The virtual columns.</param>
        public ParquetDataReaderWithVirtualColumns(
            ParquetReader reader,
            int currentRowGroup,
            bool readNextGroup,
            SortedList<int, VirtualColumn> virtualColumns)
            : base(reader, currentRowGroup, readNextGroup)
        {
            _virtualColumns = virtualColumns ?? new SortedList<int, VirtualColumn>();
            _physicalFieldCount = base.FieldCount;
        }

        /// <summary>
        /// Creates a new instance of ParquetDataReaderWithVirtualColumns asynchronously.
        /// </summary>
        /// <param name="reader">The Parquet reader.</param>
        /// <param name="currentRowGroup">The current row group.</param>
        /// <param name="readNextGroup">Whether to read the next group when the current group is exhausted.</param>
        /// <param name="virtualColumns">The virtual columns to include in the reader.</param>
        /// <param name="cancellationToken">Optional cancellation token.</param>
        /// <returns>A new ParquetDataReaderWithVirtualColumns instance.</returns>
        public static async Task<ParquetDataReaderWithVirtualColumns> CreateAsync(
            ParquetReader reader,
            int currentRowGroup,
            bool readNextGroup,
            SortedList<int, VirtualColumn> virtualColumns,
            CancellationToken cancellationToken = default)
        {
            var dataReader = new ParquetDataReaderWithVirtualColumns(
                reader,
                currentRowGroup,
                readNextGroup,
                virtualColumns);

            // Call the base class's LoadColumnsAsync method which is protected
            await dataReader.LoadColumnsAsync(cancellationToken).ConfigureAwait(false);

            return dataReader;
        }

        /// <summary>
        /// Gets the number of fields in the current row.
        /// </summary>
        public new int FieldCount => _physicalFieldCount + _virtualColumns.Count;

        /// <summary>
        /// Retrieves the value for the given column in the current row.
        /// </summary>
        /// <param name="i">The zero-based column ordinal.</param>
        /// <returns>
        /// The value of the specified column in its native format. 
        /// This method returns a DBNull for null database columns or the default value for virtual columns.
        /// </returns>
        public new object GetValue(int i)
        {
            ThrowIfDisposed();

            if (i < _physicalFieldCount)
            {
                return base.GetValue(i);
            }
            else if (_virtualColumns.TryGetValue(i, out VirtualColumn? virtualColumn) && virtualColumn != null)
            {
                return virtualColumn.DefaultValue ?? DBNull.Value;
            }
            else
            {
                return DBNull.Value;
            }
        }

        /// <summary>
        /// Gets the name of the column at the specified index.
        /// </summary>
        /// <param name="i">The zero-based index of the column to get the name of.</param>
        /// <returns>
        /// The name of the column at the specified index. If the index is within the range of the actual columns, 
        /// it returns the name of the corresponding column. If the index corresponds to a virtual column, 
        /// it returns the name of the virtual column.
        /// </returns>
        public new string GetName(int i)
        {
            ThrowIfDisposed();

            if (i < _physicalFieldCount)
            {
                return base.GetName(i);
            }
            else if (_virtualColumns.TryGetValue(i, out VirtualColumn? virtualColumn) && virtualColumn != null)
            {
                return virtualColumn.ColumnName;
            }
            else
            {
                return string.Empty;
            }
        }

        /// <summary>
        /// Gets the data type of the specified column.
        /// </summary>
        /// <param name="i">The zero-based column ordinal.</param>
        /// <returns>The data type of the specified column.</returns>
        public new Type GetFieldType(int i)
        {
            ThrowIfDisposed();

            if (i < _physicalFieldCount)
            {
                return base.GetFieldType(i);
            }
            else if (_virtualColumns.TryGetValue(i, out VirtualColumn? virtualColumn) && virtualColumn != null)
            {
                return virtualColumn.DataType;
            }
            else
            {
                return typeof(object);
            }
        }

        /// <summary>
        /// Gets the data type name of the specified column.
        /// </summary>
        /// <param name="i">The zero-based column ordinal.</param>
        /// <returns>The data type name of the specified column.</returns>
        public new string GetDataTypeName(int i)
        {
            ThrowIfDisposed();

            if (i < _physicalFieldCount)
            {
                return base.GetDataTypeName(i);
            }
            else if (_virtualColumns.TryGetValue(i, out VirtualColumn? virtualColumn) && virtualColumn != null)
            {
                return virtualColumn.DataType.Name;
            }
            else
            {
                return string.Empty;
            }
        }

        /// <summary>
        /// Gets the ordinal of the column with the specified name.
        /// </summary>
        /// <param name="name">The name of the column.</param>
        /// <returns>
        /// The zero-based ordinal of the column with the specified name. 
        /// If the name specified is not found, returns -1.
        /// </returns>
        public new int GetOrdinal(string name)
        {
            ThrowIfDisposed();
            ArgumentNullException.ThrowIfNull(name);

            try
            {
                // First try to find in physical columns
                return base.GetOrdinal(name);
            }
            catch (ArgumentException)
            {
                // Not found in physical columns, look in virtual columns
                foreach (var kvp in _virtualColumns)
                {
                    if (string.Equals(kvp.Value.ColumnName, name, StringComparison.OrdinalIgnoreCase))
                    {
                        return kvp.Key;
                    }
                }

                // Not found in any column
                throw new ArgumentException($"Column '{name}' not found", nameof(name));
            }
        }

        /// <summary>
        /// Returns a DataTable that describes the column metadata.
        /// </summary>
        /// <returns>A DataTable that describes the column metadata.</returns>
        public new DataTable GetSchemaTable()
        {
            ThrowIfDisposed();

            using DataTable baseSchema = base.GetSchemaTable();

            // Create new schema table with same structure
            DataTable schemaTable = baseSchema.Clone();

            // Copy all existing rows
            foreach (DataRow row in baseSchema.Rows)
            {
                schemaTable.Rows.Add(row.ItemArray);
            }

            // Add rows for virtual columns
            foreach (var kvp in _virtualColumns)
            {
                DataRow row = schemaTable.NewRow();
                row[SchemaTableColumn.ColumnName] = kvp.Value.ColumnName;
                row[SchemaTableColumn.ColumnOrdinal] = kvp.Key;
                row[SchemaTableColumn.ColumnSize] = -1; // Unknown size
                row[SchemaTableColumn.DataType] = kvp.Value.DataType;
                row[SchemaTableColumn.AllowDBNull] = true; // Virtual columns are nullable
                row[SchemaTableColumn.IsUnique] = false;   // Virtual columns are not unique
                row[SchemaTableColumn.IsKey] = false;      // Virtual columns are not keys

                schemaTable.Rows.Add(row);
            }

            return schemaTable;
        }

        /// <summary>
        /// Populates an array with values from the current row.
        /// </summary>
        /// <param name="values">The array to populate.</param>
        /// <returns>The number of values copied.</returns>
        public new int GetValues(object[] values)
        {
            ThrowIfDisposed();
            ArgumentNullException.ThrowIfNull(values);

            int count = Math.Min(values.Length, FieldCount);

            // Handle physical columns
            int physColumns = Math.Min(_physicalFieldCount, count);
            if (physColumns > 0)
            {
                base.GetValues(values);
            }

            // Handle virtual columns
            for (int i = _physicalFieldCount; i < count; i++)
            {
                if (_virtualColumns.TryGetValue(i, out VirtualColumn? virtualColumn) && virtualColumn != null)
                {
                    values[i] = virtualColumn.DefaultValue ?? DBNull.Value;
                }
                else
                {
                    values[i] = DBNull.Value;
                }
            }

            return count;
        }

        /// <summary>
        /// Asynchronously retrieves all the attribute values of the current record into the provided array.
        /// </summary>
        /// <param name="values">An array to hold the attribute values.</param>
        /// <param name="cancellationToken">A token to cancel the operation.</param>
        /// <returns>The number of values copied into the array.</returns>
        public new async Task<int> GetValuesAsync(object[] values, CancellationToken cancellationToken = default)
        {
            ThrowIfDisposed();
            ArgumentNullException.ThrowIfNull(values);

            int count = Math.Min(values.Length, FieldCount);

            // Handle physical columns
            int physColumns = Math.Min(_physicalFieldCount, count);
            if (physColumns > 0)
            {
                await base.GetValuesAsync(values, cancellationToken).ConfigureAwait(false);
            }

            // Handle virtual columns
            for (int i = _physicalFieldCount; i < count; i++)
            {
                if (_virtualColumns.TryGetValue(i, out VirtualColumn? virtualColumn) && virtualColumn != null)
                {
                    values[i] = virtualColumn.DefaultValue ?? DBNull.Value;
                }
                else
                {
                    values[i] = DBNull.Value;
                }
            }

            return count;
        }

        /// <summary>
        /// Determines whether the specified column contains a null value.
        /// </summary>
        /// <param name="i">The zero-based column ordinal.</param>
        /// <returns>True if the specified column value is null; otherwise, false.</returns>
        public new bool IsDBNull(int i)
        {
            ThrowIfDisposed();

            if (i < _physicalFieldCount)
            {
                return base.IsDBNull(i);
            }
            else if (_virtualColumns.TryGetValue(i, out VirtualColumn? virtualColumn) && virtualColumn != null)
            {
                return virtualColumn.DefaultValue == null || Convert.IsDBNull(virtualColumn.DefaultValue);
            }
            else
            {
                return true;
            }
        }

        /// <summary>
        /// Asynchronously determines whether the column contains a null value.
        /// </summary>
        /// <param name="i">The zero-based column ordinal.</param>
        /// <param name="cancellationToken">A token to cancel the operation.</param>
        /// <returns>True if the column value is null; otherwise, false.</returns>
        public new async Task<bool> IsDBNullAsync(int i, CancellationToken cancellationToken = default)
        {
            ThrowIfDisposed();

            if (i < _physicalFieldCount)
            {
                return await base.IsDBNullAsync(i, cancellationToken).ConfigureAwait(false);
            }
            else if (_virtualColumns.TryGetValue(i, out VirtualColumn? virtualColumn) && virtualColumn != null)
            {
                return virtualColumn.DefaultValue == null || Convert.IsDBNull(virtualColumn.DefaultValue);
            }
            else
            {
                return true;
            }
        }

        /// <summary>
        /// Advances the reader to the next record.
        /// </summary>
        /// <returns>true if there are more rows; otherwise false.</returns>
        public new bool Read()
        {
            ThrowIfDisposed();
            return base.Read();
        }

        /// <summary>
        /// Asynchronously advances the reader to the next record.
        /// </summary>
        /// <param name="cancellationToken">Optional cancellation token.</param>
        /// <returns>true if there are more rows; otherwise false.</returns>
        public new Task<bool> ReadAsync(CancellationToken cancellationToken = default)
        {
            ThrowIfDisposed();
            return base.ReadAsync(cancellationToken);
        }

        /// <summary>
        /// Advances the reader to the next result.
        /// </summary>
        /// <returns>true if there are more results; otherwise false.</returns>
        public new bool NextResult()
        {
            ThrowIfDisposed();
            return base.NextResult();
        }

        /// <summary>
        /// Asynchronously advances the reader to the next result.
        /// </summary>
        /// <param name="cancellationToken">Optional cancellation token.</param>
        /// <returns>true if there are more results; otherwise false.</returns>
        public new Task<bool> NextResultAsync(CancellationToken cancellationToken = default)
        {
            ThrowIfDisposed();
            return base.NextResultAsync(cancellationToken);
        }

        /// <summary>
        /// Releases all resources used by the ParquetDataReaderWithVirtualColumns.
        /// </summary>
        public new void Close()
        {
            if (_disposed) return;

            base.Close();
            _disposed = true;
        }

        /// <summary>
        /// Releases all resources used by the ParquetDataReaderWithVirtualColumns.
        /// </summary>
        protected override void Dispose(bool disposing)
        {
            if (_disposed) return;

            if (disposing)
            {
                _virtualColumns?.Clear();
            }

            base.Dispose(disposing);
            _disposed = true;
        }

        /// <summary>
        /// Asynchronously releases all resources used by the ParquetDataReaderWithVirtualColumns.
        /// </summary>
        /// <returns>A task representing the asynchronous dispose operation.</returns>
        public new async ValueTask DisposeAsync()
        {
            if (_disposed) return;

            // Dispose base first
            await base.DisposeAsync().ConfigureAwait(false);

            // Clear virtual columns
            _virtualColumns?.Clear();
            _disposed = true;
        }

        /// <summary>
        /// Throws an ObjectDisposedException if the reader is disposed.
        /// </summary>
        /// <exception cref="ObjectDisposedException">Thrown when the reader is disposed.</exception>
        internal new void ThrowIfDisposed()
        {
            // Update to use ObjectDisposedException.ThrowIf
            ObjectDisposedException.ThrowIf(_disposed, this);

            base.ThrowIfDisposed();
        }

        // Additional IDataReader/IDataRecord implementations that need to be changed for virtual columns

        /// <summary>
        /// Gets the column with the specified name.
        /// </summary>
        /// <param name="name">The name of the column to find.</param>
        /// <returns>The column with the specified name.</returns>
        public new object this[string name] => GetValue(GetOrdinal(name));

        /// <summary>
        /// Gets the column with the specified index.
        /// </summary>
        /// <param name="i">The zero-based column ordinal.</param>
        /// <returns>The column with the specified index.</returns>
        public new object this[int i] => GetValue(i);

        /// <summary>
        /// Gets a value indicating whether the data reader is closed.
        /// </summary>
        public new bool IsClosed => _disposed || base.IsClosed;
    }
}
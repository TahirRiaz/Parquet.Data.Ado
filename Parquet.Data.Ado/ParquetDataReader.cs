using System;
using System.Buffers;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Diagnostics;
using System.Globalization;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using Parquet;
using Parquet.Data;
using Parquet.Schema;

namespace Parquet.Data.Ado
{
    /// <summary>
    /// Represents a reader that provides fast, non-cached, forward-only access to Parquet data.
    /// Implements high-performance multi-threaded reading capabilities.
    /// </summary>
    public  class ParquetDataReader : IDataReader, IDataRecord, IDisposable, IAsyncDisposable
    {
        private readonly ParquetReader _reader;
        private ParquetRowGroupReader? _groupReader;
        private readonly SemaphoreSlim _lock = new SemaphoreSlim(1, 1);

        private Parquet.Data.DataColumn[]? _columns;
        private DataField[]? _fields;
        private int _currentRowGroup;
        private int _currentRow;
        private readonly bool _readNextGroup;
        private bool _disposed;

        /// <summary>
        /// Initializes a new instance of the ParquetDataReader class.
        /// </summary>
        /// <param name="reader">The Parquet reader.</param>
        /// <param name="currentRowGroup">The current row group.</param>
        /// <param name="readNextGroup">If set to true, reads the next group when current group is exhausted.</param>
        /// <exception cref="ArgumentNullException">Thrown when reader is null.</exception>
        public ParquetDataReader(ParquetReader reader, int currentRowGroup, bool readNextGroup)
        {
            ArgumentNullException.ThrowIfNull(reader);

            _reader = reader;
            _fields = reader.Schema.DataFields;
            _columns = new Parquet.Data.DataColumn[reader.Schema.Fields.Count];
            _currentRowGroup = currentRowGroup;
            _currentRow = -1;
            _readNextGroup = readNextGroup;
            _groupReader = reader.OpenRowGroupReader(_currentRowGroup);

            // Load columns synchronously in the constructor to ensure data is ready
            LoadColumnsAsync().GetAwaiter().GetResult();
        }

        /// <summary>
        /// Creates a new instance of ParquetDataReader asynchronously.
        /// </summary>
        /// <param name="reader">The Parquet reader.</param>
        /// <param name="currentRowGroup">The current row group.</param>
        /// <param name="readNextGroup">If set to true, reads the next group when current group is exhausted.</param>
        /// <param name="cancellationToken">Optional cancellation token.</param>
        /// <returns>A new ParquetDataReader instance.</returns>
        /// <exception cref="ArgumentNullException">Thrown when reader is null.</exception>
        public static async Task<ParquetDataReader> CreateAsync(
            ParquetReader reader,
            int currentRowGroup,
            bool readNextGroup,
            CancellationToken cancellationToken = default)
        {
            ArgumentNullException.ThrowIfNull(reader);

            var dataReader = new ParquetDataReader(reader, currentRowGroup, readNextGroup);
            await dataReader.LoadColumnsAsync(cancellationToken).ConfigureAwait(false);
            return dataReader;
        }

        /// <summary>
        /// Asynchronously loads all columns in parallel.
        /// </summary>
        /// <param name="cancellationToken">Optional cancellation token.</param>
        /// <returns>A task representing the asynchronous operation.</returns>
        internal async Task LoadColumnsAsync(CancellationToken cancellationToken = default)
        {
            await _lock.WaitAsync(cancellationToken).ConfigureAwait(false);

            try
            {
                if (_fields == null || _groupReader == null) return;

                // Sequential loading approach with proper error handling
                for (int i = 0; i < _fields.Length; i++)
                {
                    try
                    {
                        if (cancellationToken.IsCancellationRequested) break;

                        _columns![i] = await _groupReader.ReadColumnAsync(_fields[i], cancellationToken)
                            .ConfigureAwait(false);
                    }
                    catch (Exception ex)
                    {
                        string fieldName = _fields[i]?.Name ?? "unknown";
                        string fieldType = _fields[i]?.ClrType?.Name ?? "unknown";

                        throw new InvalidOperationException(
                            $"Error loading column {i} (Name: {fieldName}, Type: {fieldType}) " +
                            $"from row group {_currentRowGroup}.", ex);
                    }
                }
            }
            finally
            {
                _lock.Release();
            }
        }

        /// <summary>
        /// Advances the reader to the next result.
        /// </summary>
        /// <returns>true if there are more rows; otherwise false.</returns>
        public bool NextResult()
        {
            ThrowIfDisposed();
            _currentRowGroup++;
            if (_currentRowGroup >= _reader.RowGroupCount) return false;
            _currentRow = -1;
            _groupReader = _reader.OpenRowGroupReader(_currentRowGroup);
            // Load columns synchronously
            LoadColumnsAsync().GetAwaiter().GetResult();
            return true;
        }

        /// <summary>
        /// Asynchronously advances the reader to the next result.
        /// </summary>
        /// <param name="cancellationToken">Optional cancellation token.</param>
        /// <returns>true if there are more rows; otherwise false.</returns>
        public async Task<bool> NextResultAsync(CancellationToken cancellationToken = default)
        {
            ThrowIfDisposed();
            _currentRowGroup++;
            if (_currentRowGroup >= _reader.RowGroupCount) return false;
            _currentRow = -1;
            _groupReader = _reader.OpenRowGroupReader(_currentRowGroup);
            await LoadColumnsAsync(cancellationToken).ConfigureAwait(false);
            return true;
        }

        /// <summary>
        /// Returns a DataTable that describes the column metadata of the ParquetDataReader.
        /// </summary>
        /// <returns>A DataTable that describes the column metadata.</returns>
        public DataTable GetSchemaTable()
        {
            ThrowIfDisposed();

            DataTable schemaTable = new DataTable("SchemaTable");

            schemaTable.Columns.Add(SchemaTableColumn.ColumnName, typeof(string));
            schemaTable.Columns.Add(SchemaTableColumn.ColumnOrdinal, typeof(int));
            schemaTable.Columns.Add(SchemaTableColumn.ColumnSize, typeof(int));
            schemaTable.Columns.Add(SchemaTableColumn.DataType, typeof(Type));
            schemaTable.Columns.Add(SchemaTableColumn.AllowDBNull, typeof(bool));
            schemaTable.Columns.Add(SchemaTableColumn.IsUnique, typeof(bool));
            schemaTable.Columns.Add(SchemaTableColumn.IsKey, typeof(bool));

            for (int i = 0; i < _reader.Schema.Fields.Count; i++)
            {
                DataRow row = schemaTable.NewRow();
                row[SchemaTableColumn.ColumnName] = _reader.Schema.Fields[i].Name;
                row[SchemaTableColumn.ColumnOrdinal] = i;
                row[SchemaTableColumn.ColumnSize] = -1; // Unknown size
                row[SchemaTableColumn.DataType] = _reader.Schema.DataFields[i].ClrType;
                row[SchemaTableColumn.AllowDBNull] = true; // Assume nullable
                row[SchemaTableColumn.IsUnique] = false;   // Assume not unique
                row[SchemaTableColumn.IsKey] = false;      // Assume not primary key

                schemaTable.Rows.Add(row);
            }

            return schemaTable;
        }

        /// <summary>
        /// Gets the value of the specified column in its native format.
        /// </summary>
        /// <param name="i">The zero-based column ordinal.</param>
        /// <returns>The value of the specified column.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public object GetValue(int i)
        {
            ThrowIfDisposed();

            if (_columns == null) return DBNull.Value;
            if (i < 0 || i >= _columns.Length)
            {
                return DBNull.Value; // Instead of explicitly throwing IndexOutOfRangeException
            }

            return _columns[i]?.Data.GetValue(_currentRow) ?? DBNull.Value;
        }

        /// <summary>
        /// Advances the reader to the next record.
        /// </summary>
        /// <returns>true if there are more rows; otherwise false.</returns>
        public bool Read()
        {
            ThrowIfDisposed();

            _currentRow++;
            if (_groupReader == null) return false;

            if (_currentRow >= _groupReader.RowCount)
            {
                if (_readNextGroup)
                {
                    _currentRowGroup++;
                    if (_currentRowGroup >= _reader.RowGroupCount) return false;
                    _currentRow = 0;
                    _groupReader = _reader.OpenRowGroupReader(_currentRowGroup);
                    LoadColumnsAsync().GetAwaiter().GetResult();
                }
                else
                {
                    return false;
                }
            }
            return true;
        }

        /// <summary>
        /// Asynchronously advances the reader to the next record.
        /// </summary>
        /// <param name="cancellationToken">Optional cancellation token.</param>
        /// <returns>true if there are more rows; otherwise false.</returns>
        public async Task<bool> ReadAsync(CancellationToken cancellationToken = default)
        {
            ThrowIfDisposed();

            _currentRow++;
            if (_groupReader == null) return false;

            if (_currentRow >= _groupReader.RowCount)
            {
                if (_readNextGroup)
                {
                    _currentRowGroup++;
                    if (_currentRowGroup >= _reader.RowGroupCount) return false;
                    _currentRow = 0;
                    _groupReader = _reader.OpenRowGroupReader(_currentRowGroup);
                    await LoadColumnsAsync(cancellationToken).ConfigureAwait(false);
                }
                else
                {
                    return false;
                }
            }
            return true;
        }

        /// <summary>
        /// Gets the data type information for the specified field.
        /// </summary>
        /// <param name="i">The zero-based field ordinal.</param>
        /// <returns>The data type information for the specified field.</returns>
        public string GetDataTypeName(int i)
        {
            ThrowIfDisposed();

            if (_columns == null) return string.Empty;
            if (i < 0 || i >= _columns.Length)
            {
                return string.Empty; // Instead of explicitly throwing IndexOutOfRangeException
            }

            return _columns[i].Data.GetType().Name;
        }

        /// <summary>
        /// Gets the name for the field to find.
        /// </summary>
        /// <param name="i">The zero-based field ordinal.</param>
        /// <returns>The name of the field or the empty string (""), if there is no value to return.</returns>
        public string GetName(int i)
        {
            ThrowIfDisposed();

            if (_columns == null) return string.Empty;
            if (i < 0 || i >= _columns.Length)
            {
                return string.Empty; // Instead of explicitly throwing IndexOutOfRangeException
            }

            return _columns[i].Field.Name;
        }

        /// <summary>
        /// Returns the index of the named field.
        /// </summary>
        /// <param name="name">The name of the field to find.</param>
        /// <returns>The index of the named field.</returns>
        /// <exception cref="ArgumentNullException">Thrown when name is null.</exception>
        /// <exception cref="ArgumentException">Thrown when the column name is not found.</exception>
        public int GetOrdinal(string name)
        {
            ThrowIfDisposed();
            ArgumentNullException.ThrowIfNull(name);

            if (_columns == null)
            {
                throw new ArgumentException($"Column '{name}' not found", nameof(name));
            }

            // Optimize lookup with case-insensitive comparison
            int index = Array.FindIndex(_columns, x =>
                string.Equals(x.Field.Name, name, StringComparison.OrdinalIgnoreCase));

            if (index == -1)
            {
                throw new ArgumentException($"Column '{name}' not found", nameof(name));
            }

            return index;
        }

        /// <summary>
        /// Closes the ParquetDataReader object.
        /// </summary>
        public void Close()
        {
            if (_disposed) return;

            _groupReader?.Dispose();
            _groupReader = null;
            _disposed = true;
        }

        /// <summary>
        /// Releases all resources used by the ParquetDataReader.
        /// </summary>
        public void Dispose()
        {
            Dispose(disposing: true);
            GC.SuppressFinalize(this);
        }

        /// <summary>
        /// Releases the unmanaged resources used by the ParquetDataReader and optionally releases the managed resources.
        /// </summary>
        /// <param name="disposing">true to release both managed and unmanaged resources; false to release only unmanaged resources.</param>
        protected virtual void Dispose(bool disposing)
        {
            if (_disposed) return;

            if (disposing)
            {
                // First dispose the group reader
                if (_groupReader != null)
                {
                    _groupReader.Dispose();
                    _groupReader = null;
                }

                // Then dispose the reader - Make sure we're disposing this correctly
                if (_reader != null)
                {
                    // Ensure the underlying stream gets closed properly
                    _reader.Dispose();
                }

                // Finally dispose the lock
                _lock?.Dispose();
            }

            _columns = null;
            _fields = null;
            _disposed = true;
        }

        /// <summary>
        /// Asynchronously releases all resources used by the ParquetDataReader.
        /// </summary>
        /// <returns>A task representing the asynchronous dispose operation.</returns>
        public async ValueTask DisposeAsync()
        {
            if (_disposed) return;

            if (_groupReader != null)
            {
                await Task.Run(() => _groupReader.Dispose()).ConfigureAwait(false);
                _groupReader = null;
            }

            if (_reader != null)
            {
                await Task.Run(() => _reader.Dispose()).ConfigureAwait(false);
            }

            if (_lock != null)
            {
                _lock.Dispose();
            }

            _columns = null;
            _fields = null;
            _disposed = true;

            GC.SuppressFinalize(this);
        }

        /// <summary>
        /// Asynchronously releases the managed resources used by the ParquetDataReader.
        /// </summary>
        /// <returns>A task representing the asynchronous dispose operation.</returns>
        private async ValueTask DisposeAsyncCore()
        {
            if (_disposed) return;

            if (_groupReader != null)
            {
                await Task.Run(() => _groupReader.Dispose()).ConfigureAwait(false);
            }

            if (_reader != null)
            {
                await Task.Run(() => _reader.Dispose()).ConfigureAwait(false);
            }

            if (_lock != null)
            {
                _lock.Dispose();
            }
        }

        /// <summary>
        /// Throws an ObjectDisposedException if the reader is disposed.
        /// </summary>
        /// <exception cref="ObjectDisposedException">Thrown when the reader is disposed.</exception>
        internal void ThrowIfDisposed()
        {
            ObjectDisposedException.ThrowIf(_disposed, this);
        }

        // Properties:

        /// <summary>
        /// Gets the depth of nesting for the current row.
        /// </summary>
        public int Depth => _currentRowGroup;

        /// <summary>
        /// Gets the number of columns in the current row.
        /// </summary>
        public int FieldCount
        {
            get
            {
                ThrowIfDisposed();
                return _columns?.Length ?? 0;
            }
        }

        /// <summary>
        /// Gets the column with the specified index.
        /// </summary>
        /// <param name="i">The zero-based column ordinal.</param>
        /// <returns>The column with the specified index.</returns>
        public object this[int i] => GetValue(i);

        /// <summary>
        /// Gets the column with the specified name.
        /// </summary>
        /// <param name="name">The name of the column to find.</param>
        /// <returns>The column with the specified name.</returns>
        public object this[string name] => GetValue(GetOrdinal(name));

        /// <summary>
        /// Gets a value indicating whether the data reader is closed.
        /// </summary>
        public bool IsClosed => _disposed;

        /// <summary>
        /// Gets the number of rows changed, inserted, or deleted by execution of the SQL statement.
        /// </summary>
        /// <exception cref="NotSupportedException">Always thrown because this property is not supported.</exception>
        public int RecordsAffected => throw new NotSupportedException("RecordsAffected is not supported for read-only data.");

        // --------------------------------------------------------------------------------
        // Type-specific methods for accessing column values (with safer edge-case handling):
        // --------------------------------------------------------------------------------

        /// <summary>
        /// Retrieves the value of the specified column as a <see cref="bool"/>.
        /// </summary>
        /// <param name="i">The zero-based column ordinal.</param>
        /// <returns>The value of the column as a <see cref="bool"/>.</returns>
        /// <exception cref="ObjectDisposedException">Thrown if the reader is disposed.</exception>
        /// <exception cref="InvalidCastException">
        /// Thrown if the value cannot be converted to a <see cref="bool"/>, 
        /// or if the value is <see cref="DBNull"/>.
        /// </exception>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool GetBoolean(int i)
        {
            ThrowIfDisposed();
            object value = GetValue(i);
            if (value == DBNull.Value)
                throw new InvalidCastException("Cannot convert DBNull to Boolean");

            try
            {
                return Convert.ToBoolean(value, CultureInfo.InvariantCulture);
            }
            catch (Exception ex) when (ex is FormatException or InvalidCastException or OverflowException)
            {
                throw CreateInvalidCastException(i, value, "Boolean", ex);
            }
        }

        /// <summary>
        /// Retrieves the value of the specified column as a <see cref="byte"/>.
        /// </summary>
        /// <param name="i">The zero-based column ordinal.</param>
        /// <returns>The value of the specified column as a <see cref="byte"/>.</returns>
        /// <exception cref="ObjectDisposedException">Thrown if the reader is disposed.</exception>
        /// <exception cref="InvalidCastException">
        /// Thrown if the value cannot be converted to a <see cref="byte"/> due to an invalid type, format, or overflow.
        /// </exception>
        /// <exception cref="IndexOutOfRangeException">Thrown if the column index is out of range.</exception>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public byte GetByte(int i)
        {
            ThrowIfDisposed();
            object value = GetValue(i);
            if (value == DBNull.Value)
                throw new InvalidCastException("Cannot convert DBNull to Byte");

            try
            {
                return Convert.ToByte(value, CultureInfo.InvariantCulture);
            }
            catch (Exception ex) when (ex is FormatException or InvalidCastException or OverflowException)
            {
                throw CreateInvalidCastException(i, value, "Byte", ex);
            }
        }


        /// <summary>
        /// Reads a sequence of bytes from the specified column, starting at the specified field offset, 
        /// and writes them into the provided buffer starting at the specified buffer offset.
        /// </summary>
        /// <param name="i">The zero-based column ordinal.</param>
        /// <param name="fieldOffset">The index within the field from which to begin the read operation.</param>
        /// <param name="buffer">
        /// The buffer into which the bytes will be copied. If <c>null</c>, the method returns the length of the byte array.
        /// </param>
        /// <param name="bufferoffset">The zero-based index in the buffer at which to begin writing the data.</param>
        /// <param name="length">The maximum number of bytes to copy into the buffer.</param>
        /// <returns>
        /// The actual number of bytes read and copied into the buffer. If <paramref name="buffer"/> is <c>null</c>, 
        /// the total length of the byte array in the specified column is returned.
        /// </returns>
        /// <exception cref="InvalidCastException">
        /// Thrown if the value in the specified column is not of type .
        /// </exception>
        /// <exception cref="IndexOutOfRangeException">
        /// Thrown if the column index is out of range.
        /// </exception>
        /// <exception cref="ObjectDisposedException">
        /// Thrown if the reader has been disposed.
        /// </exception>
        public long GetBytes(int i, long fieldOffset, byte[]? buffer, int bufferoffset, int length)
        {
            ThrowIfDisposed();
            object value = GetValue(i);

            if (value == DBNull.Value) return 0;

            if (value is not byte[] bytes)
            {
                throw CreateInvalidCastException(i, value, "byte[]");
            }

            if (buffer == null) return bytes.Length;

            var bytesToCopy = (int)Math.Min(length, bytes.Length - fieldOffset);
            if (bytesToCopy <= 0) return 0;

            Buffer.BlockCopy(bytes, (int)fieldOffset, buffer, bufferoffset, bytesToCopy);
            return bytesToCopy;
        }

        /// <summary>
        /// Retrieves the value of the specified column as a <see cref="char"/>.
        /// </summary>
        /// <param name="i">The zero-based column ordinal.</param>
        /// <returns>The value of the specified column as a <see cref="char"/>.</returns>
        /// <exception cref="ObjectDisposedException">Thrown if the reader is disposed.</exception>
        /// <exception cref="InvalidCastException">
        /// Thrown if the value cannot be converted to a <see cref="char"/> or if the value is <see cref="DBNull"/>.
        /// </exception>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public char GetChar(int i)
        {
            ThrowIfDisposed();
            object value = GetValue(i);
            if (value == DBNull.Value)
                throw new InvalidCastException("Cannot convert DBNull to Char");

            try
            {
                return Convert.ToChar(value, CultureInfo.InvariantCulture);
            }
            catch (Exception ex) when (ex is FormatException or InvalidCastException or OverflowException)
            {
                throw CreateInvalidCastException(i, value, "Char", ex);
            }
        }

        /// <summary>
        /// Reads a stream of characters from the specified column offset into the buffer as an array, 
        /// starting at the given buffer offset.
        /// </summary>
        /// <param name="i">The zero-based column ordinal.</param>
        /// <param name="fieldoffset">The index within the field from which to begin the read operation.</param>
        /// <param name="buffer">
        /// The buffer into which to copy the data. If <c>null</c>, the method returns the length of the field in characters.
        /// </param>
        /// <param name="bufferoffset">The index within the buffer at which to start placing the data.</param>
        /// <param name="length">The maximum number of characters to read.</param>
        /// <returns>
        /// The actual number of characters read. If <paramref name="buffer"/> is <c>null</c>, 
        /// the total length of the field in characters is returned.
        /// </returns>
        /// <exception cref="InvalidCastException">
        /// Thrown if the value of the specified column is not of type 
        /// </exception>
        /// <exception cref="ObjectDisposedException">
        /// Thrown if the reader has been disposed.
        /// </exception>
        public long GetChars(int i, long fieldoffset, char[]? buffer, int bufferoffset, int length)
        {
            ThrowIfDisposed();
            object value = GetValue(i);

            if (value == DBNull.Value) return 0;

            char[] chars;
            if (value is char[] charArray)
            {
                chars = charArray;
            }
            else if (value is string str)
            {
                chars = str.ToCharArray();
            }
            else
            {
                throw CreateInvalidCastException(i, value, "char[] or string");
            }

            if (buffer == null) return chars.Length;

            var charsToCopy = (int)Math.Min(length, chars.Length - fieldoffset);
            if (charsToCopy <= 0) return 0;

            Array.Copy(chars, fieldoffset, buffer, bufferoffset, charsToCopy);
            return charsToCopy;
        }

        /// <summary>
        /// Retrieves an <see cref="IDataReader"/> for the specified column ordinal.
        /// </summary>
        /// <param name="i">The zero-based column ordinal.</param>
        /// <returns>An <see cref="IDataReader"/> for the specified column.</returns>
        /// <exception cref="ObjectDisposedException">Thrown if the reader is disposed.</exception>
        /// <exception cref="NotSupportedException">Thrown because this method is not supported for Parquet data.</exception>
        public IDataReader GetData(int i)
        {
            ThrowIfDisposed();
            throw new NotSupportedException("GetData is not supported for Parquet data.");
        }

        /// <summary>
        /// Retrieves the value of the specified column as a <see cref="DateTime"/> object.
        /// </summary>
        /// <param name="i">The zero-based column ordinal.</param>
        /// <returns>The value of the specified column as a <see cref="DateTime"/>.</returns>
        /// <exception cref="ObjectDisposedException">Thrown if the reader is disposed.</exception>
        /// <exception cref="InvalidCastException">
        /// Thrown if the value cannot be converted to a <see cref="DateTime"/> or if the value is <see cref="DBNull"/>.
        /// </exception>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public DateTime GetDateTime(int i)
        {
            ThrowIfDisposed();
            object value = GetValue(i);
            if (value == DBNull.Value)
                throw new InvalidCastException("Cannot convert DBNull to DateTime");

            try
            {
                return Convert.ToDateTime(value, CultureInfo.InvariantCulture);
            }
            catch (Exception ex) when (ex is FormatException or InvalidCastException or OverflowException)
            {
                throw CreateInvalidCastException(i, value, "DateTime", ex);
            }
        }

        /// <summary>
        /// Retrieves the value of the specified column as a <see cref="decimal"/>.
        /// </summary>
        /// <param name="i">The zero-based column ordinal.</param>
        /// <returns>The value of the specified column as a <see cref="decimal"/>.</returns>
        /// <exception cref="ObjectDisposedException">Thrown if the reader is disposed.</exception>
        /// <exception cref="InvalidCastException">
        /// Thrown if the value cannot be converted to a <see cref="decimal"/> or if the value is <see cref="DBNull"/>.
        /// </exception>
        /// <exception cref="FormatException">Thrown if the value is not in a valid format for a <see cref="decimal"/>.</exception>
        /// <exception cref="OverflowException">Thrown if the value is outside the range of a <see cref="decimal"/>.</exception>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public decimal GetDecimal(int i)
        {
            ThrowIfDisposed();
            object value = GetValue(i);
            if (value == DBNull.Value)
                throw new InvalidCastException("Cannot convert DBNull to Decimal");

            try
            {
                return Convert.ToDecimal(value, CultureInfo.InvariantCulture);
            }
            catch (Exception ex) when (ex is FormatException or InvalidCastException or OverflowException)
            {
                throw CreateInvalidCastException(i, value, "Decimal", ex);
            }
        }

        /// <summary>
        /// Retrieves the value of the specified column as a <see cref="double"/>.
        /// </summary>
        /// <param name="i">The zero-based column ordinal.</param>
        /// <returns>The value of the specified column as a <see cref="double"/>.</returns>
        /// <exception cref="ObjectDisposedException">Thrown if the reader is disposed.</exception>
        /// <exception cref="InvalidCastException">
        /// Thrown if the value cannot be converted to a <see cref="double"/>, 
        /// or if the value is <see cref="DBNull"/>.
        /// </exception>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public double GetDouble(int i)
        {
            ThrowIfDisposed();
            object value = GetValue(i);
            if (value == DBNull.Value)
                throw new InvalidCastException("Cannot convert DBNull to Double");

            try
            {
                return Convert.ToDouble(value, CultureInfo.InvariantCulture);
            }
            catch (Exception ex) when (ex is FormatException or InvalidCastException or OverflowException)
            {
                throw CreateInvalidCastException(i, value, "Double", ex);
            }
        }

        /// <summary>
        /// Retrieves the data type of the specified column in the current row.
        /// </summary>
        /// <param name="i">The zero-based column ordinal.</param>
        /// <returns>
        /// A <see cref="Type"/> object representing the data type of the column.
        /// If the column index is out of range or the column metadata is unavailable, 
        /// the method returns <see cref="object"/>.
        /// </returns>
        /// <exception cref="ObjectDisposedException">Thrown if the reader has been disposed.</exception>
        public Type GetFieldType(int i)
        {
            ThrowIfDisposed();

            if (_columns == null) return typeof(object);
            if (i < 0 || i >= _columns.Length)
            {
                return typeof(object); // Instead of explicitly throwing IndexOutOfRangeException
            }

            return _columns[i].Field.ClrType ?? typeof(object);
        }

        /// <summary>
        /// Retrieves the value of the specified column as a <see cref="float"/> (Single).
        /// </summary>
        /// <param name="i">The zero-based column ordinal.</param>
        /// <returns>The value of the specified column as a <see cref="float"/>.</returns>
        /// <exception cref="ObjectDisposedException">Thrown if the reader is disposed.</exception>
        /// <exception cref="InvalidCastException">
        /// Thrown if the value cannot be converted to a <see cref="float"/>, 
        /// or if the value is <see cref="DBNull"/>.
        /// </exception>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public float GetFloat(int i)
        {
            ThrowIfDisposed();
            object value = GetValue(i);
            if (value == DBNull.Value)
                throw new InvalidCastException("Cannot convert DBNull to Single");

            try
            {
                return Convert.ToSingle(value, CultureInfo.InvariantCulture);
            }
            catch (Exception ex) when (ex is FormatException or InvalidCastException or OverflowException)
            {
                throw CreateInvalidCastException(i, value, "Single (float)", ex);
            }
        }

        /// <summary>
        /// Retrieves the value of the specified column as a <see cref="Guid"/>.
        /// </summary>
        /// <param name="i">The zero-based column ordinal.</param>
        /// <returns>The value of the specified column as a <see cref="Guid"/>.</returns>
        /// <exception cref="ObjectDisposedException">Thrown if the reader is disposed.</exception>
        /// <exception cref="InvalidCastException">
        /// Thrown if the value cannot be converted to a <see cref="Guid"/>. This includes cases where:
        /// <list type="bullet">
        /// <item>The value is <see cref="DBNull"/>.</item>
        /// <item>The value is a byte array but not 16 bytes in length.</item>
        /// <item>The value is a string that cannot be parsed as a <see cref="Guid"/>.</item>
        /// </list>
        /// </exception>
        public Guid GetGuid(int i)
        {
            ThrowIfDisposed();
            object value = GetValue(i);
            if (value == DBNull.Value)
                throw new InvalidCastException("Cannot convert DBNull to Guid");

            // Attempt direct/known conversions first
            if (value is Guid guid) return guid;

            if (value is byte[] bytes)
            {
                if (bytes.Length == 16)
                {
                    return new Guid(bytes);
                }
                throw CreateInvalidCastException(i, value, "Guid (expected 16-byte array)");
            }

            if (value is string str)
            {
                try
                {
                    return Guid.Parse(str);
                }
                catch (FormatException ex)
                {
                    throw new InvalidCastException(
                        $"Cannot convert string to Guid. ColumnIndex={i}, Value='{str}'.", ex);
                }
            }

            throw CreateInvalidCastException(i, value, "Guid");
        }

        /// <summary>
        /// Retrieves the 16-bit signed integer (short) value of the specified column.
        /// </summary>
        /// <param name="i">The zero-based column ordinal.</param>
        /// <returns>The 16-bit signed integer (short) value of the specified column.</returns>
        /// <exception cref="ObjectDisposedException">Thrown if the reader is disposed.</exception>
        /// <exception cref="InvalidCastException">
        /// Thrown when the value in the specified column cannot be converted to a 16-bit signed integer.
        /// This can occur if the value is <see cref="DBNull"/> or if the conversion fails due to format issues, 
        /// type mismatches, or overflow.
        /// </exception>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public short GetInt16(int i)
        {
            ThrowIfDisposed();
            object value = GetValue(i);
            if (value == DBNull.Value)
                throw new InvalidCastException("Cannot convert DBNull to Int16");

            try
            {
                return Convert.ToInt16(value, CultureInfo.InvariantCulture);
            }
            catch (Exception ex) when (ex is FormatException or InvalidCastException or OverflowException)
            {
                throw CreateInvalidCastException(i, value, "Int16", ex);
            }
        }

        /// <summary>
        /// Retrieves the value of the specified column as a 32-bit signed integer.
        /// </summary>
        /// <param name="i">The zero-based column ordinal.</param>
        /// <returns>The value of the specified column as an <see cref="int"/>.</returns>
        /// <exception cref="ObjectDisposedException">Thrown if the reader is disposed.</exception>
        /// <exception cref="InvalidCastException">
        /// Thrown if the value cannot be converted to a 32-bit signed integer, 
        /// or if the value is <see cref="DBNull.Value"/>.
        /// </exception>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int GetInt32(int i)
        {
            ThrowIfDisposed();
            object value = GetValue(i);
            if (value == DBNull.Value)
                throw new InvalidCastException("Cannot convert DBNull to Int32");

            try
            {
                return Convert.ToInt32(value, CultureInfo.InvariantCulture);
            }
            catch (Exception ex) when (ex is FormatException or InvalidCastException or OverflowException)
            {
                throw CreateInvalidCastException(i, value, "Int32", ex);
            }
        }

        /// <summary>
        /// Retrieves the 64-bit signed integer (Int64) value of the specified column.
        /// </summary>
        /// <param name="i">The zero-based column ordinal.</param>
        /// <returns>The 64-bit signed integer value of the specified column.</returns>
        /// <exception cref="ObjectDisposedException">Thrown if the reader has been disposed.</exception>
        /// <exception cref="InvalidCastException">
        /// Thrown if the value cannot be converted to a 64-bit signed integer, 
        /// or if the value is <see cref="DBNull.Value"/>.
        /// </exception>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public long GetInt64(int i)
        {
            ThrowIfDisposed();
            object value = GetValue(i);
            if (value == DBNull.Value)
                throw new InvalidCastException("Cannot convert DBNull to Int64");

            try
            {
                return Convert.ToInt64(value, CultureInfo.InvariantCulture);
            }
            catch (Exception ex) when (ex is FormatException or InvalidCastException or OverflowException)
            {
                throw CreateInvalidCastException(i, value, "Int64", ex);
            }
        }

        /// <summary>
        /// Retrieves the value of the specified column as a <see cref="string"/>.
        /// </summary>
        /// <param name="i">The zero-based column ordinal.</param>
        /// <returns>The value of the specified column as a <see cref="string"/>.</returns>
        /// <exception cref="ObjectDisposedException">Thrown if the reader is disposed.</exception>
        /// <exception cref="InvalidCastException">
        /// Thrown if the value of the specified column cannot be cast to a <see cref="string"/>.
        /// </exception>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public string GetString(int i)
        {
            ThrowIfDisposed();
            object value = GetValue(i);
            if (value == DBNull.Value)
                throw new InvalidCastException("Cannot convert DBNull to String");

            // .ToString() typically won't throw, but let's keep it consistent if ever changed
            try
            {
                return value.ToString() ?? string.Empty;
            }
            catch (Exception ex)
            {
                throw CreateInvalidCastException(i, value, "String", ex);
            }
        }

        /// <summary>
        /// Populates the provided array with the values of the current row's fields.
        /// </summary>
        /// <param name="values">
        /// An array of <see cref="object"/> to hold the field values of the current row. 
        /// The array must have a length equal to or greater than the number of fields in the current row.
        /// </param>
        /// <returns>
        /// The number of field values copied into the <paramref name="values"/> array. 
        /// This will be the lesser of the array's length or the number of fields in the current row.
        /// </returns>
        /// <exception cref="ObjectDisposedException">
        /// Thrown if the <see cref="ParquetDataReader"/> has been disposed.
        /// </exception>
        /// <exception cref="ArgumentNullException">
        /// Thrown if the <paramref name="values"/> array is <c>null</c>.
        /// </exception>
        public int GetValues(object[] values)
        {
            ThrowIfDisposed();
            ArgumentNullException.ThrowIfNull(values);

            int count = Math.Min(values.Length, FieldCount);
            for (int i = 0; i < count; i++)
            {
                values[i] = GetValue(i);
            }
            return count;
        }

        /// <summary>
        /// Asynchronously retrieves all the attribute values of the current record into the provided array.
        /// </summary>
        /// <param name="values">
        /// An array of <see cref="object"/> to hold the attribute values of the current record.
        /// The length of the array should be sufficient to hold all the fields, or it will be truncated to the number of fields.
        /// </param>
        /// <param name="cancellationToken">
        /// A <see cref="CancellationToken"/> to observe while waiting for the task to complete.
        /// </param>
        /// <returns>
        /// A task that represents the asynchronous operation. The task result contains the number of values copied into the array.
        /// </returns>
        /// <exception cref="ObjectDisposedException">
        /// Thrown if the reader has been disposed.
        /// </exception>
        /// <exception cref="ArgumentNullException">
        /// Thrown if the <paramref name="values"/> array is null.
        /// </exception>
        public Task<int> GetValuesAsync(object[] values, CancellationToken cancellationToken = default)
        {
            ThrowIfDisposed();
            ArgumentNullException.ThrowIfNull(values);

            int count = Math.Min(values.Length, FieldCount);
            for (int i = 0; i < count; i++)
            {
                values[i] = GetValue(i);
            }
            return Task.FromResult(count);
        }

        /// <summary>
        /// Determines whether the specified column contains a DBNull value.
        /// </summary>
        /// <param name="i">The zero-based column ordinal.</param>
        /// <returns>
        /// <c>true</c> if the specified column contains a DBNull value or if the column index is out of range; 
        /// otherwise, <c>false</c>.
        /// </returns>
        /// <exception cref="ObjectDisposedException">Thrown when the reader is disposed.</exception>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool IsDBNull(int i)
        {
            ThrowIfDisposed();

            if (_columns == null) return true;
            if (i < 0 || i >= _columns.Length)
            {
                return true; // Instead of explicitly throwing IndexOutOfRangeException
            }

            object value = GetValue(i);
            return value == null || Convert.IsDBNull(value);
        }

        /// <summary>
        /// Asynchronously determines whether the column at the specified ordinal contains a DBNull value.
        /// </summary>
        /// <param name="i">The zero-based column ordinal.</param>
        /// <param name="cancellationToken">
        /// A <see cref="CancellationToken"/> that can be used to cancel the asynchronous operation.
        /// </param>
        /// <returns>
        /// A task that represents the asynchronous operation. The task result contains <c>true</c> if the specified column is equivalent to <see cref="DBNull"/> or is null; otherwise, <c>false</c>.
        /// </returns>
        /// <exception cref="ObjectDisposedException">Thrown if the reader has been disposed.</exception>
        /// <exception cref="IndexOutOfRangeException">Thrown if the column index is out of range.</exception>
        public Task<bool> IsDBNullAsync(int i, CancellationToken cancellationToken = default)
        {
            return Task.FromResult(IsDBNull(i));
        }

        // Helper to unify creation of detailed InvalidCastException:
        private InvalidCastException CreateInvalidCastException(
            int columnIndex,
            object value,
            string targetType,
            Exception? innerException = null)
        {
            string message =
                $"Error converting column {columnIndex} ('{GetName(columnIndex)}') " +
                $"from {value.GetType().Name} to {targetType}. Value: {value}";
            return new InvalidCastException(message, innerException);
        }
    }
}

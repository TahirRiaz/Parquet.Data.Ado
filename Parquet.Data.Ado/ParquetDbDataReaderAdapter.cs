using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Threading;
using System.Threading.Tasks;

namespace Parquet.Data.Reader
{
    /// <summary>
    /// Adapter class that wraps a ParquetDataReader and exposes it as a DbDataReader.
    /// </summary>
    public class ParquetDbDataReaderAdapter : DbDataReader, IEnumerable<IDataRecord>
    {
        private readonly ParquetDataReader _reader;

        //
        // Fields to support an accurate HasRows and safe buffering of the very first row.
        //
        private bool _hasRowsInitialized;
        private bool _hasRows;
        private object[]? _firstRowBuffer;
        private bool _firstRowReturned;

        /// <summary>
        /// Initializes a new instance of the <see cref="ParquetDbDataReaderAdapter"/> class.
        /// </summary>
        /// <param name="reader">The <see cref="ParquetDataReader"/> to wrap.</param>
        public ParquetDbDataReaderAdapter(ParquetDataReader reader)
        {
            _reader = reader ?? throw new ArgumentNullException(nameof(reader));
        }

        /// <inheritdoc/>
        /// <remarks>
        /// We override HasRows with a "first-row buffering" technique. The first time HasRows is checked,
        /// we do a one-time read ahead. If a row is found, we mark HasRows = true and store the row in a buffer
        /// so that subsequent Read() does not skip it.
        ///
        /// If you prefer an alternative approach (e.g. if ParquetDataReader exposes a row count), you can simply
        /// delegate to that. For example:
        ///     public override bool HasRows => !_reader.IsClosed  _reader.RowCount > 0;
        /// </remarks>
        public override bool HasRows
        {
            get
            {
                if (!_hasRowsInitialized && !IsClosed)
                {
                    _hasRowsInitialized = true;

                    // Attempt a one-time read to detect a row.
                    if (_reader.Read())
                    {
                        _hasRows = true;

                        // Buffer the entire row so the user won't lose it on the next Read().
                        _firstRowBuffer = new object[_reader.FieldCount];
                        _reader.GetValues(_firstRowBuffer);
                    }
                }

                return _hasRows;
            }
        }

        /// <inheritdoc/>
        public override object this[int ordinal] => GetValue(ordinal);

        /// <inheritdoc/>
        public override object this[string name] => GetValue(GetOrdinal(name));

        /// <inheritdoc/>
        public override int Depth => _reader.Depth;

        /// <inheritdoc/>
        public override int FieldCount => _reader.FieldCount;

        /// <inheritdoc/>
        public override bool IsClosed => _reader.IsClosed;

        /// <inheritdoc/>
        public override int RecordsAffected => _reader.RecordsAffected;

        /// <inheritdoc/>
        public override bool GetBoolean(int ordinal) => (bool)GetBufferedValue(ordinal);

        /// <inheritdoc/>
        public override byte GetByte(int ordinal) => (byte)GetBufferedValue(ordinal);

        /// <inheritdoc/>
        public override long GetBytes(int ordinal, long dataOffset, byte[]? buffer, int bufferOffset, int length)
        {
            // If the row is still buffered, retrieve from the buffered object array if needed:
            if (!_firstRowReturned && _firstRowBuffer != null)
            {
                // We have the entire row in memory as objects. For getting bytes (e.g. varbinary),
                // we can convert it from the buffered value if it's a byte[] or stream-like object.
                // If your underlying usage requires partial reads, you may need a different approach.
                object val = _firstRowBuffer[ordinal] ?? DBNull.Value;
                return GetBytesFromObject(val, dataOffset, buffer, bufferOffset, length);
            }

            return _reader.GetBytes(ordinal, dataOffset, buffer, bufferOffset, length);
        }

        /// <inheritdoc/>
        public override char GetChar(int ordinal) => (char)GetBufferedValue(ordinal);

        /// <inheritdoc/>
        public override long GetChars(int ordinal, long dataOffset, char[]? buffer, int bufferOffset, int length)
        {
            // Similar logic to GetBytes.
            if (!_firstRowReturned && _firstRowBuffer != null)
            {
                object val = _firstRowBuffer[ordinal] ?? DBNull.Value;
                return GetCharsFromObject(val, dataOffset, buffer, bufferOffset, length);
            }

            return _reader.GetChars(ordinal, dataOffset, buffer, bufferOffset, length);
        }

        /// <inheritdoc/>
        public override string GetDataTypeName(int ordinal) => _reader.GetDataTypeName(ordinal);

        /// <inheritdoc/>
        public override DateTime GetDateTime(int ordinal) => (DateTime)GetBufferedValue(ordinal);

        /// <inheritdoc/>
        public override decimal GetDecimal(int ordinal) => (decimal)GetBufferedValue(ordinal);

        /// <inheritdoc/>
        public override double GetDouble(int ordinal) => (double)GetBufferedValue(ordinal);

        /// <inheritdoc/>
        public override Type GetFieldType(int ordinal) => _reader.GetFieldType(ordinal);

        /// <inheritdoc/>
        public override float GetFloat(int ordinal) => (float)GetBufferedValue(ordinal);

        /// <inheritdoc/>
        public override Guid GetGuid(int ordinal) => (Guid)GetBufferedValue(ordinal);

        /// <inheritdoc/>
        public override short GetInt16(int ordinal) => (short)GetBufferedValue(ordinal);

        /// <inheritdoc/>
        public override int GetInt32(int ordinal) => (int)GetBufferedValue(ordinal);

        /// <inheritdoc/>
        public override long GetInt64(int ordinal) => (long)GetBufferedValue(ordinal);

        /// <inheritdoc/>
        public override string GetName(int ordinal) => _reader.GetName(ordinal);

        /// <inheritdoc/>
        public override int GetOrdinal(string name) => _reader.GetOrdinal(name);

        /// <inheritdoc/>
        public override DataTable GetSchemaTable() => _reader.GetSchemaTable();

        /// <inheritdoc/>
        public override string GetString(int ordinal) => (string)GetBufferedValue(ordinal);

        /// <inheritdoc/>
        public override object GetValue(int ordinal)
        {
            return GetBufferedValue(ordinal);
        }

        /// <inheritdoc/>
        public override int GetValues(object[] values)
        {
            ArgumentNullException.ThrowIfNull(values);

            if (!_firstRowReturned && _firstRowBuffer != null)
            {
                // Copy from buffer
                int copyLen = Math.Min(values.Length, _firstRowBuffer.Length);
                Array.Copy(_firstRowBuffer, values, copyLen);
                // Fill remainder with DBNull if any
                for (int i = copyLen; i < values.Length; i++)
                {
                    values[i] = DBNull.Value;
                }
                return copyLen;
            }

            return _reader.GetValues(values);
        }

        /// <inheritdoc/>
        public override bool IsDBNull(int ordinal)
        {
            object val = GetBufferedValue(ordinal);
            return val == null || val is DBNull;
        }

        /// <inheritdoc/>
        public override bool NextResult() => _reader.NextResult();

        /// <inheritdoc/>
        /// <remarks>
        /// We override Read so that if there is a buffered row (from HasRows), we return it first
        /// without advancing the underlying reader.
        /// </remarks>
        public override bool Read()
        {
            if (!_firstRowReturned && _firstRowBuffer != null)
            {
                _firstRowReturned = true;
                return true;
            }

            return _reader.Read();
        }

        /// <inheritdoc/>
        public override void Close() => _reader.Close();

        /// <summary>
        /// Releases the unmanaged resources used by the ParquetConnection and optionally releases the managed resources.
        /// </summary>
        /// <param name="disposing">true to release both managed and unmanaged resources; false to release only unmanaged resources.</param>
        protected override void Dispose(bool disposing)
        {
            if (disposing)
            {
                // Properly close and dispose the reader
                _reader.Close();
                _reader.Dispose();

                // Clear any references that might prevent garbage collection
                _firstRowBuffer = null;
            }
            base.Dispose(disposing);
        }

        // --------------------------------------------------
        //  IEnumerable<IDataRecord> support
        // --------------------------------------------------

        /// <inheritdoc/>
        /// <remarks>
        /// This enumerator reads *from the current position* in the data reader to the end.
        /// If you require iteration from the very beginning, you must either re-execute your
        /// query or have a means of repositioning the underlying ParquetDataReader if supported.
        ///
        /// Usage example:
        ///   foreach (IDataRecord record in adapter)
        ///   {
        ///       // ...
        ///   }
        /// </remarks>
        IEnumerator<IDataRecord> IEnumerable<IDataRecord>.GetEnumerator()
        {
            // Enumerate forward only, from the current position to the end.
            while (!IsClosed && Read())
            {
                yield return this;
            }
        }

        /// <inheritdoc/>
        /// <remarks>
        /// The standard non-generic IEnumerator is implemented by DbDataReader (via DbEnumerator),
        /// but we override GetEnumerator here so that "foreach(object obj in adapter)"
        /// enumerates rows as well.
        /// </remarks>
        public override IEnumerator GetEnumerator() => new DbEnumerator(this);

        // --------------------------------------------------
        //  Async methods
        // --------------------------------------------------

        /// <inheritdoc/>
        public override Task<bool> ReadAsync(CancellationToken cancellationToken)
        {
            // If we still have the buffered row, return it first
            if (!_firstRowReturned && _firstRowBuffer != null)
            {
                _firstRowReturned = true;
                return Task.FromResult(true);
            }

            return _reader.ReadAsync(cancellationToken);
        }

        /// <inheritdoc/>
        public override Task<bool> NextResultAsync(CancellationToken cancellationToken)
            => _reader.NextResultAsync(cancellationToken);

        /// <inheritdoc/>
        public override Task<bool> IsDBNullAsync(int ordinal, CancellationToken cancellationToken)
        {
            // If using a row buffer, handle that
            object val = GetBufferedValue(ordinal);
            if (val == null || val is DBNull) return Task.FromResult(true);

            // Or fallback to the actual async call
            return _reader.IsDBNullAsync(ordinal, cancellationToken);
        }

        /// <summary>
        /// Asynchronously releases resources used by the ParquetDbDataReaderAdapter.
        /// </summary>
        public override async ValueTask DisposeAsync()
        {
            // Ensure we properly dispose the reader asynchronously
            if (_reader is IAsyncDisposable asyncDisposable)
            {
                await asyncDisposable.DisposeAsync().ConfigureAwait(false);
            }
            else
            {
                _reader.Dispose();
            }

            // Clear any references that might prevent garbage collection
            _firstRowBuffer = null;

            await base.DisposeAsync().ConfigureAwait(false);
            GC.SuppressFinalize(this);
        }

        // --------------------------------------------------
        //  Internal buffer / helper methods
        // --------------------------------------------------

        private object GetBufferedValue(int ordinal)
        {
            if (!_firstRowReturned && _firstRowBuffer != null)
            {
                return _firstRowBuffer[ordinal] ?? DBNull.Value;
            }

            return _reader.GetValue(ordinal);
        }

        // If your ParquetDataReader usage might store large binary data, you can adapt these helpers
        // to handle partial reads. For simplicity, we treat them as if they're small or we read them in full.
        private static long GetBytesFromObject(object val, long dataOffset, byte[]? buffer, int bufferOffset, int length)
        {
            if (val is DBNull) return 0L;
            if (val is not byte[] bytes) throw new InvalidCastException("Stored value is not a byte array.");

            // If no buffer provided, just return the total length that would be copied
            if (buffer == null) return bytes.Length;

            int bytesAvailable = bytes.Length - (int)dataOffset;
            if (bytesAvailable <= 0) return 0L;

            int bytesToCopy = Math.Min(length, bytesAvailable);
            Array.Copy(bytes, (int)dataOffset, buffer, bufferOffset, bytesToCopy);
            return bytesToCopy;
        }

        private static long GetCharsFromObject(object val, long dataOffset, char[]? buffer, int bufferOffset, int length)
        {
            if (val is DBNull) return 0L;
            if (val is not string s) throw new InvalidCastException("Stored value is not a string.");

            // If no buffer provided, just return the total length that would be copied
            if (buffer == null) return s.Length;

            int charsAvailable = s.Length - (int)dataOffset;
            if (charsAvailable <= 0) return 0L;

            int charsToCopy = Math.Min(length, charsAvailable);
            s.CopyTo((int)dataOffset, buffer, bufferOffset, charsToCopy);
            return charsToCopy;
        }
    }
}

using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Parquet;
using Parquet.Schema;

namespace Parquet.Data.Ado
{
    /// <summary>
    /// Represents metadata information for a Parquet file.
    /// </summary>
    public class ParquetMetadata : IDisposable
    {
        private readonly ParquetReader _reader;
        private bool _disposed;

        /// <summary>
        /// Initializes a new instance of the <see cref="ParquetMetadata"/> class.
        /// </summary>
        /// <param name="reader">The Parquet reader from which to extract metadata.</param>
        /// <exception cref="ArgumentNullException">Thrown when reader is null.</exception>
        internal ParquetMetadata(ParquetReader reader)
        {
            _reader = reader ?? throw new ArgumentNullException(nameof(reader));

            // Initialize properties
            RowGroupCount = _reader.RowGroupCount;
            Schema = _reader.Schema;

            // Build statistics for each row group
            var rowGroupStats = new List<RowGroupMetadata>();
            for (int i = 0; i < _reader.RowGroupCount; i++)
            {
                rowGroupStats.Add(new RowGroupMetadata(_reader, i));
            }
            RowGroups = new ReadOnlyCollection<RowGroupMetadata>(rowGroupStats);

            // Calculate total row count
            TotalRowCount = RowGroups.Sum(rg => rg.RowCount);
        }

        /// <summary>
        /// Gets the number of row groups in the Parquet file.
        /// </summary>
        public int RowGroupCount { get; }

        /// <summary>
        /// Gets the schema of the Parquet file.
        /// </summary>
        public ParquetSchema Schema { get; }

        /// <summary>
        /// Gets the total number of rows across all row groups.
        /// </summary>
        public long TotalRowCount { get; }

        /// <summary>
        /// Gets metadata for each row group in the Parquet file.
        /// </summary>
        public ReadOnlyCollection<RowGroupMetadata> RowGroups { get; }

        /// <summary>
        /// Gets detailed information about each column in the file.
        /// </summary>
        public ReadOnlyCollection<ColumnMetadata> Columns
        {
            get
            {
                ThrowIfDisposed();
                var columns = Schema.DataFields.Select(f => new ColumnMetadata(f)).ToList();
                return new ReadOnlyCollection<ColumnMetadata>(columns);
            }
        }

        /// <summary>
        /// Creates a new ParquetMetadata instance from a file path.
        /// </summary>
        /// <param name="filePath">Path to the Parquet file.</param>
        /// <returns>A ParquetMetadata object with information about the file.</returns>
        /// <exception cref="ArgumentNullException">Thrown when filePath is null.</exception>
        /// <exception cref="FileNotFoundException">Thrown when the file does not exist.</exception>
        public static ParquetMetadata FromFile(string filePath)
        {
            ArgumentException.ThrowIfNullOrWhiteSpace(filePath, nameof(filePath));

            if (!System.IO.File.Exists(filePath))
                throw new FileNotFoundException("Parquet file not found", filePath);

            var reader = ParquetReader.CreateAsync(filePath)
                .ConfigureAwait(false).GetAwaiter().GetResult();

            return new ParquetMetadata(reader);
        }

        /// <summary>
        /// Asynchronously creates a new ParquetMetadata instance from a file path.
        /// </summary>
        /// <param name="filePath">Path to the Parquet file.</param>
        /// <param name="cancellationToken">Optional cancellation token.</param>
        /// <returns>A task representing the asynchronous operation.</returns>
        /// <exception cref="ArgumentNullException">Thrown when filePath is null.</exception>
        /// <exception cref="FileNotFoundException">Thrown when the file does not exist.</exception>
        public static async Task<ParquetMetadata> FromFileAsync(string filePath, CancellationToken cancellationToken = default)
        {
            ArgumentException.ThrowIfNullOrWhiteSpace(filePath, nameof(filePath));

            if (!System.IO.File.Exists(filePath))
                throw new FileNotFoundException("Parquet file not found", filePath);

            var reader = await ParquetReader.CreateAsync(filePath, cancellationToken: cancellationToken)
                .ConfigureAwait(false);

            return new ParquetMetadata(reader);
        }

        /// <summary>
        /// Creates a new ParquetMetadata instance from a stream.
        /// </summary>
        /// <param name="stream">The stream containing Parquet data.</param>
        /// <param name="leaveOpen">If true, leaves the stream open after reading; otherwise, closes it.</param>
        /// <returns>A ParquetMetadata object with information about the data.</returns>
        /// <exception cref="ArgumentNullException">Thrown when stream is null.</exception>
        public static ParquetMetadata FromStream(Stream stream, bool leaveOpen = false)
        {
            ArgumentNullException.ThrowIfNull(stream, nameof(stream));

            var reader = ParquetReader.CreateAsync(stream, leaveStreamOpen: leaveOpen)
                .ConfigureAwait(false).GetAwaiter().GetResult();

            return new ParquetMetadata(reader);
        }

        /// <summary>
        /// Asynchronously creates a new ParquetMetadata instance from a stream.
        /// </summary>
        /// <param name="stream">The stream containing Parquet data.</param>
        /// <param name="leaveOpen">If true, leaves the stream open after reading; otherwise, closes it.</param>
        /// <param name="cancellationToken">Optional cancellation token.</param>
        /// <returns>A task representing the asynchronous operation.</returns>
        /// <exception cref="ArgumentNullException">Thrown when stream is null.</exception>
        public static async Task<ParquetMetadata> FromStreamAsync(
            Stream stream,
            bool leaveOpen = false,
            CancellationToken cancellationToken = default)
        {
            ArgumentNullException.ThrowIfNull(stream, nameof(stream));

            var reader = await ParquetReader.CreateAsync(stream, leaveStreamOpen: leaveOpen, cancellationToken: cancellationToken)
                .ConfigureAwait(false);

            return new ParquetMetadata(reader);
        }

        /// <summary>
        /// Disposes resources used by the ParquetMetadata instance.
        /// </summary>
        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        /// <summary>
        /// Protected implementation of Dispose pattern.
        /// </summary>
        /// <param name="disposing">True if called from Dispose(), false if called from finalizer.</param>
        protected virtual void Dispose(bool disposing)
        {
            if (_disposed)
                return;

            if (disposing)
            {
                // Dispose managed resources
                _reader?.Dispose();
            }

            _disposed = true;
        }

        /// <summary>
        /// Throws an ObjectDisposedException if this object has been disposed.
        /// </summary>
        private void ThrowIfDisposed()
        {
            ObjectDisposedException.ThrowIf(_disposed, this);
        }


        /// <summary>
        /// Finalizer to ensure resources are cleaned up if Dispose is not called.
        /// </summary>
        ~ParquetMetadata()
        {
            Dispose(false);
        }
    }

    /// <summary>
    /// Represents metadata for a single row group in a Parquet file.
    /// </summary>
    public class RowGroupMetadata
    {
        private readonly ParquetReader _reader;
        private readonly int _rowGroupIndex;

        internal RowGroupMetadata(ParquetReader reader, int rowGroupIndex)
        {
            _reader = reader ?? throw new ArgumentNullException(nameof(reader));
            _rowGroupIndex = rowGroupIndex;

            // Get row count from the reader's row group info
            using var rowGroupReader = _reader.OpenRowGroupReader(rowGroupIndex);
            RowCount = rowGroupReader.RowCount;
        }

        /// <summary>
        /// Gets the index of this row group.
        /// </summary>
        public int Index => _rowGroupIndex;

        /// <summary>
        /// Gets the number of rows in this row group.
        /// </summary>
        public long RowCount { get; }

        /// <summary>
        /// Returns a string that represents the current object.
        /// </summary>
        public override string ToString() => $"Row Group {Index}: {RowCount} rows";
    }

    /// <summary>
    /// Represents metadata for a column in a Parquet file.
    /// </summary>
    public class ColumnMetadata
    {
        /// <summary>
        /// Gets the field definition for this column.
        /// </summary>
        public DataField Field { get; }

        /// <summary>
        /// Gets the name of the column.
        /// </summary>
        public string Name => Field.Name;

        /// <summary>
        /// Gets the data type of the column.
        /// </summary> 
        public Type DataType => Field.ClrType;

        /// <summary>
        /// Gets whether the column allows null values.
        /// </summary>
        public bool IsNullable => Field.IsNullable;

        internal ColumnMetadata(DataField field)
        {
            Field = field ?? throw new ArgumentNullException(nameof(field));
        }

        /// <summary>
        /// Returns a string representation of this column.
        /// </summary>
        public override string ToString() => $"{Name} ({DataType.Name}{(IsNullable ? ", nullable" : "")})";
    }
}
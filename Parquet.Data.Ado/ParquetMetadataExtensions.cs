using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Parquet;

namespace Parquet.Data.Ado
{
    /// <summary>
    /// Extension methods for accessing Parquet metadata.
    /// </summary>
    public static class ParquetMetadataExtensions
    {
        #region ParquetConnection Extensions

        /// <summary>
        /// Gets metadata information about the Parquet file associated with this connection.
        /// </summary>
        /// <param name="connection">The Parquet connection.</param>
        /// <returns>A ParquetMetadata object containing information about the file.</returns>
        /// <exception cref="ArgumentNullException">Thrown when connection is null.</exception>
        /// <exception cref="InvalidOperationException">Thrown when the connection is not open.</exception>
        public static ParquetMetadata GetMetadata(this ParquetConnection connection)
        {
            ArgumentNullException.ThrowIfNull(connection, nameof(connection));

            if (connection.State != System.Data.ConnectionState.Open)
                throw new InvalidOperationException("Connection must be open to get metadata");

            var reader = connection.Reader;
            return new ParquetMetadata(reader);
        }

        /// <summary>
        /// Gets the number of row groups in the Parquet file associated with this connection.
        /// </summary>
        /// <param name="connection">The Parquet connection.</param>
        /// <returns>The number of row groups in the file.</returns>
        /// <exception cref="ArgumentNullException">Thrown when connection is null.</exception>
        /// <exception cref="InvalidOperationException">Thrown when the connection is not open.</exception>
        public static int GetRowGroupCount(this ParquetConnection connection)
        {
            ArgumentNullException.ThrowIfNull(connection, nameof(connection));

            if (connection.State != System.Data.ConnectionState.Open)
                throw new InvalidOperationException("Connection must be open to get row group count");

            return connection.Reader.RowGroupCount;
        }

        /// <summary>
        /// Gets the total number of rows in the Parquet file associated with this connection.
        /// </summary>
        /// <param name="connection">The Parquet connection.</param>
        /// <returns>The total number of rows in the file.</returns>
        /// <exception cref="ArgumentNullException">Thrown when connection is null.</exception>
        /// <exception cref="InvalidOperationException">Thrown when the connection is not open.</exception>
        public static long GetTotalRowCount(this ParquetConnection connection)
        {
            using var metadata = connection.GetMetadata();
            return metadata.TotalRowCount;
        }

        #endregion

        #region ParquetDataReaderFactory Extensions

        /// <summary>
        /// Gets metadata for a Parquet file without opening a data reader.
        /// </summary>
        /// <param name="filePath">Path to the Parquet file.</param>
        /// <returns>A ParquetMetadata object containing information about the file.</returns>
        /// <exception cref="ArgumentNullException">Thrown when filePath is null.</exception>
        /// <exception cref="FileNotFoundException">Thrown when the file does not exist.</exception>
        public static ParquetMetadata GetMetadata(string filePath)
        {
            return ParquetMetadata.FromFile(filePath);
        }

        /// <summary>
        /// Gets metadata for a Parquet file without opening a data reader (async version).
        /// </summary>
        /// <param name="filePath">Path to the Parquet file.</param>
        /// <param name="cancellationToken">Optional cancellation token.</param>
        /// <returns>A task representing the asynchronous operation.</returns>
        /// <exception cref="ArgumentNullException">Thrown when filePath is null.</exception>
        /// <exception cref="FileNotFoundException">Thrown when the file does not exist.</exception>
        public static Task<ParquetMetadata> GetMetadataAsync(
            string filePath,
            CancellationToken cancellationToken = default)
        {
            return ParquetMetadata.FromFileAsync(filePath, cancellationToken);
        }

        /// <summary>
        /// Gets metadata for a Parquet file from a stream without opening a data reader.
        /// </summary>
        /// <param name="stream">The stream containing Parquet data.</param>
        /// <param name="leaveOpen">If true, leaves the stream open after reading; otherwise, closes it.</param>
        /// <returns>A ParquetMetadata object containing information about the data.</returns>
        /// <exception cref="ArgumentNullException">Thrown when stream is null.</exception>
        public static ParquetMetadata GetMetadata(Stream stream, bool leaveOpen = false)
        {
            return ParquetMetadata.FromStream(stream, leaveOpen);
        }

        /// <summary>
        /// Gets metadata for a Parquet file from a stream without opening a data reader (async version).
        /// </summary>
        /// <param name="stream">The stream containing Parquet data.</param>
        /// <param name="leaveOpen">If true, leaves the stream open after reading; otherwise, closes it.</param>
        /// <param name="cancellationToken">Optional cancellation token.</param>
        /// <returns>A task representing the asynchronous operation.</returns>
        /// <exception cref="ArgumentNullException">Thrown when stream is null.</exception>
        public static Task<ParquetMetadata> GetMetadataAsync(
            Stream stream,
            bool leaveOpen = false,
            CancellationToken cancellationToken = default)
        {
            return ParquetMetadata.FromStreamAsync(stream, leaveOpen, cancellationToken);
        }

        #endregion

        #region ParquetDataReader Extensions

        /// <summary>
        /// Gets the total number of row groups in the Parquet file.
        /// </summary>
        /// <param name="reader">The Parquet data reader.</param>
        /// <returns>The total number of row groups.</returns>
        /// <exception cref="ArgumentNullException">Thrown when reader is null.</exception>
        /// <exception cref="ObjectDisposedException">Thrown when the reader is disposed.</exception>
        public static int GetRowGroupCount(this ParquetDataReader reader)
        {
            ArgumentNullException.ThrowIfNull(reader, nameof(reader));

            reader.ThrowIfDisposed();

            // We can access the private _reader field through reflection
            // This is a bit hacky but avoids modifying the ParquetDataReader class
            var readerField = typeof(ParquetDataReader).GetField("_reader", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance);
            if (readerField == null)
                throw new InvalidOperationException("Could not access internal reader field");

            var parquetReader = readerField.GetValue(reader) as ParquetReader;
            if (parquetReader == null)
                throw new InvalidOperationException("Could not access internal ParquetReader");

            return parquetReader.RowGroupCount;
        }

        /// <summary>
        /// Gets the current row group index being read.
        /// </summary>
        /// <param name="reader">The Parquet data reader.</param>
        /// <returns>The current row group index.</returns>
        /// <exception cref="ArgumentNullException">Thrown when reader is null.</exception>
        /// <exception cref="ObjectDisposedException">Thrown when the reader is disposed.</exception>
        public static int GetCurrentRowGroup(this ParquetDataReader reader)
        {
            ArgumentNullException.ThrowIfNull(reader, nameof(reader));

            reader.ThrowIfDisposed();

            // Access the private _currentRowGroup field through reflection
            var currentRowGroupField = typeof(ParquetDataReader).GetField("_currentRowGroup",
                System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance);

            if (currentRowGroupField == null)
                throw new InvalidOperationException("Could not access current row group field");

            // Fix for CS8605: Unboxing a possibly null value
            var value = currentRowGroupField.GetValue(reader);
            if (value == null)
                throw new InvalidOperationException("Current row group value is null");

            return (int)value;
        }

        /// <summary>
        /// Gets metadata for a Parquet file without opening a data reader.
        /// </summary>
        /// <param name="filePath">Path to the Parquet file.</param>
        /// <param name="parquetOptions">Optional ParquetOptions to configure the reader.</param>
        /// <returns>A ParquetMetadata object containing information about the file.</returns>
        /// <exception cref="ArgumentNullException">Thrown when filePath is null.</exception>
        /// <exception cref="FileNotFoundException">Thrown when the file does not exist.</exception>
        public static ParquetMetadata GetMetadata(
            string filePath,
            ParquetOptions? parquetOptions = null)
        {
            ArgumentNullException.ThrowIfNull(filePath);

            if (!System.IO.File.Exists(filePath))
                throw new FileNotFoundException("Parquet file not found", filePath);

            // Open the file
            using var stream = System.IO.File.OpenRead(filePath);

            // Create a reader with the specified options
            return GetMetadata(stream, false, parquetOptions);
        }

        /// <summary>
        /// Asynchronously gets metadata for a Parquet file without opening a data reader.
        /// </summary>
        /// <param name="filePath">Path to the Parquet file.</param>
        /// <param name="parquetOptions">Optional ParquetOptions to configure the reader.</param>
        /// <param name="cancellationToken">Optional cancellation token.</param>
        /// <returns>A task representing the asynchronous operation.</returns>
        /// <exception cref="ArgumentNullException">Thrown when filePath is null.</exception>
        /// <exception cref="FileNotFoundException">Thrown when the file does not exist.</exception>
        public static async Task<ParquetMetadata> GetMetadataAsync(
            string filePath,
            ParquetOptions? parquetOptions = null,
            CancellationToken cancellationToken = default)
        {
            ArgumentNullException.ThrowIfNull(filePath);

            if (!System.IO.File.Exists(filePath))
                throw new FileNotFoundException("Parquet file not found", filePath);

            // Open file asynchronously
            var stream = new FileStream(
                filePath,
                FileMode.Open,
                FileAccess.Read,
                FileShare.Read,
                bufferSize: 4096,
                useAsync: true);

            try
            {
                // Get metadata asynchronously
                return await GetMetadataAsync(stream, false, parquetOptions, cancellationToken)
                    .ConfigureAwait(false);
            }
            catch
            {
                // Ensure stream is closed on error
                await stream.DisposeAsync().ConfigureAwait(false);
                throw;
            }
        }

        /// <summary>
        /// Gets metadata for a Parquet file from a stream without opening a data reader.
        /// </summary>
        /// <param name="stream">The stream containing Parquet data.</param>
        /// <param name="leaveOpen">If true, leaves the stream open after reading; otherwise, closes it.</param>
        /// <param name="parquetOptions">Optional ParquetOptions to configure the reader.</param>
        /// <returns>A ParquetMetadata object containing information about the data.</returns>
        /// <exception cref="ArgumentNullException">Thrown when stream is null.</exception>
        public static ParquetMetadata GetMetadata(
            Stream stream,
            bool leaveOpen = false,
            ParquetOptions? parquetOptions = null)
        {
            ArgumentNullException.ThrowIfNull(stream);

            var reader = ParquetReader
                .CreateAsync(stream, parquetOptions, leaveStreamOpen: leaveOpen, cancellationToken: CancellationToken.None)
                .ConfigureAwait(false)
                .GetAwaiter()
                .GetResult();

            return new ParquetMetadata(reader);
        }

        /// <summary>
        /// Asynchronously gets metadata for a Parquet file from a stream without opening a data reader.
        /// </summary>
        /// <param name="stream">The stream containing Parquet data.</param>
        /// <param name="leaveOpen">If true, leaves the stream open after reading; otherwise, closes it.</param>
        /// <param name="parquetOptions">Optional ParquetOptions to configure the reader.</param>
        /// <param name="cancellationToken">Optional cancellation token.</param>
        /// <returns>A task representing the asynchronous operation.</returns>
        /// <exception cref="ArgumentNullException">Thrown when stream is null.</exception>
        public static async Task<ParquetMetadata> GetMetadataAsync(
            Stream stream,
            bool leaveOpen = false,
            ParquetOptions? parquetOptions = null,
            CancellationToken cancellationToken = default)
        {
            ArgumentNullException.ThrowIfNull(stream);

            var reader = await ParquetReader
                .CreateAsync(stream, parquetOptions, leaveStreamOpen: leaveOpen, cancellationToken)
                .ConfigureAwait(false);

            return new ParquetMetadata(reader);
        }
        #endregion
    }
}
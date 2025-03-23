using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Parquet;

namespace Parquet.Data.Reader
{
    /// <summary>
    /// Factory class for creating ParquetDataReader instances.
    /// Provides convenient methods for creating readers from files or streams.
    /// </summary>
    public static class ParquetDataReaderFactory
    {
        /// <summary>
        /// Creates a new ParquetDataReader instance from a file path.
        /// </summary>
        /// <param name="filePath">The path to the Parquet file.</param>
        /// <param name="readNextGroup">If set to true, reads the next group when the current group is exhausted.</param>
        /// <param name="parquetOptions">Optional ParquetOptions to configure the reader.</param>
        /// <returns>A new ParquetDataReader instance.</returns>
        /// <exception cref="ArgumentNullException">Thrown when filePath is null.</exception>
        /// <exception cref="FileNotFoundException">Thrown when the file does not exist.</exception>
        public static ParquetDataReader Create(
            string filePath,
            bool readNextGroup = true,
            ParquetOptions? parquetOptions = null)
        {
            ArgumentNullException.ThrowIfNull(filePath);

            if (!System.IO.File.Exists(filePath))
                throw new FileNotFoundException("Parquet file not found", filePath);

            // Open the file synchronously
            var stream = System.IO.File.OpenRead(filePath);

            // Use the asynchronous factory method and block synchronously.
            var reader = ParquetReader
                .CreateAsync(stream, parquetOptions, leaveStreamOpen: false, cancellationToken: CancellationToken.None)
                .ConfigureAwait(false)
                .GetAwaiter()
                .GetResult();

            return new ParquetDataReader(reader, 0, readNextGroup);
        }

        /// <summary>
        /// Asynchronously creates a new ParquetDataReader instance from a file path.
        /// </summary>
        /// <param name="filePath">The path to the Parquet file.</param>
        /// <param name="readNextGroup">If set to true, reads the next group when the current group is exhausted.</param>
        /// <param name="parquetOptions">Optional ParquetOptions to configure the reader.</param>
        /// <param name="cancellationToken">Optional cancellation token.</param>
        /// <returns>A new ParquetDataReader instance.</returns>
        /// <exception cref="ArgumentNullException">Thrown when filePath is null.</exception>
        /// <exception cref="FileNotFoundException">Thrown when the file does not exist.</exception>
        public static async Task<ParquetDataReader> CreateAsync(
            string filePath,
            bool readNextGroup = true,
            ParquetOptions? parquetOptions = null,
            CancellationToken cancellationToken = default)
        {
            ArgumentNullException.ThrowIfNull(filePath);

            if (!System.IO.File.Exists(filePath))
                throw new FileNotFoundException("Parquet file not found", filePath);

            // Open file asynchronously.
            var stream = new FileStream(
                filePath,
                FileMode.Open,
                FileAccess.Read,
                FileShare.Read,
                bufferSize: 4096,
                useAsync: true);

            var reader = await ParquetReader
                .CreateAsync(stream, parquetOptions, leaveStreamOpen: false, cancellationToken)
                .ConfigureAwait(false);

            return await ParquetDataReader
                .CreateAsync(reader, 0, readNextGroup, cancellationToken)
                .ConfigureAwait(false);
        }

        /// <summary>
        /// Creates a new ParquetDataReader instance from a stream.
        /// </summary>
        /// <param name="stream">The stream containing Parquet data.</param>
        /// <param name="readNextGroup">If set to true, reads the next group when the current group is exhausted.</param>
        /// <param name="leaveOpen">If set to true, leaves the stream open after the reader is disposed.</param>
        /// <param name="parquetOptions">Optional ParquetOptions to configure the reader.</param>
        /// <returns>A new ParquetDataReader instance.</returns>
        /// <exception cref="ArgumentNullException">Thrown when stream is null.</exception>
        public static ParquetDataReader Create(
            Stream stream,
            bool readNextGroup = true,
            bool leaveOpen = false,
            ParquetOptions? parquetOptions = null)
        {
            ArgumentNullException.ThrowIfNull(stream);

            var reader = ParquetReader
                .CreateAsync(stream, parquetOptions, leaveStreamOpen: leaveOpen, cancellationToken: CancellationToken.None)
                .ConfigureAwait(false)
                .GetAwaiter()
                .GetResult();

            return new ParquetDataReader(reader, 0, readNextGroup);
        }

        /// <summary>
        /// Asynchronously creates a new ParquetDataReader instance from a stream.
        /// </summary>
        /// <param name="stream">The stream containing Parquet data.</param>
        /// <param name="readNextGroup">If set to true, reads the next group when the current group is exhausted.</param>
        /// <param name="leaveOpen">If set to true, leaves the stream open after the reader is disposed.</param>
        /// <param name="parquetOptions">Optional ParquetOptions to configure the reader.</param>
        /// <param name="cancellationToken">Optional cancellation token.</param>
        /// <returns>A new ParquetDataReader instance.</returns>
        /// <exception cref="ArgumentNullException">Thrown when stream is null.</exception>
        public static async Task<ParquetDataReader> CreateAsync(
            Stream stream,
            bool readNextGroup = true,
            bool leaveOpen = false,
            ParquetOptions? parquetOptions = null,
            CancellationToken cancellationToken = default)
        {
            ArgumentNullException.ThrowIfNull(stream);

            var reader = await ParquetReader
                .CreateAsync(stream, parquetOptions, leaveStreamOpen: leaveOpen, cancellationToken)
                .ConfigureAwait(false);

            return await ParquetDataReader
                .CreateAsync(reader, 0, readNextGroup, cancellationToken)
                .ConfigureAwait(false);
        }

        /// <summary>
        /// Creates multiple ParquetDataReader instances in parallel from a collection of file paths.
        /// </summary>
        /// <param name="filePaths">The paths to the Parquet files.</param>
        /// <param name="readNextGroup">If set to true, reads the next group when the current group is exhausted.</param>
        /// <param name="parquetOptions">Optional ParquetOptions to configure the reader.</param>
        /// <param name="maxDegreeOfParallelism">Maximum number of readers to create in parallel. Default is Environment.ProcessorCount.</param>
        /// <param name="cancellationToken">Optional cancellation token.</param>
        /// <returns>An array of ParquetDataReader instances.</returns>
        public static async Task<ParquetDataReader[]> CreateManyAsync(
            string[] filePaths,
            bool readNextGroup = true,
            ParquetOptions? parquetOptions = null,
            int maxDegreeOfParallelism = 0,
            CancellationToken cancellationToken = default)
        {
            ArgumentNullException.ThrowIfNull(filePaths);

            if (maxDegreeOfParallelism <= 0)
            {
                maxDegreeOfParallelism = Environment.ProcessorCount;
            }

            using var semaphore = new SemaphoreSlim(maxDegreeOfParallelism);
            var tasks = new Task<ParquetDataReader>[filePaths.Length];

            for (int i = 0; i < filePaths.Length; i++)
            {
                var filePath = filePaths[i];
                tasks[i] = CreateReaderAsync(filePath);
            }

            // Local function that handles semaphore and calls CreateAsync for a filePath
            async Task<ParquetDataReader> CreateReaderAsync(string filePath)
            {
                await semaphore.WaitAsync(cancellationToken).ConfigureAwait(false);
                try
                {
                    // Use the existing CreateAsync method for actual file reading
                    return await CreateAsync(filePath, readNextGroup, parquetOptions, cancellationToken)
                        .ConfigureAwait(false);
                }
                finally
                {
                    semaphore.Release();
                }
            }

            return await Task.WhenAll(tasks).ConfigureAwait(false);
        }
    }
}

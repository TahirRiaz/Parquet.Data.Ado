using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.IO;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using Parquet;
using Parquet.Data;
using Parquet.Schema;

namespace Parquet.Data.Ado
{
    /// <summary>
    /// Provides efficient batch reading capabilities for Parquet files with parallel processing of row groups.
    /// </summary>
    public sealed class ParquetBatchReader : IAsyncDisposable, IDisposable
    {
        private readonly string _filePath;
        private readonly ParquetSchema _schema;
        private readonly int _rowGroupCount;
        private readonly int _maxDegreeOfParallelism;
        private bool _disposed;

        /// <summary>
        /// Opens a Parquet file for batch reading. This will read the file's schema and metadata.
        /// </summary>
        /// <param name="filePath">Path to the Parquet file to read.</param>
        /// <param name="maxDegreeOfParallelism">
        /// Maximum number of row groups to read in parallel. If 0 or not specified, defaults to the number of processors.
        /// </param>
        public ParquetBatchReader(string filePath, int maxDegreeOfParallelism = 0)
        {
            if (string.IsNullOrWhiteSpace(filePath))
                throw new ArgumentException("File path must be a non-empty string.", nameof(filePath));
            if (!System.IO.File.Exists(filePath))
                throw new FileNotFoundException("Parquet file not found.", filePath);

            _filePath = filePath;
            _maxDegreeOfParallelism = (maxDegreeOfParallelism > 0) ? maxDegreeOfParallelism : Environment.ProcessorCount;

            // Synchronously open the Parquet file to read schema and row group count.
            // Using ParquetReader.CreateAsync with .GetAwaiter().GetResult() to synchronously fetch metadata.
            ParquetReader reader = ParquetReader.CreateAsync(filePath)
                .ConfigureAwait(false).GetAwaiter().GetResult();
            _schema = reader.Schema;
            _rowGroupCount = reader.RowGroupCount;
            reader.Dispose();
        }

        /// <summary>
        /// Gets the Parquet file schema.
        /// </summary>
        public ParquetSchema Schema => _schema;

        /// <summary>
        /// Gets the total number of row groups (batches) in the Parquet file.
        /// </summary>
        public int BatchCount => _rowGroupCount;

        /// <summary>
        /// Reads all batches from the Parquet file asynchronously. Each batch corresponds to one Parquet row group.
        /// Batches are read in parallel for performance, and yielded as they become available.
        /// </summary>
        /// <param name="cancellationToken">Cancellation token to cancel the read operation.</param>
        /// <returns>An asynchronous sequence of ParquetBatch objects representing each batch of rows.</returns>
        public async IAsyncEnumerable<ParquetBatch> ReadAllAsync([EnumeratorCancellation] CancellationToken cancellationToken = default)
        {
            if (_rowGroupCount <= 0) yield break; // No row groups to read.

            ObjectDisposedException.ThrowIf(_disposed, this);

            // Create a linked cancellation token source to allow internal cancellation (e.g., on error).
            using CancellationTokenSource cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            CancellationToken ct = cts.Token;

            List<Task<ParquetBatch>> pending = new();
            int nextGroupIndex = 0;

            // Start initial tasks up to maxDegreeOfParallelism or rowGroupCount (whichever is smaller).
            int initialTasks = Math.Min(_maxDegreeOfParallelism, _rowGroupCount);
            for (; nextGroupIndex < initialTasks; nextGroupIndex++)
            {
                pending.Add(ReadBatchAsync(nextGroupIndex, ct));
            }

            try
            {
                while (pending.Count > 0)
                {
                    // Wait for any batch to finish reading.
                    Task<ParquetBatch> finishedTask = await Task.WhenAny(pending).ConfigureAwait(false);
                    pending.Remove(finishedTask);

                    ParquetBatch batch;
                    try
                    {
                        batch = await finishedTask.ConfigureAwait(false);
                    }
                    catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
                    {
                        // If the exception is cancellation triggered by the external token, propagate it.
                        // Await and ignore remaining tasks to ensure file streams are closed.
                        foreach (var t in pending)
                        {
                            try { await t.ConfigureAwait(false); }
                            catch (OperationCanceledException)
                            {
                                // ignore exceptions from canceled tasks
                            }
                        }
                        pending.Clear();
                        throw;
                    }
                    catch (IOException ex)
                    {
                        // On IO exception, cancel all ongoing reads
                        await cts.CancelAsync().ConfigureAwait(false);
                        foreach (var t in pending)
                        {
                            try { await t.ConfigureAwait(false); }
                            catch (OperationCanceledException)
                            {
                                // ignore exceptions from canceled tasks
                            }
                        }
                        pending.Clear();
                        throw new IOException($"Error reading Parquet file: {ex.Message}", ex);
                    }
                    catch (ParquetException ex)
                    {
                        // On Parquet-specific exception, cancel all ongoing reads
                        await cts.CancelAsync().ConfigureAwait(false);
                        foreach (var t in pending)
                        {
                            try { await t.ConfigureAwait(false); }
                            catch (OperationCanceledException)
                            {
                                // ignore exceptions from canceled tasks
                            }
                        }
                        pending.Clear();
                        throw new ParquetException($"Error processing Parquet data: {ex.Message}", ex);
                    }

                    // Successfully got a batch, yield it to the caller.
                    yield return batch;

                    // If more row groups remain, start reading the next one.
                    if (nextGroupIndex < _rowGroupCount)
                    {
                        pending.Add(ReadBatchAsync(nextGroupIndex, ct));
                        nextGroupIndex++;
                    }
                }
            }
            finally
            {
                // In case the enumeration is terminated early, cancel remaining tasks to stop reading.
                if (pending.Count > 0 && !cts.IsCancellationRequested)
                {
                    await cts.CancelAsync().ConfigureAwait(false);
                }

                // Clean up any remaining tasks.
                foreach (var t in pending)
                {
                    try { await t.ConfigureAwait(false); }
                    catch (OperationCanceledException)
                    {
                        // ignore exceptions from canceled tasks
                    }
                }
            }
        }

        /// <summary>
        /// Asynchronously reads a single batch (row group) from the Parquet file.
        /// </summary>
        /// <param name="rowGroupIndex">Index of the row group to read.</param>
        /// <param name="cancellationToken">Cancellation token for this batch read.</param>
        private async Task<ParquetBatch> ReadBatchAsync(int rowGroupIndex, CancellationToken cancellationToken)
        {
            // Open the file stream with flags that allow deletion
            using FileStream fs = new FileStream(
                _filePath,
                FileMode.Open,
                FileAccess.Read,
                FileShare.Read,
                bufferSize: 4096,
                useAsync: true);

            try
            {
                // Create the ParquetReader
                using var reader = await ParquetReader.CreateAsync(fs, cancellationToken: cancellationToken)
                    .ConfigureAwait(false);

                // Read the columns from the specified row group
                DataColumn[] columns = await reader.ReadEntireRowGroupAsync(rowGroupIndex)
                    .ConfigureAwait(false);

                // Determine row count (if there are columns and the first column has data)
                int rowCount = 0;
                if (columns.Length > 0 && columns[0].Data is Array dataArray)
                {
                    rowCount = dataArray.Length;
                }

                // Return a new batch with the read data
                return new ParquetBatch(
                    rowGroupIndex,
                    new ReadOnlyCollection<DataColumn>(columns),
                    rowCount);
            }
            catch (Exception ex)
            {
                throw new IOException($"Error reading row group {rowGroupIndex}: {ex.Message}", ex);
            }
        }






        /// <summary>
        /// Disposes the resources used by the ParquetBatchReader (synchronous).
        /// </summary>
        public void Dispose()
        {
            Dispose(disposing: true);
            GC.SuppressFinalize(this);
        }

        /// <summary>
        /// Disposes the resources used by the ParquetBatchReader (asynchronous).
        /// </summary>
        public ValueTask DisposeAsync()
        {
            Dispose(disposing: true);
            GC.SuppressFinalize(this);
            return ValueTask.CompletedTask;
        }

        /// <summary>
        /// Actual dispose implementation. Currently, no internal resources remain open after constructor,
        /// but the pattern is preserved for future expansion.
        /// </summary>
        private void Dispose(bool disposing)
        {
            if (_disposed) return;

            if (disposing)
            {
                // If we held any managed resources that need disposal, they would be disposed here.
            }

            // If there were unmanaged resources, they would be released here.

            _disposed = true;
        }
    }

    /// <summary>
    /// Represents a batch of rows read from the Parquet file (corresponding to one row group).
    /// </summary>
    public class ParquetBatch
    {
        /// <summary>
        /// Initializes a new instance of the ParquetBatch class.
        /// </summary>
        /// <param name="rowGroupIndex">Index of the row group from which this batch was read.</param>
        /// <param name="columns">The columns of data in this batch.</param>
        /// <param name="rowCount">The number of rows in this batch.</param>
        internal ParquetBatch(int rowGroupIndex, ReadOnlyCollection<DataColumn> columns, int rowCount)
        {
            RowGroupIndex = rowGroupIndex;
            Columns = columns ?? throw new ArgumentNullException(nameof(columns));
            RowCount = rowCount;
        }

        /// <summary>
        /// The index of the row group from which this batch was read.
        /// </summary>
        public int RowGroupIndex { get; }

        /// <summary>
        /// The columns of data in this batch. Each DataColumn contains the data for one field (column) in the schema.
        /// </summary>
        public ReadOnlyCollection<DataColumn> Columns { get; }

        /// <summary>
        /// The number of rows in this batch.
        /// </summary>
        public int RowCount { get; }
    }
}

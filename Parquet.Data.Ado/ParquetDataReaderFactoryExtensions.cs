using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Parquet;
using System.IO;

namespace Parquet.Data.Reader
{
    /// <summary>
    /// Extension methods for the ParquetDataReaderFactory to support creating readers with virtual columns.
    /// </summary>
    public static class ParquetDataReaderFactoryExtensions
    {
        /// <summary>
        /// Creates a new ParquetDataReaderWithVirtualColumns instance from a file path.
        /// </summary>
        /// <param name="filePath">The path to the Parquet file.</param>
        /// <param name="virtualColumns">Collection of virtual columns to include in the reader.</param>
        /// <param name="readNextGroup">If set to true, reads the next group when the current group is exhausted.</param>
        /// <param name="parquetOptions">Optional ParquetOptions to configure the reader.</param>
        /// <returns>A new ParquetDataReaderWithVirtualColumns instance.</returns>
        /// <exception cref="ArgumentNullException">Thrown when filePath is null.</exception>
        /// <exception cref="FileNotFoundException">Thrown when the file does not exist.</exception>
        public static ParquetDataReaderWithVirtualColumns CreateWithVirtualColumns(
            string filePath,
            IEnumerable<VirtualColumn> virtualColumns,
            bool readNextGroup = true,
            ParquetOptions? parquetOptions = null)
        {
            ArgumentNullException.ThrowIfNull(filePath);
            ArgumentNullException.ThrowIfNull(virtualColumns);

            if (!System.IO.File.Exists(filePath))
                throw new FileNotFoundException("Parquet file not found", filePath);

            // Open the file synchronously
            var stream = System.IO.File.OpenRead(filePath);

            // Use the asynchronous factory method and block synchronously
            var reader = ParquetReader
                .CreateAsync(stream, parquetOptions, leaveStreamOpen: false, cancellationToken: CancellationToken.None)
                .ConfigureAwait(false)
                .GetAwaiter()
                .GetResult();

            // Prepare sorted list of virtual columns
            var virtualColumnList = PrepareSortedVirtualColumns(virtualColumns, reader.Schema.Fields.Count);

            return new ParquetDataReaderWithVirtualColumns(reader, 0, readNextGroup, virtualColumnList);
        }

        /// <summary>
        /// Asynchronously creates a new ParquetDataReaderWithVirtualColumns instance from a file path.
        /// </summary>
        /// <param name="filePath">The path to the Parquet file.</param>
        /// <param name="virtualColumns">Collection of virtual columns to include in the reader.</param>
        /// <param name="readNextGroup">If set to true, reads the next group when the current group is exhausted.</param>
        /// <param name="parquetOptions">Optional ParquetOptions to configure the reader.</param>
        /// <param name="cancellationToken">Optional cancellation token.</param>
        /// <returns>A task representing the asynchronous operation.</returns>
        /// <exception cref="ArgumentNullException">Thrown when filePath is null.</exception>
        /// <exception cref="FileNotFoundException">Thrown when the file does not exist.</exception>
        public static async Task<ParquetDataReaderWithVirtualColumns> CreateWithVirtualColumnsAsync(
            string filePath,
            IEnumerable<VirtualColumn> virtualColumns,
            bool readNextGroup = true,
            ParquetOptions? parquetOptions = null,
            CancellationToken cancellationToken = default)
        {
            ArgumentNullException.ThrowIfNull(filePath);
            ArgumentNullException.ThrowIfNull(virtualColumns);

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

            var reader = await ParquetReader
                .CreateAsync(stream, parquetOptions, leaveStreamOpen: false, cancellationToken)
                .ConfigureAwait(false);

            // Prepare sorted list of virtual columns
            var virtualColumnList = PrepareSortedVirtualColumns(virtualColumns, reader.Schema.Fields.Count);

            return await ParquetDataReaderWithVirtualColumns
                .CreateAsync(reader, 0, readNextGroup, virtualColumnList, cancellationToken)
                .ConfigureAwait(false);
        }

        /// <summary>
        /// Creates a new ParquetDataReaderWithVirtualColumns instance from a stream.
        /// </summary>
        /// <param name="stream">The stream containing Parquet data.</param>
        /// <param name="virtualColumns">Collection of virtual columns to include in the reader.</param>
        /// <param name="readNextGroup">If set to true, reads the next group when the current group is exhausted.</param>
        /// <param name="leaveOpen">If set to true, leaves the stream open after the reader is disposed.</param>
        /// <param name="parquetOptions">Optional ParquetOptions to configure the reader.</param>
        /// <returns>A new ParquetDataReaderWithVirtualColumns instance.</returns>
        /// <exception cref="ArgumentNullException">Thrown when stream is null.</exception>
        public static ParquetDataReaderWithVirtualColumns CreateWithVirtualColumns(
            Stream stream,
            IEnumerable<VirtualColumn> virtualColumns,
            bool readNextGroup = true,
            bool leaveOpen = false,
            ParquetOptions? parquetOptions = null)
        {
            ArgumentNullException.ThrowIfNull(stream);
            ArgumentNullException.ThrowIfNull(virtualColumns);

            var reader = ParquetReader
                .CreateAsync(stream, parquetOptions, leaveStreamOpen: leaveOpen, cancellationToken: CancellationToken.None)
                .ConfigureAwait(false)
                .GetAwaiter()
                .GetResult();

            // Prepare sorted list of virtual columns
            var virtualColumnList = PrepareSortedVirtualColumns(virtualColumns, reader.Schema.Fields.Count);

            return new ParquetDataReaderWithVirtualColumns(reader, 0, readNextGroup, virtualColumnList);
        }

        /// <summary>
        /// Asynchronously creates a new ParquetDataReaderWithVirtualColumns instance from a stream.
        /// </summary>
        /// <param name="stream">The stream containing Parquet data.</param>
        /// <param name="virtualColumns">Collection of virtual columns to include in the reader.</param>
        /// <param name="readNextGroup">If set to true, reads the next group when the current group is exhausted.</param>
        /// <param name="leaveOpen">If set to true, leaves the stream open after the reader is disposed.</param>
        /// <param name="parquetOptions">Optional ParquetOptions to configure the reader.</param>
        /// <param name="cancellationToken">Optional cancellation token.</param>
        /// <returns>A task representing the asynchronous operation.</returns>
        /// <exception cref="ArgumentNullException">Thrown when stream is null.</exception>
        public static async Task<ParquetDataReaderWithVirtualColumns> CreateWithVirtualColumnsAsync(
            Stream stream,
            IEnumerable<VirtualColumn> virtualColumns,
            bool readNextGroup = true,
            bool leaveOpen = false,
            ParquetOptions? parquetOptions = null,
            CancellationToken cancellationToken = default)
        {
            ArgumentNullException.ThrowIfNull(stream);
            ArgumentNullException.ThrowIfNull(virtualColumns);

            var reader = await ParquetReader
                .CreateAsync(stream, parquetOptions, leaveStreamOpen: leaveOpen, cancellationToken)
                .ConfigureAwait(false);

            // Prepare sorted list of virtual columns
            var virtualColumnList = PrepareSortedVirtualColumns(virtualColumns, reader.Schema.Fields.Count);

            return await ParquetDataReaderWithVirtualColumns
                .CreateAsync(reader, 0, readNextGroup, virtualColumnList, cancellationToken)
                .ConfigureAwait(false);
        }

        /// <summary>
        /// Helper method to prepare a sorted list of virtual columns.
        /// </summary>
        /// <param name="virtualColumns">Collection of virtual columns.</param>
        /// <param name="physicalColumnCount">Number of physical columns in the Parquet file.</param>
        /// <returns>A sorted list of virtual columns with proper indices.</returns>
        private static SortedList<int, VirtualColumn> PrepareSortedVirtualColumns(
            IEnumerable<VirtualColumn> virtualColumns,
            int physicalColumnCount)
        {
            var result = new SortedList<int, VirtualColumn>();

            int index = physicalColumnCount;
            foreach (var column in virtualColumns)
            {
                result.Add(index++, column);
            }

            return result;
        }
    }
}
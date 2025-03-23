using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Parquet.Data.Ado;
using Parquet.Data.Reader;

namespace ParquetReaderTest
{
    /// <summary>
    /// Tests for large file operations and multi-threaded scenarios with Parquet files.
    /// </summary>
    public static class LargeFileTests
    {
        private const int VERY_LARGE_ROW_COUNT = 1_000_000;
        private const int COLUMNS_COUNT = 15;

        public static async Task RunAllTests()
        {
            Console.WriteLine("\n=== LARGE FILE AND STREAMING TESTS ===");

            // Generate a file path in temp directory
            string largeFilePath = Path.Combine(Path.GetTempPath(), "very_large_parquet_test.parquet");

            try
            {
                // Create very large file for testing
                await CreateLargeParquetFile(largeFilePath);

                // Run various tests
                await TestMultiThreadedReading(largeFilePath);
                await TestStreamingReadingWithObserver(largeFilePath);
                await TestParallelRowGroupProcessing(largeFilePath);
                await TestConcurrentAggregation(largeFilePath);
                await TestAsyncStreamProcessing(largeFilePath);
                await TestCancellation(largeFilePath);
                await TestMemoryEfficiency(largeFilePath);
            }
            finally
            {
                // Clean up
                if (File.Exists(largeFilePath))
                {
                    try
                    {
                        File.Delete(largeFilePath);
                        Console.WriteLine("Large test file deleted successfully.");
                    }
                    catch (IOException ex)
                    {
                        Console.WriteLine($"Warning: Could not delete test file: {ex.Message}");
                    }
                }
            }
        }

        /// <summary>
        /// Creates a large Parquet file with millions of rows for testing
        /// </summary>
        /// <summary>
        /// Creates a large Parquet file with millions of rows for testing
        /// </summary>
        private static async Task CreateLargeParquetFile(string filePath)
        {
            Console.WriteLine($"Creating very large Parquet file with {VERY_LARGE_ROW_COUNT:N0} rows...");
            var stopwatch = Stopwatch.StartNew();

            // Create a DataTable with multiple columns
            var largeTable = new DataTable("VeryLargeTable");

            // Add columns with various data types
            largeTable.Columns.Add("ID", typeof(int));
            largeTable.Columns.Add("GUID", typeof(Guid));
            largeTable.Columns.Add("Name", typeof(string));
            largeTable.Columns.Add("Description", typeof(string));
            largeTable.Columns.Add("CreatedDate", typeof(DateTime));
            largeTable.Columns.Add("ModifiedDate", typeof(DateTime));
            largeTable.Columns.Add("Value", typeof(decimal));
            largeTable.Columns.Add("Quantity", typeof(double));
            largeTable.Columns.Add("IsActive", typeof(bool));
            largeTable.Columns.Add("Category", typeof(string));
            largeTable.Columns.Add("Tags", typeof(string));
            largeTable.Columns.Add("Priority", typeof(int));
            largeTable.Columns.Add("Score", typeof(float));
            largeTable.Columns.Add("BinaryData", typeof(byte[]));
            largeTable.Columns.Add("NullableField", typeof(string));

            Console.WriteLine("Generating row data...");

            // Generate rows in batches to avoid memory pressure
            const int BATCH_SIZE = 10000;
            for (int batchStart = 0; batchStart < VERY_LARGE_ROW_COUNT; batchStart += BATCH_SIZE)
            {
                var batchEnd = Math.Min(batchStart + BATCH_SIZE, VERY_LARGE_ROW_COUNT);
                var batchRows = new DataRow[batchEnd - batchStart];

                // CHANGED: Using a sequential approach instead of Parallel.For to avoid thread safety issues
                for (int i = batchStart; i < batchEnd; i++)
                {
                    // Create a Random instance with a deterministic seed for reproducibility
                    var random = new Random(42 + i);

                    var row = largeTable.NewRow();

                    // Generate values for all columns
                    row["ID"] = i;
                    row["GUID"] = Guid.NewGuid();
                    row["Name"] = $"Item {i}";
                    row["Description"] = $"This is a detailed description for item {i} with additional text to increase size";
                    row["CreatedDate"] = DateTime.Now.AddDays(-random.Next(1000));
                    row["ModifiedDate"] = DateTime.Now.AddDays(-random.Next(365));
                    row["Value"] = Math.Round(random.NextDouble() * 10000, 2);
                    row["Quantity"] = Math.Round(random.NextDouble() * 1000, 3);
                    row["IsActive"] = random.Next(100) < 85; // 85% active
                    row["Category"] = $"Category-{random.Next(1, 21)}"; // 20 categories
                    row["Tags"] = i % 10 == 0 ? null : $"tag{random.Next(1, 51)},tag{random.Next(1, 51)}";
                    row["Priority"] = random.Next(1, 6); // 1-5 priority
                    row["Score"] = (float)(random.NextDouble() * 100);

                    // Binary data - every 20th row gets null
                    if (i % 20 != 0)
                    {
                        byte[] binaryData = new byte[32]; // 32 bytes
                        random.NextBytes(binaryData);
                        row["BinaryData"] = binaryData;
                    }
                    else
                    {
                        row["BinaryData"] = DBNull.Value;
                    }

                    // Some null values (every 7th row)
                    if (i % 7 == 0)
                    {
                        row["NullableField"] = DBNull.Value;
                    }
                    else
                    {
                        row["NullableField"] = $"Value-{i % 100}";
                    }

                    // Store the row in our array at the correct position
                    batchRows[i - batchStart] = row;
                }

                // Add all rows from this batch to the DataTable
                foreach (var row in batchRows)
                {
                    largeTable.Rows.Add(row);
                }

                // Report progress
                Console.WriteLine($"Generated {batchEnd:N0} of {VERY_LARGE_ROW_COUNT:N0} rows ({batchEnd * 100.0 / VERY_LARGE_ROW_COUNT:F1}%)");

                // Force garbage collection to keep memory usage reasonable
                GC.Collect();
            }

            Console.WriteLine("Writing large dataset to Parquet file...");
            await largeTable.ExportToParquetAsync(filePath, "gzip");

            stopwatch.Stop();

            var fileInfo = new FileInfo(filePath);
            Console.WriteLine($"Large file created successfully at: {filePath}");
            Console.WriteLine($"File size: {fileInfo.Length / (1024.0 * 1024.0):F2} MB");
            Console.WriteLine($"Creation time: {stopwatch.Elapsed.TotalSeconds:F2} seconds");
        }

        /// <summary>
        /// Tests multi-threaded reading from a Parquet file
        /// </summary>
        private static async Task TestMultiThreadedReading(string filePath)
        {
            Console.WriteLine("\n--- MULTI-THREADED READING TEST ---");

            var stopwatch = Stopwatch.StartNew();

            // Create a thread-safe collection to store results
            var results = new ConcurrentBag<(int ThreadId, int RowsProcessed)>();

            // Number of parallel readers
            int threadCount = Environment.ProcessorCount;
            Console.WriteLine($"Starting {threadCount} parallel readers...");

            // Create and start the tasks
            var tasks = new List<Task>();
            for (int i = 0; i < threadCount; i++)
            {
                int threadId = i;
                tasks.Add(Task.Run(async () =>
                {
                    int rowsProcessed = 0;

                    // Each thread creates its own reader
                    using (var reader = await ParquetDataReaderFactory.CreateAsync(filePath))
                    {
                        // Calculate which portion of the file this thread should process
                        int rowsPerThread = VERY_LARGE_ROW_COUNT / threadCount;
                        int startRow = threadId * rowsPerThread;
                        int endRow = (threadId == threadCount - 1) ? VERY_LARGE_ROW_COUNT : (threadId + 1) * rowsPerThread;

                        // Skip rows until we reach our starting point
                        int currentRow = 0;
                        while (currentRow < startRow && await reader.ReadAsync())
                        {
                            currentRow++;
                        }

                        // Process our assigned rows
                        while (currentRow < endRow && await reader.ReadAsync())
                        {
                            // Simulate processing the row
                            ProcessRow(reader, threadId);
                            currentRow++;
                            rowsProcessed++;

                            // Progress reporting (don't want to flood the console)
                            if (rowsProcessed % 100000 == 0)
                            {
                                Console.WriteLine($"Thread {threadId}: Processed {rowsProcessed:N0} rows");
                            }
                        }
                    }

                    // Add our results to the collection
                    results.Add((threadId, rowsProcessed));
                }));
            }

            // Wait for all tasks to complete
            await Task.WhenAll(tasks);

            stopwatch.Stop();

            // Report the results
            int totalRowsProcessed = results.Sum(r => r.RowsProcessed);
            Console.WriteLine($"Multi-threaded reading completed in {stopwatch.Elapsed.TotalSeconds:F2} seconds");
            Console.WriteLine($"Total rows processed: {totalRowsProcessed:N0}");
            foreach (var result in results.OrderBy(r => r.ThreadId))
            {
                Console.WriteLine($"Thread {result.ThreadId}: Processed {result.RowsProcessed:N0} rows");
            }
        }

        /// <summary>
        /// Tests using ParquetBatchReader for parallel row group processing
        /// </summary>
        private static async Task TestParallelRowGroupProcessing(string filePath)
        {
            Console.WriteLine("\n--- PARALLEL ROW GROUP PROCESSING TEST ---");

            var stopwatch = Stopwatch.StartNew();

            try
            {
                // Use the ParquetBatchReader for parallel processing of row groups
                using (var batchReader = new ParquetBatchReader(filePath, Environment.ProcessorCount))
                {
                    Console.WriteLine($"File has {batchReader.BatchCount} row groups");

                    // Stats tracking
                    int totalRows = 0;
                    var processedBatches = new ConcurrentBag<int>();

                    // Process all batches in parallel
                    await foreach (var batch in batchReader.ReadAllAsync())
                    {
                        // Track which batch we processed
                        processedBatches.Add(batch.RowGroupIndex);

                        // Add to total rows processed
                        totalRows += batch.RowCount;

                        // Log progress
                        Console.WriteLine($"Processed batch {batch.RowGroupIndex} with {batch.RowCount:N0} rows");
                    }

                    // Report stats
                    Console.WriteLine($"Processed {processedBatches.Count} batches with {totalRows:N0} total rows");
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"ERROR in parallel row group test: {ex.Message}");
                Console.WriteLine("This might be because the ParquetBatchReader is not implemented as expected.");
            }

            stopwatch.Stop();
            Console.WriteLine($"Parallel row group processing completed in {stopwatch.Elapsed.TotalSeconds:F2} seconds");
        }

        /// <summary>
        /// Tests streaming reading with an IObserver pattern
        /// </summary>
        private static async Task TestStreamingReadingWithObserver(string filePath)
        {
            Console.WriteLine("\n--- STREAMING READING WITH OBSERVER TEST ---");

            var stopwatch = Stopwatch.StartNew();

            // Create observer to handle row data
            var observer = new ParquetRowObserver();

            // Start reading in a separate task
            Task readerTask = Task.Run(async () =>
            {
                using (var reader = await ParquetDataReaderFactory.CreateAsync(filePath))
                {
                    int rowIndex = 0;

                    // Read schema information
                    var schemaTable = reader.GetSchemaTable();
                    observer.OnSchemaReceived(schemaTable);

                    // Read rows and stream them to the observer
                    while (await reader.ReadAsync())
                    {
                        // Create a row data object
                        var values = new object[reader.FieldCount];
                        reader.GetValues(values);

                        // Send to observer
                        observer.OnNext(rowIndex, values);

                        rowIndex++;

                        // Simulate backpressure by occasionally pausing
                        if (rowIndex % 100000 == 0)
                        {
                            await Task.Delay(10); // Small delay to simulate backpressure
                        }
                    }

                    // Signal completion
                    observer.OnCompleted();
                }
            });

            // Start consumer tasks that will process the data
            var consumerTasks = new List<Task>();
            for (int i = 0; i < 4; i++)
            {
                int consumerId = i;
                consumerTasks.Add(Task.Run(() =>
                {
                    Console.WriteLine($"Consumer {consumerId} started");

                    while (!observer.IsCompleted)
                    {
                        var batch = observer.TryGetNextBatch(250);
                        if (batch.Count > 0)
                        {
                            // Process the batch
                            ProcessBatch(batch, consumerId);
                        }
                        else if (!observer.IsCompleted)
                        {
                            // No data available yet, sleep briefly
                            Thread.Sleep(1);
                        }
                    }

                    // Process any remaining data
                    var finalBatch = observer.TryGetNextBatch(int.MaxValue);
                    if (finalBatch.Count > 0)
                    {
                        ProcessBatch(finalBatch, consumerId);
                    }

                    Console.WriteLine($"Consumer {consumerId} finished");
                }));
            }

            // Wait for both producer and consumers to complete
            await readerTask;
            await Task.WhenAll(consumerTasks);

            stopwatch.Stop();

            // Report results
            Console.WriteLine($"Streaming reading completed in {stopwatch.Elapsed.TotalSeconds:F2} seconds");
            Console.WriteLine($"Total rows streamed: {observer.TotalRowsProcessed:N0}");
            Console.WriteLine($"Average queue size: {observer.AverageQueueSize:F1}");
        }

        /// <summary>
        /// Tests concurrent aggregation of Parquet data
        /// </summary>
        private static async Task TestConcurrentAggregation(string filePath)
        {
            Console.WriteLine("\n--- CONCURRENT AGGREGATION TEST ---");

            var stopwatch = Stopwatch.StartNew();

            // Aggregation results by category
            var categoryAggregations = new ConcurrentDictionary<string, (int Count, decimal TotalValue)>();
            var priorityAggregations = new ConcurrentDictionary<int, (int Count, double AverageQuantity)>();

            // Create multiple readers to process different parts of the file
            int threadCount = Environment.ProcessorCount;
            var tasks = new List<Task>();

            for (int i = 0; i < threadCount; i++)
            {
                int threadId = i;
                tasks.Add(Task.Run(async () =>
                {
                    using (var reader = await ParquetDataReaderFactory.CreateAsync(filePath))
                    {
                        // Find column indexes
                        int idCol = reader.GetOrdinal("ID");
                        int categoryCol = reader.GetOrdinal("Category");
                        int valueCol = reader.GetOrdinal("Value");
                        int priorityCol = reader.GetOrdinal("Priority");
                        int quantityCol = reader.GetOrdinal("Quantity");

                        // Each thread processes rows when ID % threadCount == threadId
                        while (await reader.ReadAsync())
                        {
                            int id = reader.GetInt32(idCol);

                            // Only process rows assigned to this thread
                            if (id % threadCount == threadId)
                            {
                                // Get values - handle potential NULL values
                                string category = reader.IsDBNull(categoryCol) ? "Unknown" : reader.GetString(categoryCol);

                                // Check for NULL before getting decimal value
                                decimal value = reader.IsDBNull(valueCol) ? 0m : reader.GetDecimal(valueCol);

                                // Check for NULL before getting int value
                                int priority = reader.IsDBNull(priorityCol) ? 0 : reader.GetInt32(priorityCol);

                                // Check for NULL before getting double value
                                double quantity = reader.IsDBNull(quantityCol) ? 0.0 : reader.GetDouble(quantityCol);

                                // Update category aggregation
                                categoryAggregations.AddOrUpdate(
                                    category,
                                    (1, value),
                                    (key, existing) => (existing.Count + 1, existing.TotalValue + value)
                                );

                                // Update priority aggregation
                                priorityAggregations.AddOrUpdate(
                                    priority,
                                    (1, quantity),
                                    (key, existing) => (existing.Count + 1,
                                        ((existing.AverageQuantity * existing.Count) + quantity) / (existing.Count + 1))
                                );
                            }
                        }
                    }
                }));
            }

            // Wait for all threads to complete
            await Task.WhenAll(tasks);

            stopwatch.Stop();

            // Report results
            Console.WriteLine($"Concurrent aggregation completed in {stopwatch.Elapsed.TotalSeconds:F2} seconds");
            Console.WriteLine($"Category aggregations: {categoryAggregations.Count}");
            Console.WriteLine($"Priority aggregations: {priorityAggregations.Count}");

            // Show some results
            Console.WriteLine("\nTop 5 categories by total value:");
            foreach (var category in categoryAggregations.OrderByDescending(c => c.Value.TotalValue).Take(5))
            {
                Console.WriteLine($"  {category.Key}: Count={category.Value.Count:N0}, TotalValue={category.Value.TotalValue:C2}");
            }

            Console.WriteLine("\nAverage quantity by priority:");
            foreach (var priority in priorityAggregations.OrderBy(p => p.Key))
            {
                Console.WriteLine($"  Priority {priority.Key}: Count={priority.Value.Count:N0}, AvgQuantity={priority.Value.AverageQuantity:F2}");
            }
        }

        /// <summary>
        /// Tests asynchronous streaming processing with IAsyncEnumerable
        /// </summary>
        private static async Task TestAsyncStreamProcessing(string filePath)
        {
            Console.WriteLine("\n--- ASYNC STREAM PROCESSING TEST ---");

            var stopwatch = Stopwatch.StartNew();

            // Create a wrapper for async enumeration
            var asyncRows = GetParquetRowsAsync(filePath);

            // Process the stream
            int processedRows = 0;
            decimal totalValue = 0;
            int activeItems = 0;

            await foreach (var row in asyncRows)
            {
                processedRows++;

                // Calculate some aggregations
                if (row.IsActive)
                {
                    activeItems++;
                    totalValue += row.Value;
                }

                // Progress reporting
                if (processedRows % 250000 == 0)
                {
                    Console.WriteLine($"Processed {processedRows:N0} rows");
                }
            }

            stopwatch.Stop();

            // Report results
            Console.WriteLine($"Async stream processing completed in {stopwatch.Elapsed.TotalSeconds:F2} seconds");
            Console.WriteLine($"Processed {processedRows:N0} rows");
            Console.WriteLine($"Active items: {activeItems:N0} ({activeItems * 100.0 / processedRows:F1}%)");
            Console.WriteLine($"Total value of active items: {totalValue:C2}");
        }

        /// <summary>
        /// Tests cancellation during Parquet reading
        /// </summary>
        private static async Task TestCancellation(string filePath)
        {
            Console.WriteLine("\n--- CANCELLATION TEST ---");

            var stopwatch = Stopwatch.StartNew();

            // Create a cancellation token that will cancel after processing some rows
            using var cts = new CancellationTokenSource();
            var cancellationToken = cts.Token;

            // Start reading task
            var readingTask = Task.Run(async () =>
            {
                int rowsProcessed = 0;

                try
                {
                    using var reader = await ParquetDataReaderFactory.CreateAsync(filePath, cancellationToken: cancellationToken);

                    while (await reader.ReadAsync(cancellationToken))
                    {
                        // Process row
                        rowsProcessed++;

                        // Cancel after processing some rows
                        if (rowsProcessed >= 500000)
                        {
                            Console.WriteLine("Cancelling operation after 500,000 rows...");
                            cts.Cancel();
                        }

                        // Simulate some processing work
                        if (rowsProcessed % 100000 == 0)
                        {
                            Console.WriteLine($"Processed {rowsProcessed:N0} rows before cancellation");
                        }
                    }

                    return rowsProcessed;
                }
                catch (OperationCanceledException)
                {
                    Console.WriteLine($"Operation was cancelled after processing {rowsProcessed:N0} rows");
                    return rowsProcessed;
                }
            });

            // Wait for completion or cancellation
            try
            {
                int rowsProcessed = await readingTask;
                Console.WriteLine($"Finished processing {rowsProcessed:N0} rows");
            }
            catch (OperationCanceledException)
            {
                Console.WriteLine("Task was cancelled as expected");
            }

            stopwatch.Stop();
            Console.WriteLine($"Cancellation test completed in {stopwatch.Elapsed.TotalSeconds:F2} seconds");
        }

        /// <summary>
        /// Tests memory efficiency during large file processing
        /// </summary>
        private static async Task TestMemoryEfficiency(string filePath)
        {
            Console.WriteLine("\n--- MEMORY EFFICIENCY TEST ---");

            // Take initial memory measurement
            GC.Collect();
            GC.WaitForPendingFinalizers();
            GC.Collect();
            long initialMemory = GC.GetTotalMemory(true);

            Console.WriteLine($"Initial memory usage: {initialMemory / (1024 * 1024):N2} MB");

            var stopwatch = Stopwatch.StartNew();

            // Process file in small batches to measure memory usage
            int rowsProcessed = 0;
            long maxMemoryUsage = initialMemory;
            long previousMemory = initialMemory;

            using (var reader = await ParquetDataReaderFactory.CreateAsync(filePath))
            {
                while (await reader.ReadAsync())
                {
                    rowsProcessed++;

                    // Check memory usage every 100,000 rows
                    if (rowsProcessed % 100000 == 0)
                    {
                        // Measure current memory usage
                        long currentMemory = GC.GetTotalMemory(false);
                        maxMemoryUsage = Math.Max(maxMemoryUsage, currentMemory);

                        // Report memory usage
                        Console.WriteLine($"Rows processed: {rowsProcessed:N0}");
                        Console.WriteLine($"Current memory: {currentMemory / (1024 * 1024):N2} MB");
                        Console.WriteLine($"Change: {(currentMemory - previousMemory) / (1024 * 1024):N2} MB");

                        previousMemory = currentMemory;
                    }
                }
            }

            // Final memory measurement after processing
            GC.Collect();
            GC.WaitForPendingFinalizers();
            GC.Collect();
            long finalMemory = GC.GetTotalMemory(true);

            stopwatch.Stop();

            // Report results
            Console.WriteLine($"Memory efficiency test completed in {stopwatch.Elapsed.TotalSeconds:F2} seconds");
            Console.WriteLine($"Total rows processed: {rowsProcessed:N0}");
            Console.WriteLine($"Initial memory: {initialMemory / (1024 * 1024):N2} MB");
            Console.WriteLine($"Maximum memory: {maxMemoryUsage / (1024 * 1024):N2} MB");
            Console.WriteLine($"Final memory: {finalMemory / (1024 * 1024):N2} MB");
            Console.WriteLine($"Memory increase: {(finalMemory - initialMemory) / (1024 * 1024):N2} MB");
        }

        #region Helper Methods and Classes

        /// <summary>
        /// Processes a row from a data reader (simulates work)
        /// </summary>
        private static void ProcessRow(IDataReader reader, int threadId)
        {
            // No actual processing, this is just a simulation
            // In a real application, you would extract and transform the data here
        }

        /// <summary>
        /// Processes a batch of rows (simulates work)
        /// </summary>
        private static void ProcessBatch(List<(int RowIndex, object[] Values)> batch, int consumerId)
        {
            // No actual processing, this is just a simulation
            // In a real application, you would process each row in the batch
        }

        /// <summary>
        /// Observer pattern implementation for Parquet row streaming
        /// </summary>
        private class ParquetRowObserver
        {
            private readonly ConcurrentQueue<(int RowIndex, object[] Values)> _queue = new();
            private readonly object _lock = new object();
            private volatile bool _completed = false;
            private int _queueSizeSamples = 0;
            private long _totalQueueSize = 0;
            private long _totalRowsProcessed = 0;

            public bool IsCompleted => _completed && _queue.IsEmpty;
            public long TotalRowsProcessed => _totalRowsProcessed;
            public double AverageQueueSize => _queueSizeSamples > 0 ? _totalQueueSize / (double)_queueSizeSamples : 0;

            public void OnSchemaReceived(DataTable schemaTable)
            {
                // In a real implementation, you might store schema information
                // or notify consumers about the schema
            }

            public void OnNext(int rowIndex, object[] values)
            {
                _queue.Enqueue((rowIndex, values));

                // Track queue size for metrics
                lock (_lock)
                {
                    _queueSizeSamples++;
                    _totalQueueSize += _queue.Count;
                    _totalRowsProcessed++;
                }
            }

            public void OnCompleted()
            {
                _completed = true;
            }

            public List<(int RowIndex, object[] Values)> TryGetNextBatch(int maxItems)
            {
                var result = new List<(int RowIndex, object[] Values)>();

                while (result.Count < maxItems && _queue.TryDequeue(out var item))
                {
                    result.Add(item);
                }

                return result;
            }
        }

        /// <summary>
        /// Data class for async enumeration
        /// </summary>
        private class ParquetRow
        {
            public int ID { get; set; }
            public string Name { get; set; }
            public string Category { get; set; }
            public decimal Value { get; set; }
            public double Quantity { get; set; }
            public bool IsActive { get; set; }
            public DateTime CreatedDate { get; set; }
            public int Priority { get; set; }
        }

        /// <summary>
        /// Creates an async enumerable for rows in a Parquet file
        /// </summary>
        private static async IAsyncEnumerable<ParquetRow> GetParquetRowsAsync(string filePath, [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancellationToken = default)
        {
            using var reader = await ParquetDataReaderFactory.CreateAsync(filePath, cancellationToken: cancellationToken);

            // Find column indexes
            int idCol = reader.GetOrdinal("ID");
            int nameCol = reader.GetOrdinal("Name");
            int categoryCol = reader.GetOrdinal("Category");
            int valueCol = reader.GetOrdinal("Value");
            int quantityCol = reader.GetOrdinal("Quantity");
            int isActiveCol = reader.GetOrdinal("IsActive");
            int createdDateCol = reader.GetOrdinal("CreatedDate");
            int priorityCol = reader.GetOrdinal("Priority");

            // Stream rows
            while (await reader.ReadAsync(cancellationToken))
            {
                // Create row object
                var row = new ParquetRow
                {
                    ID = reader.GetInt32(idCol),
                    Name = reader.GetString(nameCol),
                    Category = reader.GetString(categoryCol),
                    Value = reader.GetDecimal(valueCol),
                    Quantity = reader.GetDouble(quantityCol),
                    IsActive = reader.GetBoolean(isActiveCol),
                    CreatedDate = reader.GetDateTime(createdDateCol),
                    Priority = reader.GetInt32(priorityCol)
                };

                yield return row;
            }
        }

        #endregion
    }
}
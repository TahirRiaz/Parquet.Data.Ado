using System;
using System.Buffers;
using System.Collections.Concurrent;
using System.Data;
using System.Diagnostics;
using System.IO;
using System.IO.Pipelines;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;
using Parquet.Data.Reader;
using Parquet.Data.Ado;

namespace ParquetReaderTest
{
    /// <summary>
    /// Specialized tests for multi-threaded streaming operations with Parquet files
    /// </summary>
    public static class ParquetStreamingTests
    {
        // Constants for test configuration
        private const int LARGE_FILE_ROWS = 250_000;
        private const int STREAMING_BUFFER_SIZE = 10000;
        private const int LOG_INTERVAL = 250000;

        public static async Task RunAllTests()
        {
            Console.WriteLine("\n=== ADVANCED STREAMING TESTS ===");

            // Generate file paths
            string largeFilePath = Path.Combine(Path.GetTempPath(), "parquet_streaming_test.parquet");

            try
            {
                // Create the large test file if needed
                await CreateTestFile(largeFilePath);

                // Run various streaming tests
                await TestChannelBasedStreaming(largeFilePath);
                await TestPipelineBasedStreaming(largeFilePath);
                await TestHighThroughputConsumerProducer(largeFilePath);
                await TestDataTransformation(largeFilePath);
                await TestMultiplexedOutput(largeFilePath);
                await TestStreamingToNetwork(largeFilePath);
                await TestBackpressureHandling(largeFilePath);
            }
            finally
            {
                // Clean up
                if (File.Exists(largeFilePath))
                {
                    try
                    {
                        File.Delete(largeFilePath);
                        Console.WriteLine("Test file deleted successfully.");
                    }
                    catch (IOException ex)
                    {
                        Console.WriteLine($"Warning: Could not delete test file: {ex.Message}");
                    }
                }
            }
        }

        /// <summary>
        /// Creates a large test file for streaming tests
        /// </summary>
        private static async Task CreateTestFile(string filePath)
        {
            // Skip if file already exists with sufficient size
            if (File.Exists(filePath))
            {
                var info = new FileInfo(filePath);
                if (info.Length > 10 * 1024 * 1024) // > 10MB is probably sufficient
                {
                    Console.WriteLine($"Using existing test file: {filePath} ({info.Length / (1024.0 * 1024.0):F2} MB)");
                    return;
                }
            }

            Console.WriteLine($"Creating large Parquet test file with {LARGE_FILE_ROWS:N0} rows...");
            var stopwatch = Stopwatch.StartNew();

            // Create a large DataTable
            var table = new DataTable("StreamingTestData");
            table.Columns.Add("ID", typeof(int));
            table.Columns.Add("Timestamp", typeof(DateTime));
            table.Columns.Add("SensorID", typeof(string));
            table.Columns.Add("Temperature", typeof(double));
            table.Columns.Add("Humidity", typeof(double));
            table.Columns.Add("Pressure", typeof(double));
            table.Columns.Add("Voltage", typeof(float));
            table.Columns.Add("Status", typeof(string));
            table.Columns.Add("IsActive", typeof(bool));
            table.Columns.Add("ErrorCode", typeof(int));
            table.Columns.Add("BatchID", typeof(Guid));
            table.Columns.Add("RawData", typeof(byte[]));

            // Generate data in chunks to avoid memory pressure
            var random = new Random(42);
            const int CHUNK_SIZE = 100000;

            for (int chunkStart = 0; chunkStart < LARGE_FILE_ROWS; chunkStart += CHUNK_SIZE)
            {
                // Clear the table if not on first chunk
                if (chunkStart > 0) table.Clear();

                int chunkEnd = Math.Min(chunkStart + CHUNK_SIZE, LARGE_FILE_ROWS);

                // Generate rows for this chunk
                for (int i = chunkStart; i < chunkEnd; i++)
                {
                    var row = table.NewRow();
                    row["ID"] = i;
                    row["Timestamp"] = DateTime.Now.AddSeconds(-i);
                    row["SensorID"] = $"SEN-{random.Next(1, 1001):D4}"; // 1000 unique sensors
                    row["Temperature"] = Math.Round(20 + random.NextDouble() * 15, 2); // 20-35°C
                    row["Humidity"] = Math.Round(30 + random.NextDouble() * 60, 2); // 30-90%
                    row["Pressure"] = Math.Round(980 + random.NextDouble() * 40, 2); // 980-1020 hPa
                    row["Voltage"] = (float)Math.Round(3.0 + random.NextDouble() * 2.5, 3); // 3-5.5V

                    // Status - simulate different states
                    string[] statuses = { "Normal", "Warning", "Error", "Critical", "Offline" };
                    // Make "Normal" more common
                    int statusIndex = random.Next(0, 100) < 80 ? 0 : random.Next(0, statuses.Length);
                    row["Status"] = statuses[statusIndex];

                    row["IsActive"] = random.Next(100) < 95; // 95% active
                    row["ErrorCode"] = random.Next(0, 100) < 90 ? 0 : random.Next(1, 100); // 10% error
                    row["BatchID"] = Guid.NewGuid();

                    // Variable size binary data (only for some rows to save space)
                    if (random.Next(100) < 30) // 30% have binary data
                    {
                        int dataSize = random.Next(20, 101); // 20-100 bytes
                        byte[] data = new byte[dataSize];
                        random.NextBytes(data);
                        row["RawData"] = data;
                    }

                    table.Rows.Add(row);
                }

                // Write this chunk to the Parquet file
                if (chunkStart == 0)
                {
                    // First chunk - create new file
                    await table.ExportToParquetAsync(filePath);
                }
                else
                {
                    // For later chunks, we would ideally append - but this requires
                    // lower-level Parquet writers that can append to existing files.
                    // For simplicity in this test, we'll read the existing file, add rows, and rewrite.
                    // In a real app, you would use a more efficient approach.

                    // Read existing data
                    var existingData = new DataTable();
                    using (var reader = await ParquetDataReaderFactory.CreateAsync(filePath))
                    {
                        existingData = reader.ToDataTable();
                    }

                    // Add new rows
                    foreach (DataRow row in table.Rows)
                    {
                        existingData.Rows.Add(row.ItemArray);
                    }

                    // Write back to file
                    await existingData.ExportToParquetAsync(filePath);
                }

                Console.WriteLine($"Generated {chunkEnd:N0} of {LARGE_FILE_ROWS:N0} rows");
            }

            stopwatch.Stop();

            var fileInfo = new FileInfo(filePath);
            Console.WriteLine($"File created: {fileInfo.Length / (1024.0 * 1024.0):F2} MB in {stopwatch.Elapsed.TotalSeconds:F2} seconds");
        }

        /// <summary>
        /// Tests streaming using Channel (from System.Threading.Channels)
        /// </summary>
        private static async Task TestChannelBasedStreaming(string filePath)
        {
            Console.WriteLine("\n--- CHANNEL-BASED STREAMING TEST ---");
            var stopwatch = Stopwatch.StartNew();

            // Create a channel for streaming rows
            var channelOptions = new BoundedChannelOptions(STREAMING_BUFFER_SIZE)
            {
                FullMode = BoundedChannelFullMode.Wait, // Blocks producer when full (backpressure)
                SingleReader = false,
                SingleWriter = true
            };

            var channel = Channel.CreateBounded<SensorReading>(channelOptions);

            // Start producer task (reading from Parquet)
            var producerTask = Task.Run(async () =>
            {
                int rowCount = 0;

                try
                {
                    using var reader = await ParquetDataReaderFactory.CreateAsync(filePath);

                    // Get column indexes for efficient access
                    int idCol = reader.GetOrdinal("ID");
                    int timestampCol = reader.GetOrdinal("Timestamp");
                    int sensorIdCol = reader.GetOrdinal("SensorID");
                    int temperatureCol = reader.GetOrdinal("Temperature");
                    int humidityCol = reader.GetOrdinal("Humidity");
                    int pressureCol = reader.GetOrdinal("Pressure");
                    int statusCol = reader.GetOrdinal("Status");

                    // Read all rows and send to channel
                    while (await reader.ReadAsync())
                    {
                        var reading = new SensorReading
                        {
                            ID = reader.GetInt32(idCol),
                            Timestamp = reader.GetDateTime(timestampCol),
                            SensorID = reader.GetString(sensorIdCol),
                            Temperature = reader.GetDouble(temperatureCol),
                            Humidity = reader.GetDouble(humidityCol),
                            Pressure = reader.GetDouble(pressureCol),
                            Status = reader.GetString(statusCol)
                        };

                        // Send to channel (will wait if channel is full - backpressure)
                        await channel.Writer.WriteAsync(reading);

                        rowCount++;
                        if (rowCount % LOG_INTERVAL == 0)
                        {
                            Console.WriteLine($"Producer: Streamed {rowCount:N0} rows");
                        }
                    }

                    // Signal completion
                    channel.Writer.Complete();
                    Console.WriteLine($"Producer: Completed streaming {rowCount:N0} total rows");
                    return rowCount;
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"Producer error: {ex.Message}");
                    channel.Writer.Complete(ex);
                    throw;
                }
            });

            // Start multiple consumer tasks
            var consumerTasks = new Task<int>[4];
            for (int i = 0; i < consumerTasks.Length; i++)
            {
                int consumerId = i;
                consumerTasks[i] = Task.Run(async () =>
                {
                    int processedCount = 0;
                    double temperatureSum = 0;
                    double humiditySum = 0;
                    double pressureSum = 0;

                    try
                    {
                        await foreach (var reading in channel.Reader.ReadAllAsync())
                        {
                            // Process the reading
                            processedCount++;
                            temperatureSum += reading.Temperature;
                            humiditySum += reading.Humidity;
                            pressureSum += reading.Pressure;

                            // Simulate some processing time (slightly random to simulate real workloads)
                            if (processedCount % 10000 == 0)
                            {
                                await Task.Delay(1); // Very minor delay that adds up
                            }

                            if (processedCount % LOG_INTERVAL == 0)
                            {
                                Console.WriteLine($"Consumer {consumerId}: Processed {processedCount:N0} readings");
                            }
                        }

                        // Report consumer results
                        if (processedCount > 0)
                        {
                            Console.WriteLine($"Consumer {consumerId}: Completed with {processedCount:N0} readings processed");
                            Console.WriteLine($"  Avg Temperature: {temperatureSum / processedCount:F2}°C");
                            Console.WriteLine($"  Avg Humidity: {humiditySum / processedCount:F2}%");
                            Console.WriteLine($"  Avg Pressure: {pressureSum / processedCount:F2} hPa");
                        }

                        return processedCount;
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine($"Consumer {consumerId} error: {ex.Message}");
                        throw;
                    }
                });
            }

            // Wait for producer and consumers to complete
            int producedRows = await producerTask;
            int[] consumedRows = await Task.WhenAll(consumerTasks);

            stopwatch.Stop();

            // Validate and report results
            int totalConsumed = consumedRows.Sum();
            Console.WriteLine($"Channel streaming completed in {stopwatch.Elapsed.TotalSeconds:F2} seconds");
            Console.WriteLine($"Producer generated: {producedRows:N0} rows");
            Console.WriteLine($"Consumers processed: {totalConsumed:N0} rows");
            Console.WriteLine($"Processing rate: {producedRows / stopwatch.Elapsed.TotalSeconds:N0} rows/second");

            // Verify all rows were processed
            if (totalConsumed != producedRows * consumerTasks.Length)
            {
                Console.WriteLine($"WARNING: Expected {producedRows * consumerTasks.Length:N0} total processings, got {totalConsumed:N0}");
            }
        }

        /// <summary>
        /// Tests streaming using System.IO.Pipelines
        /// </summary>
        private static async Task TestPipelineBasedStreaming(string filePath)
        {
            Console.WriteLine("\n--- PIPELINE-BASED STREAMING TEST ---");
            var stopwatch = Stopwatch.StartNew();

            // Create a pipe for streaming binary data
            var pipe = new Pipe();

            // Start producer task (reading from Parquet and writing to pipe)
            var producerTask = Task.Run(async () =>
            {
                int rowCount = 0;

                try
                {
                    using var reader = await ParquetDataReaderFactory.CreateAsync(filePath);

                    // Get column indexes for efficient access
                    int idCol = reader.GetOrdinal("ID");
                    int sensorIdCol = reader.GetOrdinal("SensorID");
                    int temperatureCol = reader.GetOrdinal("Temperature");
                    int humidityCol = reader.GetOrdinal("Humidity");
                    int statusCol = reader.GetOrdinal("Status");

                    // Read all rows and write to pipe
                    while (await reader.ReadAsync())
                    {
                        // Create a simple binary format for each row
                        var writer = new BinaryWriter(pipe.Writer.AsStream());

                        // Write ID
                        writer.Write(reader.GetInt32(idCol));

                        // Write SensorID (as bytes with length prefix)
                        string sensorId = reader.GetString(sensorIdCol);
                        byte[] sensorIdBytes = Encoding.UTF8.GetBytes(sensorId);
                        writer.Write((short)sensorIdBytes.Length);
                        writer.Write(sensorIdBytes);

                        // Write Temperature and Humidity
                        writer.Write(reader.GetDouble(temperatureCol));
                        writer.Write(reader.GetDouble(humidityCol));

                        // Write Status
                        string status = reader.GetString(statusCol);
                        byte[] statusBytes = Encoding.UTF8.GetBytes(status);
                        writer.Write((short)statusBytes.Length);
                        writer.Write(statusBytes);

                        // Flush the binary writer (but don't complete the pipe yet)
                        writer.Flush();

                        // Mark the data as available to the reader
                        FlushResult result = await pipe.Writer.FlushAsync();
                        if (result.IsCompleted)
                        {
                            break; // Reader has completed (unlikely in this case)
                        }

                        rowCount++;
                        if (rowCount % LOG_INTERVAL == 0)
                        {
                            Console.WriteLine($"Pipe producer: Streamed {rowCount:N0} rows");
                        }
                    }

                    // Signal completion
                    await pipe.Writer.CompleteAsync();
                    Console.WriteLine($"Pipe producer: Completed streaming {rowCount:N0} total rows");
                    return rowCount;
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"Pipe producer error: {ex.Message}");
                    await pipe.Writer.CompleteAsync(ex);
                    throw;
                }
            });

            // Start consumer task (reading from pipe)
            var consumerTask = Task.Run(async () =>
            {
                int processedCount = 0;
                double temperatureSum = 0;
                double humiditySum = 0;

                try
                {
                    // Read from pipe until completed
                    while (true)
                    {
                        ReadResult result = await pipe.Reader.ReadAsync();
                        ReadOnlySequence<byte> buffer = result.Buffer;

                        if (buffer.IsEmpty && result.IsCompleted)
                        {
                            break;
                        }

                        // Process data from buffer
                        var tempBuffer = buffer;
                        var reader = new BinaryReader(new ReadOnlySequenceStream(tempBuffer));

                        SequencePosition consumed = buffer.Start;

                        // Keep reading until we don't have enough data for a complete record
                        while (reader.BaseStream.Position < reader.BaseStream.Length - 20) // Rough minimum size estimate
                        {
                            try
                            {
                                // Read ID
                                int id = reader.ReadInt32();

                                // Read SensorID
                                short sensorIdLength = reader.ReadInt16();
                                if (reader.BaseStream.Position + sensorIdLength > reader.BaseStream.Length)
                                {
                                    break; // Not enough data, wait for more
                                }
                                byte[] sensorIdBytes = reader.ReadBytes(sensorIdLength);
                                string sensorId = Encoding.UTF8.GetString(sensorIdBytes);

                                // Read Temperature and Humidity
                                double temperature = reader.ReadDouble();
                                double humidity = reader.ReadDouble();

                                // Read Status
                                short statusLength = reader.ReadInt16();
                                if (reader.BaseStream.Position + statusLength > reader.BaseStream.Length)
                                {
                                    break; // Not enough data, wait for more
                                }
                                byte[] statusBytes = reader.ReadBytes(statusLength);
                                string status = Encoding.UTF8.GetString(statusBytes);

                                // Update position in buffer
                                consumed = buffer.GetPosition(reader.BaseStream.Position);

                                // Process the reading
                                processedCount++;
                                temperatureSum += temperature;
                                humiditySum += humidity;

                                if (processedCount % LOG_INTERVAL == 0)
                                {
                                    Console.WriteLine($"Pipe consumer: Processed {processedCount:N0} readings");
                                }
                            }
                            catch (EndOfStreamException)
                            {
                                // Not enough data to complete the record, wait for more
                                break;
                            }
                        }

                        // Tell the pipe how much data we've processed
                        pipe.Reader.AdvanceTo(consumed, buffer.End);
                    }

                    // Complete the reader
                    await pipe.Reader.CompleteAsync();

                    // Report consumer results
                    if (processedCount > 0)
                    {
                        Console.WriteLine($"Pipe consumer: Completed with {processedCount:N0} readings processed");
                        Console.WriteLine($"  Avg Temperature: {temperatureSum / processedCount:F2}°C");
                        Console.WriteLine($"  Avg Humidity: {humiditySum / processedCount:F2}%");
                    }

                    return processedCount;
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"Pipe consumer error: {ex.Message}");
                    await pipe.Reader.CompleteAsync(ex);
                    throw;
                }
            });

            // Wait for producer and consumer to complete
            await Task.WhenAll(producerTask, consumerTask);

            stopwatch.Stop();

            // Validate and report results
            int producedRows = await producerTask;
            int consumedRows = await consumerTask;

            Console.WriteLine($"Pipeline streaming completed in {stopwatch.Elapsed.TotalSeconds:F2} seconds");
            Console.WriteLine($"Producer generated: {producedRows:N0} rows");
            Console.WriteLine($"Consumer processed: {consumedRows:N0} rows");
            Console.WriteLine($"Processing rate: {producedRows / stopwatch.Elapsed.TotalSeconds:N0} rows/second");

            // Verify all rows were processed
            if (consumedRows != producedRows)
            {
                Console.WriteLine($"WARNING: Expected {producedRows:N0} processed rows, got {consumedRows:N0}");
            }
        }

        /// <summary>
        /// Tests high-throughput consumer-producer pattern with multiple producers and consumers
        /// </summary>
        private static async Task TestHighThroughputConsumerProducer(string filePath)
        {
            Console.WriteLine("\n--- HIGH-THROUGHPUT CONSUMER-PRODUCER TEST ---");
            var stopwatch = Stopwatch.StartNew();

            // Create concurrent collection for metrics gathering
            var metrics = new ConcurrentDictionary<string, (int Count, double SumTemperature, double SumHumidity)>();

            // Create a thread-safe queue for work items
            var queue = new BlockingCollection<SensorReading>(STREAMING_BUFFER_SIZE);

            // Start multiple producer tasks (reading from different parts of the file)
            int producerCount = Environment.ProcessorCount / 2;
            var producerTasks = new Task[producerCount];

            for (int i = 0; i < producerCount; i++)
            {
                int producerId = i;
                producerTasks[i] = Task.Run(async () =>
                {
                    int rowsProduced = 0;

                    try
                    {
                        using var reader = await ParquetDataReaderFactory.CreateAsync(filePath);

                        // Get column indexes for efficient access
                        int idCol = reader.GetOrdinal("ID");
                        int timestampCol = reader.GetOrdinal("Timestamp");
                        int sensorIdCol = reader.GetOrdinal("SensorID");
                        int temperatureCol = reader.GetOrdinal("Temperature");
                        int humidityCol = reader.GetOrdinal("Humidity");
                        int pressureCol = reader.GetOrdinal("Pressure");
                        int statusCol = reader.GetOrdinal("Status");

                        // Each producer reads rows where ID % producerCount == producerId
                        while (await reader.ReadAsync())
                        {
                            int id = reader.GetInt32(idCol);

                            // Only process rows assigned to this producer
                            if (id % producerCount == producerId)
                            {
                                var reading = new SensorReading
                                {
                                    ID = id,
                                    Timestamp = reader.GetDateTime(timestampCol),
                                    SensorID = reader.GetString(sensorIdCol),
                                    Temperature = reader.GetDouble(temperatureCol),
                                    Humidity = reader.GetDouble(humidityCol),
                                    Pressure = reader.GetDouble(pressureCol),
                                    Status = reader.GetString(statusCol)
                                };

                                // Add to the queue (will block if queue is full)
                                queue.Add(reading);

                                rowsProduced++;
                                if (rowsProduced % LOG_INTERVAL == 0)
                                {
                                    Console.WriteLine($"Producer {producerId}: Generated {rowsProduced:N0} readings");
                                }
                            }
                        }

                        Console.WriteLine($"Producer {producerId}: Completed with {rowsProduced:N0} readings");
                        return rowsProduced;
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine($"Producer {producerId} error: {ex.Message}");
                        throw;
                    }
                });
            }

            // Start multiple consumer tasks
            int consumerCount = Environment.ProcessorCount / 2;
            var consumerTasks = new Task[consumerCount];

            // Start all consumers
            for (int i = 0; i < consumerCount; i++)
            {
                int consumerId = i;
                consumerTasks[i] = Task.Run(() =>
                {
                    int rowsProcessed = 0;

                    try
                    {
                        // Process until the queue is marked as complete and empty
                        foreach (var reading in queue.GetConsumingEnumerable())
                        {
                            // Process the reading - group by sensor ID
                            metrics.AddOrUpdate(
                                reading.SensorID,
                                // Initial value if key doesn't exist
                                (1, reading.Temperature, reading.Humidity),
                                // Update function if key exists
                                (key, existing) => (
                                    existing.Count + 1,
                                    existing.SumTemperature + reading.Temperature,
                                    existing.SumHumidity + reading.Humidity
                                )
                            );

                            rowsProcessed++;
                            if (rowsProcessed % LOG_INTERVAL == 0)
                            {
                                Console.WriteLine($"Consumer {consumerId}: Processed {rowsProcessed:N0} readings");
                            }
                        }

                        Console.WriteLine($"Consumer {consumerId}: Completed with {rowsProcessed:N0} readings processed");
                        return rowsProcessed;
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine($"Consumer {consumerId} error: {ex.Message}");
                        throw;
                    }
                });
            }

            // Wait for producers to complete, then mark the queue as complete
            await Task.WhenAll(producerTasks);
            queue.CompleteAdding();

            // Wait for consumers to complete
            await Task.WhenAll(consumerTasks);

            stopwatch.Stop();

            // Calculate and report results
            Console.WriteLine($"High-throughput test completed in {stopwatch.Elapsed.TotalSeconds:F2} seconds");
            Console.WriteLine($"Number of unique sensors detected: {metrics.Count:N0}");

            // Calculate overall metrics
            int totalReadings = metrics.Values.Sum(m => m.Count);
            double overallTempAvg = metrics.Values.Sum(m => m.SumTemperature) / totalReadings;
            double overallHumidityAvg = metrics.Values.Sum(m => m.SumHumidity) / totalReadings;

            Console.WriteLine($"Total readings processed: {totalReadings:N0}");
            Console.WriteLine($"Overall average temperature: {overallTempAvg:F2}°C");
            Console.WriteLine($"Overall average humidity: {overallHumidityAvg:F2}%");

            // Show top 5 sensors by reading count
            Console.WriteLine("\nTop 5 sensors by reading count:");
            foreach (var sensor in metrics.OrderByDescending(m => m.Value.Count).Take(5))
            {
                double avgTemp = sensor.Value.SumTemperature / sensor.Value.Count;
                double avgHumidity = sensor.Value.SumHumidity / sensor.Value.Count;
                Console.WriteLine($"  {sensor.Key}: {sensor.Value.Count:N0} readings, " +
                                  $"Avg Temp: {avgTemp:F2}°C, Avg Humidity: {avgHumidity:F2}%");
            }
        }

        /// <summary>
        /// Tests data transformation during streaming - converting raw data to processed form
        /// </summary>
        private static async Task TestDataTransformation(string filePath)
        {
            Console.WriteLine("\n--- DATA TRANSFORMATION STREAMING TEST ---");
            var stopwatch = Stopwatch.StartNew();

            // Create a channel for raw data
            var rawChannel = Channel.CreateBounded<SensorReading>(STREAMING_BUFFER_SIZE);

            // Create a channel for processed data
            var processedChannel = Channel.CreateBounded<ProcessedReading>(STREAMING_BUFFER_SIZE);

            // Start reader task (producer)
            var readerTask = Task.Run(async () =>
            {
                int rowCount = 0;

                try
                {
                    using var reader = await ParquetDataReaderFactory.CreateAsync(filePath);

                    // Get column indexes for efficient access
                    int idCol = reader.GetOrdinal("ID");
                    int timestampCol = reader.GetOrdinal("Timestamp");
                    int sensorIdCol = reader.GetOrdinal("SensorID");
                    int temperatureCol = reader.GetOrdinal("Temperature");
                    int humidityCol = reader.GetOrdinal("Humidity");
                    int pressureCol = reader.GetOrdinal("Pressure");
                    int statusCol = reader.GetOrdinal("Status");

                    // Read all rows and send to raw channel
                    while (await reader.ReadAsync())
                    {
                        var reading = new SensorReading
                        {
                            ID = reader.GetInt32(idCol),
                            Timestamp = reader.GetDateTime(timestampCol),
                            SensorID = reader.GetString(sensorIdCol),
                            Temperature = reader.GetDouble(temperatureCol),
                            Humidity = reader.GetDouble(humidityCol),
                            Pressure = reader.GetDouble(pressureCol),
                            Status = reader.GetString(statusCol)
                        };

                        // Send to raw channel
                        await rawChannel.Writer.WriteAsync(reading);

                        rowCount++;
                        if (rowCount % LOG_INTERVAL == 0)
                        {
                            Console.WriteLine($"Reader: Read {rowCount:N0} rows");
                        }
                    }

                    // Signal completion
                    rawChannel.Writer.Complete();
                    Console.WriteLine($"Reader: Completed reading {rowCount:N0} total rows");
                    return rowCount;
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"Reader error: {ex.Message}");
                    rawChannel.Writer.Complete(ex);
                    throw;
                }
            });


            // Start transformer tasks
            var transformerTasks = new Task[4];
            for (int i = 0; i < transformerTasks.Length; i++)
            {
                int transformerId = i;
                transformerTasks[i] = Task.Run(async () =>
                {
                    int processedCount = 0;

                    try
                    {
                        await foreach (var reading in rawChannel.Reader.ReadAllAsync())
                        {
                            // Transform raw reading to processed reading
                            var processed = new ProcessedReading
                            {
                                ID = reading.ID,
                                Timestamp = reading.Timestamp,
                                SensorID = reading.SensorID,
                                TemperatureF = reading.Temperature * 9 / 5 + 32, // Convert to Fahrenheit
                                HeatIndex = CalculateHeatIndex(reading.Temperature, reading.Humidity),
                                DewPoint = CalculateDewPoint(reading.Temperature, reading.Humidity),
                                PressureInHg = reading.Pressure * 0.02953, // Convert to inches of mercury
                                AlertLevel = DetermineAlertLevel(reading.Temperature, reading.Humidity, reading.Status)
                            };

                            // Send to processed channel
                            await processedChannel.Writer.WriteAsync(processed);

                            processedCount++;
                            if (processedCount % LOG_INTERVAL == 0)
                            {
                                Console.WriteLine($"Transformer {transformerId}: Processed {processedCount:N0} readings");
                            }
                        }

                        Console.WriteLine($"Transformer {transformerId}: Completed processing {processedCount:N0} readings");

                        // Complete if this is the last transformer
                        if (Interlocked.Increment(ref _transformersCompleted) == transformerTasks.Length)
                        {
                            processedChannel.Writer.Complete();
                        }

                        return processedCount;
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine($"Transformer {transformerId} error: {ex.Message}");
                        processedChannel.Writer.Complete(ex);
                        throw;
                    }
                });
            }

            // Start a consumer task
            var consumerTask = Task.Run(async () =>
            {
                int consumedCount = 0;
                var alertCounts = new Dictionary<AlertLevel, int>();

                try
                {
                    await foreach (var processed in processedChannel.Reader.ReadAllAsync())
                    {
                        // Process the transformed reading
                        consumedCount++;

                        // Track alert levels
                        if (!alertCounts.ContainsKey(processed.AlertLevel))
                        {
                            alertCounts[processed.AlertLevel] = 0;
                        }
                        alertCounts[processed.AlertLevel]++;

                        if (consumedCount % LOG_INTERVAL == 0)
                        {
                            Console.WriteLine($"Consumer: Consumed {consumedCount:N0} processed readings");
                        }
                    }

                    // Report results
                    Console.WriteLine($"Consumer: Completed consuming {consumedCount:N0} processed readings");
                    Console.WriteLine("Alert level distribution:");
                    foreach (var alert in alertCounts.OrderBy(a => a.Key))
                    {
                        Console.WriteLine($"  {alert.Key}: {alert.Value:N0} readings ({alert.Value * 100.0 / consumedCount:F1}%)");
                    }

                    return consumedCount;
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"Consumer error: {ex.Message}");
                    throw;
                }
            });

            // Wait for all tasks to complete
            await readerTask;
            await Task.WhenAll(transformerTasks);
            await consumerTask;

            stopwatch.Stop();

            // Report overall results
            Console.WriteLine($"Data transformation test completed in {stopwatch.Elapsed.TotalSeconds:F2} seconds");
            Console.WriteLine($"Processing rate: {await readerTask / stopwatch.Elapsed.TotalSeconds:N0} rows/second");
        }

        /// <summary>
        /// Tests multiplexed output - routing data to different destinations based on content
        /// </summary>
        private static async Task TestMultiplexedOutput(string filePath)
        {
            Console.WriteLine("\n--- MULTIPLEXED OUTPUT TEST ---");
            var stopwatch = Stopwatch.StartNew();

            // Create channels for different output categories
            var normalChannel = Channel.CreateBounded<SensorReading>(STREAMING_BUFFER_SIZE);
            var warningChannel = Channel.CreateBounded<SensorReading>(STREAMING_BUFFER_SIZE);
            var errorChannel = Channel.CreateBounded<SensorReading>(STREAMING_BUFFER_SIZE);

            // Start reader task
            var readerTask = Task.Run(async () =>
            {
                int normalCount = 0;
                int warningCount = 0;
                int errorCount = 0;

                try
                {
                    using var reader = await ParquetDataReaderFactory.CreateAsync(filePath);

                    // Get column indexes for efficient access
                    int idCol = reader.GetOrdinal("ID");
                    int timestampCol = reader.GetOrdinal("Timestamp");
                    int sensorIdCol = reader.GetOrdinal("SensorID");
                    int temperatureCol = reader.GetOrdinal("Temperature");
                    int humidityCol = reader.GetOrdinal("Humidity");
                    int pressureCol = reader.GetOrdinal("Pressure");
                    int statusCol = reader.GetOrdinal("Status");

                    // Read all rows and route to appropriate channel based on status
                    while (await reader.ReadAsync())
                    {
                        var reading = new SensorReading
                        {
                            ID = reader.GetInt32(idCol),
                            Timestamp = reader.GetDateTime(timestampCol),
                            SensorID = reader.GetString(sensorIdCol),
                            Temperature = reader.GetDouble(temperatureCol),
                            Humidity = reader.GetDouble(humidityCol),
                            Pressure = reader.GetDouble(pressureCol),
                            Status = reader.GetString(statusCol)
                        };

                        // Route based on status
                        switch (reading.Status.ToLower())
                        {
                            case "normal":
                                await normalChannel.Writer.WriteAsync(reading);
                                normalCount++;
                                break;

                            case "warning":
                                await warningChannel.Writer.WriteAsync(reading);
                                warningCount++;
                                break;

                            case "error":
                            case "critical":
                            case "offline":
                                await errorChannel.Writer.WriteAsync(reading);
                                errorCount++;
                                break;
                        }

                        // Log progress
                        int totalRows = normalCount + warningCount + errorCount;
                        if (totalRows % LOG_INTERVAL == 0)
                        {
                            Console.WriteLine($"Reader: Processed {totalRows:N0} rows");
                        }
                    }

                    // Signal completion for all channels
                    normalChannel.Writer.Complete();
                    warningChannel.Writer.Complete();
                    errorChannel.Writer.Complete();

                    Console.WriteLine($"Reader: Completed with {normalCount:N0} normal, {warningCount:N0} warnings, {errorCount:N0} errors");

                    return (normalCount, warningCount, errorCount);
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"Reader error: {ex.Message}");

                    // Complete all channels with error
                    normalChannel.Writer.Complete(ex);
                    warningChannel.Writer.Complete(ex);
                    errorChannel.Writer.Complete(ex);

                    throw;
                }
            });

            // Start processor tasks for each channel
            var normalTask = ProcessChannel(normalChannel, "Normal");
            var warningTask = ProcessChannel(warningChannel, "Warning");
            var errorTask = ProcessChannel(errorChannel, "Error");

            // Wait for completion
            var (normalCount, warningCount, errorCount) = await readerTask;
            await Task.WhenAll(normalTask, warningTask, errorTask);

            stopwatch.Stop();

            // Report results
            Console.WriteLine($"Multiplexed output test completed in {stopwatch.Elapsed.TotalSeconds:F2} seconds");
            Console.WriteLine($"Normal readings: {normalCount:N0}");
            Console.WriteLine($"Warning readings: {warningCount:N0}");
            Console.WriteLine($"Error readings: {errorCount:N0}");

            async Task<int> ProcessChannel(Channel<SensorReading> channel, string name)
            {
                int processedCount = 0;
                double tempSum = 0;
                double humiditySum = 0;

                try
                {
                    await foreach (var reading in channel.Reader.ReadAllAsync())
                    {
                        // Process the reading
                        processedCount++;
                        tempSum += reading.Temperature;
                        humiditySum += reading.Humidity;

                        // Log progress
                        if (processedCount % LOG_INTERVAL == 0)
                        {
                            Console.WriteLine($"{name} processor: Processed {processedCount:N0} readings");
                        }
                    }

                    // Report results
                    if (processedCount > 0)
                    {
                        Console.WriteLine($"{name} processor: Completed with {processedCount:N0} readings");
                        Console.WriteLine($"  Avg Temperature: {tempSum / processedCount:F2}°C");
                        Console.WriteLine($"  Avg Humidity: {humiditySum / processedCount:F2}%");
                    }
                    else
                    {
                        Console.WriteLine($"{name} processor: No readings to process");
                    }

                    return processedCount;
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"{name} processor error: {ex.Message}");
                    throw;
                }
            }
        }

        /// <summary>
        /// Tests streaming to simulated network endpoint (memory stream in this case)
        /// </summary>
        private static async Task TestStreamingToNetwork(string filePath)
        {
            Console.WriteLine("\n--- NETWORK STREAMING TEST ---");
            var stopwatch = Stopwatch.StartNew();

            // Create a memory stream to simulate network connection
            using var networkStream = new MemoryStream();

            // Start producer task
            var producerTask = Task.Run(async () =>
            {
                int rowCount = 0;

                try
                {
                    using var reader = await ParquetDataReaderFactory.CreateAsync(filePath);
                    using var writer = new StreamWriter(networkStream, Encoding.UTF8, 8192, true);

                    // Write CSV header
                    await writer.WriteLineAsync("ID,Timestamp,SensorID,Temperature,Humidity,Pressure,Status");

                    // Get column indexes for efficient access
                    int idCol = reader.GetOrdinal("ID");
                    int timestampCol = reader.GetOrdinal("Timestamp");
                    int sensorIdCol = reader.GetOrdinal("SensorID");
                    int temperatureCol = reader.GetOrdinal("Temperature");
                    int humidityCol = reader.GetOrdinal("Humidity");
                    int pressureCol = reader.GetOrdinal("Pressure");
                    int statusCol = reader.GetOrdinal("Status");

                    // Read and stream data as CSV
                    while (await reader.ReadAsync())
                    {
                        // Format as CSV line
                        string line = string.Format("{0},{1},{2},{3},{4},{5},{6}",
                            reader.GetInt32(idCol),
                            reader.GetDateTime(timestampCol).ToString("yyyy-MM-dd HH:mm:ss"),
                            reader.GetString(sensorIdCol),
                            reader.GetDouble(temperatureCol).ToString("F2"),
                            reader.GetDouble(humidityCol).ToString("F2"),
                            reader.GetDouble(pressureCol).ToString("F2"),
                            reader.GetString(statusCol));

                        // Write to network stream
                        await writer.WriteLineAsync(line);

                        // Flush periodically to simulate network transmission
                        rowCount++;
                        if (rowCount % 10000 == 0)
                        {
                            await writer.FlushAsync();
                        }

                        if (rowCount % LOG_INTERVAL == 0)
                        {
                            Console.WriteLine($"Network producer: Streamed {rowCount:N0} rows");
                        }
                    }

                    // Final flush
                    await writer.FlushAsync();

                    Console.WriteLine($"Network producer: Completed streaming {rowCount:N0} rows");
                    return rowCount;
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"Network producer error: {ex.Message}");
                    throw;
                }
            });

            // Wait for completion
            int rowsStreamed = await producerTask;

            stopwatch.Stop();

            // Report results
            Console.WriteLine($"Network streaming test completed in {stopwatch.Elapsed.TotalSeconds:F2} seconds");
            Console.WriteLine($"Streamed {rowsStreamed:N0} rows to network");
            Console.WriteLine($"Network data size: {networkStream.Length / (1024.0 * 1024.0):F2} MB");
            Console.WriteLine($"Average bytes per row: {networkStream.Length / (double)rowsStreamed:F2}");
            Console.WriteLine($"Streaming rate: {rowsStreamed / stopwatch.Elapsed.TotalSeconds:N0} rows/second");
        }

        /// <summary>
        /// Tests backpressure handling in streaming pipeline
        /// </summary>
        private static async Task TestBackpressureHandling(string filePath)
        {
            Console.WriteLine("\n--- BACKPRESSURE HANDLING TEST ---");
            var stopwatch = Stopwatch.StartNew();

            // Create a small buffer to force backpressure
            var options = new BoundedChannelOptions(100) // Very small buffer to force backpressure
            {
                FullMode = BoundedChannelFullMode.Wait
            };

            var channel = Channel.CreateBounded<SensorReading>(options);

            // Start fast producer
            var producerTask = Task.Run(async () =>
            {
                int rowCount = 0;
                var producerStopwatch = Stopwatch.StartNew();

                try
                {
                    using var reader = await ParquetDataReaderFactory.CreateAsync(filePath);

                    // Get column indexes for efficient access
                    int idCol = reader.GetOrdinal("ID");
                    int timestampCol = reader.GetOrdinal("Timestamp");
                    int sensorIdCol = reader.GetOrdinal("SensorID");
                    int temperatureCol = reader.GetOrdinal("Temperature");
                    int humidityCol = reader.GetOrdinal("Humidity");
                    int pressureCol = reader.GetOrdinal("Pressure");
                    int statusCol = reader.GetOrdinal("Status");

                    // Time tracking
                    var lastLogTime = DateTime.Now;
                    int rowsSinceLastLog = 0;

                    // Read all rows at maximum speed
                    while (await reader.ReadAsync())
                    {
                        var reading = new SensorReading
                        {
                            ID = reader.GetInt32(idCol),
                            Timestamp = reader.GetDateTime(timestampCol),
                            SensorID = reader.GetString(sensorIdCol),
                            Temperature = reader.GetDouble(temperatureCol),
                            Humidity = reader.GetDouble(humidityCol),
                            Pressure = reader.GetDouble(pressureCol),
                            Status = reader.GetString(statusCol)
                        };

                        // Try to write to channel - this will block when channel is full
                        await channel.Writer.WriteAsync(reading);

                        rowCount++;
                        rowsSinceLastLog++;

                        // Log progress with rate information
                        if (rowCount % 50000 == 0)
                        {
                            var now = DateTime.Now;
                            var elapsed = now - lastLogTime;
                            var rate = rowsSinceLastLog / elapsed.TotalSeconds;

                            Console.WriteLine($"Producer: Generated {rowCount:N0} rows, current rate: {rate:N0} rows/second");

                            lastLogTime = now;
                            rowsSinceLastLog = 0;
                        }
                    }

                    // Signal completion
                    channel.Writer.Complete();

                    producerStopwatch.Stop();
                    Console.WriteLine($"Producer: Completed generating {rowCount:N0} rows in {producerStopwatch.Elapsed.TotalSeconds:F2} seconds");
                    Console.WriteLine($"Producer average rate: {rowCount / producerStopwatch.Elapsed.TotalSeconds:N0} rows/second");

                    return rowCount;
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"Producer error: {ex.Message}");
                    channel.Writer.Complete(ex);
                    throw;
                }
            });

            // Start slow consumer
            var consumerTask = Task.Run(async () =>
            {
                int rowCount = 0;
                var consumerStopwatch = Stopwatch.StartNew();

                try
                {
                    // Time tracking
                    var lastLogTime = DateTime.Now;
                    int rowsSinceLastLog = 0;

                    await foreach (var reading in channel.Reader.ReadAllAsync())
                    {
                        // Process reading (simulate slow consumer)
                        rowCount++;
                        rowsSinceLastLog++;

                        // Simulate processing time
                        if (rowCount % 10 == 0) // Every 10th item has a delay
                        {
                            await Task.Delay(1); // Small delay that adds up
                        }

                        // Log progress with rate information
                        if (rowCount % 50000 == 0)
                        {
                            var now = DateTime.Now;
                            var elapsed = now - lastLogTime;
                            var rate = rowsSinceLastLog / elapsed.TotalSeconds;

                            Console.WriteLine($"Consumer: Processed {rowCount:N0} rows, current rate: {rate:N0} rows/second");

                            lastLogTime = now;
                            rowsSinceLastLog = 0;
                        }
                    }

                    consumerStopwatch.Stop();
                    Console.WriteLine($"Consumer: Completed processing {rowCount:N0} rows in {consumerStopwatch.Elapsed.TotalSeconds:F2} seconds");
                    Console.WriteLine($"Consumer average rate: {rowCount / consumerStopwatch.Elapsed.TotalSeconds:N0} rows/second");

                    return rowCount;
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"Consumer error: {ex.Message}");
                    throw;
                }
            });

            // Wait for both tasks
            await Task.WhenAll(producerTask, consumerTask);

            stopwatch.Stop();

            // Report results
            Console.WriteLine($"Backpressure test completed in {stopwatch.Elapsed.TotalSeconds:F2} seconds");
            Console.WriteLine($"Overall throughput: {await producerTask / stopwatch.Elapsed.TotalSeconds:N0} rows/second");
        }

        #region Helper Methods and Classes

        // Static field for tracking transformer completion
        private static int _transformersCompleted = 0;

        /// <summary>
        /// Sensor reading class for raw data
        /// </summary>
        private class SensorReading
        {
            public int ID { get; set; }
            public DateTime Timestamp { get; set; }
            public string SensorID { get; set; }
            public double Temperature { get; set; }
            public double Humidity { get; set; }
            public double Pressure { get; set; }
            public string Status { get; set; }
        }

        /// <summary>
        /// Processed reading after transformation
        /// </summary>
        private class ProcessedReading
        {
            public int ID { get; set; }
            public DateTime Timestamp { get; set; }
            public string SensorID { get; set; }
            public double TemperatureF { get; set; }
            public double HeatIndex { get; set; }
            public double DewPoint { get; set; }
            public double PressureInHg { get; set; }
            public AlertLevel AlertLevel { get; set; }
        }

        /// <summary>
        /// Alert levels for sensor readings
        /// </summary>
        private enum AlertLevel
        {
            Normal,
            Advisory,
            Watch,
            Warning,
            Emergency
        }

        /// <summary>
        /// Calculate heat index from temperature and humidity
        /// </summary>
        private static double CalculateHeatIndex(double tempC, double humidity)
        {
            // Convert to Fahrenheit for the standard heat index formula
            double tempF = tempC * 9 / 5 + 32;

            // Simple heat index calculation
            double hi = 0.5 * (tempF + 61.0 + ((tempF - 68.0) * 1.2) + (humidity * 0.094));

            // More precise if temperature is high enough
            if (hi > 80)
            {
                hi = -42.379 + 2.04901523 * tempF + 10.14333127 * humidity
                    - 0.22475541 * tempF * humidity - 6.83783e-3 * tempF * tempF
                    - 5.481717e-2 * humidity * humidity + 1.22874e-3 * tempF * tempF * humidity
                    + 8.5282e-4 * tempF * humidity * humidity - 1.99e-6 * tempF * tempF * humidity * humidity;
            }

            // Convert back to Celsius
            return (hi - 32) * 5 / 9;
        }

        /// <summary>
        /// Calculate dew point from temperature and humidity
        /// </summary>
        private static double CalculateDewPoint(double tempC, double humidity)
        {
            // Constants for Magnus-Tetens formula
            const double a = 17.27;
            const double b = 237.7;

            // Calculate gamma parameter
            double gamma = (a * tempC) / (b + tempC) + Math.Log(humidity / 100.0);

            // Calculate dew point
            return b * gamma / (a - gamma);
        }

        /// <summary>
        /// Determine alert level from temperature, humidity, and status
        /// </summary>
        private static AlertLevel DetermineAlertLevel(double temperature, double humidity, string status)
        {
            // First, check status
            switch (status.ToLower())
            {
                case "critical":
                    return AlertLevel.Emergency;
                case "error":
                    return AlertLevel.Warning;
                case "warning":
                    return AlertLevel.Watch;
                case "offline":
                    return AlertLevel.Warning;
            }

            // Then check environmental conditions
            double heatIndex = CalculateHeatIndex(temperature, humidity);

            if (heatIndex > 40) return AlertLevel.Emergency;
            if (heatIndex > 35) return AlertLevel.Warning;
            if (heatIndex > 30) return AlertLevel.Watch;
            if (heatIndex > 27) return AlertLevel.Advisory;

            return AlertLevel.Normal;
        }

        /// <summary>
        /// Helper class for reading from a ReadOnlySequence in a stream-like way
        /// </summary>
        /// <summary>
        /// Helper class for reading from a ReadOnlySequence in a stream-like way
        /// </summary>
        private class ReadOnlySequenceStream : Stream
        {
            private ReadOnlySequence<byte> _sequence;
            private SequencePosition _position;
            private long _positionFromStart;

            public ReadOnlySequenceStream(ReadOnlySequence<byte> sequence)
            {
                _sequence = sequence;
                _position = sequence.Start;
                _positionFromStart = 0;
            }

            public override bool CanRead => true;
            public override bool CanSeek => false;
            public override bool CanWrite => false;
            public override long Length => _sequence.Length;
            public override long Position
            {
                get => _positionFromStart;
                set => throw new NotSupportedException();
            }

            public override void Flush() { }

            public override int Read(byte[] buffer, int offset, int count)
            {
                var remaining = _sequence.Slice(_position);
                if (remaining.Length == 0)
                    return 0;

                var toCopy = (int)Math.Min(count, remaining.Length);
                var firstSegment = remaining.First;

                if (firstSegment.Length >= toCopy)
                {
                    // Copy the bytes from the ReadOnlyMemory to the array
                    firstSegment.Slice(0, toCopy).Span.CopyTo(buffer.AsSpan(offset, toCopy));
                    _position = _sequence.GetPosition(toCopy, _position);
                }
                else
                {
                    // Need to copy from multiple segments
                    int copied = 0;
                    foreach (var segment in remaining)
                    {
                        if (copied == toCopy)
                            break;

                        var bytesToCopy = Math.Min(segment.Length, toCopy - copied);
                        // Copy the bytes from the ReadOnlyMemory to the array
                        segment.Slice(0, bytesToCopy).Span.CopyTo(buffer.AsSpan(offset + copied, (int)bytesToCopy));
                        copied += (int)bytesToCopy;
                    }

                    _position = _sequence.GetPosition(toCopy, _position);
                }

                _positionFromStart += toCopy;
                return toCopy;
            }

            public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();
            public override void SetLength(long value) => throw new NotSupportedException();
            public override void Write(byte[] buffer, int offset, int count) => throw new NotSupportedException();
        }

        #endregion
    }
}
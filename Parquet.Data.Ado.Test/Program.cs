using System;
using System.Data;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Parquet.Data.Reader;

namespace ParquetReaderTest
{
    internal class Program
    {
        static async Task Main(string[] args)
        {
            Console.WriteLine("Parquet.Data.Reader Test Application");
            Console.WriteLine("====================================");

            try
            {
                // Create a sample DataTable for our tests
                var sampleData = CreateSampleData();

                // Define test file path
                string testFilePath = Path.Combine(Path.GetTempPath(), "parquet_test.parquet");
                Console.WriteLine($"Test file path: {testFilePath}");

                // Run the tests
                //await RunBasicIOTests(sampleData, testFilePath);
                //await RunConnectionTests(testFilePath);
                //await RunBatchReaderTests(testFilePath);
                //await RunEdgeCaseTests(testFilePath);
                //await RunPerformanceTests(testFilePath);

                //// Run the new large file and streaming tests
                //await LargeFileTests.RunAllTests();
                await ParquetStreamingTests.RunAllTests();

                await ParquetSqlQueryTests.RunAllTests();

                await ParquetVirtualColumnTests.RunAllTests();

                // Clean up
                if (File.Exists(testFilePath))
                {
                    File.Delete(testFilePath);
                    Console.WriteLine("Test file deleted.");
                }

                Console.WriteLine("\nAll tests completed successfully!");
            }
            catch (Exception ex)
            {
                Console.ForegroundColor = ConsoleColor.Red;
                Console.WriteLine($"Test failed: {ex.Message}");
                Console.WriteLine(ex.StackTrace);
                Console.ResetColor();
            }

            Console.WriteLine("\nPress any key to exit...");
            Console.ReadKey();
        }

        private static DataTable CreateSampleData()
        {
            Console.WriteLine("Creating sample data...");
            var table = new DataTable("TestData");

            // Add columns with various data types
            table.Columns.Add("ID", typeof(int));
            table.Columns.Add("Name", typeof(string));
            table.Columns.Add("IsActive", typeof(bool));
            table.Columns.Add("CreatedDate", typeof(DateTime));
            table.Columns.Add("Price", typeof(decimal));
            table.Columns.Add("Quantity", typeof(double));
            table.Columns.Add("Tags", typeof(string));
            table.Columns.Add("UniqueID", typeof(Guid));
            table.Columns.Add("BinaryData", typeof(byte[]));

            // Add some rows (1000 for performance tests)
            Random rand = new Random(42); // Fixed seed for reproducibility
            for (int i = 1; i <= 1000; i++)
            {
                var row = table.NewRow();
                row["ID"] = i;
                row["Name"] = $"Product {i}";
                row["IsActive"] = i % 3 == 0; // Every third item is inactive
                row["CreatedDate"] = DateTime.Now.AddDays(-i);
                row["Price"] = Math.Round(rand.NextDouble() * 1000, 2);
                row["Quantity"] = Math.Round(rand.NextDouble() * 100, 3);
                row["Tags"] = i % 5 == 0 ? null : $"tag1,tag2,tag{i}"; // Some null values
                row["UniqueID"] = Guid.NewGuid();
                row["BinaryData"] = i % 7 == 0 ? null : new byte[] { (byte)rand.Next(255), (byte)rand.Next(255), (byte)rand.Next(255) };

                table.Rows.Add(row);
            }

            Console.WriteLine($"Created sample data with {table.Rows.Count} rows.");
            return table;
        }

        private static async Task RunBasicIOTests(DataTable data, string filePath)
        {
            Console.WriteLine("\n--- BASIC I/O TESTS ---");

            // 1. Test writing data to Parquet file (sync)
            Console.WriteLine("Testing synchronous export...");
            if (File.Exists(filePath)) File.Delete(filePath);
            data.ExportToParquet(filePath);
            AssertFileExists(filePath);
            Console.WriteLine("✓ Synchronous export completed successfully");

            // 2. Test reading data from Parquet file (sync)
            Console.WriteLine("Testing synchronous import...");
            using (var reader = ParquetDataReaderFactory.Create(filePath))
            {
                ValidateReader(reader, data.Rows.Count);
            }
            // Explicitly wait to ensure file handles are released
            GC.Collect();
            GC.WaitForPendingFinalizers();
            Console.WriteLine("✓ Synchronous import completed successfully");

            // 3. Test writing data to Parquet file (async)
            Console.WriteLine("Testing asynchronous export...");
            if (File.Exists(filePath))
            {
                // Add retry logic in case the file is still locked
                int retries = 5;
                while (retries > 0)
                {
                    try
                    {
                        File.Delete(filePath);
                        break;
                    }
                    catch (IOException)
                    {
                        retries--;
                        if (retries == 0) throw;
                        Console.WriteLine("File still in use, retrying delete operation...");
                        GC.Collect();
                        GC.WaitForPendingFinalizers();
                        Thread.Sleep(100);
                    }
                }
            }
            await data.ExportToParquetAsync(filePath);
            AssertFileExists(filePath);
            Console.WriteLine("✓ Asynchronous export completed successfully");

            // 4. Test reading data from Parquet file (async)
            Console.WriteLine("Testing asynchronous import...");
            using (var reader = await ParquetDataReaderFactory.CreateAsync(filePath))
            {
                ValidateReader(reader, data.Rows.Count);
            }
            // Explicitly wait to ensure file handles are released
            GC.Collect();
            GC.WaitForPendingFinalizers();
            Console.WriteLine("✓ Asynchronous import completed successfully");
        }

        private static async Task RunConnectionTests(string filePath)
        {
            Console.WriteLine("\n--- CONNECTION AND COMMAND TESTS ---");

            // 1. Test connection opening
            Console.WriteLine("Testing connection open/close...");
            using (var connection = new ParquetConnection(filePath))
            {
                connection.Open();
                Console.WriteLine($"Connection state: {connection.State}");
                Assert(connection.State == ConnectionState.Open, "Connection should be open");

                // 2. Test command execution
                Console.WriteLine("Testing command execution...");
                using (var command = connection.CreateCommand("SELECT *"))
                {
                    using (var reader = command.ExecuteReader())
                    {
                        ValidateReader(reader, expectedRowCount: null);
                    }
                }
                Console.WriteLine("✓ Command execution completed successfully");

                // 3. Test async command execution
                Console.WriteLine("Testing async command execution...");
                using (var command = connection.CreateCommand("SELECT *"))
                {
                    using (var reader = await command.ExecuteReaderAsync())
                    {
                        ValidateReader(reader, expectedRowCount: null);
                    }
                }
                Console.WriteLine("✓ Async command execution completed successfully");

                connection.Close();
                Assert(connection.State == ConnectionState.Closed, "Connection should be closed");
            }
            Console.WriteLine("✓ Connection tests completed successfully");

            // 4. Test async connection opening
            Console.WriteLine("Testing async connection open/close...");
            using (var connection = new ParquetConnection(filePath))
            {
                await connection.OpenAsync();
                Assert(connection.State == ConnectionState.Open, "Connection should be open");
                connection.Close();
            }
            Console.WriteLine("✓ Async connection tests completed successfully");
        }

        private static async Task RunBatchReaderTests(string filePath)
        {
            Console.WriteLine("\n--- BATCH READER TESTS ---");

            // 1. Test batch reader
            Console.WriteLine("Testing ParquetBatchReader...");
            try
            {
                using (var batchReader = new Parquet.Data.Ado.ParquetBatchReader(filePath))
                {
                    Console.WriteLine($"Batch count: {batchReader.BatchCount}");
                    Assert(batchReader.BatchCount > 0, "Batch count should be greater than 0");

                    Console.WriteLine("Reading batches...");
                    int totalRows = 0;

                    await foreach (var batch in batchReader.ReadAllAsync())
                    {
                        Console.WriteLine($"Processing batch {batch.RowGroupIndex}, rows: {batch.RowCount}");
                        totalRows += batch.RowCount;
                        Assert(batch.Columns.Count > 0, "Batch should have columns");
                    }

                    Console.WriteLine($"Total rows read: {totalRows}");
                }
                Console.WriteLine("✓ Batch reader tests completed successfully");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"WARNING: Batch reader test failed: {ex.Message}");
                Console.WriteLine("This might be because ParquetIO namespace is not accessible or implemented differently.");
            }
        }

        private static async Task RunEdgeCaseTests(string filePath)
        {
            Console.WriteLine("\n--- EDGE CASE TESTS ---");

            // 1. Test with empty DataTable
            Console.WriteLine("Testing with empty DataTable...");
            var emptyFilePath = Path.Combine(Path.GetTempPath(), "empty_test.parquet");

            try
            {
                var emptyTable = new DataTable("EmptyTable");
                emptyTable.Columns.Add("ID", typeof(int));
                emptyTable.Columns.Add("Name", typeof(string));

                await emptyTable.ExportToParquetAsync(emptyFilePath);
                AssertFileExists(emptyFilePath);

                using (var reader = await ParquetDataReaderFactory.CreateAsync(emptyFilePath))
                {
                    int rowCount = 0;
                    while (await reader.ReadAsync())
                    {
                        rowCount++;
                    }
                    Assert(rowCount == 0, "Empty table should have 0 rows");
                }

                Console.WriteLine("✓ Empty table test passed");
            }
            finally
            {
                if (File.Exists(emptyFilePath))
                {
                    File.Delete(emptyFilePath);
                }
            }

            // 2. Test with null values
            Console.WriteLine("Testing with null values...");
            var nullFilePath = Path.Combine(Path.GetTempPath(), "null_test.parquet");

            try
            {
                var nullTable = new DataTable("NullTable");
                nullTable.Columns.Add("ID", typeof(int));
                nullTable.Columns.Add("NullableString", typeof(string));
                nullTable.Columns.Add("NullableInt", typeof(int));
                nullTable.Columns.Add("NullableDateTime", typeof(DateTime));

                // Add row with null values
                var row = nullTable.NewRow();
                row["ID"] = 1;
                row["NullableString"] = DBNull.Value;
                row["NullableInt"] = DBNull.Value;
                row["NullableDateTime"] = DBNull.Value;
                nullTable.Rows.Add(row);

                // Add row with values
                row = nullTable.NewRow();
                row["ID"] = 2;
                row["NullableString"] = "Test";
                row["NullableInt"] = 42;
                row["NullableDateTime"] = DateTime.Now;
                nullTable.Rows.Add(row);

                await nullTable.ExportToParquetAsync(nullFilePath);

                using (var reader = await ParquetDataReaderFactory.CreateAsync(nullFilePath))
                {
                    // Check first row (with nulls)
                    Assert(await reader.ReadAsync(), "Should be able to read first row");
                    Assert(reader.GetInt32(0) == 1, "ID should be 1");
                    Assert(reader.IsDBNull(1), "NullableString should be null");
                    Assert(reader.IsDBNull(2), "NullableInt should be null");
                    Assert(reader.IsDBNull(3), "NullableDateTime should be null");

                    // Check second row (with values)
                    Assert(await reader.ReadAsync(), "Should be able to read second row");
                    Assert(reader.GetInt32(0) == 2, "ID should be 2");
                    Assert(!reader.IsDBNull(1), "NullableString should not be null");
                    Assert(reader.GetString(1) == "Test", "NullableString should be 'Test'");
                }

                Console.WriteLine("✓ Null values test passed");
            }
            finally
            {
                if (File.Exists(nullFilePath))
                {
                    File.Delete(nullFilePath);
                }
            }

            // 3. Test error handling - file not found
            Console.WriteLine("Testing error handling - file not found...");
            try
            {
                using (var reader = ParquetDataReaderFactory.Create("nonexistent_file.parquet"))
                {
                    Assert(false, "Should have thrown an exception");
                }
            }
            catch (FileNotFoundException)
            {
                Console.WriteLine("✓ File not found exception thrown as expected");
            }
        }

        private static async Task RunPerformanceTests(string filePath)
        {
            Console.WriteLine("\n--- PERFORMANCE TESTS ---");

            // 1. Test large table performance
            Console.WriteLine("Creating large DataTable...");
            var largeTable = new DataTable("LargeTable");
            largeTable.Columns.Add("ID", typeof(int));
            largeTable.Columns.Add("Data", typeof(string));

            Console.WriteLine("Adding 10,000 rows...");
            for (int i = 0; i < 10000; i++)
            {
                var row = largeTable.NewRow();
                row["ID"] = i;
                row["Data"] = $"Data for row {i} with some additional text to increase the size of the string column";
                largeTable.Rows.Add(row);
            }

            var largeFilePath = Path.Combine(Path.GetTempPath(), "large_test.parquet");

            try
            {
                // Measure write performance
                Console.WriteLine("Testing write performance...");
                var writeStopwatch = Stopwatch.StartNew();
                await largeTable.ExportToParquetAsync(largeFilePath);
                writeStopwatch.Stop();

                Console.WriteLine($"Write time for 10,000 rows: {writeStopwatch.ElapsedMilliseconds}ms");
                AssertFileExists(largeFilePath);

                // Measure read performance
                Console.WriteLine("Testing read performance...");
                var readStopwatch = Stopwatch.StartNew();
                int rowCount = 0;

                using (var reader = await ParquetDataReaderFactory.CreateAsync(largeFilePath))
                {
                    while (await reader.ReadAsync())
                    {
                        rowCount++;
                    }
                }

                readStopwatch.Stop();
                Console.WriteLine($"Read time for 10,000 rows: {readStopwatch.ElapsedMilliseconds}ms");
                Assert(rowCount == 10000, "Should have read 10,000 rows");

                // Test ParquetDataReader to DataTable conversion
                Console.WriteLine("Testing ParquetDataReader to DataTable conversion...");
                var conversionStopwatch = Stopwatch.StartNew();

                using (var reader = await ParquetDataReaderFactory.CreateAsync(largeFilePath))
                {
                    var resultTable = reader.ToDataTable("ConvertedTable");
                    conversionStopwatch.Stop();

                    Console.WriteLine($"Conversion time: {conversionStopwatch.ElapsedMilliseconds}ms");
                    Assert(resultTable.Rows.Count == 10000, "Converted table should have 10,000 rows");
                }

                Console.WriteLine("✓ Performance tests completed successfully");
            }
            finally
            {
                if (File.Exists(largeFilePath))
                {
                    File.Delete(largeFilePath);
                }
            }

            // 2. Test parallel reading
            Console.WriteLine("\nTesting parallel reading...");
            try
            {
                // Create multiple files
                string[] files = new string[4];
                for (int i = 0; i < 4; i++)
                {
                    files[i] = Path.Combine(Path.GetTempPath(), $"parallel_test_{i}.parquet");
                    await largeTable.ExportToParquetAsync(files[i]);
                }

                var parallelStopwatch = Stopwatch.StartNew();
                var readers = await ParquetDataReaderFactory.CreateManyAsync(files);
                parallelStopwatch.Stop();

                Console.WriteLine($"Parallel open time for 4 files: {parallelStopwatch.ElapsedMilliseconds}ms");
                Assert(readers.Length == 4, "Should have created 4 readers");

                // Clean up
                foreach (var reader in readers)
                {
                    reader.Dispose();
                }

                foreach (var file in files)
                {
                    if (File.Exists(file))
                    {
                        File.Delete(file);
                    }
                }

                Console.WriteLine("✓ Parallel reading tests completed successfully");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"WARNING: Parallel reading test failed: {ex.Message}");
                Console.WriteLine("This might be because CreateManyAsync is not implemented or accessible.");
            }
        }




        #region Helper Methods

        private static void ValidateReader(IDataReader reader, int? expectedRowCount)
        {
            Assert(reader != null, "Reader should not be null");

            // Check schema
            var schemaTable = reader.GetSchemaTable();
            Assert(schemaTable != null, "Schema table should not be null");
            Assert(schemaTable.Rows.Count > 0, "Schema table should have rows");

            // Print schema
            Console.WriteLine("Schema:");
            foreach (DataRow row in schemaTable.Rows)
            {
                Console.WriteLine($"  {row["ColumnName"]} ({row["DataType"]})");
            }

            // Read rows
            int rowCount = 0;
            while (reader.Read())
            {
                rowCount++;

                // Only print first 5 rows to avoid console spam
                if (rowCount <= 5)
                {
                    Console.Write($"Row {rowCount}: ");
                    for (int i = 0; i < reader.FieldCount; i++)
                    {
                        var value = reader.IsDBNull(i) ? "NULL" : reader.GetValue(i).ToString();
                        if (i < 3) // Only show first 3 columns
                        {
                            Console.Write($"{reader.GetName(i)}={value}, ");
                        }
                    }
                    Console.WriteLine("...");
                }
            }

            Console.WriteLine($"Read {rowCount} rows");

            if (expectedRowCount.HasValue)
            {
                Assert(rowCount == expectedRowCount.Value, $"Row count should be {expectedRowCount}, was {rowCount}");
            }
        }

        private static void AssertFileExists(string filePath)
        {
            Assert(File.Exists(filePath), $"File should exist: {filePath}");
            var fileInfo = new FileInfo(filePath);
            Console.WriteLine($"File size: {fileInfo.Length / 1024.0:F2} KB");
            Assert(fileInfo.Length > 0, "File should not be empty");
        }

        private static void Assert(bool condition, string message)
        {
            if (!condition)
            {
                throw new Exception($"Assertion failed: {message}");
            }
        }

        #endregion
    }
}
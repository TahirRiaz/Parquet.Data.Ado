using System;
using System.Data;
using System.IO;
using System.Threading.Tasks;
using System.Diagnostics;
using System.Linq;
using System.Collections.Generic;
using Parquet.Schema;
using Parquet;
using Parquet.Data;
using Parquet.Data.Ado;
using Parquet.Data.Ado;

namespace ParquetReaderTest
{
    /// <summary>
    /// Sample program to demonstrate metadata extraction capabilities with Parquet files.
    /// </summary>
    public class ParquetMetadataTests
    {
        public static async Task RunAllTests()
        {
            Console.WriteLine("\n--- PARQUET METADATA TESTS ---");

            try
            {
                // Create sample data
                var sampleData = CreateSampleData();
                string filePath = Path.Combine(Path.GetTempPath(), "metadata_test.parquet");

                try
                {
                    // Export sample data to Parquet with multiple row groups
                    Console.WriteLine("Creating test Parquet file with multiple row groups...");
                    await ExportToParquetWithMultipleRowGroups(sampleData, filePath, rowGroupSize: 3);
                    Console.WriteLine($"File created at: {filePath}");

                    // Test basic metadata extraction
                    await TestBasicMetadata(filePath);

                    // Test schema metadata
                    await TestSchemaMetadata(filePath);

                    // Test row group metadata
                    await TestRowGroupMetadata(filePath);

                    // Test metadata with different data types
                    await TestDataTypeMetadata(filePath);

                    // Test column statistics
                    await TestColumnMetadata(filePath);

                    // Test connection metadata methods
                    await TestConnectionMetadata(filePath);

                    // Test batch reader metadata
                    await TestBatchReaderMetadata(filePath);

                    // Test metadata performance with large file
                    await TestMetadataPerformance(filePath);

                    Console.WriteLine("✓ All metadata tests completed successfully!");
                }
                finally
                {
                    // Force garbage collection to release any remaining file handles
                    GC.Collect();
                    GC.WaitForPendingFinalizers();

                    if (System.IO.File.Exists(filePath))
                    {
                        try
                        {
                            System.IO.File.Delete(filePath);
                            Console.WriteLine($"Test file deleted: {filePath}");
                        }
                        catch (IOException ex)
                        {
                            Console.WriteLine($"WARNING: Could not delete test file: {ex.Message}");
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"ERROR: {ex.Message}");
                Console.WriteLine(ex.StackTrace);
            }
        }

        private static DataTable CreateSampleData()
        {
            var table = new DataTable("Products");

            // Define columns with different data types
            table.Columns.Add("ProductID", typeof(int));
            table.Columns.Add("ProductName", typeof(string));
            table.Columns.Add("Category", typeof(string));
            table.Columns.Add("Price", typeof(decimal));
            table.Columns.Add("InStock", typeof(bool));
            table.Columns.Add("LastUpdated", typeof(DateTime));
            table.Columns.Add("Rating", typeof(double));
            table.Columns.Add("Tags", typeof(string));
            table.Columns.Add("ImageData", typeof(byte[]));
            table.Columns.Add("SKU", typeof(Guid));

            // Add sample data
            byte[] sampleImageData = new byte[16] { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16 };

            table.Rows.Add(1, "Laptop", "Electronics", 999.99m, true, DateTime.Now.AddDays(-10), 4.5, "laptop,computer", sampleImageData, Guid.NewGuid());
            table.Rows.Add(2, "Smartphone", "Electronics", 699.99m, true, DateTime.Now.AddDays(-5), 4.7, "phone,mobile", sampleImageData, Guid.NewGuid());
            table.Rows.Add(3, "Headphones", "Electronics", 149.99m, true, DateTime.Now.AddDays(-15), 4.3, "audio", sampleImageData, Guid.NewGuid());
            table.Rows.Add(4, "Coffee Maker", "Appliances", 89.99m, false, DateTime.Now.AddDays(-30), 4.0, "kitchen", sampleImageData, Guid.NewGuid());
            table.Rows.Add(5, "Blender", "Appliances", 49.99m, true, DateTime.Now.AddDays(-20), 3.8, "kitchen", sampleImageData, Guid.NewGuid());
            table.Rows.Add(6, "Office Chair", "Furniture", 199.99m, false, DateTime.Now.AddDays(-45), 4.2, "office", sampleImageData, Guid.NewGuid());
            table.Rows.Add(7, "Desk", "Furniture", 299.99m, true, DateTime.Now.AddDays(-25), 4.4, "office", sampleImageData, Guid.NewGuid());
            table.Rows.Add(8, "Bookshelf", "Furniture", 149.99m, true, DateTime.Now.AddDays(-60), 4.1, "storage", sampleImageData, Guid.NewGuid());
            table.Rows.Add(9, "Monitor", "Electronics", 349.99m, false, DateTime.Now.AddDays(-7), 4.6, "computer,display", sampleImageData, Guid.NewGuid());
            table.Rows.Add(10, "Keyboard", "Electronics", 79.99m, true, DateTime.Now.AddDays(-12), 4.2, "computer,input", sampleImageData, Guid.NewGuid());

            Console.WriteLine($"Created sample data with {table.Rows.Count} rows and {table.Columns.Count} columns");
            return table;
        }

        private static async Task ExportToParquetWithMultipleRowGroups(DataTable dataTable, string filePath, int rowGroupSize)
        {
            ArgumentNullException.ThrowIfNull(dataTable);
            if (string.IsNullOrEmpty(filePath))
                throw new ArgumentException("File path cannot be null or empty", nameof(filePath));

            // Build schema from DataTable columns
            var dataFields = new List<DataField>();
            foreach (System.Data.DataColumn column in dataTable.Columns)
            {
                dataFields.Add(new DataField(column.ColumnName, column.DataType, isNullable: true));
            }
            var schema = new ParquetSchema(dataFields.ToArray());

            using var stream = new FileStream(filePath, FileMode.Create, FileAccess.Write);
            using var writer = await ParquetWriter.CreateAsync(schema, stream);

            // Write data in row groups of specified size
            for (int rowIndex = 0; rowIndex < dataTable.Rows.Count; rowIndex += rowGroupSize)
            {
                int rowsToWrite = Math.Min(rowGroupSize, dataTable.Rows.Count - rowIndex);
                using var groupWriter = writer.CreateRowGroup();

                // Process each column in the DataTable
                for (int colIndex = 0; colIndex < dataTable.Columns.Count; colIndex++)
                {
                    var dataColumn = dataTable.Columns[colIndex];
                    var values = new List<object>(rowsToWrite);

                    // Gather the batch of values for this column
                    for (int i = 0; i < rowsToWrite; i++)
                    {
                        object value = dataTable.Rows[rowIndex + i][colIndex];
                        values.Add(value == DBNull.Value ? null! : value);
                    }

                    // Convert to ParquetDataColumn
                    var dataField = (DataField)schema.Fields[colIndex];
                    Type elementType = dataField.ClrType.IsValueType ? typeof(Nullable<>).MakeGenericType(dataField.ClrType) : dataField.ClrType;
                    Array typedArray = Array.CreateInstance(elementType, values.Count);

                    for (int i = 0; i < values.Count; i++)
                    {
                        object value = values[i];
                        if (value == null)
                        {
                            typedArray.SetValue(null, i);
                        }
                        else
                        {
                            if (elementType != value.GetType() && elementType.IsGenericType)
                            {
                                var underlyingType = Nullable.GetUnderlyingType(elementType);
                                if (underlyingType != null && value.GetType() == underlyingType)
                                {
                                    // Handle converting value type to nullable value type
                                    var nullableType = typeof(Nullable<>).MakeGenericType(underlyingType);
                                    value = Activator.CreateInstance(nullableType, value)!;
                                }
                            }
                            typedArray.SetValue(value, i);
                        }
                    }

                    // Fixed: Use fully qualified name for Parquet.Data.DataColumn
                    var parquetColumn = new Parquet.Data.DataColumn(dataField, typedArray);
                    await groupWriter.WriteColumnAsync(parquetColumn);
                }
            }
        }

        private static async Task TestBasicMetadata(string filePath)
        {
            Console.WriteLine("\nTest: Basic Metadata Extraction");

            // Get metadata directly from file and ensure it's disposed
            using (var metadata = await ParquetMetadata.FromFileAsync(filePath))
            {
                Console.WriteLine($"File: {Path.GetFileName(filePath)}");
                Console.WriteLine($"Total Rows: {metadata.TotalRowCount}");
                Console.WriteLine($"Row Groups: {metadata.RowGroupCount}");
                Console.WriteLine($"Columns: {metadata.Schema.Fields.Count}");

                // Print row group information
                Console.WriteLine("\nRow Group Details:");
                for (int i = 0; i < metadata.RowGroups.Count; i++)
                {
                    var rowGroup = metadata.RowGroups[i];
                    Console.WriteLine($"  Row Group {rowGroup.Index}: {rowGroup.RowCount} rows");
                }
            } // ParquetMetadata is automatically disposed here, releasing file handles
        }

        private static async Task TestSchemaMetadata(string filePath)
        {
            Console.WriteLine("\nTest: Schema Metadata");

            // Get metadata from file
            using (var metadata = await ParquetMetadata.FromFileAsync(filePath))
            {
                // Extract and display schema information
                Console.WriteLine("Schema Fields:");
                foreach (var field in metadata.Schema.Fields)
                {
                    string nullableInfo = field.IsNullable ? "nullable" : "non-nullable";
                    // Fix: Use DataType instead of ClrType for Field objects
                    Console.WriteLine($"  {field.Name}: {field.SchemaType} ({nullableInfo})");
                }

                // Test schema compatibility with DataTable
                var dataTable = new DataTable("SchemaTest");
                foreach (var field in metadata.Schema.DataFields)
                {
                    // Create DataColumn from Parquet field
                    Type columnType = field.ClrType;
                    if (columnType.IsValueType && field.IsNullable)
                    {
                        // For nullable value types, use the underlying type
                        Type? underlyingType = Nullable.GetUnderlyingType(columnType);
                        if (underlyingType != null)
                        {
                            columnType = underlyingType;
                        }
                    }
                    dataTable.Columns.Add(field.Name, columnType);
                }

                Console.WriteLine($"\nGenerated DataTable schema with {dataTable.Columns.Count} columns");
                foreach (System.Data.DataColumn column in dataTable.Columns)
                {
                    Console.WriteLine($"  {column.ColumnName}: {column.DataType.Name}");
                }
            }
        }

        private static async Task TestRowGroupMetadata(string filePath)
        {
            Console.WriteLine("\nTest: Row Group Metadata");

            // Access row group details
            using (var connection = new ParquetConnection(filePath))
            {
                await connection.OpenAsync();
                var reader = connection.Reader;

                Console.WriteLine($"File has {reader.RowGroupCount} row groups");
            }

            // Use batch reader to process row groups
            using (var batchReader = new ParquetBatchReader(filePath))
            {
                Console.WriteLine($"BatchReader reports {batchReader.BatchCount} batches (row groups)");

                // Read batches and report statistics
                int batchIndex = 0;
                await foreach (var batch in batchReader.ReadAllAsync())
                {
                    Console.WriteLine($"  Batch {batch.RowGroupIndex}: {batch.RowCount} rows, {batch.Columns.Count} columns");

                    // Report the first row's product name in each batch
                    if (batch.RowCount > 0)
                    {
                        var nameColumn = batch.Columns.FirstOrDefault(c => c.Field.Name == "ProductName");
                        if (nameColumn != null && nameColumn.Data is Array data && data.Length > 0)
                        {
                            object? firstValue = data.GetValue(0);
                            Console.WriteLine($"    First product: {firstValue}");
                        }
                    }

                    batchIndex++;
                    if (batchIndex >= 3) break; // Limit output for brevity
                }
            }
        }

        private static async Task TestDataTypeMetadata(string filePath)
        {
            Console.WriteLine("\nTest: Data Type Metadata");

            // Get file metadata
            using (var metadata = await ParquetMetadata.FromFileAsync(filePath))
            {
                // Group columns by data type
                var columnsByType = new Dictionary<Type, List<string>>();
                foreach (var field in metadata.Schema.DataFields)
                {
                    if (!columnsByType.ContainsKey(field.ClrType))
                    {
                        columnsByType[field.ClrType] = new List<string>();
                    }
                    columnsByType[field.ClrType].Add(field.Name);
                }

                // Display columns grouped by type
                Console.WriteLine("Columns by data type:");
                foreach (var typeGroup in columnsByType)
                {
                    Console.WriteLine($"  {typeGroup.Key.Name}: {string.Join(", ", typeGroup.Value)}");
                }
            }

            // Test reading data of different types
            using (var reader = await ParquetDataReaderFactory.CreateAsync(filePath))
            {
                var dataTable = await reader.ToDataTableAsync();

                Console.WriteLine("\nRead data with appropriate CLR types:");
                foreach (System.Data.DataColumn column in dataTable.Columns)
                {
                    // Get first non-null value for type validation
                    object? firstValue = null;
                    Type? concreteType = null;

                    for (int i = 0; i < Math.Min(dataTable.Rows.Count, 3); i++)
                    {
                        object value = dataTable.Rows[i][column];
                        if (value != DBNull.Value)
                        {
                            firstValue = value;
                            concreteType = value.GetType();
                            break;
                        }
                    }

                    string exampleValue = firstValue == null ? "NULL" :
                        firstValue is byte[]? "<binary data>" :
                        firstValue.ToString();

                    Console.WriteLine($"  {column.ColumnName}: {column.DataType.Name}" +
                        (concreteType != null ? $" (concrete type: {concreteType.Name})" : "") +
                        $", example value: {exampleValue}");
                }
            }
        }

        private static async Task TestColumnMetadata(string filePath)
        {
            Console.WriteLine("\nTest: Column Metadata");

            // Get metadata
            using (var metadata = await ParquetMetadata.FromFileAsync(filePath))
            {
                Console.WriteLine("Column metadata:");
                foreach (var column in metadata.Columns)
                {
                    Console.WriteLine($"  {column.Name}: {column.DataType.Name}, " +
                        $"Nullable: {column.IsNullable}");
                }
            }

            // Get data to examine column contents
            using (var reader = await ParquetDataReaderFactory.CreateAsync(filePath))
            {
                var dataTable = await reader.ToDataTableAsync();

                // For each column, calculate basic statistics
                Console.WriteLine("\nColumn statistics:");
                foreach (System.Data.DataColumn column in dataTable.Columns)
                {
                    int nullCount = 0;
                    int totalCount = dataTable.Rows.Count;
                    HashSet<object> distinctValues = new HashSet<object>();

                    foreach (DataRow row in dataTable.Rows)
                    {
                        object value = row[column];
                        if (value == DBNull.Value)
                        {
                            nullCount++;
                        }
                        else
                        {
                            distinctValues.Add(value);
                        }
                    }

                    Console.WriteLine($"  {column.ColumnName}: {distinctValues.Count} distinct values, " +
                        $"{nullCount} nulls out of {totalCount} rows");
                }
            }
        }

        private static async Task TestConnectionMetadata(string filePath)
        {
            Console.WriteLine("\nTest: Connection Metadata Methods");

            using (var connection = new ParquetConnection(filePath))
            {
                await connection.OpenAsync();

                // Test connection metadata properties
                Console.WriteLine($"Connection Timeout: {connection.ConnectionTimeout}");
                Console.WriteLine($"Database: {connection.Database}");
                Console.WriteLine($"DataSource: {connection.DataSource}");
                Console.WriteLine($"ServerVersion: {connection.ServerVersion}");

                // Test ParquetConnectionExtensions methods
                int rowGroupCount = connection.GetRowGroupCount();
                long totalRowCount = connection.GetTotalRowCount();

                Console.WriteLine($"Row Group Count: {rowGroupCount}");
                Console.WriteLine($"Total Row Count: {totalRowCount}");

                // Get full metadata from connection
                using (var metadata = connection.GetMetadata())
                {
                    Console.WriteLine($"Columns via Metadata: {metadata.Schema.Fields.Count}");
                }
            }
        }

        private static async Task TestBatchReaderMetadata(string filePath)
        {
            Console.WriteLine("\nTest: Batch Reader Metadata");

            // Create a batch reader
            using (var batchReader = new ParquetBatchReader(filePath))
            {
                Console.WriteLine($"Schema has {batchReader.Schema.Fields.Count} fields");
                Console.WriteLine($"Batch (row group) count: {batchReader.BatchCount}");

                // Test batch enumeration with cancellation
                using (var cts = new CancellationTokenSource())
                {
                    Console.WriteLine("\nReading batches with explicit cancellation support:");
                    int batchCount = 0;

                    try
                    {
                        await foreach (var batch in batchReader.ReadAllAsync(cts.Token))
                        {
                            Console.WriteLine($"  Batch {batch.RowGroupIndex}: {batch.RowCount} rows");
                            batchCount++;

                            if (batchCount >= 2)
                            {
                                // Simulate early cancellation
                                Console.WriteLine("  Demonstrating early cancellation...");
                                cts.Cancel();
                            }
                        }
                    }
                    catch (OperationCanceledException)
                    {
                        Console.WriteLine("  Batch reading was canceled as expected");
                    }
                }
            }
        }

        private static async Task TestMetadataPerformance(string filePath)
        {
            Console.WriteLine("\nTest: Metadata Performance with Large File");

            // Create a larger dataset for performance testing
            var largeData = new DataTable("LargeData");
            largeData.Columns.Add("ID", typeof(int));
            largeData.Columns.Add("Name", typeof(string));
            largeData.Columns.Add("Value", typeof(double));
            largeData.Columns.Add("Category", typeof(string));
            largeData.Columns.Add("Date", typeof(DateTime));

            Console.WriteLine("Creating 100,000 rows for performance testing...");
            var random = new Random(42); // Fixed seed for reproducibility
            string[] categories = { "A", "B", "C", "D", "E" };

            for (int i = 0; i < 100_000; i++)
            {
                largeData.Rows.Add(
                    i,
                    $"Item-{i}",
                    Math.Round(random.NextDouble() * 1000, 2),
                    categories[random.Next(categories.Length)],
                    DateTime.Now.AddDays(-random.Next(365))
                );
            }

            string largeFilePath = Path.Combine(Path.GetTempPath(), "large_metadata_test.parquet");

            try
            {
                // Export large data to Parquet with multiple row groups
                Console.WriteLine("Writing large file with 10 row groups...");
                var sw = Stopwatch.StartNew();
                await ExportToParquetWithMultipleRowGroups(largeData, largeFilePath, rowGroupSize: 10_000);
                sw.Stop();
                Console.WriteLine($"File created in {sw.ElapsedMilliseconds}ms at: {largeFilePath}");

                // Test 1: Simple metadata read performance
                Console.WriteLine("Testing metadata read performance...");
                sw.Restart();
                using (var metadata = await ParquetMetadata.FromFileAsync(largeFilePath))
                {
                    sw.Stop();

                    Console.WriteLine($"Metadata read in {sw.ElapsedMilliseconds}ms");
                    Console.WriteLine($"File statistics: {metadata.TotalRowCount} rows in {metadata.RowGroupCount} row groups");
                }

                // Test 2: Row group read performance
                Console.WriteLine("\nTesting row group enumeration performance...");
                int totalRows = 0;
                int batchCount = 0;

                sw.Restart();
                using (var batchReader = new ParquetBatchReader(largeFilePath))
                {
                    batchCount = batchReader.BatchCount;
                    await foreach (var batch in batchReader.ReadAllAsync())
                    {
                        totalRows += batch.RowCount;
                    }
                } // Ensure batchReader is disposed here
                sw.Stop();

                Console.WriteLine($"Read {totalRows} rows from {batchCount} batches in {sw.ElapsedMilliseconds}ms");

                // Test 3: Connection metadata performance
                Console.WriteLine("\nTesting connection metadata performance...");
                sw.Restart();
                using (var connection = new ParquetConnection(largeFilePath))
                {
                    await connection.OpenAsync();
                    using (var connMetadata = connection.GetMetadata())
                    {
                        // Access some property to ensure metadata is fully loaded
                        var fieldCount = connMetadata.Schema.Fields.Count;
                    }
                }
                sw.Stop();

                Console.WriteLine($"Connection metadata read in {sw.ElapsedMilliseconds}ms");

                // Force garbage collection to release any remaining file handles
                GC.Collect();
                GC.WaitForPendingFinalizers();
            }
            finally
            {
                // Try multiple times to delete the file with a short delay
                for (int attempt = 1; attempt <= 3; attempt++)
                {
                    try
                    {
                        if (System.IO.File.Exists(largeFilePath))
                        {
                            System.IO.File.Delete(largeFilePath);
                            Console.WriteLine($"Test file deleted: {largeFilePath}");
                            break;
                        }
                    }
                    catch (IOException) when (attempt < 3)
                    {
                        // If file is still in use, wait briefly and try again
                        Console.WriteLine($"File still in use, retrying deletion (attempt {attempt}/3)...");
                        Thread.Sleep(500 * attempt); // Increasing delay with each attempt
                    }
                }
            }
        }
    }
}
using System;
using System.Data;
using System.IO;
using System.Threading.Tasks;
using System.Diagnostics;
using Parquet.Schema;
using Parquet;
using Parquet.Data;
using Parquet.Data.Ado;

namespace ParquetReaderTest
{
    /// <summary>
    /// Sample program to demonstrate SQL querying capabilities with Parquet files.
    /// </summary>
    public class ParquetSqlQueryTests
    {
        public static async Task RunAllTests()
        {
            Console.WriteLine("\n--- PARQUET SQL QUERY TESTS ---");

            try
            {
                // Create sample data
                var sampleData = CreateSampleData();
                string filePath = Path.Combine(Path.GetTempPath(), "sql_test.parquet");

                try
                {
                    // Export sample data to Parquet
                    Console.WriteLine("Creating test Parquet file...");
                    await sampleData.ExportToParquetAsync(filePath);
                    Console.WriteLine($"File created at: {filePath}");

                    // Test basic SELECT
                    await TestBasicSelect(filePath);

                    // Test column selection
                    await TestColumnSelection(filePath);

                    // Test WHERE clause filtering
                    await TestWhereClause(filePath);

                    // Test complex conditions
                    await TestComplexConditions(filePath);

                    // Test performance with large dataset
                    await TestPerformance(filePath);

                    Console.WriteLine("✓ All SQL query tests completed successfully!");
                }
                finally
                {
                    if (System.IO.File.Exists(filePath))
                    {
                        System.IO.File.Delete(filePath);
                        Console.WriteLine($"Test file deleted: {filePath}");
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

            // Define columns
            table.Columns.Add("ProductID", typeof(int));
            table.Columns.Add("ProductName", typeof(string));
            table.Columns.Add("Category", typeof(string));
            table.Columns.Add("Price", typeof(decimal));
            table.Columns.Add("InStock", typeof(bool));
            table.Columns.Add("LastUpdated", typeof(DateTime));

            // Add sample data
            table.Rows.Add(1, "Laptop", "Electronics", 999.99m, true, DateTime.Now.AddDays(-10));
            table.Rows.Add(2, "Smartphone", "Electronics", 699.99m, true, DateTime.Now.AddDays(-5));
            table.Rows.Add(3, "Headphones", "Electronics", 149.99m, true, DateTime.Now.AddDays(-15));
            table.Rows.Add(4, "Coffee Maker", "Appliances", 89.99m, false, DateTime.Now.AddDays(-30));
            table.Rows.Add(5, "Blender", "Appliances", 49.99m, true, DateTime.Now.AddDays(-20));
            table.Rows.Add(6, "Office Chair", "Furniture", 199.99m, false, DateTime.Now.AddDays(-45));
            table.Rows.Add(7, "Desk", "Furniture", 299.99m, true, DateTime.Now.AddDays(-25));
            table.Rows.Add(8, "Bookshelf", "Furniture", 149.99m, true, DateTime.Now.AddDays(-60));
            table.Rows.Add(9, "Monitor", "Electronics", 349.99m, false, DateTime.Now.AddDays(-7));
            table.Rows.Add(10, "Keyboard", "Electronics", 79.99m, true, DateTime.Now.AddDays(-12));

            Console.WriteLine($"Created sample data with {table.Rows.Count} rows");
            return table;
        }

        private static async Task TestBasicSelect(string filePath)
        {
            Console.WriteLine("\nTest: Basic SELECT");

            using var connection = new ParquetConnection(filePath);
            await connection.OpenAsync();

            string query = "SELECT * FROM Products";
            Console.WriteLine($"Executing query: {query}");

            var result = await connection.ExecuteSqlQueryToDataTableAsync(query);

            Console.WriteLine($"Result: {result.Rows.Count} rows, {result.Columns.Count} columns");
            PrintSampleData(result, 5);
        }

        private static async Task TestColumnSelection(string filePath)
        {
            Console.WriteLine("\nTest: Column Selection");

            using var connection = new ParquetConnection(filePath);
            await connection.OpenAsync();

            string query = "SELECT ProductID, ProductName, Price FROM Products";
            Console.WriteLine($"Executing query: {query}");

            var result = await connection.ExecuteSqlQueryToDataTableAsync(query);

            Console.WriteLine($"Result: {result.Rows.Count} rows, {result.Columns.Count} columns");
            Console.WriteLine("Columns: " + string.Join(", ", GetColumnNames(result)));
            PrintSampleData(result, 5);
        }

        private static async Task TestWhereClause(string filePath)
        {
            Console.WriteLine("\nTest: WHERE Clause");

            using var connection = new ParquetConnection(filePath);
            await connection.OpenAsync();

            string query = "SELECT ProductName, Category, Price FROM Products WHERE Category = 'Electronics'";
            Console.WriteLine($"Executing query: {query}");

            var result = await connection.ExecuteSqlQueryToDataTableAsync(query);

            Console.WriteLine($"Result: {result.Rows.Count} rows, {result.Columns.Count} columns");
            PrintSampleData(result, 5);

            query = "SELECT ProductName, Price FROM Products WHERE Price > 200";
            Console.WriteLine($"Executing query: {query}");

            result = await connection.ExecuteSqlQueryToDataTableAsync(query);

            Console.WriteLine($"Result: {result.Rows.Count} rows, {result.Columns.Count} columns");
            PrintSampleData(result, 5);
        }

        private static async Task TestComplexConditions(string filePath)
        {
            Console.WriteLine("\nTest: Complex Conditions");

            using var connection = new ParquetConnection(filePath);
            await connection.OpenAsync();

            string query = "SELECT ProductName, Category, Price FROM Products " +
                           "WHERE ( Category = 'Electronics' OR Category = 'Furniture' ) AND Price > 100 AND InStock = 1";
            Console.WriteLine($"Executing query: {query}");

            var result = await connection.ExecuteSqlQueryToDataTableAsync(query);

            Console.WriteLine($"Result: {result.Rows.Count} rows, {result.Columns.Count} columns");
            PrintSampleData(result, 5);

            query = "SELECT ProductName, Price FROM Products WHERE ProductName LIKE '%a%'";
            Console.WriteLine($"Executing query: {query}");

            result = await connection.ExecuteSqlQueryToDataTableAsync(query);

            Console.WriteLine($"Result: {result.Rows.Count} rows, {result.Columns.Count} columns");
            PrintSampleData(result, 5);
        }

        private static async Task TestPerformance(string filePath)
        {
            Console.WriteLine("\nTest: Performance with Larger Dataset");

            // Create a larger dataset for performance testing
            var largeData = new DataTable("LargeData");
            largeData.Columns.Add("ID", typeof(int));
            largeData.Columns.Add("Name", typeof(string));
            largeData.Columns.Add("Value", typeof(double));
            largeData.Columns.Add("Category", typeof(string));
            largeData.Columns.Add("Date", typeof(DateTime));

            Console.WriteLine("Creating 10,000 rows for performance testing...");
            var random = new Random(42); // Fixed seed for reproducibility
            string[] categories = { "A", "B", "C", "D", "E" };

            for (int i = 0; i < 10000; i++)
            {
                largeData.Rows.Add(
                    i,
                    $"Item-{i}",
                    Math.Round(random.NextDouble() * 1000, 2),
                    categories[random.Next(categories.Length)],
                    DateTime.Now.AddDays(-random.Next(365))
                );
            }

            string largeFilePath = Path.Combine(Path.GetTempPath(), "large_sql_test.parquet");

            try
            {
                // Export large data to Parquet
                await largeData.ExportToParquetAsync(largeFilePath);

                using var connection = new ParquetConnection(largeFilePath);
                await connection.OpenAsync();

                // Test 1: Simple query
                string query1 = "SELECT * FROM LargeData";
                Console.WriteLine($"Executing query: {query1}");

                var stopwatch = Stopwatch.StartNew();
                var result1 = await connection.ExecuteSqlQueryToDataTableAsync(query1);
                stopwatch.Stop();

                Console.WriteLine($"Full scan: {result1.Rows.Count} rows in {stopwatch.ElapsedMilliseconds}ms");

                // Test 2: Filtered query
                string query2 = "SELECT ID, Name, Value FROM LargeData WHERE Category = 'A'";
                Console.WriteLine($"Executing query: {query2}");

                stopwatch.Restart();
                var result2 = await connection.ExecuteSqlQueryToDataTableAsync(query2);
                stopwatch.Stop();

                Console.WriteLine($"Filtered query: {result2.Rows.Count} rows in {stopwatch.ElapsedMilliseconds}ms");

                // Test 3: Complex filter
                string query3 = "SELECT ID, Name, Value FROM LargeData WHERE Value > 500 AND Category IN ('A', 'B') AND Date > '2024-01-01'";
                Console.WriteLine($"Executing query: {query3}");

                stopwatch.Restart();
                var result3 = await connection.ExecuteSqlQueryToDataTableAsync(query3);
                stopwatch.Stop();

                Console.WriteLine($"Complex filter: {result3.Rows.Count} rows in {stopwatch.ElapsedMilliseconds}ms");
            }
            finally
            {
                if (System.IO.File.Exists(largeFilePath))
                {
                    System.IO.File.Delete(largeFilePath);
                }
            }
        }

        private static void PrintSampleData(DataTable table, int maxRows)
        {
            if (table.Rows.Count == 0)
            {
                Console.WriteLine("  (No data)");
                return;
            }

            int rowsToPrint = Math.Min(maxRows, table.Rows.Count);

            for (int i = 0; i < rowsToPrint; i++)
            {
                var row = table.Rows[i];
                Console.Write($"  Row {i}: ");

                for (int j = 0; j < table.Columns.Count; j++)
                {
                    Console.Write($"{table.Columns[j].ColumnName}=");
                    Console.Write(row[j] == DBNull.Value ? "NULL" : row[j]);

                    if (j < table.Columns.Count - 1)
                    {
                        Console.Write(", ");
                    }
                }

                Console.WriteLine();
            }

            if (table.Rows.Count > maxRows)
            {
                Console.WriteLine($"  ... ({table.Rows.Count - maxRows} more rows)");
            }
        }

        private static string[] GetColumnNames(DataTable table)
        {
            string[] names = new string[table.Columns.Count];
            for (int i = 0; i < table.Columns.Count; i++)
            {
                names[i] = table.Columns[i].ColumnName;
            }
            return names;
        }
    }
}
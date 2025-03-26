using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using System.Diagnostics;
using Parquet.Data.Ado;
using Parquet.Schema;
using Parquet;
using Parquet.Data;

namespace ParquetReaderTest
{
    /// <summary>
    /// Sample program to demonstrate virtual column capabilities with Parquet files.
    /// </summary>
    public class ParquetVirtualColumnTests
    {
        public static async Task RunAllTests()
        {
            Console.WriteLine("\n--- PARQUET VIRTUAL COLUMN TESTS ---");

            try
            {
                // Create sample data
                var sampleData = CreateSampleData();
                string filePath = Path.Combine(Path.GetTempPath(), "virtual_columns_test.parquet");

                try
                {
                    // Export sample data to Parquet
                    Console.WriteLine("Creating test Parquet file...");
                    await sampleData.ExportToParquetAsync(filePath);
                    Console.WriteLine($"File created at: {filePath}");

                    // Test basic virtual column usage
                    await TestBasicVirtualColumns(filePath);

                    // Test multiple virtual columns
                    await TestMultipleVirtualColumns(filePath);

                    // Test different data types for virtual columns
                    await TestVirtualColumnDataTypes(filePath);

                    // Test virtual columns with default values
                    await TestVirtualColumnsWithDefaultValues(filePath);

                    // Test SQL queries with virtual columns
                    await TestSqlQueriesWithVirtualColumns(filePath);

                    Console.WriteLine("✓ All virtual column tests completed successfully!");
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
            var table = new DataTable("Employees");

            // Define columns
            table.Columns.Add("EmployeeID", typeof(int));
            table.Columns.Add("FirstName", typeof(string));
            table.Columns.Add("LastName", typeof(string));
            table.Columns.Add("Salary", typeof(decimal));
            table.Columns.Add("HireDate", typeof(DateTime));
            table.Columns.Add("Department", typeof(string));
            table.Columns.Add("IsFullTime", typeof(bool));

            // Add sample data
            table.Rows.Add(1, "John", "Doe", 75000m, new DateTime(2020, 5, 15), "Engineering", true);
            table.Rows.Add(2, "Jane", "Smith", 82000m, new DateTime(2019, 3, 10), "Marketing", true);
            table.Rows.Add(3, "Michael", "Johnson", 65000m, new DateTime(2021, 1, 20), "Engineering", true);
            table.Rows.Add(4, "Emily", "Williams", 78000m, new DateTime(2018, 7, 5), "HR", true);
            table.Rows.Add(5, "David", "Brown", 62000m, new DateTime(2022, 2, 28), "Finance", true);
            table.Rows.Add(6, "Sarah", "Miller", 85000m, new DateTime(2017, 11, 12), "Marketing", true);
            table.Rows.Add(7, "James", "Anderson", 68000m, new DateTime(2021, 8, 3), "Engineering", false);
            table.Rows.Add(8, "Jessica", "Wilson", 71000m, new DateTime(2019, 5, 22), "HR", true);
            table.Rows.Add(9, "Robert", "Taylor", 90000m, new DateTime(2016, 9, 15), "Finance", true);
            table.Rows.Add(10, "Amanda", "Thomas", 67000m, new DateTime(2020, 12, 7), "Engineering", false);

            Console.WriteLine($"Created sample data with {table.Rows.Count} rows");
            return table;
        }

        private static async Task TestBasicVirtualColumns(string filePath)
        {
            Console.WriteLine("\nTest: Basic Virtual Column Usage");

            try
            {
                // Define a single virtual column
                var virtualColumns = new List<VirtualColumn>
                {
                    new VirtualColumn("FullName", typeof(string), "")
                };

                // Create a reader with virtual columns
                using var reader = await ParquetDataReaderFactoryExtensions.CreateWithVirtualColumnsAsync(
                    filePath, virtualColumns);

                // Verify the reader contains the virtual column
                int fieldCount = reader.FieldCount;
                Console.WriteLine($"Reader has {fieldCount} fields (including virtual columns)");

                // Get column names including virtual column
                Console.Write("Columns: ");
                for (int i = 0; i < fieldCount; i++)
                {
                    Console.Write($"{reader.GetName(i)}");
                    if (i < fieldCount - 1) Console.Write(", ");
                }
                Console.WriteLine();

                // Create a DataTable from the reader
                var table = new DataTable();
                for (int i = 0; i < reader.FieldCount; i++)
                {
                    table.Columns.Add(reader.GetName(i), reader.GetFieldType(i));
                }

                while (await reader.ReadAsync())
                {
                    var row = table.NewRow();
                    for (int i = 0; i < reader.FieldCount; i++)
                    {
                        row[i] = reader.GetValue(i);
                    }
                    table.Rows.Add(row);
                }

                Console.WriteLine($"DataTable has {table.Rows.Count} rows and {table.Columns.Count} columns");
                PrintSampleData(table, 3);
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error in TestBasicVirtualColumns: {ex.Message}");
                throw;
            }
        }

        private static async Task TestMultipleVirtualColumns(string filePath)
        {
            Console.WriteLine("\nTest: Multiple Virtual Columns");

            try
            {
                // Define multiple virtual columns
                var virtualColumns = new List<VirtualColumn>
                {
                    new VirtualColumn("FullName", typeof(string), ""),
                    new VirtualColumn("YearsEmployed", typeof(int), 0),
                    new VirtualColumn("BonusEligible", typeof(bool), false)
                };

                // Create a reader with virtual columns
                using var reader = await ParquetDataReaderFactoryExtensions.CreateWithVirtualColumnsAsync(
                    filePath, virtualColumns);

                // Verify the reader contains the virtual columns
                int fieldCount = reader.FieldCount;
                Console.WriteLine($"Reader has {fieldCount} fields (including virtual columns)");

                // Get schema table to see all columns
                using var schemaTable = reader.GetSchemaTable();
                Console.WriteLine("Schema table columns (including virtual):");
                foreach (DataRow row in schemaTable.Rows)
                {
                    string colName = row["ColumnName"].ToString();
                    Type colType = (Type)row["DataType"];
                    int colOrdinal = (int)row["ColumnOrdinal"];
                    Console.WriteLine($"  {colOrdinal}: {colName} ({colType.Name})");
                }

                // Create a DataTable from the reader
                var table = new DataTable();
                for (int i = 0; i < reader.FieldCount; i++)
                {
                    table.Columns.Add(reader.GetName(i), reader.GetFieldType(i));
                }

                while (await reader.ReadAsync())
                {
                    var row = table.NewRow();
                    for (int i = 0; i < reader.FieldCount; i++)
                    {
                        row[i] = reader.GetValue(i);
                    }
                    table.Rows.Add(row);
                }

                Console.WriteLine($"DataTable has {table.Rows.Count} rows and {table.Columns.Count} columns");
                PrintSampleData(table, 3);
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error in TestMultipleVirtualColumns: {ex.Message}");
                throw;
            }
        }

        private static async Task TestVirtualColumnDataTypes(string filePath)
        {
            Console.WriteLine("\nTest: Virtual Column Data Types");

            try
            {
                // Define virtual columns with different data types
                var virtualColumns = new List<VirtualColumn>
                {
                    new VirtualColumn("StringColumn", typeof(string), "Default String"),
                    new VirtualColumn("IntColumn", typeof(int), 42),
                    new VirtualColumn("DecimalColumn", typeof(decimal), 123.45m),
                    new VirtualColumn("DateTimeColumn", typeof(DateTime), DateTime.Now),
                    new VirtualColumn("BoolColumn", typeof(bool), true),
                    new VirtualColumn("GuidColumn", typeof(Guid), Guid.NewGuid())
                };

                // Create a reader with virtual columns
                using var reader = await ParquetDataReaderFactoryExtensions.CreateWithVirtualColumnsAsync(
                    filePath, virtualColumns);

                // Get the schema table
                using var schemaTable = reader.GetSchemaTable();
                Console.WriteLine("Virtual column data types:");

                // Find the virtual columns in the schema
                var ordinals = virtualColumns.Select(vc =>
                {
                    for (int i = 0; i < schemaTable.Rows.Count; i++)
                    {
                        if (string.Equals(schemaTable.Rows[i]["ColumnName"].ToString(), vc.ColumnName))
                            return i;
                    }
                    return -1;
                }).Where(o => o >= 0).ToList();

                foreach (var ord in ordinals)
                {
                    string colName = schemaTable.Rows[ord]["ColumnName"].ToString();
                    Type colType = (Type)schemaTable.Rows[ord]["DataType"];
                    Console.WriteLine($"  {colName}: {colType.FullName}");
                }

                // Read a row and verify data types
                if (await reader.ReadAsync())
                {
                    Console.WriteLine("\nSample values from first row:");
                    foreach (var vc in virtualColumns)
                    {
                        int ordinal = reader.GetOrdinal(vc.ColumnName);
                        object value = reader.GetValue(ordinal);
                        string typeName = value == DBNull.Value ? "DBNull" : value.GetType().Name;
                        Console.WriteLine($"  {vc.ColumnName} = {value} (Type: {typeName})");
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error in TestVirtualColumnDataTypes: {ex.Message}");
                throw;
            }
        }

        private static async Task TestVirtualColumnsWithDefaultValues(string filePath)
        {
            Console.WriteLine("\nTest: Virtual Columns with Default Values");

            try
            {
                // Current year for calculations
                int currentYear = DateTime.Now.Year;

                // Define virtual columns with default values
                var virtualColumns = new List<VirtualColumn>
                {
                    new VirtualColumn("EmailAddress", typeof(string),
                        "employee@company.com"),
                    new VirtualColumn("TaxRate", typeof(decimal), 0.25m),
                    new VirtualColumn("NextReviewDate", typeof(DateTime),
                        new DateTime(currentYear + 1, 1, 1)),
                    new VirtualColumn("EmployeeType", typeof(string),
                        "Regular")
                };

                // Create a reader with virtual columns
                using var reader = await ParquetDataReaderFactoryExtensions.CreateWithVirtualColumnsAsync(
                    filePath, virtualColumns);

                // Create a DataTable from the reader
                var table = new DataTable();
                for (int i = 0; i < reader.FieldCount; i++)
                {
                    table.Columns.Add(reader.GetName(i), reader.GetFieldType(i));
                }

                while (await reader.ReadAsync())
                {
                    var row = table.NewRow();
                    for (int i = 0; i < reader.FieldCount; i++)
                    {
                        row[i] = reader.GetValue(i);
                    }
                    table.Rows.Add(row);
                }

                Console.WriteLine("Default values for virtual columns:");
                foreach (var vc in virtualColumns)
                {
                    // Find the column index in the DataTable
                    int colIndex = table.Columns.IndexOf(vc.ColumnName);
                    if (colIndex >= 0)
                    {
                        // All rows should have the same default value
                        object value = table.Rows[0][colIndex];
                        Console.WriteLine($"  {vc.ColumnName} = {value} (Type: {value.GetType().Name})");
                    }
                }

                Console.WriteLine("\nSample rows with default values:");
                PrintSampleData(table, 3);
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error in TestVirtualColumnsWithDefaultValues: {ex.Message}");
                throw;
            }
        }

        private static async Task TestSqlQueriesWithVirtualColumns(string filePath)
        {
            Console.WriteLine("\nTest: SQL Queries with Virtual Columns");

            try
            {
                // First let's create a ParquetConnection to test SQL functionality
                using var connection = new ParquetConnection(filePath);
                await connection.OpenAsync();

                // Execute a basic SQL query to check support
                string query = "SELECT * FROM Employees";
                Console.WriteLine($"Executing query: {query}");

                var result = await connection.ExecuteSqlQueryToDataTableAsync(query);
                Console.WriteLine($"Basic SQL result: {result.Rows.Count} rows, {result.Columns.Count} columns");

                // Now let's verify we can use the Data Reader with Virtual Columns approach
                // Create a DataTable with the SQL result
                var sqlTable = result.Copy();

                // Add virtual columns to the table
                sqlTable.Columns.Add("FullName", typeof(string));
                sqlTable.Columns.Add("MonthlySalary", typeof(decimal));
                sqlTable.Columns.Add("YearsOfService", typeof(int));

                // Calculate values for the virtual columns
                foreach (DataRow row in sqlTable.Rows)
                {
                    // Combine first and last name for FullName column
                    row["FullName"] = $"{row["FirstName"]} {row["LastName"]}";

                    // Calculate monthly salary (annual / 12)
                    decimal annualSalary = (decimal)row["Salary"];
                    row["MonthlySalary"] = Math.Round(annualSalary / 12, 2);

                    // Calculate years of service
                    DateTime hireDate = (DateTime)row["HireDate"];
                    int yearsOfService = DateTime.Now.Year - hireDate.Year;
                    if (DateTime.Now.DayOfYear < hireDate.DayOfYear)
                        yearsOfService--; // Adjust for hire date not reached yet this year
                    row["YearsOfService"] = yearsOfService;
                }

                Console.WriteLine("\nDataTable with calculated virtual columns:");
                PrintSampleData(sqlTable, 3);

                // Now, demonstrate how we could use ParquetDataReaderWithVirtualColumns 
                // together with SQL querying (after the SQL query is executed)
                Console.WriteLine("\nDemonstrating virtual columns with filtering:");

                // Define a filtered view to simulate a WHERE clause
                DataView filteredView = new DataView(sqlTable);
                filteredView.RowFilter = "Department = 'Engineering' AND YearsOfService > 2";

                Console.WriteLine($"Filtered result: {filteredView.Count} rows");

                // Create a result table from the filtered view
                var filteredTable = filteredView.ToTable();
                PrintSampleData(filteredTable, 3);
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error in TestSqlQueriesWithVirtualColumns: {ex.Message}");
                throw;
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
    }
}
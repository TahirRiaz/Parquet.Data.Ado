# Parquet.Data.Ado

A .NET library that provides ADO.NET support for Parquet files, enabling seamless integration of Parquet data into .NET applications through familiar ADO.NET abstractions. This library is part of the Data Automation framework [SQLFlow](https://github.com/TahirRiaz/SQLFlow).

## Overview

Parquet.Data.Ado bridges the gap between the Parquet file format and .NET applications by implementing standard ADO.NET interfaces. This allows developers to work with Parquet files using the same patterns they use for traditional database access.

## Features

### ADO.NET Integration

- **DbConnection implementation**: Connect to Parquet files using the familiar connection string pattern
- **DbCommand support**: Execute commands against Parquet files
- **DbDataReader interface**: Process Parquet data using forward-only, read-only cursor pattern
- **DataTable conversion**: Convert Parquet data to DataTable objects for easy manipulation

### SQL Support

- **SQL query execution**: Query Parquet files using a subset of SQL syntax
- **WHERE clause filtering**: Filter data using SQL-like conditions
- **Column projection**: Select specific columns from Parquet files
- **SQL Parser**: Parse and execute SQL SELECT statements against Parquet files

### Advanced Features

- **Virtual columns**: Add computed or placeholder columns that aren't physically present in the Parquet file
- **Batch reading**: Process large Parquet files efficiently with batch operations
- **Asynchronous operations**: Full async support for I/O bound operations
- **Parallel processing**: Multi-threaded reading of row groups for improved performance
- **Type conversion**: Smart handling of data type conversions between Parquet and .NET types
- **Export capabilities**: Convert DataTable objects to Parquet format

## Getting Started

### Installation

```sh
dotnet add package Parquet.Data.Ado
```

### Basic Usage

```csharp
// Connect to a Parquet file
using var connection = new ParquetConnection("path/to/file.parquet");
connection.Open();

// Create a command
using var command = connection.CreateCommand();
command.CommandText = "SELECT * FROM data";

// Execute and read the data
using var reader = command.ExecuteReader();
while (reader.Read())
{
    // Access data by column index or name
    var value = reader["column_name"];
    // Process the data...
}
```

### Using SQL Queries

```csharp
using var connection = new ParquetConnection("path/to/file.parquet");
connection.Open();

// Create an SQL command
using var sqlCommand = connection.CreateSqlCommand("SELECT column1, column2 FROM data WHERE column3 > 100");

// Execute the query and get a DataTable
var dataTable = await ParquetExtensions.ToDataTableAsync(sqlCommand.ExecuteReader());
```

### Working with Virtual Columns

```csharp
// Create a virtual column definition
var virtualColumn = new VirtualColumn("ComputedColumn", typeof(decimal), 0.0m);

// Open a reader with the virtual column
using var reader = ParquetDataReaderFactory.CreateWithVirtualColumns(
    "path/to/file.parquet",
    new[] { virtualColumn });

// Now the reader includes the virtual column
while (reader.Read())
{
    decimal computedValue = reader.GetDecimal(reader.GetOrdinal("ComputedColumn"));
    // Use the virtual column value...
}
```

### Batch Processing

```csharp
// Create a batch reader for large files
using var batchReader = new ParquetBatchReader("path/to/large_file.parquet");

// Process batches asynchronously
await foreach (var batch in batchReader.ReadAllAsync())
{
    Console.WriteLine($"Processing batch {batch.RowGroupIndex} with {batch.RowCount} rows");
    
    // Process the batch...
    foreach (var column in batch.Columns)
    {
        // Access column data...
    }
}
```

### Converting DataTable to Parquet

```csharp
// Create or obtain a DataTable
var dataTable = new DataTable("MyData");
// ... populate the table ...

// Export to Parquet
await dataTable.ExportToParquetAsync("output.parquet");
```

## Key Components

- **ParquetConnection**: Manages connection to Parquet files
- **ParquetCommand**: Executes commands against Parquet files
- **ParquetDataReader**: Reads data from Parquet files
- **ParquetSqlCommand**: Executes SQL queries against Parquet files
- **ParquetDataReaderFactory**: Creates data readers with various options
- **ParquetBatchReader**: Processes large Parquet files in batches
- **ParquetExtensions**: Utility methods for Parquet operations

## Advanced SQL Capabilities

The library includes a SQL parser that supports:

- `SELECT` statements with column selection
- `WHERE` clauses with complex conditions
- Basic comparison operators (`=`, `<>`, `<`, `>`, `<=`, `>=`)
- Logical operators (`AND`, `OR`, `NOT`)
- Pattern matching with `LIKE`
- Value checking with `IN`, `BETWEEN`, and `IS NULL`

## Requirements

- .NET 9.0 or later
- Dependencies:
  - Parquet.Net (for Parquet file handling)
  - Microsoft.SqlServer.TransactSql.ScriptDom (for SQL parsing)

## Testing and Extensibility

The GitHub repository includes extensive tests that demonstrate the library's functionality. These tests can be used as examples and modified to suit your specific requirements.

## Part of SQLFlow

This library is a component of [SQLFlow](https://github.com/TahirRiaz/SQLFlow), a comprehensive Data Automation framework that enables seamless data integration, transformation, and analysis across various data sources.

## License

This project is licensed under the MIT License - see the LICENSE file for details.

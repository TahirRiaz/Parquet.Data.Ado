# Parquet.Data.Ado v1.1.0 Release Notes

## Overview
I am excited to announce the release of version 1.1.0 of Parquet.Data.Ado, featuring enhanced metadata capabilities that provide deeper insights into your Parquet files without fully loading the data. This release builds on our core functionality while making it easier to inspect and understand your Parquet data structure.

## New Features

### 1. Metadata Extraction & Inspection

The new `ParquetMetadata` class and related extensions provide comprehensive metadata information about your Parquet files:

```csharp
// Get metadata from a file path
ParquetMetadata metadata = ParquetMetadata.FromFile("data.parquet");

// Or asynchronously
ParquetMetadata metadata = await ParquetMetadata.FromFileAsync("data.parquet");

// From a stream
using var stream = File.OpenRead("data.parquet");
ParquetMetadata metadata = ParquetMetadata.FromStream(stream);

// From an existing connection
using var connection = new ParquetConnection("data.parquet");
connection.Open();
ParquetMetadata metadata = connection.GetMetadata();

// From a reader
using var reader = ParquetDataReaderFactory.Create("data.parquet");
ParquetMetadata metadata = reader.GetMetadata();
```

### 2. Available Metadata Information

The `ParquetMetadata` class provides access to:

- **Schema Information**: Types and structures of all columns
- **Row Groups**: Count and details of each row group
- **Total Row Count**: Quickly get the total number of rows across all row groups
- **Column Details**: Data types, nullability, and other column-specific information

```csharp
// Get basic file information
Console.WriteLine($"Total rows: {metadata.TotalRowCount}");
Console.WriteLine($"Row groups: {metadata.RowGroupCount}");

// Explore schema
foreach (var column in metadata.Columns)
{
    Console.WriteLine($"Column: {column.Name}, Type: {column.DataType.Name}, Nullable: {column.IsNullable}");
}

// Inspect row groups
foreach (var group in metadata.RowGroups)
{
    Console.WriteLine($"Row Group {group.Index}: {group.RowCount} rows");
}
```

### 3. Extension Methods for Easy Access

We've added extension methods to quickly get metadata from various sources:

```csharp
// Get row group count from a connection
int groupCount = connection.GetRowGroupCount();

// Get total row count without reading all data
long totalRows = connection.GetTotalRowCount();

// Get metadata directly from a file path
ParquetMetadata metadata = ParquetMetadataExtensions.GetMetadata("data.parquet");

// Get current row group from a reader
int currentGroup = reader.GetCurrentRowGroup();
```

### 4. Asynchronous Support

All metadata operations are available in both synchronous and asynchronous forms:

```csharp
// Synchronous
ParquetMetadata metadata = ParquetMetadata.FromFile("data.parquet");

// Asynchronous with cancellation support
var cts = new CancellationTokenSource();
ParquetMetadata metadata = await ParquetMetadata.FromFileAsync("data.parquet", cts.Token);
```

## Usage Examples

### Example 1: Quick File Inspection

```csharp
using Parquet.Data.Reader;

public void InspectParquetFile(string filePath)
{
    var metadata = ParquetMetadata.FromFile(filePath);
    
    Console.WriteLine($"File: {Path.GetFileName(filePath)}");
    Console.WriteLine($"Rows: {metadata.TotalRowCount:N0}");
    Console.WriteLine($"Row Groups: {metadata.RowGroupCount}");
    Console.WriteLine("\nColumns:");
    
    foreach (var column in metadata.Columns)
    {
        Console.WriteLine($"  - {column.Name} ({column.DataType.Name}{(column.IsNullable ? ", nullable" : "")})");
    }
}
```

### Example 2: Analyzing Row Group Distribution

```csharp
using Parquet.Data.Reader;

public void AnalyzeRowGroups(string filePath)
{
    var metadata = ParquetMetadata.FromFile(filePath);
    
    Console.WriteLine($"Row Group Analysis for {Path.GetFileName(filePath)}");
    Console.WriteLine("=================================================");
    Console.WriteLine($"Total Row Groups: {metadata.RowGroupCount}");
    Console.WriteLine($"Total Rows: {metadata.TotalRowCount:N0}");
    
    if (metadata.RowGroupCount > 0)
    {
        double avgRowsPerGroup = (double)metadata.TotalRowCount / metadata.RowGroupCount;
        Console.WriteLine($"Average Rows per Group: {avgRowsPerGroup:N1}");
        
        var smallest = metadata.RowGroups.OrderBy(g => g.RowCount).First();
        var largest = metadata.RowGroups.OrderByDescending(g => g.RowCount).First();
        
        Console.WriteLine($"Smallest Group: Group {smallest.Index} with {smallest.RowCount:N0} rows");
        Console.WriteLine($"Largest Group: Group {largest.Index} with {largest.RowCount:N0} rows");
    }
}
```

### Example 3: Integration with Data Loading

```csharp
using Parquet.Data.Reader;
using System.Data;

public async Task OptimizedDataLoading(string filePath)
{
    // Check file metadata before loading
    var metadata = await ParquetMetadata.FromFileAsync(filePath);
    
    // Only load if the file has a reasonable size
    if (metadata.TotalRowCount > 1_000_000)
    {
        Console.WriteLine("Warning: Large file detected. Consider processing in batches.");
        
        // Process row groups individually
        using var connection = new ParquetConnection(filePath);
        await connection.OpenAsync();
        
        for (int groupIndex = 0; groupIndex < metadata.RowGroupCount; groupIndex++)
        {
            string query = $"SELECT * FROM [{Path.GetFileNameWithoutExtension(filePath)}]";
            using var reader = await connection.ExecuteSqlQueryAsync(query);
            
            // Process each row group
            Console.WriteLine($"Processing row group {groupIndex} of {metadata.RowGroupCount}...");
            
            // Your processing logic here...
        }
    }
    else
    {
        // For smaller files, load all at once
        using var reader = ParquetDataReaderFactory.Create(filePath);
        DataTable table = reader.ToDataTable();
        Console.WriteLine($"Loaded {table.Rows.Count} rows into memory.");
    }
}
```

## Breaking Changes

None. This release is fully backward compatible with previous versions.

## Bug Fixes

- Fixed issue with resource leaks in ParquetConnection when exceptions occurred during opening
- Improved error messaging for schema mismatches
- Enhanced thread safety in metadata extraction operations

## System Requirements

- .NET Standard 2.0 or higher
- Parquet.Net 4.0.0 or higher

## Installation

Install via NuGet Package Manager:

```
Install-Package Parquet.Data.Ado -Version 1.1.0
```

Or via .NET CLI:

```
dotnet add package Parquet.Data.Ado --version 1.1.0
```

## Feedback and Contributions

We welcome your feedback and contributions! Please open issues on our GitHub repository with any bugs or feature requests.
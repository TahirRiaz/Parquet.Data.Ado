using System;
using System.Collections.Generic;
using System.Data;
using System.Globalization;
using System.Linq;
using System.Threading.Tasks;
using Parquet;
using Parquet.Data;
using Parquet.Schema;

namespace Parquet.Data.Reader
{
    /// <summary>
    /// Provides functionality to execute SQL SELECT queries against Parquet files.
    /// </summary>
    /// <remarks>
    /// This class supports parsing and executing SQL SELECT queries with features such as:
    /// - Selecting all columns or specific columns.
    /// - Applying WHERE clauses with complex conditions.
    /// - Reading and filtering data from Parquet files asynchronously.
    /// </remarks>
    public static class ParquetSqlQuery
    {
        /// <summary>
        /// Parses a SQL SELECT query and executes it against the given Parquet file, returning the result as a DataTable.
        /// Supports '*' (all columns) or specific column names, and complex WHERE clauses.
        /// </summary>
        public static async Task<DataTable> ExecuteQueryAsync(string parquetFilePath, string sqlQuery)
        {
            if (string.IsNullOrWhiteSpace(sqlQuery))
                throw new ArgumentException("Query cannot be null or empty.", nameof(sqlQuery));
            if (string.IsNullOrWhiteSpace(parquetFilePath))
                throw new ArgumentException("Parquet file path cannot be null or empty.", nameof(parquetFilePath));

            // Parse the SQL query using QueryListener
            var listener = new SqlParser();
            listener.ParseQuery(sqlQuery);

            if (!listener.StatementFound)
                throw new InvalidOperationException("Only SELECT queries are supported by ParquetSqlQuery.");

            // If the query says SELECT * then read all columns; otherwise the user-specified list
            bool selectAllColumns = listener.SelectAll;
            List<string> selectedColumns = listener.SelectedColumns ?? new List<string>();

            // Prepare the result DataTable
            var resultTable = new System.Data.DataTable();

            // Open the Parquet file
            ParquetReader reader = await ParquetReader.CreateAsync(parquetFilePath).ConfigureAwait(false);
            using (reader)
            {
                // Use the columns extracted by the parser
                var whereColumns = listener.WhereColumnReferences;

                // Merge SELECT columns + WHERE columns into "allRequiredColumns" if not selecting all
                HashSet<string> allRequiredColumns = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
                if (!selectAllColumns)
                {
                    foreach (string col in selectedColumns)
                        allRequiredColumns.Add(col);
                    foreach (string wcol in whereColumns)
                        allRequiredColumns.Add(wcol);
                }

                // Figure out which columns to read
                DataField[] allFields = reader.Schema.GetDataFields();
                DataField[] fieldsToRead;
                if (selectAllColumns)
                {
                    fieldsToRead = allFields;
                }
                else
                {
                    var tempList = new List<DataField>();
                    foreach (string colName in allRequiredColumns)
                    {
                        DataField? field = Array.Find(
                            allFields, f => f.Name.Equals(colName, StringComparison.OrdinalIgnoreCase));
                        if (field == null)
                            throw new InvalidOperationException($"Column '{colName}' not found in Parquet file schema.");
                        tempList.Add(field);
                    }
                    fieldsToRead = tempList.ToArray();
                }

                // Create DataTable columns based on the fieldsToRead
                foreach (DataField field in fieldsToRead)
                {
                    Type clrType = field.ClrType;
                    // Create a System.Data.DataColumn, set AllowDBNull = true
                    var col = new System.Data.DataColumn(field.Name, clrType)
                    {
                        AllowDBNull = true
                    };
                    resultTable.Columns.Add(col);
                }

                // Read row groups, populate the DataTable
                for (int groupIndex = 0; groupIndex < reader.RowGroupCount; groupIndex++)
                {
                    Parquet.Data.DataColumn[] dataColumns =
                        await reader.ReadEntireRowGroupAsync(groupIndex).ConfigureAwait(false);

                    int rowCount = dataColumns.Length > 0 ? dataColumns[0].Data.Length : 0;

                    for (int i = 0; i < rowCount; i++)
                    {
                        object[] rowValues = new object[resultTable.Columns.Count];

                        // Fill rowValues from the parquet data
                        for (int colIdx = 0; colIdx < fieldsToRead.Length; colIdx++)
                        {
                            DataField field = fieldsToRead[colIdx];
                            var colData = Array.Find(dataColumns, dc => dc.Field.Equals(field));

                            object? value = colData?.Data.GetValue(i);
                            if (value == null) value = DBNull.Value;
                            rowValues[colIdx] = value;
                        }

                        // Create a dictionary of column values for condition evaluation
                        Dictionary<string, object> rowData = new Dictionary<string, object>(StringComparer.OrdinalIgnoreCase);
                        for (int colIdx = 0; colIdx < resultTable.Columns.Count; colIdx++)
                        {
                            rowData[resultTable.Columns[colIdx].ColumnName] = rowValues[colIdx];
                        }

                        // Apply the WHERE filter if present
                        // Use the WhereConditionTree from QueryListener instead of manual parsing
                        if (listener.WhereConditionTree == null || listener.WhereConditionTree.Evaluate(rowData))
                        {
                            resultTable.Rows.Add(rowValues);
                        }
                    }
                }
            }

            return resultTable;
        }
    }
}
using System;
using System.ComponentModel.DataAnnotations;

namespace Parquet.Data.Reader
{
    /// <summary>
    /// Represents a data column in a Parquet data reader, including virtual columns that don't exist in the physical file.
    /// </summary>
    public class VirtualColumn
    {
        /// <summary>
        /// Gets or sets the name of the column.
        /// </summary>
        public string ColumnName { get; set; }

        /// <summary>
        /// Gets or sets the CLR data type of the column.
        /// </summary>
        /// <remarks>
        /// This property represents the data type of the column, which may differ from the physical column type
        /// in cases where the column is virtual or has been transformed.
        /// </remarks>
        public Type DataType { get; set; }

        /// <summary>
        /// Gets or sets the default value for the column.
        /// </summary>
        public object DefaultValue { get; set; }

        /// <summary>
        /// Gets or sets whether the column is a virtual column (not physically present in the Parquet file).
        /// </summary>
        public bool IsVirtual { get; set; }

        /// <summary>
        /// Initializes a new instance of the <see cref="VirtualColumn"/> class.
        /// </summary>
        /// <param name="columnName">The name of the column.</param>
        /// <param name="columnType">The CLR data type of the column.</param>
        /// <param name="defaultValue">The default value for the column.</param>
        /// <param name="isVirtual">Whether the column is a virtual column.</param>
        public VirtualColumn(string columnName, Type columnType, object? defaultValue = null, bool isVirtual = true)
        {
            ColumnName = columnName ?? throw new ArgumentNullException(nameof(columnName));
            DataType = columnType ?? throw new ArgumentNullException(nameof(columnType));
            DefaultValue = defaultValue ?? DBNull.Value;
            IsVirtual = isVirtual;
        }
    }
}
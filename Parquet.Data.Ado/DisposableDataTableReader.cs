﻿using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Parquet.Data.Ado
{
    /// <summary>
    /// Wrapper class that ensures both the DataTableReader and its parent DataTable
    /// are properly disposed when the DbDataReader is disposed.
    /// </summary>
    internal sealed class DisposableDataTableReader : DbDataReader
    {
        private readonly DataTable _dataTable;
        private readonly DataTableReader _reader;
        private bool _disposed;

        public DisposableDataTableReader(DataTable dataTable)
        {
            _dataTable = dataTable ?? throw new ArgumentNullException(nameof(dataTable));
            _reader = dataTable.CreateDataReader();
        }

        // Forward all DbDataReader members to the inner reader
        public override object this[int ordinal] => _reader[ordinal];
        public override object this[string name] => _reader[name];
        public override int Depth => _reader.Depth;
        public override int FieldCount => _reader.FieldCount;
        public override bool HasRows => _reader.HasRows;
        public override bool IsClosed => _reader.IsClosed;
        public override int RecordsAffected => _reader.RecordsAffected;

        public override bool GetBoolean(int ordinal) => _reader.GetBoolean(ordinal);
        public override byte GetByte(int ordinal) => _reader.GetByte(ordinal);
        public override long GetBytes(int ordinal, long dataOffset, byte[]? buffer, int bufferOffset, int length)
            => _reader.GetBytes(ordinal, dataOffset, buffer, bufferOffset, length);
        public override char GetChar(int ordinal) => _reader.GetChar(ordinal);
        public override long GetChars(int ordinal, long dataOffset, char[]? buffer, int bufferOffset, int length)
            => _reader.GetChars(ordinal, dataOffset, buffer, bufferOffset, length);
        public override string GetDataTypeName(int ordinal) => _reader.GetDataTypeName(ordinal);
        public override DateTime GetDateTime(int ordinal) => _reader.GetDateTime(ordinal);
        public override decimal GetDecimal(int ordinal) => _reader.GetDecimal(ordinal);
        public override double GetDouble(int ordinal) => _reader.GetDouble(ordinal);
        public override Type GetFieldType(int ordinal) => _reader.GetFieldType(ordinal);
        public override float GetFloat(int ordinal) => _reader.GetFloat(ordinal);
        public override Guid GetGuid(int ordinal) => _reader.GetGuid(ordinal);
        public override short GetInt16(int ordinal) => _reader.GetInt16(ordinal);
        public override int GetInt32(int ordinal) => _reader.GetInt32(ordinal);
        public override long GetInt64(int ordinal) => _reader.GetInt64(ordinal);
        public override string GetName(int ordinal) => _reader.GetName(ordinal);
        public override int GetOrdinal(string name) => _reader.GetOrdinal(name);
        public override string GetString(int ordinal) => _reader.GetString(ordinal);
        public override object GetValue(int ordinal) => _reader.GetValue(ordinal);
        public override int GetValues(object[] values) => _reader.GetValues(values);
        public override bool IsDBNull(int ordinal) => _reader.IsDBNull(ordinal);
        public override bool NextResult() => _reader.NextResult();
        public override bool Read() => _reader.Read();

        public override System.Collections.IEnumerator GetEnumerator() => _reader.GetEnumerator();

        protected override void Dispose(bool disposing)
        {
            if (!_disposed)
            {
                if (disposing)
                {
                    // Dispose the reader first
                    _reader.Dispose();

                    // Then dispose the DataTable
                    _dataTable.Dispose();
                }

                _disposed = true;
            }

            base.Dispose(disposing);
        }
    }
}

using Microsoft.SqlServer.TransactSql.ScriptDom;
using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;

namespace Parquet.Data.Reader
{
    /// <summary>
    /// Enhanced query parser using Microsoft.SqlServer.TransactSql.ScriptDom to extract detailed information
    /// about SQL queries including SELECT columns, WHERE clauses, GROUP BY, aggregations, and more.
    /// </summary>
    internal sealed class SqlParser
    {
        #region Public Properties (Core Functionality)

        /// <summary>
        /// Set to true if a SELECT statement was found in the parsed query.
        /// </summary>
        public bool StatementFound { get; private set; }

        /// <summary>
        /// Set to true if a SELECT * was found (no individual column list).
        /// </summary>
        public bool SelectAll { get; private set; }

        /// <summary>
        /// List of columns found in the SELECT clause.
        /// </summary>
        public List<string> SelectedColumns { get; } = new List<string>();

        /// <summary>
        /// Detailed representation of selected columns, including aliases and expressions.
        /// </summary>
        public List<ColumnInfo> DetailedColumns { get; } = new List<ColumnInfo>();

        /// <summary>
        /// Text of the WHERE clause if found.
        /// </summary>
        public string? WhereClauseText { get; private set; }

        /// <summary>
        /// Column references found in the WHERE clause.
        /// </summary>
        public HashSet<string> WhereColumnReferences { get; } = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

        /// <summary>
        /// True if any parse or processing errors were encountered.
        /// </summary>
        public bool HasErrors { get; private set; }

        /// <summary>
        /// List of error messages encountered during parsing or processing.
        /// </summary>
        public List<string> ErrorMessages { get; } = new List<string>();

        /// <summary>
        /// Structured representation of the WHERE clause as a condition tree.
        /// </summary>
        public ConditionTree? WhereConditionTree { get; private set; }

        /// <summary>
        /// Structured representation of the HAVING clause as a condition tree.
        /// </summary>
        public ConditionTree? HavingConditionTree { get; private set; }

        #endregion

        #region Advanced Query Features

        /// <summary>
        /// Tracks if the query contains a GROUP BY clause.
        /// </summary>
        public bool HasGroupBy { get; private set; }

        /// <summary>
        /// Columns used in the GROUP BY clause.
        /// </summary>
        public List<string> GroupByColumns { get; } = new List<string>();

        /// <summary>
        /// Tracks if the query contains a HAVING clause.
        /// </summary>
        public bool HasHaving { get; private set; }

        /// <summary>
        /// Text of the HAVING clause if found.
        /// </summary>
        public string? HavingClauseText { get; private set; }

        /// <summary>
        /// Column references found in the HAVING clause.
        /// </summary>
        public HashSet<string> HavingColumnReferences { get; } = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

        /// <summary>
        /// Tracks if the query contains an ORDER BY clause.
        /// </summary>
        public bool HasOrderBy { get; private set; }

        /// <summary>
        /// Columns used in the ORDER BY clause, with sort direction.
        /// </summary>
        public List<OrderByInfo> OrderByColumns { get; } = new List<OrderByInfo>();

        /// <summary>
        /// Tracks if the query uses JOIN operations.
        /// </summary>
        public bool HasJoins { get; private set; }

        /// <summary>
        /// Detailed information about joins in the query.
        /// </summary>
        public List<JoinInfo> Joins { get; } = new List<JoinInfo>();

        /// <summary>
        /// Tables referenced in the FROM clause.
        /// </summary>
        public List<TableInfo> Tables { get; } = new List<TableInfo>();

        /// <summary>
        /// Tracks if any aggregation functions are used.
        /// </summary>
        public bool HasAggregates { get; private set; }

        /// <summary>
        /// Aggregate functions used in the query.
        /// </summary>
        public List<AggregateInfo> Aggregates { get; } = new List<AggregateInfo>();

        /// <summary>
        /// Tracks DISTINCT usage.
        /// </summary>
        public bool HasDistinct { get; private set; }

        /// <summary>
        /// Tracks TOP clause usage.
        /// </summary>
        public bool HasTopClause { get; private set; }

        /// <summary>
        /// Value of the TOP clause if specified.
        /// </summary>
        public string? TopValue { get; private set; }

        /// <summary>
        /// Whether TOP is used with PERCENT.
        /// </summary>
        public bool TopIsPercent { get; private set; }

        /// <summary>
        /// Maps column aliases to expressions.
        /// </summary>
        public Dictionary<string, string> ColumnAliases { get; } =
            new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);

        #endregion

        #region Constructors / No-Op Overrides (Preserving Old Signatures)

        // The old ANTLR-based code used to rely on TSqlParserBaseListener overrides.
        // We keep the same method names and signatures to avoid breaking changes.
        // They are no-ops here, because ScriptDom uses a different visitation model.
        public SqlParser()
        {
        }

        #endregion

        #region Primary Entry Point for the Microsoft Parser

        /// <summary>
        /// Main method to parse the given T-SQL query and populate all properties.
        /// </summary>
        /// <param name="query">The T-SQL query string to parse.</param>
        public void ParseQuery(string query)
        {
            // Clear any existing data
            ClearAllState();

            if (string.IsNullOrWhiteSpace(query))
            {
                return;
            }

            try
            {
                // Choose T-SQL version-specific parser as appropriate.
                // TSql150Parser covers a broad set of modern T-SQL syntax
                TSql150Parser parser = new TSql150Parser(false);

                IList<ParseError> parseErrors;
                TSqlFragment fragment;
                using (var sr = new StringReader(query))
                {
                    fragment = parser.Parse(sr, out parseErrors);
                }

                // Check parse errors
                if (parseErrors != null && parseErrors.Count > 0)
                {
                    HasErrors = true;
                    foreach (var err in parseErrors)
                    {
                        ErrorMessages.Add(string.Format(CultureInfo.InvariantCulture,
                            "Error at line {0}:{1} - {2}", err.Line, err.Column, err.Message));
                    }
                }

                // If we got a parse tree, proceed to analyze it via visitors
                if (fragment != null)
                {
                    // Primary visitor processes the query's main structure
                    var visitor = new SqlStructureVisitor(this);
                    fragment.Accept(visitor);
                }
            }
            catch (InvalidOperationException ex)
            {
                HasErrors = true;
                ErrorMessages.Add(string.Format(CultureInfo.InvariantCulture,
                    "Invalid operation during parsing: {0}", ex.Message));
            }
            catch (IOException ex) // For StringReader operations
            {
                HasErrors = true;
                ErrorMessages.Add(string.Format(CultureInfo.InvariantCulture,
                    "IO error during parsing: {0}", ex.Message));
            }
            catch (Exception ex) when (
                ex is not OutOfMemoryException &&
                ex is not StackOverflowException &&
                ex is not AccessViolationException)
            {
                // Avoid catching fatal exceptions but still catch other unexpected issues
                HasErrors = true;
                ErrorMessages.Add(string.Format(CultureInfo.InvariantCulture,
                    "Error during parsing: {0}", ex.Message));
            }
        }

        /// <summary>
        /// Clears all state to prepare for a new query parse.
        /// </summary>
        private void ClearAllState()
        {
            // Clear basic properties from original implementation
            StatementFound = false;
            SelectAll = false;
            SelectedColumns.Clear();
            DetailedColumns.Clear();
            WhereClauseText = null;
            WhereColumnReferences.Clear();
            HasErrors = false;
            ErrorMessages.Clear();

            // Clear advanced query properties
            HasGroupBy = false;
            GroupByColumns.Clear();
            HasHaving = false;
            HavingClauseText = null;
            HavingColumnReferences.Clear();
            HasOrderBy = false;
            OrderByColumns.Clear();
            HasJoins = false;
            Joins.Clear();
            Tables.Clear();
            HasAggregates = false;
            Aggregates.Clear();
            HasDistinct = false;
            HasTopClause = false;
            TopValue = null;
            TopIsPercent = false;
            ColumnAliases.Clear();
            // Clear condition trees
            WhereConditionTree = null;
            HavingConditionTree = null;
        }

        #endregion

        #region Structures for Advanced Query Information
        /// <summary>
        /// Detailed information about a column in a SELECT statement.
        /// </summary>
        public sealed class ColumnInfo
        {
            /// <summary>
            /// Column name or expression text.
            /// </summary>
            public string Expression { get; set; } = string.Empty;

            /// <summary>
            /// Column alias if specified.
            /// </summary>
            public string? Alias { get; set; }

            /// <summary>
            /// True if the column is a function call (aggregate or scalar).
            /// </summary>
            public bool IsFunction { get; set; }

            /// <summary>
            /// Function name if IsFunction is true.
            /// </summary>
            public string? FunctionName { get; set; }

            /// <summary>
            /// True if this is an aggregate function (COUNT, SUM, etc.).
            /// </summary>
            public bool IsAggregate { get; set; }

            /// <summary>
            /// Column names referenced in this expression.
            /// </summary>
            public HashSet<string> ReferencedColumns { get; } = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

            /// <summary>
            /// True if this is a computed column (expression, not simple column).
            /// </summary>
            public bool IsComputed { get; set; }

            /// <summary>
            /// True if this column appears to be a literal/constant.
            /// </summary>
            public bool IsLiteral { get; set; }

            /// <summary>
            /// The full text of the column specification including alias.
            /// </summary>
            public string FullText { get; set; } = string.Empty;
        }

        /// <summary>
        /// Information about an ORDER BY column.
        /// </summary>
        public sealed class OrderByInfo
        {
            /// <summary>
            /// Column name or expression text.
            /// </summary>
            public string ColumnName { get; set; } = string.Empty;

            /// <summary>
            /// True if ordering is descending.
            /// </summary>
            public bool IsDescending { get; set; }

            /// <summary>
            /// Full text of the ORDER BY element.
            /// </summary>
            public string FullText { get; set; } = string.Empty;
        }

        /// <summary>
        /// Information about a JOIN clause.
        /// </summary>
        public sealed class JoinInfo
        {
            /// <summary>
            /// Type of join (INNER, LEFT, RIGHT, FULL, CROSS).
            /// </summary>
            public string JoinType { get; set; } = string.Empty;

            /// <summary>
            /// Left table in the join.
            /// </summary>
            public string LeftTable { get; set; } = string.Empty;

            /// <summary>
            /// Right table in the join.
            /// </summary>
            public string RightTable { get; set; } = string.Empty;

            /// <summary>
            /// ON condition of the join.
            /// </summary>
            public string Condition { get; set; } = string.Empty;

            /// <summary>
            /// Columns referenced in the join condition.
            /// </summary>
            public HashSet<string> ReferencedColumns { get; } = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        }

        /// <summary>
        /// Information about a table referenced in the query.
        /// </summary>
        public sealed class TableInfo
        {
            /// <summary>
            /// Table name.
            /// </summary>
            public string Name { get; set; } = string.Empty;

            /// <summary>
            /// Schema name if specified.
            /// </summary>
            public string? Schema { get; set; }

            /// <summary>
            /// Table alias if specified.
            /// </summary>
            public string? Alias { get; set; }

            /// <summary>
            /// Full table reference text.
            /// </summary>
            public string FullText { get; set; } = string.Empty;
        }

        /// <summary>
        /// Information about an aggregate function.
        /// </summary>
        public sealed class AggregateInfo
        {
            /// <summary>
            /// Type of aggregate (COUNT, SUM, AVG, MIN, MAX, etc.).
            /// </summary>
            public string FunctionName { get; set; } = string.Empty;

            /// <summary>
            /// Column or expression the aggregate operates on.
            /// </summary>
            public string Column { get; set; } = string.Empty;

            /// <summary>
            /// Alias of the aggregate result if specified.
            /// </summary>
            public string? Alias { get; set; }

            /// <summary>
            /// True if the aggregate uses DISTINCT.
            /// </summary>
            public bool IsDistinct { get; set; }

            /// <summary>
            /// Full text of the aggregate expression.
            /// </summary>
            public string FullText { get; set; } = string.Empty;
        }

        #endregion

        #region SQL Structure Visitor

        /// <summary>
        /// Primary visitor that processes the entire SQL structure to populate all properties.
        /// </summary>
        private sealed class SqlStructureVisitor : TSqlFragmentVisitor
        {
            private readonly SqlParser _owner;

            public SqlStructureVisitor(SqlParser owner)
            {
                _owner = owner;
            }

            #region SELECT Statement Processing
            public override void Visit(SelectStatement node)
            {
                _owner.StatementFound = true;

                // If the query expression is a QuerySpecification, 
                // it contains the SELECT columns, WHERE, GROUP BY, etc.
                if (node.QueryExpression is QuerySpecification qs)
                {
                    // Process SELECT columns
                    ProcessSelectElements(qs);

                    // Process TOP clause if present
                    if (qs.TopRowFilter != null)
                    {
                        ProcessTopClause(qs.TopRowFilter);
                    }

                    // Process WHERE clause
                    if (qs.WhereClause != null)
                    {
                        _owner.WhereClauseText = ScriptFragment(qs.WhereClause.SearchCondition);

                        // Collect column references from WHERE
                        var whereColumnVisitor = new ColumnReferenceVisitor();
                        qs.WhereClause.Accept(whereColumnVisitor);
                        foreach (var column in whereColumnVisitor.ColumnReferences)
                        {
                            _owner.WhereColumnReferences.Add(column);
                        }

                        // Build the condition tree
                        var conditionTreeBuilder = new ConditionTreeBuilder();
                        qs.WhereClause.SearchCondition.Accept(conditionTreeBuilder);
                        _owner.WhereConditionTree = new ConditionTree(conditionTreeBuilder.RootNode);
                    }

                    // Process FROM clause and its tables
                    if (qs.FromClause != null)
                    {
                        ProcessFromClause(qs.FromClause);
                    }

                    // Process GROUP BY clause
                    if (qs.GroupByClause != null)
                    {
                        ProcessGroupByClause(qs.GroupByClause);
                    }

                    // Process HAVING clause
                    if (qs.HavingClause != null)
                    {
                        _owner.HasHaving = true;
                        _owner.HavingClauseText = ScriptFragment(qs.HavingClause.SearchCondition);

                        // Collect column references from HAVING
                        var havingColumnVisitor = new ColumnReferenceVisitor();
                        qs.HavingClause.Accept(havingColumnVisitor);
                        foreach (var column in havingColumnVisitor.ColumnReferences)
                        {
                            _owner.HavingColumnReferences.Add(column);
                        }

                        // Build the condition tree for HAVING clause
                        var havingConditionTreeBuilder = new ConditionTreeBuilder();
                        qs.HavingClause.SearchCondition.Accept(havingConditionTreeBuilder);
                        _owner.HavingConditionTree = new ConditionTree(havingConditionTreeBuilder.RootNode);
                    }

                    // Process ORDER BY clause
                    if (qs.OrderByClause != null)
                    {
                        ProcessOrderByClause(qs.OrderByClause);
                    }
                }

                base.Visit(node);
            }

            private void ProcessSelectElements(QuerySpecification qs)
            {
                if (qs.SelectElements == null || qs.SelectElements.Count == 0) return;

                // Check for DISTINCT
                _owner.HasDistinct = qs.UniqueRowFilter == UniqueRowFilter.Distinct;

                foreach (var se in qs.SelectElements)
                {
                    if (se is SelectStarExpression)
                    {
                        // SELECT *
                        _owner.SelectAll = true;
                        _owner.SelectedColumns.Clear();
                        _owner.DetailedColumns.Clear();
                        return;
                    }
                    else if (se is SelectScalarExpression sse)
                    {
                        ProcessSelectScalarExpression(sse);
                    }
                }
            }

            private void ProcessSelectScalarExpression(SelectScalarExpression sse)
            {
                var columnInfo = new ColumnInfo
                {
                    FullText = ScriptFragment(sse),
                    Alias = sse.ColumnName?.Value
                };

                // Process the expression
                if (sse.Expression is ColumnReferenceExpression colRef)
                {
                    // Simple column reference
                    string colName = ExtractColumnNameFromRef(colRef);
                    if (!string.IsNullOrEmpty(colName))
                    {
                        _owner.SelectedColumns.Add(colName);
                        columnInfo.Expression = colName;
                        columnInfo.ReferencedColumns.Add(colName);
                    }
                }
                else if (sse.Expression is FunctionCall funcCall)
                {
                    // Function call (could be an aggregate)
                    ProcessFunctionInSelect(funcCall, columnInfo);
                }
                else if (sse.Expression is ParenthesisExpression parenExpr)
                {
                    // Expression in parentheses
                    columnInfo.IsComputed = true;
                    columnInfo.Expression = ScriptFragment(parenExpr);

                    // Extract column references
                    var colVisitor = new ColumnReferenceVisitor();
                    parenExpr.Accept(colVisitor);
                    foreach (var col in colVisitor.ColumnReferences)
                    {
                        columnInfo.ReferencedColumns.Add(col);
                    }
                }
                else if (sse.Expression is BinaryExpression binExpr)
                {
                    // Binary expression (e.g., a + b)
                    columnInfo.IsComputed = true;
                    columnInfo.Expression = ScriptFragment(binExpr);

                    // Extract column references
                    var colVisitor = new ColumnReferenceVisitor();
                    binExpr.Accept(colVisitor);
                    foreach (var col in colVisitor.ColumnReferences)
                    {
                        columnInfo.ReferencedColumns.Add(col);
                    }
                }
                else if (sse.Expression is CaseExpression caseExpr)
                {
                    // CASE expression
                    columnInfo.IsComputed = true;
                    columnInfo.Expression = ScriptFragment(caseExpr);

                    // Extract column references
                    var colVisitor = new ColumnReferenceVisitor();
                    caseExpr.Accept(colVisitor);
                    foreach (var col in colVisitor.ColumnReferences)
                    {
                        columnInfo.ReferencedColumns.Add(col);
                    }
                }
                else if (sse.Expression is Literal litExpr)
                {
                    // Literal value
                    columnInfo.IsLiteral = true;
                    columnInfo.Expression = ScriptFragment(litExpr);
                }
                else if (sse.Expression is UnaryExpression unaryExpr)
                {
                    // Unary expression (e.g., -a)
                    columnInfo.IsComputed = true;
                    columnInfo.Expression = ScriptFragment(unaryExpr);

                    // Extract column references
                    var colVisitor = new ColumnReferenceVisitor();
                    unaryExpr.Accept(colVisitor);
                    foreach (var col in colVisitor.ColumnReferences)
                    {
                        columnInfo.ReferencedColumns.Add(col);
                    }
                }
                else
                {
                    // Other expressions
                    columnInfo.Expression = ScriptFragment(sse.Expression);
                    columnInfo.IsComputed = true;

                    // Extract column references
                    var colVisitor = new ColumnReferenceVisitor();
                    sse.Expression.Accept(colVisitor);
                    foreach (var col in colVisitor.ColumnReferences)
                    {
                        columnInfo.ReferencedColumns.Add(col);
                    }
                }

                // Add the column info to our list
                _owner.DetailedColumns.Add(columnInfo);

                // Map alias to expression
                if (!string.IsNullOrEmpty(columnInfo.Alias))
                {
                    _owner.ColumnAliases[columnInfo.Alias] = columnInfo.Expression;
                }

                // Add to selected columns if not already added
                if (!string.IsNullOrEmpty(columnInfo.Expression) &&
                    !_owner.SelectedColumns.Contains(columnInfo.Expression) &&
                    !columnInfo.IsComputed && !columnInfo.IsFunction && !columnInfo.IsLiteral)
                {
                    _owner.SelectedColumns.Add(columnInfo.Expression);
                }
            }

            private void ProcessFunctionInSelect(FunctionCall funcCall, ColumnInfo columnInfo)
            {
                string functionName = funcCall.FunctionName.Value ?? string.Empty;
                columnInfo.IsFunction = true;
                columnInfo.FunctionName = functionName;
                columnInfo.Expression = ScriptFragment(funcCall);

                // Check if it's an aggregate function
                bool isAggregate = IsAggregateFunction(functionName);
                columnInfo.IsAggregate = isAggregate;

                if (isAggregate)
                {
                    _owner.HasAggregates = true;

                    // Create aggregate info
                    var aggInfo = new AggregateInfo
                    {
                        FunctionName = functionName,
                        Alias = columnInfo.Alias,
                        FullText = ScriptFragment(funcCall)
                    };

                    // Check for DISTINCT in the aggregate
                    if (string.Equals(funcCall.FunctionName.Value, "COUNT", StringComparison.OrdinalIgnoreCase))
                    {
                        // COUNT might have DISTINCT
                        if (funcCall.Parameters.Count > 0)
                        {
                            // Check if the first parameter is a distinct expression
                            // In SQL Server ScriptDom, DISTINCT isn't directly accessible in the object model
                            // We have to infer it from the SQL text or the parameter properties
                            var param = funcCall.Parameters[0];
                            string paramText = ScriptFragment(param);
                            aggInfo.IsDistinct = paramText.Contains("DISTINCT", StringComparison.OrdinalIgnoreCase);

                            // Extract the column name from the parameter
                            if (param is ColumnReferenceExpression colRef)
                            {
                                aggInfo.Column = ExtractColumnNameFromRef(colRef);
                            }
                            else
                            {
                                aggInfo.Column = paramText;
                            }
                        }
                        else
                        {
                            aggInfo.Column = "*"; // COUNT(*)
                        }
                    }
                    else if (funcCall.Parameters.Count > 0)
                    {
                        // Other aggregates (SUM, AVG, MIN, MAX)
                        var param = funcCall.Parameters[0];
                        string paramText = ScriptFragment(param);

                        // Check for DISTINCT
                        aggInfo.IsDistinct = paramText.Contains("DISTINCT", StringComparison.OrdinalIgnoreCase);

                        // Extract the column reference
                        if (param is ColumnReferenceExpression colRef)
                        {
                            aggInfo.Column = ExtractColumnNameFromRef(colRef);
                        }
                        else
                        {
                            aggInfo.Column = paramText;
                        }
                    }

                    _owner.Aggregates.Add(aggInfo);
                }

                // Extract column references from function parameters
                var colVisitor = new ColumnReferenceVisitor();
                funcCall.Accept(colVisitor);
                foreach (var col in colVisitor.ColumnReferences)
                {
                    columnInfo.ReferencedColumns.Add(col);
                }
            }

            private void ProcessTopClause(TopRowFilter topRowFilter)
            {
                _owner.HasTopClause = true;
                _owner.TopIsPercent = topRowFilter.Percent;

                // Extract TOP value
                if (topRowFilter.Expression is Literal litExpr)
                {
                    _owner.TopValue = litExpr.Value;
                }
                else
                {
                    _owner.TopValue = ScriptFragment(topRowFilter.Expression);
                }
            }

            #endregion

            #region FROM Clause Processing

            private void ProcessFromClause(FromClause fromClause)
            {
                if (fromClause.TableReferences == null || fromClause.TableReferences.Count == 0) return;

                // Process each table reference
                foreach (var tableRef in fromClause.TableReferences)
                {
                    ProcessTableReference(tableRef);
                }
            }

            private void ProcessTableReference(TableReference tableRef)
            {
                if (tableRef is NamedTableReference namedTable)
                {
                    // Simple table reference
                    ProcessNamedTable(namedTable);
                }
                else if (tableRef is QualifiedJoin qualJoin)
                {
                    // JOIN operation
                    ProcessJoin(qualJoin);
                }
                else if (tableRef is JoinParenthesisTableReference parenTable && parenTable.Join != null)
                {
                    // Table reference in parentheses or with alias
                    ProcessTableReference(parenTable.Join);
                }
                else if (tableRef is QueryDerivedTable derivedTable)
                {
                    // Subquery in FROM clause
                    var tableInfo = new TableInfo
                    {
                        Name = "<derived table>",
                        Alias = derivedTable.Alias?.Value,
                        FullText = ScriptFragment(derivedTable)
                    };
                    _owner.Tables.Add(tableInfo);

                    // Process the subquery
                    if (derivedTable.QueryExpression != null)
                    {
                        // We could recursively analyze the subquery if needed
                        // But for now, we just note that it exists
                    }
                }
            }

            private void ProcessNamedTable(NamedTableReference namedTable)
            {
                var tableInfo = new TableInfo
                {
                    FullText = ScriptFragment(namedTable)
                };

                // Extract table name and schema
                if (namedTable.SchemaObject != null && namedTable.SchemaObject.Identifiers.Count > 0)
                {
                    // Last identifier is the table name
                    var identifiers = namedTable.SchemaObject.Identifiers;
                    tableInfo.Name = identifiers[identifiers.Count - 1].Value ?? string.Empty;

                    // If we have multiple identifiers, the second-to-last is the schema
                    if (identifiers.Count > 1)
                    {
                        tableInfo.Schema = identifiers[identifiers.Count - 2].Value;
                    }
                }

                // Extract alias
                tableInfo.Alias = namedTable.Alias?.Value;

                _owner.Tables.Add(tableInfo);
            }

            private void ProcessJoin(QualifiedJoin join)
            {
                _owner.HasJoins = true;

                var joinInfo = new JoinInfo
                {
                    JoinType = join.QualifiedJoinType.ToString(),
                    Condition = join.SearchCondition != null ? ScriptFragment(join.SearchCondition) : string.Empty
                };

                // Process left and right tables
                if (join.FirstTableReference is NamedTableReference leftTable)
                {
                    joinInfo.LeftTable = GetNamedTableIdentifier(leftTable);
                    ProcessNamedTable(leftTable);
                }
                else if (join.FirstTableReference != null)
                {
                    joinInfo.LeftTable = "<complex table>";
                    ProcessTableReference(join.FirstTableReference);
                }

                if (join.SecondTableReference is NamedTableReference rightTable)
                {
                    joinInfo.RightTable = GetNamedTableIdentifier(rightTable);
                    ProcessNamedTable(rightTable);
                }
                else if (join.SecondTableReference != null)
                {
                    joinInfo.RightTable = "<complex table>";
                    ProcessTableReference(join.SecondTableReference);
                }

                // Extract column references from the join condition
                if (join.SearchCondition != null)
                {
                    var colVisitor = new ColumnReferenceVisitor();
                    join.SearchCondition.Accept(colVisitor);
                    foreach (var col in colVisitor.ColumnReferences)
                    {
                        joinInfo.ReferencedColumns.Add(col);
                    }
                }

                _owner.Joins.Add(joinInfo);
            }

            private static string GetNamedTableIdentifier(NamedTableReference namedTable)
            {
                if (namedTable.SchemaObject != null && namedTable.SchemaObject.Identifiers.Count > 0)
                {
                    var identifiers = namedTable.SchemaObject.Identifiers;
                    string tableName = identifiers[identifiers.Count - 1].Value ?? string.Empty;

                    // Include alias if specified
                    if (namedTable.Alias != null && !string.IsNullOrEmpty(namedTable.Alias.Value))
                    {
                        return string.Format(CultureInfo.InvariantCulture, "{0} AS {1}", tableName,
                            namedTable.Alias.Value);
                    }

                    return tableName;
                }

                return string.Empty;
            }

            #endregion

            #region GROUP BY, HAVING, and ORDER BY Processing

            private void ProcessGroupByClause(GroupByClause groupByClause)
            {
                _owner.HasGroupBy = true;

                foreach (var groupByItem in groupByClause.GroupingSpecifications)
                {
                    if (groupByItem is ExpressionGroupingSpecification exprGroup)
                    {
                        if (exprGroup.Expression is ColumnReferenceExpression colRef)
                        {
                            string colName = ExtractColumnNameFromRef(colRef);
                            if (!string.IsNullOrEmpty(colName))
                            {
                                _owner.GroupByColumns.Add(colName);
                            }
                        }
                        else
                        {
                            // For expressions, just add the scripted text
                            _owner.GroupByColumns.Add(ScriptFragment(exprGroup.Expression));
                        }
                    }
                }
            }

            private void ProcessOrderByClause(OrderByClause orderByClause)
            {
                _owner.HasOrderBy = true;

                foreach (var orderByItem in orderByClause.OrderByElements)
                {
                    var orderByInfo = new OrderByInfo
                    {
                        IsDescending = orderByItem.SortOrder == SortOrder.Descending,
                        FullText = ScriptFragment(orderByItem)
                    };

                    if (orderByItem.Expression is ColumnReferenceExpression colRef)
                    {
                        orderByInfo.ColumnName = ExtractColumnNameFromRef(colRef);
                    }
                    else
                    {
                        // For expressions, use the full text
                        orderByInfo.ColumnName = ScriptFragment(orderByItem.Expression);
                    }

                    _owner.OrderByColumns.Add(orderByInfo);
                }
            }

            #endregion

            #region Helper Methods

            private static string ExtractColumnNameFromRef(ColumnReferenceExpression colRef)
            {
                if (colRef.MultiPartIdentifier != null && colRef.MultiPartIdentifier.Identifiers.Count > 0)
                {
                    // For qualified names (t.column), get just the column part (last identifier)
                    var identifiers = colRef.MultiPartIdentifier.Identifiers;
                    return identifiers[identifiers.Count - 1].Value ?? string.Empty;
                }

                return string.Empty;
            }

            private static bool IsAggregateFunction(string functionName)
            {
                if (string.IsNullOrEmpty(functionName)) return false;

                var aggregateFunctions = new HashSet<string>(StringComparer.OrdinalIgnoreCase)
                {
                    "COUNT", "SUM", "AVG", "MIN", "MAX", "STDEV", "STDEVP", "VAR", "VARP",
                    "CHECKSUM_AGG", "COUNT_BIG", "GROUPING", "GROUPING_ID", "STRING_AGG"
                };

                return aggregateFunctions.Contains(functionName);
            }

            #endregion
        }

        #endregion

        #region Column Reference Visitor

        /// <summary>
        /// Visitor that collects column references from expressions.
        /// </summary>
        private sealed class ColumnReferenceVisitor : TSqlFragmentVisitor
        {
            /// <summary>
            /// List of column names found.
            /// </summary>
            public HashSet<string> ColumnReferences { get; } = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

            public override void Visit(ColumnReferenceExpression node)
            {
                if (node.MultiPartIdentifier != null && node.MultiPartIdentifier.Identifiers.Count > 0)
                {
                    // The last identifier is the column name
                    var identifiers = node.MultiPartIdentifier.Identifiers;
                    string columnName = identifiers[identifiers.Count - 1].Value ?? string.Empty;

                    if (!string.IsNullOrEmpty(columnName))
                    {
                        ColumnReferences.Add(columnName);
                    }
                }

                base.Visit(node);
            }
        }

        #endregion

        #region Helper Methods

        /// <summary>
        /// Renders (scripts) a TSqlFragment back to a string, 
        /// preserving T-SQL formatting as best as possible.
        /// </summary>
        private static string ScriptFragment(TSqlFragment fragment)
        {
            if (fragment == null) return string.Empty;

            var generator = new Sql150ScriptGenerator(new SqlScriptGeneratorOptions
            {
                KeywordCasing = KeywordCasing.Uppercase
            });

            generator.GenerateScript(fragment, out string script);
            return script.Trim();
        }

        /// <summary>
        /// Checks if the string is a common SQL keyword.
        /// </summary>
        private static bool IsSqlKeyword(string word)
        {
            if (string.IsNullOrEmpty(word)) return false;

            // Common SQL keywords that might appear in expressions
            HashSet<string> keywords = new HashSet<string>(StringComparer.OrdinalIgnoreCase)
            {
                "SELECT", "FROM", "WHERE", "AND", "OR", "NOT", "IN", "BETWEEN", "LIKE",
                "IS", "NULL", "TRUE", "FALSE", "AS", "ON", "JOIN", "INNER", "OUTER",
                "LEFT", "RIGHT", "FULL", "CROSS", "GROUP", "BY", "HAVING", "ORDER",
                "DESC", "ASC", "LIMIT", "OFFSET", "UNION", "ALL", "DISTINCT", "TOP"
            };

            return keywords.Contains(word);
        }

        /// <summary>
        /// Returns a simple string representation of the query structure for debugging.
        /// </summary>
        public string GetQuerySummary()
        {
            var sb = new StringBuilder();

            sb.AppendLine("QUERY SUMMARY:");
            sb.AppendLine(string.Format(CultureInfo.InvariantCulture, "Statement Found: {0}", StatementFound));
            sb.AppendLine(string.Format(CultureInfo.InvariantCulture, "Select All (*): {0}", SelectAll));

            if (SelectedColumns.Count > 0)
            {
                sb.AppendLine("Selected Columns:");
                foreach (var col in SelectedColumns)
                {
                    sb.AppendLine(string.Format(CultureInfo.InvariantCulture, "  - {0}", col));
                }
            }

            if (DetailedColumns.Count > 0)
            {
                sb.AppendLine("Detailed Columns:");
                foreach (var col in DetailedColumns)
                {
                    sb.AppendLine(string.Format(CultureInfo.InvariantCulture,
                        "  - {0}{1}{2}{3}",
                        col.Expression,
                        col.Alias != null ? string.Format(CultureInfo.InvariantCulture, " AS {0}", col.Alias) : "",
                        col.IsFunction
                            ? string.Format(CultureInfo.InvariantCulture, " (Function: {0})", col.FunctionName)
                            : "",
                        col.IsAggregate ? " (Aggregate)" : ""));
                }
            }

            if (!string.IsNullOrEmpty(WhereClauseText))
            {
                sb.AppendLine(string.Format(CultureInfo.InvariantCulture, "Where Clause: {0}", WhereClauseText));
                sb.AppendLine("Where Column References:");
                foreach (var col in WhereColumnReferences)
                {
                    sb.AppendLine(string.Format(CultureInfo.InvariantCulture, "  - {0}", col));
                }
            }

            if (HasGroupBy)
            {
                sb.AppendLine("Group By Columns:");
                foreach (var col in GroupByColumns)
                {
                    sb.AppendLine(string.Format(CultureInfo.InvariantCulture, "  - {0}", col));
                }
            }

            if (HasHaving)
            {
                sb.AppendLine(string.Format(CultureInfo.InvariantCulture, "Having Clause: {0}", HavingClauseText));
                sb.AppendLine("Having Column References:");
                foreach (var col in HavingColumnReferences)
                {
                    sb.AppendLine(string.Format(CultureInfo.InvariantCulture, "  - {0}", col));
                }
            }

            if (HasOrderBy)
            {
                sb.AppendLine("Order By Columns:");
                foreach (var col in OrderByColumns)
                {
                    sb.AppendLine(string.Format(CultureInfo.InvariantCulture,
                        "  - {0} {1}", col.ColumnName, col.IsDescending ? "DESC" : "ASC"));
                }
            }

            if (Tables.Count > 0)
            {
                sb.AppendLine("Tables:");
                foreach (var table in Tables)
                {
                    sb.AppendLine(string.Format(CultureInfo.InvariantCulture,
                        "  - {0}{1}{2}",
                        table.Name,
                        table.Schema != null
                            ? string.Format(CultureInfo.InvariantCulture, " (Schema: {0})", table.Schema)
                            : "",
                        table.Alias != null
                            ? string.Format(CultureInfo.InvariantCulture, " AS {0}", table.Alias)
                            : ""));
                }
            }

            if (HasJoins)
            {
                sb.AppendLine("Joins:");
                foreach (var join in Joins)
                {
                    sb.AppendLine(string.Format(CultureInfo.InvariantCulture,
                        "  - {0} JOIN {1} ON {2}", join.JoinType, join.RightTable, join.Condition));
                }
            }

            if (HasAggregates)
            {
                sb.AppendLine("Aggregates:");
                foreach (var agg in Aggregates)
                {
                    sb.AppendLine(string.Format(CultureInfo.InvariantCulture,
                        "  - {0}({1}{2}){3}",
                        agg.FunctionName,
                        agg.IsDistinct ? "DISTINCT " : "",
                        agg.Column,
                        agg.Alias != null ? string.Format(CultureInfo.InvariantCulture, " AS {0}", agg.Alias) : ""));
                }
            }

            return sb.ToString();
        }
        #endregion

        #region Condition Tree Implementation

        /// <summary>
        /// Base class for all condition nodes in the SQL condition tree.
        /// </summary>
        public abstract class ConditionNode
        {
            /// <summary>
            /// Evaluates this condition against the specified row data.
            /// </summary>
            public abstract bool Evaluate(IDictionary<string, object> rowData);
        }

        /// <summary>
        /// Represents a logical operator (AND, OR, NOT) in the condition tree.
        /// </summary>
        public sealed class LogicalOperatorNode : ConditionNode
        {
            /// <summary>
            /// Type of logical operator.
            /// </summary>
            public enum LogicalOperatorType
            {
                And,
                Or,
                Not
            }

            /// <summary>
            /// The operator type.
            /// </summary>
            public LogicalOperatorType OperatorType { get; }

            /// <summary>
            /// Left operand (maybe null for NOT).
            /// </summary>
            public ConditionNode? Left { get; }

            /// <summary>
            /// Right operand (maybe null for NOT).
            /// </summary>
            public ConditionNode? Right { get; }

            public LogicalOperatorNode(LogicalOperatorType operatorType, ConditionNode? left, ConditionNode? right)
            {
                OperatorType = operatorType;
                Left = left;
                Right = right;
            }

            public override bool Evaluate(IDictionary<string, object> rowData)
            {
                return OperatorType switch
                {
                    LogicalOperatorType.And => Left!.Evaluate(rowData) && Right!.Evaluate(rowData),
                    LogicalOperatorType.Or => Left!.Evaluate(rowData) || Right!.Evaluate(rowData),
                    LogicalOperatorType.Not => !Right!.Evaluate(rowData),
                    _ => throw new NotSupportedException($"Logical operator {OperatorType} not supported.")
                };
            }
        }

        /// <summary>
        /// Represents a comparison operation in the condition tree.
        /// </summary>
        public sealed class ComparisonNode : ConditionNode
        {
            /// <summary>
            /// Type of comparison operation.
            /// </summary>
            public enum ComparisonType
            {
                Equals,
                NotEquals,
                LessThan,
                LessThanOrEqual,
                GreaterThan,
                GreaterThanOrEqual,
                Like,
                In,
                Between,
                IsNull,
                IsNotNull
            }

            /// <summary>
            /// The comparison type.
            /// </summary>
            public ComparisonType Comparison { get; }

            /// <summary>
            /// Left operand of the comparison (usually a column).
            /// </summary>
            public ExpressionNode Left { get; }

            /// <summary>
            /// Right operand of the comparison (usually a value).
            /// </summary>
            public ExpressionNode? Right { get; }

            /// <summary>
            /// Second right operand (for BETWEEN).
            /// </summary>
            public ExpressionNode? Right2 { get; }

            /// <summary>
            /// List of values for IN comparison.
            /// </summary>
            public List<ExpressionNode>? InValues { get; }

            public ComparisonNode(ComparisonType comparison, ExpressionNode left, ExpressionNode? right = null,
                                    ExpressionNode? right2 = null, List<ExpressionNode>? inValues = null)
            {
                Comparison = comparison;
                Left = left;
                Right = right;
                Right2 = right2;
                InValues = inValues;
            }

            public override bool Evaluate(IDictionary<string, object> rowData)
            {
                object? leftValue = Left.GetValue(rowData);

                switch (Comparison)
                {
                    case ComparisonType.IsNull:
                        return leftValue == null || leftValue == DBNull.Value;

                    case ComparisonType.IsNotNull:
                        return leftValue != null && leftValue != DBNull.Value;

                    case ComparisonType.In:
                        if (leftValue == null) return false;
                        return InValues!.Any(v => CompareValues(leftValue, v.GetValue(rowData), ComparisonType.Equals));

                    case ComparisonType.Between:
                        if (leftValue == null) return false;
                        object? lower = Right!.GetValue(rowData);
                        object? upper = Right2!.GetValue(rowData);
                        return CompareValues(leftValue, lower, ComparisonType.GreaterThanOrEqual) &&
                                CompareValues(leftValue, upper, ComparisonType.LessThanOrEqual);

                    default:
                        if (Right == null) throw new InvalidOperationException($"Right operand missing for {Comparison} comparison");
                        object? rightValue = Right.GetValue(rowData);
                        return CompareValues(leftValue, rightValue, Comparison);
                }
            }

            private static bool CompareValues(object? left, object? right, ComparisonType comparisonType)
            {
                // Handle null values
                if (left == null || left == DBNull.Value || right == null || right == DBNull.Value)
                {
                    // Special handling for NULL comparisons
                    // In SQL, NULL compared to anything (including another NULL) is UNKNOWN, not TRUE or FALSE
                    // The only exceptions are IS NULL and IS NOT NULL
                    return false;
                }

                // Try to convert types for comparison
                if (left.GetType() != right.GetType())
                {
                    try
                    {
                        if (left is IConvertible convertibleLeft && right is IConvertible convertibleRight)
                        {
                            // Convert right to match left's type if possible
                            Type leftType = left.GetType();
                            right = Convert.ChangeType(right, leftType, CultureInfo.InvariantCulture);
                        }
                    }
                    catch (InvalidCastException)
                    {
                        // Failed type conversion, can't compare
                        return false;
                    }
                    catch (FormatException)
                    {
                        // Failed type conversion, can't compare
                        return false;
                    }
                }

                // If the objects are comparable
                if (left is IComparable comparable)
                {
                    int result = comparable.CompareTo(right);

                    return comparisonType switch
                    {
                        ComparisonType.Equals => result == 0,
                        ComparisonType.NotEquals => result != 0,
                        ComparisonType.LessThan => result < 0,
                        ComparisonType.LessThanOrEqual => result <= 0,
                        ComparisonType.GreaterThan => result > 0,
                        ComparisonType.GreaterThanOrEqual => result >= 0,
                        ComparisonType.Like when left is string leftStr && right is string rightStr =>
                            SqlLikeOperator.Like(leftStr, rightStr),
                        _ => false
                    };
                }

                // For non-comparable objects, we can only check equality
                return comparisonType == ComparisonType.Equals ? left.Equals(right) : false;
            }
        }

        /// <summary>
        /// Base class for expression nodes.
        /// </summary>
        public abstract class ExpressionNode
        {
            /// <summary>
            /// Gets the value of this expression from the row data.
            /// </summary>
            public abstract object? GetValue(IDictionary<string, object> rowData);
        }

        /// <summary>
        /// Represents a column reference in an expression.
        /// </summary>
        public sealed class ColumnReferenceNode : ExpressionNode
        {
            /// <summary>
            /// The name of the column.
            /// </summary>
            public string ColumnName { get; }

            public ColumnReferenceNode(string columnName)
            {
                ColumnName = columnName;
            }

            public override object? GetValue(IDictionary<string, object> rowData)
            {
                return rowData.TryGetValue(ColumnName, out object? value) ? value : null;
            }
        }

        /// <summary>
        /// Represents a literal value in an expression.
        /// </summary>
        public sealed class LiteralNode : ExpressionNode
        {
            /// <summary>
            /// The literal value.
            /// </summary>
            public object? Value { get; }

            /// <summary>
            /// The SQL type of the literal.
            /// </summary>
            public SqlDataType DataType { get; }

            public LiteralNode(object? value, SqlDataType dataType)
            {
                Value = value;
                DataType = dataType;
            }

            public override object? GetValue(IDictionary<string, object> rowData)
            {
                return Value;
            }
        }

        /// <summary>
        /// Represents a parameter (@Name) in an expression.
        /// </summary>
        public sealed class ParameterNode : ExpressionNode
        {
            /// <summary>
            /// The name of the parameter (including @).
            /// </summary>
            public string ParameterName { get; }

            public ParameterNode(string parameterName)
            {
                ParameterName = parameterName;
            }

            public override object? GetValue(IDictionary<string, object> rowData)
            {
                string key = ParameterName.TrimStart('@');
                return rowData.TryGetValue(key, out object? value) ? value : null;
            }
        }

        /// <summary>
        /// Represents a function call in an expression.
        /// </summary>
        public sealed class FunctionNode : ExpressionNode
        {
            /// <summary>
            /// The name of the function.
            /// </summary>
            public string FunctionName { get; }

            /// <summary>
            /// The arguments to the function.
            /// </summary>
            public List<ExpressionNode> Arguments { get; }

            public FunctionNode(string functionName, List<ExpressionNode> arguments)
            {
                FunctionName = functionName;
                Arguments = arguments;
            }

            [SuppressMessage("Globalization", "CA1308:Normalize strings to uppercase", Justification = "Implementing SQL LOWER() function which requires lowercase")]
            public override object? GetValue(IDictionary<string, object> rowData)
            {
                // Get argument values
                object?[] args = Arguments.Select(arg => arg.GetValue(rowData)).ToArray();

                // Execute the function based on its name
                return FunctionName.ToUpperInvariant() switch
                {
                    "YEAR" when args.Length == 1 && args[0] is DateTime date => date.Year,
                    "MONTH" when args.Length == 1 && args[0] is DateTime date => date.Month,
                    "DAY" when args.Length == 1 && args[0] is DateTime date => date.Day,
                    "GETDATE" when args.Length == 0 => DateTime.Now,
                    "DATEADD" when args.Length == 3 && args[1] is int value =>
                        AddToDate(args[0]?.ToString(), value, args[2] as DateTime?),
                    "UPPER" when args.Length == 1 && args[0] is string str => str.ToUpperInvariant(),
                    "LOWER" when args.Length == 1 && args[0] is string str => str.ToLowerInvariant(),
                    _ => throw new NotSupportedException($"Function {FunctionName} not supported or invalid arguments.")
                };
            }

            private static DateTime? AddToDate(string? part, int value, DateTime? date)
            {
                if (date == null || part == null) return null;

                return part.ToUpperInvariant() switch
                {
                    "YEAR" or "YY" or "YYYY" => date.Value.AddYears(value),
                    "MONTH" or "MM" or "M" => date.Value.AddMonths(value),
                    "DAY" or "DD" or "D" => date.Value.AddDays(value),
                    "HOUR" or "HH" => date.Value.AddHours(value),
                    "MINUTE" or "MI" or "N" => date.Value.AddMinutes(value),
                    "SECOND" or "SS" or "S" => date.Value.AddSeconds(value),
                    _ => throw new ArgumentException($"Invalid date part: {part}")
                };
            }
        }

        /// <summary>
        /// Represents a SQL data type.
        /// </summary>
        public enum SqlDataType
        {
            Unknown,
            String,
            Int,
            Decimal,
            DateTime,
            Boolean,
            Null
        }

        /// <summary>
        /// Utility for SQL LIKE operator.
        /// </summary>
        public static class SqlLikeOperator
        {
            /// <summary>
            /// Implements the SQL LIKE operator.
            /// </summary>
            public static bool Like(string input, string pattern)
            {
                // Convert SQL LIKE pattern to regex
                string regexPattern = "^" + Regex.Escape(pattern)
                    .Replace("%", ".*", StringComparison.Ordinal)
                    .Replace("_", ".", StringComparison.Ordinal)
                    .Replace(@"\[", "[", StringComparison.Ordinal)
                    .Replace(@"\]", "]", StringComparison.Ordinal)
                    .Replace(@"\^", "^", StringComparison.Ordinal) + "$";

                return Regex.IsMatch(input, regexPattern, RegexOptions.IgnoreCase | RegexOptions.CultureInvariant);
            }
        }

        /// <summary>
        /// The root of a condition tree, representing a complete WHERE or HAVING clause.
        /// </summary>
        public sealed class ConditionTree
        {
            /// <summary>
            /// The root node of the condition tree.
            /// </summary>
            public ConditionNode? Root { get; }

            public ConditionTree(ConditionNode? root)
            {
                Root = root;
            }

            /// <summary>
            /// Evaluates the condition tree against the specified row data.
            /// </summary>
            public bool Evaluate(IDictionary<string, object> rowData)
            {
                return Root == null || Root.Evaluate(rowData);
            }
        }

        #endregion

        #region Condition Tree Builder
        /// <summary>
        /// Builds a condition tree from SQL search conditions.
        /// </summary>
        private sealed class ConditionTreeBuilder : TSqlFragmentVisitor
        {
            private ConditionNode? _currentNode;
            private readonly Dictionary<string, ExpressionNode> _parameters = new Dictionary<string, ExpressionNode>(StringComparer.OrdinalIgnoreCase);

            public ConditionNode? RootNode => _currentNode;


            public override void Visit(BooleanBinaryExpression node)
            {
                // Process the left sub-expression
                node.FirstExpression.Accept(this);
                ConditionNode? leftNode = _currentNode;

                // Process the right sub-expression
                node.SecondExpression.Accept(this);
                ConditionNode? rightNode = _currentNode;

                if (leftNode != null && rightNode != null)
                {
                    var opType = node.BinaryExpressionType switch
                    {
                        BooleanBinaryExpressionType.And => LogicalOperatorNode.LogicalOperatorType.And,
                        BooleanBinaryExpressionType.Or => LogicalOperatorNode.LogicalOperatorType.Or,
                        _ => throw new NotSupportedException($"Unsupported boolean operator: {node.BinaryExpressionType}")
                    };

                    _currentNode = new LogicalOperatorNode(opType, leftNode, rightNode);
                }
            }

            private void ProcessComparisonExpression(BooleanComparisonExpression compExpr)
            {
                // Extract left and right expressions
                ExpressionNode? leftExpr = ProcessScalarExpression(compExpr.FirstExpression);
                ExpressionNode? rightExpr = ProcessScalarExpression(compExpr.SecondExpression);

                if (leftExpr == null || rightExpr == null) return;

                // Determine comparison type
                ComparisonNode.ComparisonType comparisonType;

                // Use string comparison since property names might differ across versions
                string compTypeStr = compExpr.ComparisonType.ToString();

                comparisonType = compTypeStr switch
                {
                    var s when s.Contains("Equals", StringComparison.Ordinal) => ComparisonNode.ComparisonType.Equals,
                    var s when s.Contains("NotEqual", StringComparison.Ordinal) || s.Contains("NotEquals", StringComparison.Ordinal) => ComparisonNode.ComparisonType.NotEquals,
                    var s when s.Contains("GreaterThan", StringComparison.Ordinal) && s.Contains("Equal", StringComparison.Ordinal) => ComparisonNode.ComparisonType.GreaterThanOrEqual,
                    var s when s.Contains("LessThan", StringComparison.Ordinal) && s.Contains("Equal", StringComparison.Ordinal) => ComparisonNode.ComparisonType.LessThanOrEqual,
                    var s when s.Contains("GreaterThan", StringComparison.Ordinal) => ComparisonNode.ComparisonType.GreaterThan,
                    var s when s.Contains("LessThan", StringComparison.Ordinal) => ComparisonNode.ComparisonType.LessThan,
                    _ => ComparisonNode.ComparisonType.Equals // Default
                };

                _currentNode = new ComparisonNode(comparisonType, leftExpr, rightExpr);
            }

            private void ProcessIsNullExpression(BooleanIsNullExpression isNullExpr)
            {
                ExpressionNode? expr = ProcessScalarExpression(isNullExpr.Expression);
                if (expr == null) return;

                ComparisonNode.ComparisonType comparisonType = isNullExpr.IsNot
                    ? ComparisonNode.ComparisonType.IsNotNull
                    : ComparisonNode.ComparisonType.IsNull;

                _currentNode = new ComparisonNode(comparisonType, expr);
            }

            private void ProcessInPredicate(InPredicate inPredicate)
            {
                ExpressionNode? expr = ProcessScalarExpression(inPredicate.Expression);
                if (expr == null) return;

                var inValues = new List<ExpressionNode>();
                if (inPredicate.Values != null)
                {
                    foreach (var value in inPredicate.Values)
                    {
                        ExpressionNode? valueExpr = ProcessScalarExpression(value);
                        if (valueExpr != null)
                        {
                            inValues.Add(valueExpr);
                        }
                    }
                }

                ComparisonNode.ComparisonType comparisonType = inPredicate.NotDefined
                    ? ComparisonNode.ComparisonType.NotEquals   // NOT IN is handled as multiple NOT EQUALS
                    : ComparisonNode.ComparisonType.In;

                _currentNode = new ComparisonNode(comparisonType, expr, null, null, inValues);
            }

            // Corrected to handle BetweenPredicate
            private void ProcessBetweenPredicate(ScalarExpression expression, ScalarExpression lowerBound, ScalarExpression upperBound)
            {
                ExpressionNode? expr = ProcessScalarExpression(expression);
                ExpressionNode? lower = ProcessScalarExpression(lowerBound);
                ExpressionNode? upper = ProcessScalarExpression(upperBound);

                if (expr == null || lower == null || upper == null) return;

                // Instead of creating ScriptDom objects, create our own ConditionNode objects directly
                _currentNode = new ComparisonNode(
                    ComparisonNode.ComparisonType.Between,
                    expr,
                    lower,
                    upper
                );
            }

            private void ProcessLikePredicate(LikePredicate likePredicate)
            {
                ExpressionNode? expr = ProcessScalarExpression(likePredicate.FirstExpression);
                ExpressionNode? patternExpr = ProcessScalarExpression(likePredicate.SecondExpression);

                if (expr == null || patternExpr == null) return;

                ComparisonNode.ComparisonType comparisonType = likePredicate.NotDefined
                    ? ComparisonNode.ComparisonType.NotEquals   // NOT LIKE is handled as NOT EQUALS
                    : ComparisonNode.ComparisonType.Like;

                _currentNode = new ComparisonNode(comparisonType, expr, patternExpr);
            }

            private ExpressionNode? ProcessScalarExpression(ScalarExpression? expression)
            {
                if (expression == null) return null;

                if (expression is ColumnReferenceExpression colRef)
                {
                    // Handle column references
                    if (colRef.MultiPartIdentifier != null && colRef.MultiPartIdentifier.Identifiers.Count > 0)
                    {
                        var identifiers = colRef.MultiPartIdentifier.Identifiers;
                        string columnName = identifiers[identifiers.Count - 1].Value ?? string.Empty;

                        return new ColumnReferenceNode(columnName);
                    }
                }
                else if (expression is Literal literal)
                {
                    // Handle literals
                    return ProcessLiteral(literal);
                }
                else if (expression is VariableReference varRef)
                {
                    // Handle parameters (@Name)
                    string paramName = varRef.Name;
                    if (_parameters.TryGetValue(paramName, out ExpressionNode? paramNode))
                    {
                        return paramNode;
                    }

                    paramNode = new ParameterNode(paramName);
                    _parameters[paramName] = paramNode;
                    return paramNode;
                }
                else if (expression is FunctionCall funcCall)
                {
                    // Handle function calls
                    return ProcessFunction(funcCall);
                }
                else if (expression is UnaryExpression unaryExpr)
                {
                    // Handle unary expressions (e.g., -a)
                    var innerExpr = ProcessScalarExpression(unaryExpr.Expression);

                    if (innerExpr is LiteralNode litNode && unaryExpr.UnaryExpressionType == UnaryExpressionType.Negative)
                    {
                        // For numeric literals, negate the value
                        if (litNode.Value is int intVal)
                        {
                            return new LiteralNode(-intVal, SqlDataType.Int);
                        }
                        else if (litNode.Value is decimal decVal)
                        {
                            return new LiteralNode(-decVal, SqlDataType.Decimal);
                        }
                        else if (litNode.Value is double dblVal)
                        {
                            return new LiteralNode(-dblVal, SqlDataType.Decimal);
                        }
                    }

                    return innerExpr; // For now, just return the inner expression
                }
                else if (expression is ParenthesisExpression parenExpr)
                {
                    // Handle expressions in parentheses
                    return ProcessScalarExpression(parenExpr.Expression);
                }

                return null;
            }

            private static LiteralNode ProcessLiteral(Literal literal)
            {
                SqlDataType dataType = SqlDataType.Unknown;
                object? value = null;

                if (literal is NullLiteral)
                {
                    dataType = SqlDataType.Null;
                    value = null;
                }
                else if (literal is StringLiteral strLiteral)
                {
                    dataType = SqlDataType.String;
                    value = strLiteral.Value;
                }
                else if (literal is IntegerLiteral intLiteral)
                {
                    dataType = SqlDataType.Int;
                    if (int.TryParse(intLiteral.Value, System.Globalization.CultureInfo.InvariantCulture, out int intValue))
                    {
                        value = intValue;
                    }
                }
                else if (literal is NumericLiteral numLiteral)
                {
                    dataType = SqlDataType.Decimal;
                    if (decimal.TryParse(numLiteral.Value, System.Globalization.CultureInfo.InvariantCulture, out decimal decValue))
                    {
                        value = decValue;
                    }
                }
                else if (literal is BinaryLiteral binLiteral)
                {
                    // Handle binary literals (0x...)
                    dataType = SqlDataType.String; // Default to string for now
                    value = binLiteral.Value;
                }
                else if (literal is MoneyLiteral moneyLiteral)
                {
                    dataType = SqlDataType.Decimal;
                    string moneyStr = moneyLiteral.Value;
                    if (moneyStr.StartsWith('$'))
                    {
                        moneyStr = moneyStr[1..];
                    }

                    if (decimal.TryParse(moneyStr, System.Globalization.CultureInfo.InvariantCulture, out decimal decValue))
                    {
                        value = decValue;
                    }
                }
                else if (literal is DefaultLiteral)
                {
                    // DEFAULT keyword - for now, just return null
                    dataType = SqlDataType.Null;
                    value = null;
                }

                return new LiteralNode(value, dataType);
            }

            // Make this non-static since it needs to call ProcessScalarExpression
            private FunctionNode? ProcessFunction(FunctionCall funcCall)
            {
                string functionName = funcCall.FunctionName.Value ?? string.Empty;
                var arguments = new List<ExpressionNode>();

                foreach (var param in funcCall.Parameters)
                {
                    ExpressionNode? paramNode = ProcessScalarExpression(param);
                    if (paramNode != null)
                    {
                        arguments.Add(paramNode);
                    }
                }

                return new FunctionNode(functionName, arguments);
            }
        }

        #endregion
    }
}
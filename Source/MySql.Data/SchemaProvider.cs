// Copyright © 2004, 2013, Oracle and/or its affiliates. All rights reserved.
//
// MySQL Connector/NET is licensed under the terms of the GPLv2
// <http://www.gnu.org/licenses/old-licenses/gpl-2.0.html>, like most 
// MySQL Connectors. There are special exceptions to the terms and 
// conditions of the GPLv2 as it is applied to this software, see the 
// FLOSS License Exception
// <http://www.mysql.com/about/legal/licensing/foss-exception.html>.
//
// This program is free software; you can redistribute it and/or modify 
// it under the terms of the GNU General Public License as published 
// by the Free Software Foundation; version 2 of the License.
//
// This program is distributed in the hope that it will be useful, but 
// WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY 
// or FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License 
// for more details.
//
// You should have received a copy of the GNU General Public License along 
// with this program; if not, write to the Free Software Foundation, Inc., 
// 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA

using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.IO;
using System.Reflection;
using System.Text;
using System.Text.RegularExpressions;
using MySql.Data.Constants.ColumnNames;
using MySql.Data.Constants.ColumnNames.Constraints;
using MySql.Data.Constants.ColumnNames.Columns;
using MySql.Data.Constants.ColumnNames.Indexes;
using MySql.Data.Constants.ColumnNames.Procedures;
using MySql.Data.Constants.ColumnNames.Tables;
using MySql.Data.Constants.ColumnNames.Shared;
using MySql.Data.MySqlClient.Properties;
using MySql.Data.Types;
using MySql.Data.Constants.Types;

namespace MySql.Data.MySqlClient {
    internal class SchemaProvider {
        protected MySqlConnection Connection;
        public static string MetaCollection = "MetaDataCollections";
        public SchemaProvider( MySqlConnection connectionToUse ) { Connection = connectionToUse; }
        public virtual MySqlSchemaCollection GetSchema( string collection, string[] restrictions ) {
            if ( Connection.State != ConnectionState.Open ) throw new MySqlException( "GetSchema can only be called on an open connection." );
            var c = GetSchemaInternal( collection.InvariantToUpper(), restrictions );
            if ( c == null ) throw new ArgumentException( "Invalid collection name" );
            return c;
        }
        public virtual MySqlSchemaCollection GetDatabases( string[] restrictions ) {
            Regex regex = null;
            var caseSetting = Int32.Parse( Connection.Driver.Property( "lower_case_table_names" ) );
            var sql = "SHOW DATABASES";
            // if lower_case_table_names is zero, then case lookup should be sensitive
            // so we can use LIKE to do the matching.
            if ( caseSetting == 0 && restrictions?.Length >= 1 )
                sql += string.Format( " LIKE '{0}'", restrictions[ 0 ] );

            var c = QueryCollection( "Databases", sql );

            if ( caseSetting != 0 && restrictions?[ 0 ] != null ) regex = new Regex( restrictions[ 0 ], RegexOptions.IgnoreCase );

            var c2 = new MySqlSchemaCollection( "Databases" );
            c2.AddColumn( "CATALOG_NAME", TString );
            c2.AddColumn( SchemaName, TString );

            foreach ( var row in c.Rows ) {
                if ( regex != null
                     && !regex.Match( row[ 0 ].ToString() ).Success ) continue;
                var newRow = c2.AddRow();
                newRow[ 1 ] = row[ 0 ];
            }
            return c2;
        }
        public virtual MySqlSchemaCollection GetTables( string[] restrictions ) {
            var c = new MySqlSchemaCollection( "Tables" );
            c.AddColumn( TableCatalog, TString );
            c.AddColumn( TableSchema, TString );
            c.AddColumn( TableName, TString );
            c.AddColumn( TableType, TString );
            c.AddColumn( Engine, TString );
            c.AddColumn( Tables.Version, TUInt64 );
            c.AddColumn( RowFormat, TString );
            c.AddColumn( TableRows, TUInt64 );
            c.AddColumn( AvgRowLength, TUInt64 );
            c.AddColumn( DataLength, TUInt64 );
            c.AddColumn( MaxDataLength, TUInt64 );
            c.AddColumn( IndexLength, TUInt64 );
            c.AddColumn( DataFree, TUInt64 );
            c.AddColumn( AutoIncrement, TUInt64 );
            c.AddColumn( CreateTime, TDateTime );
            c.AddColumn( UpdateTime, TDateTime );
            c.AddColumn( CheckTime, TDateTime );
            c.AddColumn( TableCollation, TString );
            c.AddColumn( Checksum, TUInt64 );
            c.AddColumn( CreateOptions, TString );
            c.AddColumn( TableComment, TString );

            // we have to new up a new restriction array here since
            // GetDatabases takes the database in the first slot
            var dbRestriction = new string[4];
            if ( restrictions != null
                 && restrictions.Length >= 2 ) dbRestriction[ 0 ] = restrictions[ 1 ];
            var databases = GetDatabases( dbRestriction );

            if ( restrictions != null ) Array.Copy( restrictions, dbRestriction, Math.Min( dbRestriction.Length, restrictions.Length ) );

            foreach ( var row in databases.Rows ) {
                dbRestriction[ 1 ] = row[ SchemaName ].ToString();
                FindTables( c, dbRestriction );
            }
            return c;
        }

        protected void QuoteDefaultValues( MySqlSchemaCollection schemaCollection ) {
            if ( schemaCollection == null ) return;
            if ( !schemaCollection.ContainsColumn( ColumnDefault ) ) return;
            foreach ( var row in schemaCollection.Rows ) {
                var defaultValue = row[ ColumnDefault ];
                if ( MetaData.IsTextType( row[ DataType ].ToString() ) ) row[ ColumnDefault ] = String.Format( "{0}", defaultValue );
            }
        }

        public virtual MySqlSchemaCollection GetColumns( string[] restrictions ) {
            var c = new MySqlSchemaCollection( "Columns" );
            c.AddColumn( TableCatalog, TString );
            c.AddColumn( TableSchema, TString );
            c.AddColumn( TableName, TString );
            c.AddColumn( ColumnName, TString );
            c.AddColumn( OrdinalPosition, TUInt64 );
            c.AddColumn( ColumnDefault, TString );
            c.AddColumn( IsNullable, TString );
            c.AddColumn( DataType, TString );
            c.AddColumn( CharacterMaximumLength, TUInt64 );
            c.AddColumn( CharacterOctetLength, TUInt64 );
            c.AddColumn( NumericPrecision, TUInt64 );
            c.AddColumn( NumericScale, TUInt64 );
            c.AddColumn( CharacterSetName, TString );
            c.AddColumn( CollationName, TString );
            c.AddColumn( ColumnType, TString );
            c.AddColumn( ColumnKey, TString );
            c.AddColumn( Extra, TString );
            c.AddColumn( Privileges, TString );
            c.AddColumn( ColumnComment, TString );

            // we don't allow restricting on table type here
            string columnName = null;
            if ( restrictions != null
                 && restrictions.Length == 4 ) {
                columnName = restrictions[ 3 ];
                restrictions[ 3 ] = null;
            }
            var tables = GetTables( restrictions );

            foreach ( var row in tables.Rows ) LoadTableColumns( c, row[ TableSchema ].ToString(), row[ TableName ].ToString(), columnName );

            QuoteDefaultValues( c );
            return c;
        }

        private void LoadTableColumns( MySqlSchemaCollection schemaCollection, string schema, string tableName, string columnRestriction ) {
            var sql = String.Format( "SHOW FULL COLUMNS FROM `{0}`.`{1}`", schema, tableName );
            var cmd = new MySqlCommand( sql, Connection );

            var pos = 1;
            using ( var reader = cmd.ExecuteReader() )
                while ( reader.Read() ) {
                    var colName = reader.GetString( 0 );
                    if ( columnRestriction != null
                         && colName != columnRestriction ) continue;
                    var row = schemaCollection.AddRow();
                    row[ TableCatalog ] = DBNull.Value;
                    row[ TableSchema ] = schema;
                    row[ TableName ] = tableName;
                    row[ ColumnName ] = colName;
                    row[ OrdinalPosition ] = pos++;
                    row[ ColumnDefault ] = reader.GetValue( 5 );
                    row[ IsNullable ] = reader.GetString( 3 );
                    row[ DataType ] = reader.GetString( 1 );
                    row[ CharacterMaximumLength ] = DBNull.Value;
                    row[ CharacterOctetLength ] = DBNull.Value;
                    row[ NumericPrecision ] = DBNull.Value;
                    row[ NumericScale ] = DBNull.Value;
                    row[ CharacterSetName ] = reader.GetValue( 2 );
                    row[ CollationName ] = row[ CharacterSetName ];
                    row[ ColumnType ] = reader.GetString( 1 );
                    row[ ColumnKey ] = reader.GetString( 4 );
                    row[ Extra ] = reader.GetString( 6 );
                    row[ Privileges ] = reader.GetString( 7 );
                    row[ ColumnComment ] = reader.GetString( 8 );
                    ParseColumnRow( row );
                }
        }

        private static void ParseColumnRow( MySqlSchemaRow row ) {
            // first parse the character set name
            var charset = row[ CharacterSetName ].ToString();
            var index = charset.IndexOf( '_' );
            if ( index != -1 ) row[ CharacterSetName ] = charset.Substring( 0, index );

            // now parse the data type
            var dataType = row[ DataType ].ToString();
            index = dataType.IndexOf( '(' );
            if ( index == -1 ) return;
            row[ DataType ] = dataType.Substring( 0, index );
            var stop = dataType.IndexOf( ')', index );
            var dataLen = dataType.Substring( index + 1, stop - ( index + 1 ) );
            var lowerType = row[ DataType ].ToString().ToLower();
            switch ( lowerType ) {
                case "char":
                case "varchar":
                    row[ CharacterMaximumLength ] = dataLen;
                    break;
                case "real":
                case "decimal":
                    var lenparts = dataLen.Split( ',' );
                    row[ NumericPrecision ] = lenparts[ 0 ];
                    if ( lenparts.Length == 2 ) row[ NumericScale ] = lenparts[ 1 ];
                    break;
            }
        }

        public virtual MySqlSchemaCollection GetIndexes( string[] restrictions ) {
            var dt = new MySqlSchemaCollection( "Indexes" );
            dt.AddColumn( IndexCatalog, TString );
            dt.AddColumn( IndexSchema, TString );
            dt.AddColumn( IndexName, TString );
            dt.AddColumn( TableName, TString );
            dt.AddColumn( Unique, TBoolean );
            dt.AddColumn( Primary, TBoolean );
            dt.AddColumn( Indexes.Type, TString );
            dt.AddColumn( Comment, TString );

            // Get the list of tables first
            var max = restrictions?.Length ?? 4;
            var tableRestrictions = new string[Math.Max( max, 4 )];
            restrictions?.CopyTo( tableRestrictions, 0 );
            tableRestrictions[ 3 ] = "BASE TABLE";
            var tables = GetTables( tableRestrictions );

            foreach ( var table in tables.Rows ) {
                var sql = String.Format(
                    "SHOW INDEX FROM `{0}`.`{1}`",
                    MySqlHelper.DoubleQuoteString( (string) table[ TableSchema ] ),
                    MySqlHelper.DoubleQuoteString( (string) table[ TableName ] ) );
                var indexes = QueryCollection( "indexes", sql );

                foreach ( var index in indexes.Rows ) {
                    var seqIndex = (long) index[ "SEQ_IN_INDEX" ];
                    if ( seqIndex != 1 ) continue;
                    if ( restrictions != null
                         && restrictions.Length == 4
                         && restrictions[ 3 ] != null
                         && !index[ KeyName ].Equals( restrictions[ 3 ] ) ) continue;
                    var row = dt.AddRow();
                    row[ IndexCatalog ] = null;
                    row[ IndexSchema ] = table[ TableSchema ];
                    row[ IndexName ] = index[ KeyName ];
                    row[ TableName ] = index[ Table ];
                    row[ Unique ] = (long) index[ NonUnique ] == 0;
                    row[ Primary ] = index[ KeyName ].Equals( Primary );
                    row[ Indexes.Type ] = index[ IndexType ];
                    row[ Comment ] = index[ Comment ];
                }
            }

            return dt;
        }

        public virtual MySqlSchemaCollection GetIndexColumns( string[] restrictions ) {
            var dt = new MySqlSchemaCollection( "IndexColumns" );
            dt.AddColumn( IndexCatalog, TString );
            dt.AddColumn( IndexSchema, TString );
            dt.AddColumn( IndexName, TString );
            dt.AddColumn( TableName, TString );
            dt.AddColumn( ColumnName, TString );
            dt.AddColumn( OrdinalPosition, TInt32 );
            dt.AddColumn( SortOrder, TString );

            var max = restrictions?.Length ?? 4;
            var tableRestrictions = new string[Math.Max( max, 4 )];
            restrictions?.CopyTo( tableRestrictions, 0 );
            tableRestrictions[ 3 ] = "BASE TABLE";
            var tables = GetTables( tableRestrictions );

            foreach ( var table in tables.Rows ) {
                var sql = String.Format( "SHOW INDEX FROM `{0}`.`{1}`", table[ TableSchema ], table[ TableName ] );
                var cmd = new MySqlCommand( sql, Connection );
                using ( var reader = cmd.ExecuteReader() )
                    while ( reader.Read() ) {
                        var keyName = GetString( reader, reader.GetOrdinal( KeyName ) );
                        var colName = GetString( reader, reader.GetOrdinal( ColumnName ) );

                        if ( restrictions != null ) {
                            if ( restrictions.Length >= 4
                                 && restrictions[ 3 ] != null
                                 && keyName != restrictions[ 3 ] ) continue;
                            if ( restrictions.Length >= 5
                                 && restrictions[ 4 ] != null
                                 && colName != restrictions[ 4 ] ) continue;
                        }
                        var row = dt.AddRow();
                        row[ IndexCatalog ] = null;
                        row[ IndexSchema ] = table[ TableSchema ];
                        row[ IndexName ] = keyName;
                        row[ TableName ] = GetString( reader, reader.GetOrdinal( Table ) );
                        row[ ColumnName ] = colName;
                        row[ OrdinalPosition ] = reader.GetValue( reader.GetOrdinal( "SEQ_IN_INDEX" ) );
                        row[ SortOrder ] = reader.GetString( "COLLATION" );
                    }
            }

            return dt;
        }

        public virtual MySqlSchemaCollection GetForeignKeys( string[] restrictions ) {
            var dt = new MySqlSchemaCollection( "Foreign Keys" );
            dt.AddColumn( ConstraintCatalog, TString );
            dt.AddColumn( ConstraintSchema, TString );
            dt.AddColumn( ConstraintName, TString );
            dt.AddColumn( TableCatalog, TString );
            dt.AddColumn( TableSchema, TString );
            dt.AddColumn( TableName, TString );
            dt.AddColumn( MatchOption, TString );
            dt.AddColumn( UpdateRule, TString );
            dt.AddColumn( DeleteRule, TString );
            dt.AddColumn( ReferencedTableCatalog, TString );
            dt.AddColumn( ReferencedTableSchema, TString );
            dt.AddColumn( ReferencedTableName, TString );

            // first we use our restrictions to get a list of tables that should be
            // consulted.  We save the keyname restriction since GetTables doesn't 
            // understand that.
            string keyName = null;
            if ( restrictions != null
                 && restrictions.Length >= 4 ) {
                keyName = restrictions[ 3 ];
                restrictions[ 3 ] = null;
            }

            var tables = GetTables( restrictions );

            // now for each table retrieved, we call our helper function to
            // parse it's foreign keys
            foreach ( var table in tables.Rows ) GetForeignKeysOnTable( dt, table, keyName, false );

            return dt;
        }

        public virtual MySqlSchemaCollection GetForeignKeyColumns( string[] restrictions ) {
            var dt = new MySqlSchemaCollection( "Foreign Keys" );
            dt.AddColumn( ConstraintCatalog, TString );
            dt.AddColumn( ConstraintSchema, TString );
            dt.AddColumn( ConstraintName, TString );
            dt.AddColumn( TableCatalog, TString );
            dt.AddColumn( TableSchema, TString );
            dt.AddColumn( TableName, TString );
            dt.AddColumn( ColumnName, TString );
            dt.AddColumn( OrdinalPosition, TInt32 );
            dt.AddColumn( ReferencedTableCatalog, TString );
            dt.AddColumn( ReferencedTableSchema, TString );
            dt.AddColumn( ReferencedTableName, TString );
            dt.AddColumn( ReferencedColumnName, TString );

            // first we use our restrictions to get a list of tables that should be
            // consulted.  We save the keyname restriction since GetTables doesn't 
            // understand that.
            string keyName = null;
            if ( restrictions != null
                 && restrictions.Length >= 4 ) {
                keyName = restrictions[ 3 ];
                restrictions[ 3 ] = null;
            }

            var tables = GetTables( restrictions );

            // now for each table retrieved, we call our helper function to
            // parse it's foreign keys
            foreach ( var table in tables.Rows ) GetForeignKeysOnTable( dt, table, keyName, true );
            return dt;
        }

        private string GetSqlMode() => new MySqlCommand( "SELECT @@SQL_MODE", Connection ).ExecuteScalar().ToString();

        #region Foreign Key routines
        /// <summary>
        /// GetForeignKeysOnTable retrieves the foreign keys on the given table.
        /// Since MySQL supports foreign keys on versions prior to 5.0, we can't  use
        /// information schema.  MySQL also does not include any type of SHOW command
        /// for foreign keys so we have to resort to use SHOW CREATE TABLE and parsing
        /// the output.
        /// </summary>
        /// <param name="fkTable">The table to store the key info in.</param>
        /// <param name="tableToParse">The table to get the foeign key info for.</param>
        /// <param name="filterName">Only get foreign keys that match this name.</param>
        /// <param name="includeColumns">Should column information be included in the table.</param>
        private void GetForeignKeysOnTable(
            MySqlSchemaCollection fkTable,
            MySqlSchemaRow tableToParse,
            string filterName,
            bool includeColumns ) {
            var sqlMode = GetSqlMode();

            //todo: check not used variable
            if ( filterName != null ) filterName = filterName.InvariantToLower();

            var sql = string.Format( "SHOW CREATE TABLE `{0}`.`{1}`", tableToParse[ TableSchema ], tableToParse[ TableName ] );
            string lowerBody;
            var cmd = new MySqlCommand( sql, Connection );
            using ( var reader = cmd.ExecuteReader() ) {
                reader.Read();
                var body = reader.GetString( 1 );
                lowerBody = body.InvariantToLower();
            }

            var tokenizer = new MySqlTokenizer( lowerBody ) {
                AnsiQuotes = sqlMode.InvariantContains("ANSI_QUOTES"),
                BackslashEscapes = sqlMode.InvariantContains( "NO_BACKSLASH_ESCAPES")
            };

            while ( true ) {
                var token = tokenizer.NextToken();
                // look for a starting contraint
                while ( token != null
                        && ( token != "constraint" || tokenizer.Quoted ) ) token = tokenizer.NextToken();
                if ( token == null ) break;

                ParseConstraint( fkTable, tableToParse, tokenizer, includeColumns );
            }
        }

        private static void ParseConstraint(
            MySqlSchemaCollection fkTable,
            MySqlSchemaRow table,
            MySqlTokenizer tokenizer,
            bool includeColumns ) {
            var name = tokenizer.NextToken();
            var row = fkTable.AddRow();

            // make sure this constraint is a FK
            var token = tokenizer.NextToken();
            if ( token != "foreign"
                 || tokenizer.Quoted ) return;
            tokenizer.NextToken(); // read off the 'KEY' symbol
            tokenizer.NextToken(); // read off the '(' symbol

            row[ ConstraintCatalog ] = table[ TableCatalog ];
            row[ ConstraintSchema ] = table[ TableSchema ];
            row[ TableCatalog ] = table[ TableCatalog ];
            row[ TableSchema ] = table[ TableSchema ];
            row[ TableName ] = table[ TableName ];
            row[ ReferencedTableCatalog ] = null;
            row[ ConstraintName ] = name.Trim( '\'', '`' );

            var srcColumns = includeColumns ? ParseColumns( tokenizer ) : null;

            // now look for the references section
            while ( token != "references"
                    || tokenizer.Quoted ) token = tokenizer.NextToken();
            var target1 = tokenizer.NextToken();
            var target2 = tokenizer.NextToken();
            if ( target2.InvariantStartsWith( ".") ) {
                row[ ReferencedTableSchema ] = target1;
                row[ ReferencedTableName ] = target2.Substring( 1 ).Trim( '\'', '`' );
                tokenizer.NextToken(); // read off the '('
            }
            else {
                row[ ReferencedTableSchema ] = table[ TableSchema ];
                row[ ReferencedTableName ] = target1.Substring( 1 ).Trim( '\'', '`' );
            }

            // if we are supposed to include columns, read the target columns
            var targetColumns = includeColumns ? ParseColumns( tokenizer ) : null;

            if ( includeColumns ) ProcessColumns( fkTable, row, srcColumns, targetColumns );
            else fkTable.Rows.Add( row );
        }

        private static List<string> ParseColumns( MySqlTokenizer tokenizer ) {
            var sc = new List<string>();
            var token = tokenizer.NextToken();
            while ( token != ")" ) {
                if ( token != "," ) sc.Add( token );
                token = tokenizer.NextToken();
            }
            return sc;
        }

        private static void ProcessColumns(
            MySqlSchemaCollection fkTable,
            MySqlSchemaRow row,
            List<string> srcColumns,
            List<string> targetColumns ) {
            for ( var i = 0; i < srcColumns.Count; i++ ) {
                var newRow = fkTable.AddRow();
                row.CopyRow( newRow );
                newRow[ ColumnName ] = srcColumns[ i ];
                newRow[ OrdinalPosition ] = i;
                newRow[ ReferencedColumnName ] = targetColumns[ i ];
                fkTable.Rows.Add( newRow );
            }
        }
        #endregion

        public virtual MySqlSchemaCollection GetUsers( string[] restrictions ) {
            var sb = new StringBuilder( "SELECT Host, User FROM mysql.user" );
            if ( restrictions != null
                 && restrictions.Length > 0 ) sb.InvariantAppendFormat( " WHERE User LIKE '{0}'", restrictions[ 0 ] );

            var c = QueryCollection( "Users", sb.ToString() );
            c.Columns[ 0 ].Name = "HOST";
            c.Columns[ 1 ].Name = "USERNAME";

            return c;
        }

        public virtual MySqlSchemaCollection GetProcedures( string[] restrictions ) {
            var dt = new MySqlSchemaCollection( "Procedures" );
            dt.AddColumn( SpecificName, TString );
            dt.AddColumn( RoutineCatalog, TString );
            dt.AddColumn( RoutineSchema, TString );
            dt.AddColumn( RoutineName, TString );
            dt.AddColumn( RoutineType, TString );
            dt.AddColumn( DtdIdentifier, TString );
            dt.AddColumn( RoutineBody, TString );
            dt.AddColumn( RoutineDefinition, TString );
            dt.AddColumn( ExternalName, TString );
            dt.AddColumn( ExternalLanguage, TString );
            dt.AddColumn( ParameterStyle, TString );
            dt.AddColumn( IsDeterministic, TString );
            dt.AddColumn( SqlDataAccess, TString );
            dt.AddColumn( SqlPath, TString );
            dt.AddColumn( SecurityType, TString );
            dt.AddColumn( Created, TDateTime );
            dt.AddColumn( LastAltered, TDateTime );
            dt.AddColumn( SqlMode, TString );
            dt.AddColumn( RoutineComment, TString );
            dt.AddColumn( Definer, TString );

            var sql = new StringBuilder( "SELECT * FROM mysql.proc WHERE 1=1" );
            if ( restrictions != null ) {
                if ( restrictions.Length >= 2
                     && restrictions[ 1 ] != null ) sql.InvariantAppendFormat( " AND db LIKE '{0}'", restrictions[ 1 ] );
                if ( restrictions.Length >= 3
                     && restrictions[ 2 ] != null ) sql.InvariantAppendFormat( " AND name LIKE '{0}'", restrictions[ 2 ] );
                if ( restrictions.Length >= 4
                     && restrictions[ 3 ] != null ) sql.InvariantAppendFormat( " AND type LIKE '{0}'", restrictions[ 3 ] );
            }

            var cmd = new MySqlCommand( sql.ToString(), Connection );
            using ( var reader = cmd.ExecuteReader() )
                while ( reader.Read() ) {
                    var row = dt.AddRow();
                    row[ SpecificName ] = reader.GetString( "specific_name" );
                    row[ RoutineCatalog ] = DBNull.Value;
                    row[ RoutineSchema ] = reader.GetString( "db" );
                    row[ RoutineName ] = reader.GetString( "name" );
                    var routineType = reader.GetString( "type" );
                    row[ RoutineType ] = routineType;
                    row[ DtdIdentifier ] = routineType.InvariantToLower() == "function"
                                                  ? (object) reader.GetString( "returns" )
                                                  : DBNull.Value;
                    row[ RoutineBody ] = "SQL";
                    row[ RoutineDefinition ] = reader.GetString( "body" );
                    row[ ExternalName ] = DBNull.Value;
                    row[ ExternalLanguage ] = DBNull.Value;
                    row[ ParameterStyle ] = "SQL";
                    row[ IsDeterministic ] = reader.GetString( "is_deterministic" );
                    row[ SqlDataAccess ] = reader.GetString( "sql_data_access" );
                    row[ SqlPath ] = DBNull.Value;
                    row[ SecurityType ] = reader.GetString( "security_type" );
                    row[ Created ] = reader.GetDateTime( "created" );
                    row[ LastAltered ] = reader.GetDateTime( "modified" );
                    row[ SqlMode ] = reader.GetString( "sql_mode" );
                    row[ RoutineComment ] = reader.GetString( "comment" );
                    row[ Definer ] = reader.GetString( "definer" );
                }

            return dt;
        }

        protected virtual MySqlSchemaCollection GetCollections() {
            var collections = new[] {
                new object[] { "MetaDataCollections", 0, 0 }, new object[] { "DataSourceInformation", 0, 0 },
                new object[] { "DataTypes", 0, 0 }, new object[] { "Restrictions", 0, 0 }, new object[] { "ReservedWords", 0, 0 },
                new object[] { "Databases", 1, 1 }, new object[] { "Tables", 4, 2 }, new object[] { "Columns", 4, 4 },
                new object[] { "Users", 1, 1 }, new object[] { "Foreign Keys", 4, 3 }, new object[] { "IndexColumns", 5, 4 },
                new object[] { "Indexes", 4, 3 }, new object[] { "Foreign Key Columns", 4, 3 }, new object[] { "UDF", 1, 1 }
            };

            var dt = new MySqlSchemaCollection( "MetaDataCollections" );
            dt.AddColumn( "CollectionName", TString );
            dt.AddColumn( "NumberOfRestrictions", TInt32 );
            dt.AddColumn( "NumberOfIdentifierParts", TInt32 );

            FillTable( dt, collections );

            return dt;
        }

        private MySqlSchemaCollection GetDataSourceInformation() {
            var dt = new MySqlSchemaCollection( "DataSourceInformation" );
            dt.AddColumn( "CompositeIdentifierSeparatorPattern", TString );
            dt.AddColumn( "DataSourceProductName", TString );
            dt.AddColumn( "DataSourceProductVersion", TString );
            dt.AddColumn( "DataSourceProductVersionNormalized", TString );
            dt.AddColumn( "GroupByBehavior", TGroupByBehavior );
            dt.AddColumn( "IdentifierPattern", TString );
            dt.AddColumn( "IdentifierCase", TIdentifierCase );
            dt.AddColumn( "OrderByColumnsInSelect", TBoolean );
            dt.AddColumn( "ParameterMarkerFormat", TString );
            dt.AddColumn( "ParameterMarkerPattern", TString );
            dt.AddColumn( "ParameterNameMaxLength", TInt32 );
            dt.AddColumn( "ParameterNamePattern", TString );
            dt.AddColumn( "QuotedIdentifierPattern", TString );
            dt.AddColumn( "QuotedIdentifierCase", TIdentifierCase );
            dt.AddColumn( "StatementSeparatorPattern", TString );
            dt.AddColumn( "StringLiteralPattern", TString );
            dt.AddColumn( "SupportedJoinOperators", TSupportedJoinOperators );

            var v = Connection.Driver.Version;
            var ver = String.Format( "{0:0}.{1:0}.{2:0}", v.Major, v.Minor, v.Build );

            var row = dt.AddRow();
            row[ "CompositeIdentifierSeparatorPattern" ] = "\\.";
            row[ "DataSourceProductName" ] = "MySQL";
            row[ "DataSourceProductVersion" ] = Connection.ServerVersion;
            row[ "DataSourceProductVersionNormalized" ] = ver;
            row[ "GroupByBehavior" ] = GroupByBehavior.Unrelated;
            row[ "IdentifierPattern" ] =
                @"(^\`\p{Lo}\p{Lu}\p{Ll}_@#][\p{Lo}\p{Lu}\p{Ll}\p{Nd}@$#_]*$)|(^\`[^\`\0]|\`\`+\`$)|(^\"" + [^\""\0]|\""\""+\""$)";
            row[ "IdentifierCase" ] = IdentifierCase.Insensitive;
            row[ "OrderByColumnsInSelect" ] = false;
            row[ "ParameterMarkerFormat" ] = "{0}";
            row[ "ParameterMarkerPattern" ] = "(@[A-Za-z0-9_$#]*)";
            row[ "ParameterNameMaxLength" ] = 128;
            row[ "ParameterNamePattern" ] = @"^[\p{Lo}\p{Lu}\p{Ll}\p{Lm}_@#][\p{Lo}\p{Lu}\p{Ll}\p{Lm}\p{Nd}\uff3f_@#\$]*(?=\s+|$)";
            row[ "QuotedIdentifierPattern" ] = @"(([^\`]|\`\`)*)";
            row[ "QuotedIdentifierCase" ] = IdentifierCase.Sensitive;
            row[ "StatementSeparatorPattern" ] = ";";
            row[ "StringLiteralPattern" ] = "'(([^']|'')*)'";
            row[ "SupportedJoinOperators" ] = 15;
            dt.Rows.Add( row );

            return dt;
        }

        private static MySqlSchemaCollection GetDataTypes() {
            var dt = new MySqlSchemaCollection( "DataTypes" );
            dt.AddColumn( "TypeName", TString );
            dt.AddColumn( "ProviderDbType", TInt32 );
            dt.AddColumn( "ColumnSize", TInt64 );
            dt.AddColumn( "CreateFormat", TString );
            dt.AddColumn( "CreateParameters", TString );
            dt.AddColumn( "DataType", TString );
            dt.AddColumn( "IsAutoincrementable", TBoolean );
            dt.AddColumn( "IsBestMatch", TBoolean );
            dt.AddColumn( "IsCaseSensitive", TBoolean );
            dt.AddColumn( "IsFixedLength", TBoolean );
            dt.AddColumn( "IsFixedPrecisionScale", TBoolean );
            dt.AddColumn( "IsLong", TBoolean );
            dt.AddColumn( "IsNullable", TBoolean );
            dt.AddColumn( "IsSearchable", TBoolean );
            dt.AddColumn( "IsSearchableWithLike", TBoolean );
            dt.AddColumn( "IsUnsigned", TBoolean );
            dt.AddColumn( "MaximumScale", TInt16 );
            dt.AddColumn( "MinimumScale", TInt16 );
            dt.AddColumn( "IsConcurrencyType", TBoolean );
            dt.AddColumn( "IsLiteralSupported", TBoolean );
            dt.AddColumn( "LiteralPrefix", TString );
            dt.AddColumn( "LiteralSuffix", TString );
            dt.AddColumn( "NativeDataType", TString );

            // have each one of the types contribute to the datatypes collection
            MySqlBit.SetDsInfo( dt );
            MySqlBinary.SetDsInfo( dt );
            MySqlDateTime.SetDsInfo( dt );
            MySqlTimeSpan.SetDsInfo( dt );
            MySqlString.SetDsInfo( dt );
            MySqlDouble.SetDsInfo( dt );
            MySqlSingle.SetDsInfo( dt );
            MySqlByte.SetDsInfo( dt );
            MySqlInt16.SetDsInfo( dt );
            MySqlInt32.SetDsInfo( dt );
            MySqlInt64.SetDsInfo( dt );
            MySqlDecimal.SetDsInfo( dt );
            MySqlUByte.SetDsInfo( dt );
            MySqlUInt16.SetDsInfo( dt );
            MySqlUInt32.SetDsInfo( dt );
            MySqlUInt64.SetDsInfo( dt );

            return dt;
        }

        protected virtual MySqlSchemaCollection GetRestrictions() {
            var restrictions = new[] {
                new object[] { "Users", "Name", "", 0 }, new object[] { "Databases", "Name", "", 0 },
                new object[] { "Tables", "Database", "", 0 }, new object[] { "Tables", "Schema", "", 1 },
                new object[] { "Tables", "Table", "", 2 }, new object[] { "Tables", "TableType", "", 3 },
                new object[] { "Columns", "Database", "", 0 }, new object[] { "Columns", "Schema", "", 1 },
                new object[] { "Columns", "Table", "", 2 }, new object[] { "Columns", "Column", "", 3 },
                new object[] { "Indexes", "Database", "", 0 }, new object[] { "Indexes", "Schema", "", 1 },
                new object[] { "Indexes", "Table", "", 2 }, new object[] { "Indexes", "Name", "", 3 },
                new object[] { "IndexColumns", "Database", "", 0 }, new object[] { "IndexColumns", "Schema", "", 1 },
                new object[] { "IndexColumns", "Table", "", 2 }, new object[] { "IndexColumns", "ConstraintName", "", 3 },
                new object[] { "IndexColumns", "Column", "", 4 }, new object[] { "Foreign Keys", "Database", "", 0 },
                new object[] { "Foreign Keys", "Schema", "", 1 }, new object[] { "Foreign Keys", "Table", "", 2 },
                new object[] { "Foreign Keys", "Constraint Name", "", 3 }, new object[] { "Foreign Key Columns", "Catalog", "", 0 },
                new object[] { "Foreign Key Columns", "Schema", "", 1 }, new object[] { "Foreign Key Columns", "Table", "", 2 },
                new object[] { "Foreign Key Columns", "Constraint Name", "", 3 }, new object[] { "UDF", "Name", "", 0 }
            };

            var dt = new MySqlSchemaCollection( "Restrictions" );
            dt.AddColumn( "CollectionName", TString );
            dt.AddColumn( "RestrictionName", TString );
            dt.AddColumn( "RestrictionDefault", TString );
            dt.AddColumn( "RestrictionNumber", TInt32 );

            FillTable( dt, restrictions );

            return dt;
        }

        private static MySqlSchemaCollection GetReservedWords() {
            var dt = new MySqlSchemaCollection( "ReservedWords" );
            dt.AddColumn( DbMetaDataColumnNames.ReservedWord, TString );
            var str = Assembly.GetExecutingAssembly().GetManifestResourceStream( "MySql.Data.MySqlClient.Properties.ReservedWords.txt" );
            var sr = new StreamReader( str );
            var line = sr.ReadLine();
            while ( line != null ) {
                var keywords = line.Split( ' ' );
                foreach ( var s in keywords ) {
                    if ( String.IsNullOrEmpty( s ) ) continue;
                    var row = dt.AddRow();
                    row[ 0 ] = s;
                }
                line = sr.ReadLine();
            }
            sr.Dispose();
            str?.Close();

            return dt;
        }

        protected static void FillTable( MySqlSchemaCollection dt, object[][] data ) {
            foreach ( var dataItem in data ) {
                var row = dt.AddRow();
                for ( var i = 0; i < dataItem.Length; i++ ) row[ i ] = dataItem[ i ];
            }
        }

        private void FindTables( MySqlSchemaCollection schema, string[] restrictions ) {
            var sql = new StringBuilder();
            var where = new StringBuilder();
            sql.InvariantAppendFormat( "SHOW TABLE STATUS FROM `{0}`", restrictions[ 1 ] );
            if ( restrictions.Length >= 3
                 && restrictions[ 2 ] != null ) where.InvariantAppendFormat( " LIKE '{0}'", restrictions[ 2 ] );
            sql.Append( @where );

            var tableType = restrictions[ 1 ].ToLower() == "information_schema" ? "SYSTEM VIEW" : "BASE TABLE";

            var cmd = new MySqlCommand( sql.ToString(), Connection );
            using ( var reader = cmd.ExecuteReader() )
                while ( reader.Read() ) {
                    var row = schema.AddRow();
                    row[ TableCatalog ] = null;
                    row[ TableSchema ] = restrictions[ 1 ];
                    row[ TableName ] = reader.GetString( 0 );
                    row[ TableType ] = tableType;
                    row[ Engine ] = GetString( reader, 1 );
                    row[ Tables.Version ] = reader.GetValue( 2 );
                    row[ RowFormat ] = GetString( reader, 3 );
                    row[ TableRows ] = reader.GetValue( 4 );
                    row[ AvgRowLength ] = reader.GetValue( 5 );
                    row[ DataLength ] = reader.GetValue( 6 );
                    row[ MaxDataLength ] = reader.GetValue( 7 );
                    row[ IndexLength ] = reader.GetValue( 8 );
                    row[ DataFree ] = reader.GetValue( 9 );
                    row[ AutoIncrement ] = reader.GetValue( 10 );
                    row[ CreateTime ] = reader.GetValue( 11 );
                    row[ UpdateTime ] = reader.GetValue( 12 );
                    row[ CheckTime ] = reader.GetValue( 13 );
                    row[ TableCollation ] = GetString( reader, 14 );
                    row[ Checksum ] = reader.GetValue( 15 );
                    row[ CreateOptions ] = GetString( reader, 16 );
                    row[ TableComment ] = GetString( reader, 17 );
                }
        }

        private static string GetString( MySqlDataReader reader, int index ) => reader.IsDBNull( index ) ? null : reader.GetString( index );

        public virtual MySqlSchemaCollection GetUdf( string[] restrictions ) {
            var sql = "SELECT name,ret,dl FROM mysql.func";
            if ( restrictions?.Length >= 1
                 && !String.IsNullOrEmpty( restrictions[ 0 ] ) ) sql += String.Format( " WHERE name LIKE '{0}'", restrictions[ 0 ] );

            var dt = new MySqlSchemaCollection( "User-defined Functions" );
            dt.AddColumn( "NAME", TString );
            dt.AddColumn( "RETURN_TYPE", TInt32 );
            dt.AddColumn( "LIBRARY_NAME", TString );

            var cmd = new MySqlCommand( sql, Connection );
            try {
                using ( var reader = cmd.ExecuteReader() )
                    while ( reader.Read() ) {
                        var row = dt.AddRow();
                        row[ 0 ] = reader.GetString( 0 );
                        row[ 1 ] = reader.GetInt32( 1 );
                        row[ 2 ] = reader.GetString( 2 );
                    }
            }
            catch ( MySqlException ex ) {
                if ( ex.Number != (int) MySqlErrorCode.TableAccessDenied ) throw;
                throw new MySqlException( Resources.UnableToEnumerateUDF, ex );
            }

            return dt;
        }

        protected virtual MySqlSchemaCollection GetSchemaInternal( string collection, string[] restrictions ) {
            switch ( collection ) {
                // common collections
                case "METADATACOLLECTIONS":
                    return GetCollections();
                case "DATASOURCEINFORMATION":
                    return GetDataSourceInformation();
                case "DATATYPES":
                    return GetDataTypes();
                case "RESTRICTIONS":
                    return GetRestrictions();
                case "RESERVEDWORDS":
                    return GetReservedWords();

                // collections specific to our provider
                case "USERS":
                    return GetUsers( restrictions );
                case "DATABASES":
                    return GetDatabases( restrictions );
                case "UDF":
                    return GetUdf( restrictions );
            }

            // if we have a current database and our users have
            // not specified a database, then default to the currently
            // selected one.
            if ( restrictions == null ) restrictions = new string[2];
            if ( !string.IsNullOrEmpty( Connection?.Database ) && restrictions.Length > 1 && restrictions[ 1 ] == null )
                restrictions[ 1 ] = Connection.Database;

            switch ( collection ) {
                case "TABLES":
                    return GetTables( restrictions );
                case "COLUMNS":
                    return GetColumns( restrictions );
                case "INDEXES":
                    return GetIndexes( restrictions );
                case "INDEXCOLUMNS":
                    return GetIndexColumns( restrictions );
                case "FOREIGN KEYS":
                    return GetForeignKeys( restrictions );
                case "FOREIGN KEY COLUMNS":
                    return GetForeignKeyColumns( restrictions );
            }
            return null;
        }

        internal string[] CleanRestrictions( string[] restrictionValues ) {
            if ( restrictionValues == null ) return null;
            var restrictions = (string[]) restrictionValues.Clone();

            for ( var x = 0; x < restrictions.Length; x++ ) {
                var s = restrictions[ x ];
                if ( s == null ) continue;
                restrictions[ x ] = s.Trim( '`' );
            }
            return restrictions;
        }

        protected MySqlSchemaCollection QueryCollection( string name, string sql ) {
            var c = new MySqlSchemaCollection( name );
            var cmd = new MySqlCommand( sql, Connection );
            var reader = cmd.ExecuteReader();

            for ( var i = 0; i < reader.FieldCount; i++ ) c.AddColumn( reader.GetName( i ), reader.GetFieldType( i ) );

            using ( reader )
                while ( reader.Read() ) {
                    var row = c.AddRow();
                    for ( var i = 0; i < reader.FieldCount; i++ ) row[ i ] = reader.GetValue( i );
                }
            return c;
        }
    }
}
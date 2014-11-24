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
using System.Globalization;
using System.IO;
using System.Reflection;
using System.Text;
using System.Text.RegularExpressions;
using MySql.Data.MySqlClient.common;
using MySql.Data.MySqlClient.Properties;
using MySql.Data.Types;
#if !RT
using System.Data;
using System.Data.Common;

#endif

namespace MySql.Data.MySqlClient {
    internal class SchemaProvider {
        protected MySqlConnection Connection;
        public static string MetaCollection = "MetaDataCollections";

        public SchemaProvider( MySqlConnection connectionToUse ) { Connection = connectionToUse; }

        public virtual MySqlSchemaCollection GetSchema( string collection, String[] restrictions ) {
            if ( Connection.State != ConnectionState.Open ) throw new MySqlException( "GetSchema can only be called on an open connection." );

            collection = StringUtility.ToUpperInvariant( collection );

            var c = GetSchemaInternal( collection, restrictions );

            if ( c == null ) throw new ArgumentException( "Invalid collection name" );
            return c;
        }

        public virtual MySqlSchemaCollection GetDatabases( string[] restrictions ) {
            Regex regex = null;
            var caseSetting = Int32.Parse( Connection.Driver.Property( "lower_case_table_names" ) );

            var sql = "SHOW DATABASES";

            // if lower_case_table_names is zero, then case lookup should be sensitive
            // so we can use LIKE to do the matching.
            if ( caseSetting == 0 )
                if ( restrictions != null
                     && restrictions.Length >= 1 ) sql = sql + " LIKE '" + restrictions[ 0 ] + "'";

            var c = QueryCollection( "Databases", sql );

            if ( caseSetting != 0
                 && restrictions != null
                 && restrictions.Length >= 1
                 && restrictions[ 0 ] != null ) regex = new Regex( restrictions[ 0 ], RegexOptions.IgnoreCase );

            var c2 = new MySqlSchemaCollection( "Databases" );
            c2.AddColumn( "CATALOG_NAME", TypeConstants.String );
            c2.AddColumn( "SCHEMA_NAME", TypeConstants.String );

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
            c.AddColumn( "TABLE_CATALOG", TypeConstants.String );
            c.AddColumn( "TABLE_SCHEMA", TypeConstants.String );
            c.AddColumn( "TABLE_NAME", TypeConstants.String );
            c.AddColumn( "TABLE_TYPE", TypeConstants.String );
            c.AddColumn( "ENGINE", TypeConstants.String );
            c.AddColumn( "VERSION", TypeConstants.UInt64 );
            c.AddColumn( "ROW_FORMAT", TypeConstants.String );
            c.AddColumn( "TABLE_ROWS", TypeConstants.UInt64 );
            c.AddColumn( "AVG_ROW_LENGTH", TypeConstants.UInt64 );
            c.AddColumn( "DATA_LENGTH", TypeConstants.UInt64 );
            c.AddColumn( "MAX_DATA_LENGTH", TypeConstants.UInt64 );
            c.AddColumn( "INDEX_LENGTH", TypeConstants.UInt64 );
            c.AddColumn( "DATA_FREE", TypeConstants.UInt64 );
            c.AddColumn( "AUTO_INCREMENT", TypeConstants.UInt64 );
            c.AddColumn( "CREATE_TIME", TypeConstants.DateTime );
            c.AddColumn( "UPDATE_TIME", TypeConstants.DateTime );
            c.AddColumn( "CHECK_TIME", TypeConstants.DateTime );
            c.AddColumn( "TABLE_COLLATION", TypeConstants.String );
            c.AddColumn( "CHECKSUM", TypeConstants.UInt64 );
            c.AddColumn( "CREATE_OPTIONS", TypeConstants.String );
            c.AddColumn( "TABLE_COMMENT", TypeConstants.String );

            // we have to new up a new restriction array here since
            // GetDatabases takes the database in the first slot
            var dbRestriction = new string[4];
            if ( restrictions != null
                 && restrictions.Length >= 2 ) dbRestriction[ 0 ] = restrictions[ 1 ];
            var databases = GetDatabases( dbRestriction );

            if ( restrictions != null ) Array.Copy( restrictions, dbRestriction, Math.Min( dbRestriction.Length, restrictions.Length ) );

            foreach ( var row in databases.Rows ) {
                dbRestriction[ 1 ] = row[ "SCHEMA_NAME" ].ToString();
                FindTables( c, dbRestriction );
            }
            return c;
        }

        protected void QuoteDefaultValues( MySqlSchemaCollection schemaCollection ) {
            if ( schemaCollection == null ) return;
            if ( !schemaCollection.ContainsColumn( "COLUMN_DEFAULT" ) ) return;

            foreach ( var row in schemaCollection.Rows ) {
                var defaultValue = row[ "COLUMN_DEFAULT" ];
                if ( MetaData.IsTextType( row[ "DATA_TYPE" ].ToString() ) ) row[ "COLUMN_DEFAULT" ] = String.Format( "{0}", defaultValue );
            }
        }

        public virtual MySqlSchemaCollection GetColumns( string[] restrictions ) {
            var c = new MySqlSchemaCollection( "Columns" );
            c.AddColumn( "TABLE_CATALOG", TypeConstants.String );
            c.AddColumn( "TABLE_SCHEMA", TypeConstants.String );
            c.AddColumn( "TABLE_NAME", TypeConstants.String );
            c.AddColumn( "COLUMN_NAME", TypeConstants.String );
            c.AddColumn( "ORDINAL_POSITION", TypeConstants.UInt64 );
            c.AddColumn( "COLUMN_DEFAULT", TypeConstants.String );
            c.AddColumn( "IS_NULLABLE", TypeConstants.String );
            c.AddColumn( "DATA_TYPE", TypeConstants.String );
            c.AddColumn( "CHARACTER_MAXIMUM_LENGTH", TypeConstants.UInt64 );
            c.AddColumn( "CHARACTER_OCTET_LENGTH", TypeConstants.UInt64 );
            c.AddColumn( "NUMERIC_PRECISION", TypeConstants.UInt64 );
            c.AddColumn( "NUMERIC_SCALE", TypeConstants.UInt64 );
            c.AddColumn( "CHARACTER_SET_NAME", TypeConstants.String );
            c.AddColumn( "COLLATION_NAME", TypeConstants.String );
            c.AddColumn( "COLUMN_TYPE", TypeConstants.String );
            c.AddColumn( "COLUMN_KEY", TypeConstants.String );
            c.AddColumn( "EXTRA", TypeConstants.String );
            c.AddColumn( "PRIVILEGES", TypeConstants.String );
            c.AddColumn( "COLUMN_COMMENT", TypeConstants.String );

            // we don't allow restricting on table type here
            string columnName = null;
            if ( restrictions != null
                 && restrictions.Length == 4 ) {
                columnName = restrictions[ 3 ];
                restrictions[ 3 ] = null;
            }
            var tables = GetTables( restrictions );

            foreach ( var row in tables.Rows ) LoadTableColumns( c, row[ "TABLE_SCHEMA" ].ToString(), row[ "TABLE_NAME" ].ToString(), columnName );

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
                    row[ "TABLE_CATALOG" ] = DBNull.Value;
                    row[ "TABLE_SCHEMA" ] = schema;
                    row[ "TABLE_NAME" ] = tableName;
                    row[ "COLUMN_NAME" ] = colName;
                    row[ "ORDINAL_POSITION" ] = pos++;
                    row[ "COLUMN_DEFAULT" ] = reader.GetValue( 5 );
                    row[ "IS_NULLABLE" ] = reader.GetString( 3 );
                    row[ "DATA_TYPE" ] = reader.GetString( 1 );
                    row[ "CHARACTER_MAXIMUM_LENGTH" ] = DBNull.Value;
                    row[ "CHARACTER_OCTET_LENGTH" ] = DBNull.Value;
                    row[ "NUMERIC_PRECISION" ] = DBNull.Value;
                    row[ "NUMERIC_SCALE" ] = DBNull.Value;
                    row[ "CHARACTER_SET_NAME" ] = reader.GetValue( 2 );
                    row[ "COLLATION_NAME" ] = row[ "CHARACTER_SET_NAME" ];
                    row[ "COLUMN_TYPE" ] = reader.GetString( 1 );
                    row[ "COLUMN_KEY" ] = reader.GetString( 4 );
                    row[ "EXTRA" ] = reader.GetString( 6 );
                    row[ "PRIVILEGES" ] = reader.GetString( 7 );
                    row[ "COLUMN_COMMENT" ] = reader.GetString( 8 );
                    ParseColumnRow( row );
                }
        }

        private static void ParseColumnRow( MySqlSchemaRow row ) {
            // first parse the character set name
            var charset = row[ "CHARACTER_SET_NAME" ].ToString();
            var index = charset.IndexOf( '_' );
            if ( index != -1 ) row[ "CHARACTER_SET_NAME" ] = charset.Substring( 0, index );

            // now parse the data type
            var dataType = row[ "DATA_TYPE" ].ToString();
            index = dataType.IndexOf( '(' );
            if ( index == -1 ) return;
            row[ "DATA_TYPE" ] = dataType.Substring( 0, index );
            var stop = dataType.IndexOf( ')', index );
            var dataLen = dataType.Substring( index + 1, stop - ( index + 1 ) );
            var lowerType = row[ "DATA_TYPE" ].ToString().ToLower();
            if ( lowerType == "char"
                 || lowerType == "varchar" ) row[ "CHARACTER_MAXIMUM_LENGTH" ] = dataLen;
            else if ( lowerType == "real"
                      || lowerType == "decimal" ) {
                var lenparts = dataLen.Split( ',' );
                row[ "NUMERIC_PRECISION" ] = lenparts[ 0 ];
                if ( lenparts.Length == 2 ) row[ "NUMERIC_SCALE" ] = lenparts[ 1 ];
            }
        }

        public virtual MySqlSchemaCollection GetIndexes( string[] restrictions ) {
            var dt = new MySqlSchemaCollection( "Indexes" );
            dt.AddColumn( "INDEX_CATALOG", TypeConstants.String );
            dt.AddColumn( "INDEX_SCHEMA", TypeConstants.String );
            dt.AddColumn( "INDEX_NAME", TypeConstants.String );
            dt.AddColumn( "TABLE_NAME", TypeConstants.String );
            dt.AddColumn( "UNIQUE", TypeConstants.Boolean );
            dt.AddColumn( "PRIMARY", TypeConstants.Boolean );
            dt.AddColumn( "TYPE", TypeConstants.String );
            dt.AddColumn( "COMMENT", TypeConstants.String );

            // Get the list of tables first
            var max = restrictions?.Length ?? 4;
            var tableRestrictions = new string[Math.Max( max, 4 )];
            restrictions?.CopyTo( tableRestrictions, 0 );
            tableRestrictions[ 3 ] = "BASE TABLE";
            var tables = GetTables( tableRestrictions );

            foreach ( var table in tables.Rows ) {
                var sql = String.Format(
                    "SHOW INDEX FROM `{0}`.`{1}`",
                    MySqlHelper.DoubleQuoteString( (string) table[ "TABLE_SCHEMA" ] ),
                    MySqlHelper.DoubleQuoteString( (string) table[ "TABLE_NAME" ] ) );
                var indexes = QueryCollection( "indexes", sql );

                foreach ( var index in indexes.Rows ) {
                    var seqIndex = (long) index[ "SEQ_IN_INDEX" ];
                    if ( seqIndex != 1 ) continue;
                    if ( restrictions != null
                         && restrictions.Length == 4
                         && restrictions[ 3 ] != null
                         && !index[ "KEY_NAME" ].Equals( restrictions[ 3 ] ) ) continue;
                    var row = dt.AddRow();
                    row[ "INDEX_CATALOG" ] = null;
                    row[ "INDEX_SCHEMA" ] = table[ "TABLE_SCHEMA" ];
                    row[ "INDEX_NAME" ] = index[ "KEY_NAME" ];
                    row[ "TABLE_NAME" ] = index[ "TABLE" ];
                    row[ "UNIQUE" ] = (long) index[ "NON_UNIQUE" ] == 0;
                    row[ "PRIMARY" ] = index[ "KEY_NAME" ].Equals( "PRIMARY" );
                    row[ "TYPE" ] = index[ "INDEX_TYPE" ];
                    row[ "COMMENT" ] = index[ "COMMENT" ];
                }
            }

            return dt;
        }

        public virtual MySqlSchemaCollection GetIndexColumns( string[] restrictions ) {
            var dt = new MySqlSchemaCollection( "IndexColumns" );
            dt.AddColumn( "INDEX_CATALOG", TypeConstants.String );
            dt.AddColumn( "INDEX_SCHEMA", TypeConstants.String );
            dt.AddColumn( "INDEX_NAME", TypeConstants.String );
            dt.AddColumn( "TABLE_NAME", TypeConstants.String );
            dt.AddColumn( "COLUMN_NAME", TypeConstants.String );
            dt.AddColumn( "ORDINAL_POSITION", TypeConstants.Int32 );
            dt.AddColumn( "SORT_ORDER", TypeConstants.String );

            var max = restrictions?.Length ?? 4;
            var tableRestrictions = new string[Math.Max( max, 4 )];
            restrictions?.CopyTo( tableRestrictions, 0 );
            tableRestrictions[ 3 ] = "BASE TABLE";
            var tables = GetTables( tableRestrictions );

            foreach ( var table in tables.Rows ) {
                var sql = String.Format( "SHOW INDEX FROM `{0}`.`{1}`", table[ "TABLE_SCHEMA" ], table[ "TABLE_NAME" ] );
                var cmd = new MySqlCommand( sql, Connection );
                using ( var reader = cmd.ExecuteReader() )
                    while ( reader.Read() ) {
                        var keyName = GetString( reader, reader.GetOrdinal( "KEY_NAME" ) );
                        var colName = GetString( reader, reader.GetOrdinal( "COLUMN_NAME" ) );

                        if ( restrictions != null ) {
                            if ( restrictions.Length >= 4
                                 && restrictions[ 3 ] != null
                                 && keyName != restrictions[ 3 ] ) continue;
                            if ( restrictions.Length >= 5
                                 && restrictions[ 4 ] != null
                                 && colName != restrictions[ 4 ] ) continue;
                        }
                        var row = dt.AddRow();
                        row[ "INDEX_CATALOG" ] = null;
                        row[ "INDEX_SCHEMA" ] = table[ "TABLE_SCHEMA" ];
                        row[ "INDEX_NAME" ] = keyName;
                        row[ "TABLE_NAME" ] = GetString( reader, reader.GetOrdinal( "TABLE" ) );
                        row[ "COLUMN_NAME" ] = colName;
                        row[ "ORDINAL_POSITION" ] = reader.GetValue( reader.GetOrdinal( "SEQ_IN_INDEX" ) );
                        row[ "SORT_ORDER" ] = reader.GetString( "COLLATION" );
                    }
            }

            return dt;
        }

        public virtual MySqlSchemaCollection GetForeignKeys( string[] restrictions ) {
            var dt = new MySqlSchemaCollection( "Foreign Keys" );
            dt.AddColumn( "CONSTRAINT_CATALOG", TypeConstants.String );
            dt.AddColumn( "CONSTRAINT_SCHEMA", TypeConstants.String );
            dt.AddColumn( "CONSTRAINT_NAME", TypeConstants.String );
            dt.AddColumn( "TABLE_CATALOG", TypeConstants.String );
            dt.AddColumn( "TABLE_SCHEMA", TypeConstants.String );
            dt.AddColumn( "TABLE_NAME", TypeConstants.String );
            dt.AddColumn( "MATCH_OPTION", TypeConstants.String );
            dt.AddColumn( "UPDATE_RULE", TypeConstants.String );
            dt.AddColumn( "DELETE_RULE", TypeConstants.String );
            dt.AddColumn( "REFERENCED_TABLE_CATALOG", TypeConstants.String );
            dt.AddColumn( "REFERENCED_TABLE_SCHEMA", TypeConstants.String );
            dt.AddColumn( "REFERENCED_TABLE_NAME", TypeConstants.String );

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
            dt.AddColumn( "CONSTRAINT_CATALOG", TypeConstants.String );
            dt.AddColumn( "CONSTRAINT_SCHEMA", TypeConstants.String );
            dt.AddColumn( "CONSTRAINT_NAME", TypeConstants.String );
            dt.AddColumn( "TABLE_CATALOG", TypeConstants.String );
            dt.AddColumn( "TABLE_SCHEMA", TypeConstants.String );
            dt.AddColumn( "TABLE_NAME", TypeConstants.String );
            dt.AddColumn( "COLUMN_NAME", TypeConstants.String );
            dt.AddColumn( "ORDINAL_POSITION", TypeConstants.Int32 );
            dt.AddColumn( "REFERENCED_TABLE_CATALOG", TypeConstants.String );
            dt.AddColumn( "REFERENCED_TABLE_SCHEMA", TypeConstants.String );
            dt.AddColumn( "REFERENCED_TABLE_NAME", TypeConstants.String );
            dt.AddColumn( "REFERENCED_COLUMN_NAME", TypeConstants.String );

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

        private string GetSqlMode() {
            var cmd = new MySqlCommand( "SELECT @@SQL_MODE", Connection );
            return cmd.ExecuteScalar().ToString();
        }

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
            if ( filterName != null ) filterName = StringUtility.ToLowerInvariant( filterName );

            var sql = string.Format( "SHOW CREATE TABLE `{0}`.`{1}`", tableToParse[ "TABLE_SCHEMA" ], tableToParse[ "TABLE_NAME" ] );
            string lowerBody;
            var cmd = new MySqlCommand( sql, Connection );
            using ( var reader = cmd.ExecuteReader() ) {
                reader.Read();
                var body = reader.GetString( 1 );
                lowerBody = StringUtility.ToLowerInvariant( body );
            }

            var tokenizer = new MySqlTokenizer( lowerBody ) {
                AnsiQuotes = sqlMode.IndexOf("ANSI_QUOTES", StringComparison.Ordinal) != -1,
                BackslashEscapes = sqlMode.IndexOf("NO_BACKSLASH_ESCAPES", StringComparison.Ordinal) != -1
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

            row[ "CONSTRAINT_CATALOG" ] = table[ "TABLE_CATALOG" ];
            row[ "CONSTRAINT_SCHEMA" ] = table[ "TABLE_SCHEMA" ];
            row[ "TABLE_CATALOG" ] = table[ "TABLE_CATALOG" ];
            row[ "TABLE_SCHEMA" ] = table[ "TABLE_SCHEMA" ];
            row[ "TABLE_NAME" ] = table[ "TABLE_NAME" ];
            row[ "REFERENCED_TABLE_CATALOG" ] = null;
            row[ "CONSTRAINT_NAME" ] = name.Trim( '\'', '`' );

            var srcColumns = includeColumns ? ParseColumns( tokenizer ) : null;

            // now look for the references section
            while ( token != "references"
                    || tokenizer.Quoted ) token = tokenizer.NextToken();
            var target1 = tokenizer.NextToken();
            var target2 = tokenizer.NextToken();
            if ( target2.StartsWith( ".", StringComparison.Ordinal ) ) {
                row[ "REFERENCED_TABLE_SCHEMA" ] = target1;
                row[ "REFERENCED_TABLE_NAME" ] = target2.Substring( 1 ).Trim( '\'', '`' );
                tokenizer.NextToken(); // read off the '('
            }
            else {
                row[ "REFERENCED_TABLE_SCHEMA" ] = table[ "TABLE_SCHEMA" ];
                row[ "REFERENCED_TABLE_NAME" ] = target1.Substring( 1 ).Trim( '\'', '`' );
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
                newRow[ "COLUMN_NAME" ] = srcColumns[ i ];
                newRow[ "ORDINAL_POSITION" ] = i;
                newRow[ "REFERENCED_COLUMN_NAME" ] = targetColumns[ i ];
                fkTable.Rows.Add( newRow );
            }
        }
        #endregion

        public virtual MySqlSchemaCollection GetUsers( string[] restrictions ) {
            var sb = new StringBuilder( "SELECT Host, User FROM mysql.user" );
            if ( restrictions != null
                 && restrictions.Length > 0 ) sb.AppendFormat( CultureInfo.InvariantCulture, " WHERE User LIKE '{0}'", restrictions[ 0 ] );

            var c = QueryCollection( "Users", sb.ToString() );
            c.Columns[ 0 ].Name = "HOST";
            c.Columns[ 1 ].Name = "USERNAME";

            return c;
        }

        public virtual MySqlSchemaCollection GetProcedures( string[] restrictions ) {
            var dt = new MySqlSchemaCollection( "Procedures" );
            dt.AddColumn( "SPECIFIC_NAME", TypeConstants.String );
            dt.AddColumn( "ROUTINE_CATALOG", TypeConstants.String );
            dt.AddColumn( "ROUTINE_SCHEMA", TypeConstants.String );
            dt.AddColumn( "ROUTINE_NAME", TypeConstants.String );
            dt.AddColumn( "ROUTINE_TYPE", TypeConstants.String );
            dt.AddColumn( "DTD_IDENTIFIER", TypeConstants.String );
            dt.AddColumn( "ROUTINE_BODY", TypeConstants.String );
            dt.AddColumn( "ROUTINE_DEFINITION", TypeConstants.String );
            dt.AddColumn( "EXTERNAL_NAME", TypeConstants.String );
            dt.AddColumn( "EXTERNAL_LANGUAGE", TypeConstants.String );
            dt.AddColumn( "PARAMETER_STYLE", TypeConstants.String );
            dt.AddColumn( "IS_DETERMINISTIC", TypeConstants.String );
            dt.AddColumn( "SQL_DATA_ACCESS", TypeConstants.String );
            dt.AddColumn( "SQL_PATH", TypeConstants.String );
            dt.AddColumn( "SECURITY_TYPE", TypeConstants.String );
            dt.AddColumn( "CREATED", TypeConstants.DateTime );
            dt.AddColumn( "LAST_ALTERED", TypeConstants.DateTime );
            dt.AddColumn( "SQL_MODE", TypeConstants.String );
            dt.AddColumn( "ROUTINE_COMMENT", TypeConstants.String );
            dt.AddColumn( "DEFINER", TypeConstants.String );

            var sql = new StringBuilder( "SELECT * FROM mysql.proc WHERE 1=1" );
            if ( restrictions != null ) {
                if ( restrictions.Length >= 2
                     && restrictions[ 1 ] != null ) sql.AppendFormat( CultureInfo.InvariantCulture, " AND db LIKE '{0}'", restrictions[ 1 ] );
                if ( restrictions.Length >= 3
                     && restrictions[ 2 ] != null ) sql.AppendFormat( CultureInfo.InvariantCulture, " AND name LIKE '{0}'", restrictions[ 2 ] );
                if ( restrictions.Length >= 4
                     && restrictions[ 3 ] != null ) sql.AppendFormat( CultureInfo.InvariantCulture, " AND type LIKE '{0}'", restrictions[ 3 ] );
            }

            var cmd = new MySqlCommand( sql.ToString(), Connection );
            using ( var reader = cmd.ExecuteReader() )
                while ( reader.Read() ) {
                    var row = dt.AddRow();
                    row[ "SPECIFIC_NAME" ] = reader.GetString( "specific_name" );
                    row[ "ROUTINE_CATALOG" ] = DBNull.Value;
                    row[ "ROUTINE_SCHEMA" ] = reader.GetString( "db" );
                    row[ "ROUTINE_NAME" ] = reader.GetString( "name" );
                    var routineType = reader.GetString( "type" );
                    row[ "ROUTINE_TYPE" ] = routineType;
                    row[ "DTD_IDENTIFIER" ] = StringUtility.ToLowerInvariant( routineType ) == "function"
                                                  ? (object) reader.GetString( "returns" )
                                                  : DBNull.Value;
                    row[ "ROUTINE_BODY" ] = "SQL";
                    row[ "ROUTINE_DEFINITION" ] = reader.GetString( "body" );
                    row[ "EXTERNAL_NAME" ] = DBNull.Value;
                    row[ "EXTERNAL_LANGUAGE" ] = DBNull.Value;
                    row[ "PARAMETER_STYLE" ] = "SQL";
                    row[ "IS_DETERMINISTIC" ] = reader.GetString( "is_deterministic" );
                    row[ "SQL_DATA_ACCESS" ] = reader.GetString( "sql_data_access" );
                    row[ "SQL_PATH" ] = DBNull.Value;
                    row[ "SECURITY_TYPE" ] = reader.GetString( "security_type" );
                    row[ "CREATED" ] = reader.GetDateTime( "created" );
                    row[ "LAST_ALTERED" ] = reader.GetDateTime( "modified" );
                    row[ "SQL_MODE" ] = reader.GetString( "sql_mode" );
                    row[ "ROUTINE_COMMENT" ] = reader.GetString( "comment" );
                    row[ "DEFINER" ] = reader.GetString( "definer" );
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
            dt.AddColumn( "CollectionName", TypeConstants.String );
            dt.AddColumn( "NumberOfRestrictions", TypeConstants.Int32 );
            dt.AddColumn( "NumberOfIdentifierParts", TypeConstants.Int32 );

            FillTable( dt, collections );

            return dt;
        }

        private MySqlSchemaCollection GetDataSourceInformation() {
#if CF || RT
      throw new NotSupportedException();
#else
            var dt = new MySqlSchemaCollection( "DataSourceInformation" );
            dt.AddColumn( "CompositeIdentifierSeparatorPattern", TypeConstants.String );
            dt.AddColumn( "DataSourceProductName", TypeConstants.String );
            dt.AddColumn( "DataSourceProductVersion", TypeConstants.String );
            dt.AddColumn( "DataSourceProductVersionNormalized", TypeConstants.String );
            dt.AddColumn( "GroupByBehavior", typeof( GroupByBehavior ) );
            dt.AddColumn( "IdentifierPattern", TypeConstants.String );
            dt.AddColumn( "IdentifierCase", typeof( IdentifierCase ) );
            dt.AddColumn( "OrderByColumnsInSelect", TypeConstants.Boolean );
            dt.AddColumn( "ParameterMarkerFormat", TypeConstants.String );
            dt.AddColumn( "ParameterMarkerPattern", TypeConstants.String );
            dt.AddColumn( "ParameterNameMaxLength", TypeConstants.Int32 );
            dt.AddColumn( "ParameterNamePattern", TypeConstants.String );
            dt.AddColumn( "QuotedIdentifierPattern", TypeConstants.String );
            dt.AddColumn( "QuotedIdentifierCase", typeof( IdentifierCase ) );
            dt.AddColumn( "StatementSeparatorPattern", TypeConstants.String );
            dt.AddColumn( "StringLiteralPattern", TypeConstants.String );
            dt.AddColumn( "SupportedJoinOperators", typeof( SupportedJoinOperators ) );

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
#endif
        }

        private static MySqlSchemaCollection GetDataTypes() {
            var dt = new MySqlSchemaCollection( "DataTypes" );
            dt.AddColumn( "TypeName", TypeConstants.String );
            dt.AddColumn( "ProviderDbType", TypeConstants.Int32 );
            dt.AddColumn( "ColumnSize", typeof( long ) );
            dt.AddColumn( "CreateFormat", TypeConstants.String );
            dt.AddColumn( "CreateParameters", TypeConstants.String );
            dt.AddColumn( "DataType", TypeConstants.String );
            dt.AddColumn( "IsAutoincrementable", TypeConstants.Boolean );
            dt.AddColumn( "IsBestMatch", TypeConstants.Boolean );
            dt.AddColumn( "IsCaseSensitive", TypeConstants.Boolean );
            dt.AddColumn( "IsFixedLength", TypeConstants.Boolean );
            dt.AddColumn( "IsFixedPrecisionScale", TypeConstants.Boolean );
            dt.AddColumn( "IsLong", TypeConstants.Boolean );
            dt.AddColumn( "IsNullable", TypeConstants.Boolean );
            dt.AddColumn( "IsSearchable", TypeConstants.Boolean );
            dt.AddColumn( "IsSearchableWithLike", TypeConstants.Boolean );
            dt.AddColumn( "IsUnsigned", TypeConstants.Boolean );
            dt.AddColumn( "MaximumScale", typeof( short ) );
            dt.AddColumn( "MinimumScale", typeof( short ) );
            dt.AddColumn( "IsConcurrencyType", TypeConstants.Boolean );
            dt.AddColumn( "IsLiteralSupported", TypeConstants.Boolean );
            dt.AddColumn( "LiteralPrefix", TypeConstants.String );
            dt.AddColumn( "LiteralSuffix", TypeConstants.String );
            dt.AddColumn( "NativeDataType", TypeConstants.String );

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
            dt.AddColumn( "CollectionName", TypeConstants.String );
            dt.AddColumn( "RestrictionName", TypeConstants.String );
            dt.AddColumn( "RestrictionDefault", TypeConstants.String );
            dt.AddColumn( "RestrictionNumber", TypeConstants.Int32 );

            FillTable( dt, restrictions );

            return dt;
        }

        private static MySqlSchemaCollection GetReservedWords() {
            var dt = new MySqlSchemaCollection( "ReservedWords" );
#if !RT
            dt.AddColumn( DbMetaDataColumnNames.ReservedWord, TypeConstants.String );
            var str = Assembly.GetExecutingAssembly().GetManifestResourceStream( "MySql.Data.MySqlClient.Properties.ReservedWords.txt" );
#else
      dt.AddColumn("ReservedWord", TypeConstants.String);
      Stream str = typeof(SchemaProvider).GetTypeInfo().Assembly.GetManifestResourceStream("MySql.Data.MySqlClient.Properties.ReservedWords.txt");
#endif
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
#if !CF
            sr.Dispose();
#else
      sr.Close();
#endif
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
            sql.AppendFormat( CultureInfo.InvariantCulture, "SHOW TABLE STATUS FROM `{0}`", restrictions[ 1 ] );
            if ( restrictions.Length >= 3
                 && restrictions[ 2 ] != null ) where.AppendFormat( CultureInfo.InvariantCulture, " LIKE '{0}'", restrictions[ 2 ] );
            sql.Append( @where );

            var tableType = restrictions[ 1 ].ToLower() == "information_schema" ? "SYSTEM VIEW" : "BASE TABLE";

            var cmd = new MySqlCommand( sql.ToString(), Connection );
            using ( var reader = cmd.ExecuteReader() )
                while ( reader.Read() ) {
                    var row = schema.AddRow();
                    row[ "TABLE_CATALOG" ] = null;
                    row[ "TABLE_SCHEMA" ] = restrictions[ 1 ];
                    row[ "TABLE_NAME" ] = reader.GetString( 0 );
                    row[ "TABLE_TYPE" ] = tableType;
                    row[ "ENGINE" ] = GetString( reader, 1 );
                    row[ "VERSION" ] = reader.GetValue( 2 );
                    row[ "ROW_FORMAT" ] = GetString( reader, 3 );
                    row[ "TABLE_ROWS" ] = reader.GetValue( 4 );
                    row[ "AVG_ROW_LENGTH" ] = reader.GetValue( 5 );
                    row[ "DATA_LENGTH" ] = reader.GetValue( 6 );
                    row[ "MAX_DATA_LENGTH" ] = reader.GetValue( 7 );
                    row[ "INDEX_LENGTH" ] = reader.GetValue( 8 );
                    row[ "DATA_FREE" ] = reader.GetValue( 9 );
                    row[ "AUTO_INCREMENT" ] = reader.GetValue( 10 );
                    row[ "CREATE_TIME" ] = reader.GetValue( 11 );
                    row[ "UPDATE_TIME" ] = reader.GetValue( 12 );
                    row[ "CHECK_TIME" ] = reader.GetValue( 13 );
                    row[ "TABLE_COLLATION" ] = GetString( reader, 14 );
                    row[ "CHECKSUM" ] = reader.GetValue( 15 );
                    row[ "CREATE_OPTIONS" ] = GetString( reader, 16 );
                    row[ "TABLE_COMMENT" ] = GetString( reader, 17 );
                }
        }

        private static string GetString( MySqlDataReader reader, int index ) {
            return reader.IsDBNull( index ) ? null : reader.GetString( index );
        }

        public virtual MySqlSchemaCollection GetUdf( string[] restrictions ) {
            var sql = "SELECT name,ret,dl FROM mysql.func";
            if ( restrictions?.Length >= 1
                 && !String.IsNullOrEmpty( restrictions[ 0 ] ) ) sql += String.Format( " WHERE name LIKE '{0}'", restrictions[ 0 ] );

            var dt = new MySqlSchemaCollection( "User-defined Functions" );
            dt.AddColumn( "NAME", TypeConstants.String );
            dt.AddColumn( "RETURN_TYPE", TypeConstants.Int32 );
            dt.AddColumn( "LIBRARY_NAME", TypeConstants.String );

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
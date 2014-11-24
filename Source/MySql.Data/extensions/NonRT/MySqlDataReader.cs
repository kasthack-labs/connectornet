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
using System.Collections;
using System.Data;
using System.Data.Common;
using MySql.Data.MySqlClient.common;
using MySql.Data.Types;

namespace MySql.Data.MySqlClient {
    public sealed partial class MySqlDataReader : DbDataReader, IDataReader {
        /// <summary>
        /// Gets a value indicating the depth of nesting for the current row.  This method is not 
        /// supported currently and always returns 0.
        /// </summary>
        public override int Depth => 0;

#if !CF
        public MySqlGeometry GetMySqlGeometry( int i ) {
            try {
                var v = GetFieldValue( i, false );
                if ( v is MySqlGeometry
                     || v is MySqlBinary ) return new MySqlGeometry( MySqlDbType.Geometry, (Byte[]) v.Value );
            }
            catch {
                Throw( new Exception( "Can't get MySqlGeometry from value" ) );
            }
            return new MySqlGeometry( true );
        }

        public MySqlGeometry GetMySqlGeometry( string column ) { return GetMySqlGeometry( GetOrdinal( column ) ); }
#endif

        /// <summary>
        /// Returns a DataTable that describes the column metadata of the MySqlDataReader.
        /// </summary>
        /// <returns></returns>
        public override DataTable GetSchemaTable() {
            // Only Results from SQL SELECT Queries 
            // get a DataTable for schema of the result
            // otherwise, DataTable is null reference
            if ( FieldCount == 0 ) return null;

            var dataTableSchema = new DataTable( "SchemaTable" );

            dataTableSchema.Columns.Add( "ColumnName", TypeConstants.String );
            dataTableSchema.Columns.Add( "ColumnOrdinal", TypeConstants.Int32 );
            dataTableSchema.Columns.Add( "ColumnSize", TypeConstants.Int32 );
            dataTableSchema.Columns.Add( "NumericPrecision", TypeConstants.Int32 );
            dataTableSchema.Columns.Add( "NumericScale", TypeConstants.Int32 );
            dataTableSchema.Columns.Add( "IsUnique", TypeConstants.Boolean );
            dataTableSchema.Columns.Add( "IsKey", TypeConstants.Boolean );
            var dc = dataTableSchema.Columns[ "IsKey" ];
            dc.AllowDBNull = true; // IsKey can have a DBNull
            dataTableSchema.Columns.Add( "BaseCatalogName", TypeConstants.String );
            dataTableSchema.Columns.Add( "BaseColumnName", TypeConstants.String );
            dataTableSchema.Columns.Add( "BaseSchemaName", TypeConstants.String );
            dataTableSchema.Columns.Add( "BaseTableName", TypeConstants.String );
            dataTableSchema.Columns.Add( "DataType", TypeConstants.Type );
            dataTableSchema.Columns.Add( "AllowDBNull", TypeConstants.Boolean );
            dataTableSchema.Columns.Add( "ProviderType", TypeConstants.Int32 );
            dataTableSchema.Columns.Add( "IsAliased", TypeConstants.Boolean );
            dataTableSchema.Columns.Add( "IsExpression", TypeConstants.Boolean );
            dataTableSchema.Columns.Add( "IsIdentity", TypeConstants.Boolean );
            dataTableSchema.Columns.Add( "IsAutoIncrement", TypeConstants.Boolean );
            dataTableSchema.Columns.Add( "IsRowVersion", TypeConstants.Boolean );
            dataTableSchema.Columns.Add( "IsHidden", TypeConstants.Boolean );
            dataTableSchema.Columns.Add( "IsLong", TypeConstants.Boolean );
            dataTableSchema.Columns.Add( "IsReadOnly", TypeConstants.Boolean );

            var ord = 1;
            for ( var i = 0; i < FieldCount; i++ ) {
                var f = ResultSet.Fields[ i ];
                var r = dataTableSchema.NewRow();
                r[ "ColumnName" ] = f.ColumnName;
                r[ "ColumnOrdinal" ] = ord++;
                r[ "ColumnSize" ] = f.IsTextField ? f.ColumnLength / f.MaxLength : f.ColumnLength;
                var prec = f.Precision;
                var pscale = f.Scale;
                if ( prec != -1 ) r[ "NumericPrecision" ] = (short) prec;
                if ( pscale != -1 ) r[ "NumericScale" ] = (short) pscale;
                r[ "DataType" ] = GetFieldType( i );
                r[ "ProviderType" ] = (int) f.Type;
                r[ "IsLong" ] = f.IsBlob && f.ColumnLength > 255;
                r[ "AllowDBNull" ] = f.AllowsNull;
                r[ "IsReadOnly" ] = false;
                r[ "IsRowVersion" ] = false;
                r[ "IsUnique" ] = false;
                r[ "IsKey" ] = f.IsPrimaryKey;
                r[ "IsAutoIncrement" ] = f.IsAutoIncrement;
                r[ "BaseSchemaName" ] = f.DatabaseName;
                r[ "BaseCatalogName" ] = null;
                r[ "BaseTableName" ] = f.RealTableName;
                r[ "BaseColumnName" ] = f.OriginalColumnName;

                dataTableSchema.Rows.Add( r );
            }

            return dataTableSchema;
        }

        /// <summary>
        /// Returns an <see cref="IEnumerator"/> that iterates through the <see cref="MySqlDataReader"/>. 
        /// </summary>
        /// <returns></returns>
        public override IEnumerator GetEnumerator() {
            return new DbEnumerator( this, ( CommandBehavior & CommandBehavior.CloseConnection ) != 0 );
        }
    }
}
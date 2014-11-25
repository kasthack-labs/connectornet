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
using System.ComponentModel;
using System.Data;
using System.Data.Common;
using System.Text;
using MySql.Data.Constants.ColumnNames;
using MySql.Data.Constants.ColumnNames.Columns;
using MySql.Data.MySqlClient.Properties;
using MySql.Data.Types;

namespace MySql.Data.MySqlClient {
    /// <include file='docs/MySqlCommandBuilder.xml' path='docs/class/*'/>
    [ToolboxItem( false )]
    [DesignerCategory( "Code" )]
    public sealed class MySqlCommandBuilder : DbCommandBuilder {
        /// <include file='docs/MySqlCommandBuilder.xml' path='docs/Ctor/*'/>
        public MySqlCommandBuilder() {
            QuotePrefix = QuoteSuffix = "`";
        }

        /// <include file='docs/MySqlCommandBuilder.xml' path='docs/Ctor2/*'/>
        public MySqlCommandBuilder( MySqlDataAdapter adapter ) : this() {
            DataAdapter = adapter;
        }

        /// <include file='docs/mysqlcommandBuilder.xml' path='docs/DataAdapter/*'/>
        public new MySqlDataAdapter DataAdapter {
            get {
                return (MySqlDataAdapter) base.DataAdapter;
            }
            set {
                base.DataAdapter = value;
            }
        }

        #region Public Methods
        /// <summary>
        /// Retrieves parameter information from the stored procedure specified 
        /// in the MySqlCommand and populates the Parameters collection of the 
        /// specified MySqlCommand object.
        /// This method is not currently supported since stored procedures are 
        /// not available in MySql.
        /// </summary>
        /// <param name="command">The MySqlCommand referencing the stored 
        /// procedure from which the parameter information is to be derived. 
        /// The derived parameters are added to the Parameters collection of the 
        /// MySqlCommand.</param>
        /// <exception cref="InvalidOperationException">The command text is not 
        /// a valid stored procedure name.</exception>
        public static void DeriveParameters( MySqlCommand command ) {
            if ( command.CommandType != CommandType.StoredProcedure ) throw new InvalidOperationException( Resources.CanNotDeriveParametersForTextCommands );

            // retrieve the proc definition from the cache.
            var spName = command.CommandText;
            if ( !spName.InvariantContains( "." ) ) spName = command.Connection.Database + "." + spName;

            try {
                var entry = command.Connection.ProcedureCache.GetProcedure( command.Connection, spName, null );
                command.Parameters.Clear();
                foreach ( var row in entry.Parameters.Rows ) {
                    var p = new MySqlParameter { ParameterName = String.Format( "@{0}", row[ Procedures.ParameterName ] ) };
                    if ( row[ OrdinalPosition ].Equals( 0 ) && p.ParameterName == "@" )
                        p.ParameterName = "@RETURN_VALUE";
                    p.Direction = GetDirection( row );
                    var unsigned = StoredProcedure.GetFlags( row[ Procedures.DtdIdentifier ].ToString() ).InvariantContains( "UNSIGNED" );
                    var realAsFloat = entry.Procedure.Rows[ 0 ][ Procedures.SqlMode ].ToString().InvariantContains( "REAL_AS_FLOAT" );
                    p.MySqlDbType = MetaData.NameToType( row[ DataType ].ToString(), unsigned, realAsFloat, command.Connection );
                    if ( row[ CharacterMaximumLength ] != null ) p.Size = (int) row[ CharacterMaximumLength ];
                    if ( row[ NumericPrecision ] != null ) p.Precision = Convert.ToByte( row[ NumericPrecision ] );
                    if ( row[ NumericScale ] != null ) p.Scale = Convert.ToByte( row[ NumericScale ] );
                    if ( p.MySqlDbType == MySqlDbType.Set
                         || p.MySqlDbType == MySqlDbType.Enum ) p.PossibleValues = GetPossibleValues( row );
                    command.Parameters.Add( p );
                }
            }
            catch ( InvalidOperationException ioe ) {
                throw new MySqlException( Resources.UnableToDeriveParameters, ioe );
            }
        }

        private static List<string> GetPossibleValues( MySqlSchemaRow row ) {
            var types = new[] { "ENUM", "SET" };
            var dtdIdentifier = row[ Procedures.DtdIdentifier ].ToString().Trim();

            var index = 0;
            for ( ; index < 2; index++ ) if ( dtdIdentifier.StartsWith( types[ index ], StringComparison.OrdinalIgnoreCase ) ) break;
            if ( index == 2 ) return null;
            dtdIdentifier = dtdIdentifier.Substring( types[ index ].Length ).Trim();
            dtdIdentifier = dtdIdentifier.Trim( '(', ')' ).Trim();

            var values = new List<string>();
            var tokenzier = new MySqlTokenizer( dtdIdentifier );
            var token = tokenzier.NextToken();
            var start = tokenzier.StartIndex;
            while ( true ) {
                if ( token == null
                     || token == "," ) {
                    var end = dtdIdentifier.Length - 1;
                    if ( token == "," ) end = tokenzier.StartIndex;

                    var value = dtdIdentifier.Substring( start, end - start ).Trim().Trim( '\'', '\"' ).Trim();
                    values.Add( value );
                    start = tokenzier.StopIndex;
                }
                if ( token == null ) break;
                token = tokenzier.NextToken();
            }
            return values;
        }

        private static ParameterDirection GetDirection( MySqlSchemaRow row ) {
            if ( Convert.ToInt32( row[ OrdinalPosition ] ) == 0 )
                return ParameterDirection.ReturnValue;
            switch ( row[ Procedures.ParameterMode ].ToString() ) {
                case "IN":
                    return ParameterDirection.Input;
                case "OUT":
                    return ParameterDirection.Output;
                default:
                    return ParameterDirection.InputOutput;
            }
        }

        /// <summary>
        /// Gets the delete command.
        /// </summary>
        /// <returns></returns>
        public new MySqlCommand GetDeleteCommand() => (MySqlCommand) base.GetDeleteCommand();

        /// <summary>
        /// Gets the update command.
        /// </summary>
        /// <returns></returns>
        public new MySqlCommand GetUpdateCommand() => (MySqlCommand) base.GetUpdateCommand();

        /// <summary>
        /// Gets the insert command.
        /// </summary>
        /// <returns></returns>
        public new MySqlCommand GetInsertCommand() => (MySqlCommand) GetInsertCommand( false );

        public override string QuoteIdentifier( string unquotedIdentifier ) {
            if ( unquotedIdentifier == null ) throw new ArgumentNullException( "unquotedIdentifier" );

            // don't quote again if it is already quoted
            if ( unquotedIdentifier.StartsWith( QuotePrefix )
                 && unquotedIdentifier.EndsWith( QuoteSuffix ) ) return unquotedIdentifier;

            unquotedIdentifier = unquotedIdentifier.Replace( QuotePrefix, QuotePrefix + QuotePrefix );

            return String.Format( "{0}{1}{2}", QuotePrefix, unquotedIdentifier, QuoteSuffix );
        }

        public override string UnquoteIdentifier( string quotedIdentifier ) {
            if ( quotedIdentifier == null ) throw new ArgumentNullException( "quotedIdentifier" );

            // don't unquote again if it is already unquoted
            if ( !quotedIdentifier.StartsWith( QuotePrefix )
                 || !quotedIdentifier.EndsWith( QuoteSuffix ) ) return quotedIdentifier;

            if ( quotedIdentifier.StartsWith( QuotePrefix ) ) quotedIdentifier = quotedIdentifier.Substring( 1 );
            if ( quotedIdentifier.EndsWith( QuoteSuffix ) ) quotedIdentifier = quotedIdentifier.Substring( 0, quotedIdentifier.Length - 1 );

            quotedIdentifier = quotedIdentifier.Replace( QuotePrefix + QuotePrefix, QuotePrefix );

            return quotedIdentifier;
        }
        #endregion

        protected override DataTable GetSchemaTable( DbCommand sourceCommand ) {
            var schemaTable = base.GetSchemaTable( sourceCommand );
            //todo: check type!!!
            foreach ( DataRow row in schemaTable.Rows )
                if ( row[ SchemaTable.BaseSchemaName ].Equals( sourceCommand.Connection.Database ) )
                    row[ SchemaTable.BaseSchemaName ] = null;

            return schemaTable;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="parameterName"></param>
        /// <returns></returns>
        protected override string GetParameterName( string parameterName ) {
            var sb = new StringBuilder( parameterName );
            sb.Replace( " ", "" );
            sb.Replace( "/", "_per_" );
            sb.Replace( "-", "_" );
            sb.Replace( ")", "_cb_" );
            sb.Replace( "(", "_ob_" );
            sb.Replace( "%", "_pct_" );
            sb.Replace( "<", "_lt_" );
            sb.Replace( ">", "_gt_" );
            sb.Replace( ".", "_pt_" );
            return String.Format( "@{0}", sb );
        }

        protected override void ApplyParameterInfo( DbParameter parameter, DataRow row, StatementType statementType, bool whereClause ) {
            ( (MySqlParameter) parameter ).MySqlDbType = (MySqlDbType) row[ SchemaTable.ProviderType ];
        }

        protected override string GetParameterName( int parameterOrdinal ) => String.Format( "@p{0}", parameterOrdinal.InvariantToString() );

        protected override string GetParameterPlaceholder( int parameterOrdinal ) => String.Format( "@p{0}", parameterOrdinal.InvariantToString() );

        protected override void SetRowUpdatingHandler( DbDataAdapter adapter ) {
            var myAdapter = ( adapter as MySqlDataAdapter );
            if ( adapter != base.DataAdapter ) myAdapter.RowUpdating += RowUpdating;
            else myAdapter.RowUpdating -= RowUpdating;
        }

        private void RowUpdating( object sender, MySqlRowUpdatingEventArgs args ) { RowUpdatingHandler( args ); }
    }
}
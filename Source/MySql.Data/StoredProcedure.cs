// Copyright (c) 2004-2008 MySQL AB, 2008-2009 Sun Microsystems, Inc.
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
using System.Data;
using System.Globalization;
using System.Linq;
using System.Text;
using MySql.Data.MySqlClient.Properties;
using MySql.Data.Types;

namespace MySql.Data.MySqlClient {
    /// <summary>
    /// Summary description for StoredProcedure.
    /// </summary>
    internal class StoredProcedure : PreparableStatement {
        private string _outSelect;
        private string _resolvedCommandText;

        // Prefix used for to generate inout or output parameters names
        internal const string ParameterPrefix = "_cnet_param_";

        public StoredProcedure( MySqlCommand cmd, string text ) : base( cmd, text ) { }

        private MySqlParameter GetReturnParameter() => Parameters?.Cast<MySqlParameter>().FirstOrDefault( p => p.Direction == ParameterDirection.ReturnValue );

        public bool ServerProvidingOutputParameters { get; private set; }

        public override string ResolvedCommandText => _resolvedCommandText;

        internal string GetCacheKey( string spName ) {
            var retValue = String.Empty;
            var key = new StringBuilder( spName );
            key.Append( "(" );
            var delimiter = "";
            foreach ( MySqlParameter p in Command.Parameters )
                if ( p.Direction == ParameterDirection.ReturnValue ) retValue = "?=";
                else {
                    key.InvariantAppendFormat( "{0}?", delimiter );
                    delimiter = ",";
                }
            key.Append( ")" );
            return retValue + key;
        }

        private ProcedureCacheEntry GetParameters( string procName ) {
            var procCacheKey = GetCacheKey( procName );
            var entry = Connection.ProcedureCache.GetProcedure( Connection, procName, procCacheKey );
            return entry;
        }

        public static string GetFlags( string dtd ) {
            var x = dtd.Length - 1;
            while ( x > 0
                    && ( Char.IsLetterOrDigit( dtd[ x ] ) || dtd[ x ] == ' ' ) ) x--;
            var dtdSubstring = dtd.Substring( x );
            return dtdSubstring.InvariantToUpper();
        }

        private string FixProcedureName( string name ) {
            var parts = name.Split( '.' );
            for ( var i = 0; i < parts.Length; i++ )
                if ( !parts[ i ].InvariantStartsWith( "`" ) )
                        parts[ i ] = String.Format( "`{0}`", parts[ i ] );
            if ( parts.Length == 1 ) return parts[ 0 ];
            return String.Format( "{0}.{1}", parts[ 0 ], parts[ 1 ] );
        }

        private MySqlParameter GetAndFixParameter( string spName, MySqlSchemaRow param, bool realAsFloat, MySqlParameter returnParameter ) {
            var mode = (string) param[ "PARAMETER_MODE" ];
            var pName = (string) param[ "PARAMETER_NAME" ];

            if ( param[ "ORDINAL_POSITION" ].Equals( 0 ) ) {
                if ( returnParameter == null ) throw new InvalidOperationException( String.Format( Resources.RoutineRequiresReturnParameter, spName ) );
                pName = returnParameter.ParameterName;
            }

            // make sure the parameters given to us have an appropriate type set if it's not already
            var p = Command.Parameters.GetParameterFlexible( pName, true );
            if ( p.TypeHasBeenSet ) return p;
            var datatype = (string) param[ "DATA_TYPE" ];
            var unsigned = GetFlags( param[ "DTD_IDENTIFIER" ].ToString() ).InvariantIndexOf( "UNSIGNED" ) != -1;
            p.MySqlDbType = MetaData.NameToType( datatype, unsigned, realAsFloat, Connection );
            return p;
        }

        private MySqlParameterCollection CheckParameters( string spName ) {
            var newParms = new MySqlParameterCollection( Command );
            var returnParameter = GetReturnParameter();
            var entry = GetParameters( spName );
            if ( entry.Procedure == null || entry.Procedure.Rows.Count == 0 )
                throw new InvalidOperationException( String.Format( Resources.RoutineNotFound, spName ) );
            var realAsFloat = entry.Procedure.Rows[ 0 ][ "SQL_MODE" ].ToString().IgnoreCaseContains( "REAL_AS_FLOAT" );
            foreach ( var param in entry.Parameters.Rows ) newParms.Add( GetAndFixParameter( spName, param, realAsFloat, returnParameter ) );
            return newParms;
        }

        public override void Resolve( bool preparing ) {
            // check to see if we are already resolved
            if ( _resolvedCommandText != null ) return;

            ServerProvidingOutputParameters = Driver.SupportsOutputParameters && preparing;

            // first retrieve the procedure definition from our
            // procedure cache
            var spName = CommandText;
            if ( !spName.InvariantContains( "." ) && !String.IsNullOrEmpty( Connection.Database ) )
                spName = Connection.Database + "." + spName;
            spName = FixProcedureName( spName );
            var returnParameter = GetReturnParameter();
            var parms = Command.Connection.Settings.CheckParameters ? CheckParameters( spName ) : Parameters;
            var setSql = SetUserVariables( parms, preparing );
            var callSql = CreateCallStatement( spName, returnParameter, parms );
            var outSql = CreateOutputSelect( parms, preparing );
            _resolvedCommandText = String.Format( "{0}{1}{2}", setSql, callSql, outSql );
        }

        private string SetUserVariables( MySqlParameterCollection parms, bool preparing ) {
            if ( ServerProvidingOutputParameters ) return String.Empty;
            var setSql = new StringBuilder();
            var delimiter = String.Empty;
            foreach ( MySqlParameter p in parms ) {
                if ( p.Direction != ParameterDirection.InputOutput ) continue;
                var pName = "@" + p.BaseName;
                var uName = "@" + ParameterPrefix + p.BaseName;
                var sql = String.Format( "SET {0}={1}", uName, pName );
                if ( Command.Connection.Settings.AllowBatch && !preparing ) {
                    setSql.InvariantAppendFormat( "{0}{1}", delimiter, sql );
                    delimiter = "; ";
                }
                else {
                    using ( var cmd = new MySqlCommand( sql, Command.Connection ) ) {
                        cmd.Parameters.Add( p );
                        cmd.ExecuteNonQuery();
                    }
                }
            }
            if ( setSql.Length > 0 ) setSql.Append( "; " );
            return setSql.ToString();
        }

        private string CreateCallStatement( string spName, MySqlParameter returnParameter, MySqlParameterCollection parms ) {
            var callSql = new StringBuilder();
            var delimiter = String.Empty;
            foreach ( MySqlParameter p in parms ) {
                if ( p.Direction == ParameterDirection.ReturnValue ) continue;
                var pName = "@" + p.BaseName;
                var uName = "@" + ParameterPrefix + p.BaseName;
                var useRealVar = p.Direction == ParameterDirection.Input || ServerProvidingOutputParameters;
                callSql.InvariantAppendFormat( "{0}{1}", delimiter, useRealVar ? pName : uName );
                delimiter = ", ";
            }
            if ( returnParameter == null ) return String.Format( "CALL {0}({1})", spName, callSql );
            return String.Format( "SET @{0}{1}={2}({3})", ParameterPrefix, returnParameter.BaseName, spName, callSql );
        }

        private string CreateOutputSelect( MySqlParameterCollection parms, bool preparing ) {
            var outSql = new StringBuilder();
            var delimiter = String.Empty;
            foreach ( MySqlParameter p in parms ) {
                switch ( p.Direction ) {
                    case ParameterDirection.Input:
                        continue;
                    case ParameterDirection.InputOutput:
                    case ParameterDirection.Output:
                        if ( ServerProvidingOutputParameters ) continue;
                        break;
                }
                var pName = "@" + p.BaseName;
                var uName = "@" + ParameterPrefix + p.BaseName;
                outSql.InvariantAppendFormat( "{0}{1}", delimiter, uName );
                delimiter = ", ";
            }
            if ( outSql.Length == 0 ) return String.Empty;
            if ( Command.Connection.Settings.AllowBatch && !preparing )
                return String.Format( ";SELECT {0}", outSql );
            _outSelect = String.Format( "SELECT {0}", outSql );
            return String.Empty;//wut?
        }
        internal void ProcessOutputParameters( MySqlDataReader reader ) {
            // We apparently need to always adjust our output types since the server
            // provided data types are not always right
            AdjustOutputTypes( reader );
            if ( ( reader.CommandBehavior & CommandBehavior.SchemaOnly ) != 0 ) return;
            // now read the output parameters data row
            reader.Read();
            const string prefix = "@" + ParameterPrefix;
            for ( var i = 0; i < reader.FieldCount; i++ ) {
                var fieldName = reader.GetName( i );
                if ( fieldName.StartsWith( prefix, StringComparison.OrdinalIgnoreCase ) ) fieldName = fieldName.Remove( 0, prefix.Length );
                var parameter = Command.Parameters.GetParameterFlexible( fieldName, true );
                parameter.Value = reader.GetValue( i );
            }
        }

        private void AdjustOutputTypes( MySqlDataReader reader ) {
            // since MySQL likes to return user variables as strings
            // we reset the types of the readers internal value objects
            // this will allow those value objects to parse the string based
            // return values
            for ( var i = 0; i < reader.FieldCount; i++ ) {
                var fieldName = reader.GetName( i );
                if ( fieldName.InvariantContains( ParameterPrefix ) )
                    fieldName = fieldName.Remove( 0, ParameterPrefix.Length + 1 );
                var parameter = Command.Parameters.GetParameterFlexible( fieldName, true );
                var v = MySqlField.GetIMySqlValue( parameter.MySqlDbType );
                if ( v is MySqlBit ) {
                    var bit = (MySqlBit) v;
                    bit.ReadAsString = true;
                    reader.ResultSet.SetValueObject( i, bit );
                }
                else reader.ResultSet.SetValueObject( i, v );
            }
        }

        public override void Close( MySqlDataReader reader ) {
            base.Close( reader );
            if ( String.IsNullOrEmpty( _outSelect ) ) return;
            if ( ( reader.CommandBehavior & CommandBehavior.SchemaOnly ) != 0 ) return;
            using (var cmd = new MySqlCommand( _outSelect, Command.Connection ))
            using (var rdr = cmd.ExecuteReader( reader.CommandBehavior ))
                ProcessOutputParameters( rdr );
        }
    }
}
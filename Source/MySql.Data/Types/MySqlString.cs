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
using MySql.Data.MySqlClient;
using MySql.Data.MySqlClient.common;

namespace MySql.Data.Types {
    internal struct MySqlString : IMySqlValue {
        private readonly string _mValue;
        private readonly bool _isNull;
        private readonly MySqlDbType _type;

        public MySqlString( MySqlDbType type, bool isNull ) {
            this._type = type;
            this._isNull = isNull;
            _mValue = String.Empty;
        }

        public MySqlString( MySqlDbType type, string val ) {
            this._type = type;
            _isNull = false;
            _mValue = val;
        }

        #region IMySqlValue Members
        public bool IsNull => _isNull;

        MySqlDbType IMySqlValue.MySqlDbType => _type;

        object IMySqlValue.Value => _mValue;

        public string Value => _mValue;

        Type IMySqlValue.SystemType => TypeConstants.String;

        string IMySqlValue.MySqlTypeName => _type == MySqlDbType.Set ? "SET" : _type == MySqlDbType.Enum ? "ENUM" : "VARCHAR";

        void IMySqlValue.WriteValue( MySqlPacket packet, bool binary, object val, int length ) {
            var v = val.ToString();
            if ( length > 0 ) {
                length = Math.Min( length, v.Length );
                v = v.Substring( 0, length );
            }

            if ( binary ) packet.WriteLenString( v );
            else packet.WriteStringNoNull( "'" + MySqlHelper.EscapeString( v ) + "'" );
        }

        IMySqlValue IMySqlValue.ReadValue( MySqlPacket packet, long length, bool nullVal ) {
            if ( nullVal ) return new MySqlString( _type, true );

            var s = String.Empty;
            if ( length == -1 ) s = packet.ReadLenString();
            else s = packet.ReadString( length );
            var str = new MySqlString( _type, s );
            return str;
        }

        void IMySqlValue.SkipValue( MySqlPacket packet ) {
            var len = (int) packet.ReadFieldLength();
            packet.Position += len;
        }
        #endregion

        internal static void SetDsInfo( MySqlSchemaCollection sc ) {
            var types = new[] { "CHAR", "NCHAR", "VARCHAR", "NVARCHAR", "SET", "ENUM", "TINYTEXT", "TEXT", "MEDIUMTEXT", "LONGTEXT" };
            var dbtype = new[] {
                MySqlDbType.String, MySqlDbType.String, MySqlDbType.VarChar, MySqlDbType.VarChar, MySqlDbType.Set, MySqlDbType.Enum,
                MySqlDbType.TinyText, MySqlDbType.Text, MySqlDbType.MediumText, MySqlDbType.LongText
            };

            // we use name indexing because this method will only be called
            // when GetSchema is called for the DataSourceInformation 
            // collection and then it wil be cached.
            for ( var x = 0; x < types.Length; x++ ) {
                var row = sc.AddRow();
                row[ "TypeName" ] = types[ x ];
                row[ "ProviderDbType" ] = dbtype[ x ];
                row[ "ColumnSize" ] = 0;
                row[ "CreateFormat" ] = x < 4 ? types[ x ] + "({0})" : types[ x ];
                row[ "CreateParameters" ] = x < 4 ? "size" : null;
                row[ "DataType" ] = "System.String";
                row[ "IsAutoincrementable" ] = false;
                row[ "IsBestMatch" ] = true;
                row[ "IsCaseSensitive" ] = false;
                row[ "IsFixedLength" ] = false;
                row[ "IsFixedPrecisionScale" ] = true;
                row[ "IsLong" ] = false;
                row[ "IsNullable" ] = true;
                row[ "IsSearchable" ] = true;
                row[ "IsSearchableWithLike" ] = true;
                row[ "IsUnsigned" ] = false;
                row[ "MaximumScale" ] = 0;
                row[ "MinimumScale" ] = 0;
                row[ "IsConcurrencyType" ] = DBNull.Value;
                row[ "IsLiteralSupported" ] = false;
                row[ "LiteralPrefix" ] = null;
                row[ "LiteralSuffix" ] = null;
                row[ "NativeDataType" ] = null;
            }
        }
    }
}
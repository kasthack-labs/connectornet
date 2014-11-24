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
using System.Globalization;
using MySql.Data.MySqlClient;
using MySql.Data.Constants;

namespace MySql.Data.Types {
    internal struct MySqlUInt32 : IMySqlValue {
        private readonly uint _mValue;
        private readonly bool _isNull;
        private readonly bool _is24Bit;

        private MySqlUInt32( MySqlDbType type ) {
            _is24Bit = type == MySqlDbType.Int24;
            _isNull = true;
            _mValue = 0;
        }

        public MySqlUInt32( MySqlDbType type, bool isNull ) : this( type ) { this._isNull = isNull; }

        public MySqlUInt32( MySqlDbType type, uint val ) : this( type ) {
            _isNull = false;
            _mValue = val;
        }

        #region IMySqlValue Members
        public bool IsNull => _isNull;

        MySqlDbType IMySqlValue.MySqlDbType => MySqlDbType.UInt32;

        object IMySqlValue.Value => _mValue;

        public uint Value => _mValue;

        Type IMySqlValue.SystemType => Constants.Types.UInt32;

        string IMySqlValue.MySqlTypeName => _is24Bit ? "MEDIUMINT" : "INT";

        void IMySqlValue.WriteValue( MySqlPacket packet, bool binary, object v, int length ) {
            var val = v as uint? ?? Convert.ToUInt32( v );
            if ( binary ) packet.WriteInteger( val, _is24Bit ? 3 : 4 );
            else packet.WriteStringNoNull( val.InvariantToString() );
        }

        IMySqlValue IMySqlValue.ReadValue( MySqlPacket packet, long length, bool nullVal ) {
            if ( nullVal ) return new MySqlUInt32( ( this as IMySqlValue ).MySqlDbType, true );

            if ( length == -1 ) return new MySqlUInt32( ( this as IMySqlValue ).MySqlDbType, (uint) packet.ReadInteger( 4 ) );
            return new MySqlUInt32(
                ( this as IMySqlValue ).MySqlDbType,
                UInt32.Parse( packet.ReadString( length ), NumberStyles.Any, CultureInfo.InvariantCulture ) );
        }

        void IMySqlValue.SkipValue( MySqlPacket packet ) { packet.Position += 4; }
        #endregion

        internal static void SetDsInfo( MySqlSchemaCollection sc ) {
            var types = new[] { "MEDIUMINT", "INT" };
            var dbtype = new[] { MySqlDbType.UInt24, MySqlDbType.UInt32 };

            // we use name indexing because this method will only be called
            // when GetSchema is called for the DataSourceInformation 
            // collection and then it wil be cached.
            for ( var x = 0; x < types.Length; x++ ) {
                var row = sc.AddRow();
                row[ "TypeName" ] = types[ x ];
                row[ "ProviderDbType" ] = dbtype[ x ];
                row[ "ColumnSize" ] = 0;
                row[ "CreateFormat" ] = types[ x ] + " UNSIGNED";
                row[ "CreateParameters" ] = null;
                row[ "DataType" ] = "System.UInt32";
                row[ "IsAutoincrementable" ] = true;
                row[ "IsBestMatch" ] = true;
                row[ "IsCaseSensitive" ] = false;
                row[ "IsFixedLength" ] = true;
                row[ "IsFixedPrecisionScale" ] = true;
                row[ "IsLong" ] = false;
                row[ "IsNullable" ] = true;
                row[ "IsSearchable" ] = true;
                row[ "IsSearchableWithLike" ] = false;
                row[ "IsUnsigned" ] = true;
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
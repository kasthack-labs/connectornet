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
using MySql.Data.Constants;

namespace MySql.Data.Types {
    /// <summary>
    /// Summary description for MySqlUInt64.
    /// </summary>
    internal struct MySqlBit : IMySqlValue {
        private ulong _mValue;
        private bool _isNull;
        private bool _readAsString;

        public MySqlBit( bool isnull ) {
            _mValue = 0;
            _isNull = isnull;
            _readAsString = false;
        }

        public bool ReadAsString {
            get {
                return _readAsString;
            }
            set {
                _readAsString = value;
            }
        }

        public bool IsNull => _isNull;

        MySqlDbType IMySqlValue.MySqlDbType => MySqlDbType.Bit;

        object IMySqlValue.Value => _mValue;

        Type IMySqlValue.SystemType => Constants.Types.UInt64;

        string IMySqlValue.MySqlTypeName => "BIT";

        public void WriteValue( MySqlPacket packet, bool binary, object value, int length ) {
            var v = value as ulong? ?? Convert.ToUInt64( value );
            if ( binary ) packet.WriteInteger( (long) v, 8 );
            else packet.WriteStringNoNull( v.ToString() );
        }

        public IMySqlValue ReadValue( MySqlPacket packet, long length, bool isNull ) {
            _isNull = isNull;
            if ( isNull ) return this;

            if ( length == -1 ) length = packet.ReadFieldLength();

            _mValue = ReadAsString ? UInt64.Parse( packet.ReadString( length ) ) : packet.ReadBitValue( (int) length );
            return this;
        }

        public void SkipValue( MySqlPacket packet ) {
            var len = (int) packet.ReadFieldLength();
            packet.Position += len;
        }

        internal static void SetDsInfo( MySqlSchemaCollection sc ) {
            // we use name indexing because this method will only be called
            // when GetSchema is called for the DataSourceInformation 
            // collection and then it wil be cached.
            var row = sc.AddRow();
            row[ "TypeName" ] = "BIT";
            row[ "ProviderDbType" ] = MySqlDbType.Bit;
            row[ "ColumnSize" ] = 64;
            row[ "CreateFormat" ] = "BIT";
            row[ "CreateParameters" ] = DBNull.Value;
            row[ "DataType" ] = Constants.Types.UInt64.ToString();
            row[ "IsAutoincrementable" ] = false;
            row[ "IsBestMatch" ] = true;
            row[ "IsCaseSensitive" ] = false;
            row[ "IsFixedLength" ] = false;
            row[ "IsFixedPrecisionScale" ] = true;
            row[ "IsLong" ] = false;
            row[ "IsNullable" ] = true;
            row[ "IsSearchable" ] = true;
            row[ "IsSearchableWithLike" ] = false;
            row[ "IsUnsigned" ] = false;
            row[ "MaximumScale" ] = 0;
            row[ "MinimumScale" ] = 0;
            row[ "IsConcurrencyType" ] = DBNull.Value;
            row[ "IsLiteralSupported" ] = false;
            row[ "LiteralPrefix" ] = DBNull.Value;
            row[ "LiteralSuffix" ] = DBNull.Value;
            row[ "NativeDataType" ] = DBNull.Value;
        }
    }
}
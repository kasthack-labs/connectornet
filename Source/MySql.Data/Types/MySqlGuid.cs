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
using MySql.Data.MySqlClient.Properties;

namespace MySql.Data.Types {
    internal struct MySqlGuid : IMySqlValue {
        private Guid _mValue;
        private bool _isNull;
        private readonly byte[] _bytes;
        private bool _oldGuids;

        public MySqlGuid( byte[] buff ) {
            _oldGuids = false;
            _mValue = new Guid( buff );
            _isNull = false;
            _bytes = buff;
        }

        public byte[] Bytes => _bytes;

        public bool OldGuids {
            get {
                return _oldGuids;
            }
            set {
                _oldGuids = value;
            }
        }

        #region IMySqlValue Members
        public bool IsNull => _isNull;

        MySqlDbType IMySqlValue.MySqlDbType => MySqlDbType.Guid;

        object IMySqlValue.Value => _mValue;

        public Guid Value => _mValue;

        Type IMySqlValue.SystemType => typeof( Guid );

        string IMySqlValue.MySqlTypeName => OldGuids ? "BINARY(16)" : "CHAR(36)";

        void IMySqlValue.WriteValue( MySqlPacket packet, bool binary, object val, int length ) {
            var guid = Guid.Empty;
            var valAsString = val as string;
            var valAsByte = val as byte[];

            if ( val is Guid ) guid = (Guid) val;
            else
                try {
                    if ( valAsString != null ) guid = new Guid( valAsString );
                    else if ( valAsByte != null ) guid = new Guid( valAsByte );
                }
                catch ( Exception ex ) {
                    throw new MySqlException( Resources.DataNotInSupportedFormat, ex );
                }

            if ( OldGuids ) WriteOldGuid( packet, guid, binary );
            else {
                guid.ToString( "D" );

                if ( binary ) packet.WriteLenString( guid.ToString( "D" ) );
                else packet.WriteStringNoNull( "'" + MySqlHelper.EscapeString( guid.ToString( "D" ) ) + "'" );
            }
        }

        private void WriteOldGuid( MySqlPacket packet, Guid guid, bool binary ) {
            var bytes = guid.ToByteArray();

            if ( binary ) {
                packet.WriteLength( bytes.Length );
                packet.Write( bytes );
            }
            else {
                packet.WriteStringNoNull( "_binary " );
                packet.WriteByte( (byte) '\'' );
                EscapeByteArray( bytes, bytes.Length, packet );
                packet.WriteByte( (byte) '\'' );
            }
        }

        private static void EscapeByteArray( byte[] bytes, int length, MySqlPacket packet ) {
            for ( var x = 0; x < length; x++ ) {
                var b = bytes[ x ];
                if ( b == '\0' ) {
                    packet.WriteByte( (byte) '\\' );
                    packet.WriteByte( (byte) '0' );
                }

                else if ( b == '\\'
                          || b == '\''
                          || b == '\"' ) {
                    packet.WriteByte( (byte) '\\' );
                    packet.WriteByte( b );
                }
                else packet.WriteByte( b );
            }
        }

        private MySqlGuid ReadOldGuid( MySqlPacket packet, long length ) {
            if ( length == -1 ) length = packet.ReadFieldLength();

            var buff = new byte[length];
            packet.Read( buff, 0, (int) length );
            return new MySqlGuid( buff ) { OldGuids = OldGuids };
        }

        IMySqlValue IMySqlValue.ReadValue( MySqlPacket packet, long length, bool nullVal ) {
            var g = new MySqlGuid { _isNull = true, OldGuids = OldGuids };
            if ( nullVal ) return g;
            if ( OldGuids ) return ReadOldGuid( packet, length );
            var s = length == -1 ? packet.ReadLenString() : packet.ReadString( length );
            g._mValue = new Guid( s );
            g._isNull = false;
            return g;
        }

        void IMySqlValue.SkipValue( MySqlPacket packet ) {
            var len = (int) packet.ReadFieldLength();
            packet.Position += len;
        }
        #endregion

        public static void SetDsInfo( MySqlSchemaCollection sc ) {
            // we use name indexing because this method will only be called
            // when GetSchema is called for the DataSourceInformation 
            // collection and then it wil be cached.
            var row = sc.AddRow();
            row[ "TypeName" ] = "GUID";
            row[ "ProviderDbType" ] = MySqlDbType.Guid;
            row[ "ColumnSize" ] = 0;
            row[ "CreateFormat" ] = "BINARY(16)";
            row[ "CreateParameters" ] = null;
            row[ "DataType" ] = "System.Guid";
            row[ "IsAutoincrementable" ] = false;
            row[ "IsBestMatch" ] = true;
            row[ "IsCaseSensitive" ] = false;
            row[ "IsFixedLength" ] = true;
            row[ "IsFixedPrecisionScale" ] = true;
            row[ "IsLong" ] = false;
            row[ "IsNullable" ] = true;
            row[ "IsSearchable" ] = false;
            row[ "IsSearchableWithLike" ] = false;
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
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
using MySql.Data.Constants.Types;
namespace MySql.Data.Types {
    internal struct MySqlGuid : IMySqlValue {
        private const string MySqlTypeString = "GUID";
        private const string MySqlFormatString = "BINARY(16)";
        private Guid _mValue;
        private bool _isNull;
        private readonly byte[] _bytes;
        public MySqlGuid( byte[] buff ) : this() {
            OldGuids = false;
            _mValue = new Guid( buff );
            _isNull = false;
            _bytes = buff;
        }
        public byte[] Bytes => _bytes;
        public bool OldGuids { get; set; }
        #region IMySqlValue Members
        public bool IsNull => _isNull;
        MySqlDbType IMySqlValue.MySqlDbType => MySqlDbType.Guid;
        object IMySqlValue.Value => _mValue;
        public Guid Value => _mValue;
        Type IMySqlValue.SystemType => typeof( Guid );
        string IMySqlValue.MySqlTypeName => OldGuids ? MySqlFormatString : "CHAR(36)";
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
                var value = guid.ToString( "D" );
                if ( binary ) packet.WriteLenString( value );
                else packet.WriteStringNoNull( "'" + MySqlHelper.EscapeString( value ) + "'" );
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
                switch ( (char)b ) {
                    case '\0':
                        packet.WriteByte( (byte) '\\' );
                        packet.WriteByte( (byte) '0' );
                        break;
                    case '\\':
                    case '\'':
                    case '\"':
                        packet.WriteByte( (byte) '\\' );
                        packet.WriteByte( b );
                        break;
                    default:
                        packet.WriteByte( b );
                        break;
                }
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
        void IMySqlValue.SkipValue( MySqlPacket packet ) => packet.Position += (int) packet.ReadFieldLength();
        #endregion

        public static void SetDsInfo( MySqlSchemaCollection sc ) {
            var row = sc.AddRow();
            DsInfoHelper.FillRow( row, MySqlTypeString, MySqlDbType.Guid, TGuid, createFormat: MySqlFormatString );
            row[ "IsSearchable" ] = false;
        }
    }
}
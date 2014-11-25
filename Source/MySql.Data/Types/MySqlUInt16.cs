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
using MySql.Data.Constants.Types;
namespace MySql.Data.Types {
    internal struct MySqlUInt16 : IMySqlValue {
        private const string MySqlTypeString = "SMALLINT";
        private readonly ushort _mValue;
        private readonly bool _isNull;
        public MySqlUInt16( bool isNull ) {
            _isNull = isNull;
            _mValue = 0;
        }
        public MySqlUInt16( ushort val ) {
            _isNull = false;
            _mValue = val;
        }
        public bool IsNull => _isNull;
        MySqlDbType IMySqlValue.MySqlDbType => MySqlDbType.UInt16;
        object IMySqlValue.Value => _mValue;
        public ushort Value => _mValue;
        Type IMySqlValue.SystemType => TUInt16;
        string IMySqlValue.MySqlTypeName => MySqlTypeString;
        void IMySqlValue.WriteValue( MySqlPacket packet, bool binary, object val, int length ) {
            var v = val as ushort? ?? Convert.ToUInt16( val );
            if ( binary ) packet.WriteInteger( v, 2 );
            else packet.WriteStringNoNull( v.InvariantToString() );
        }
        IMySqlValue IMySqlValue.ReadValue( MySqlPacket packet, long length, bool nullVal ) {
            if ( nullVal ) return new MySqlUInt16( true );
            return new MySqlUInt16( length == -1?( ushort)packet.ReadInteger( 2 ):UInt16.Parse( packet.ReadString( length ) ) );
        }
        void IMySqlValue.SkipValue( MySqlPacket packet ) { packet.Position += 2; }
        internal static void SetDsInfo( MySqlSchemaCollection sc ) =>
            DsInfoHelper.FillRow( sc.AddRow(), MySqlTypeString, MySqlDbType.UInt16, TUInt16, 0, "SMALLINT UNSIGNED", true, isUnsigned: true );
    }
}
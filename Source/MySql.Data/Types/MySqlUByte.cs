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
    internal struct MySqlUByte : IMySqlValue {
        private const string MySqlTypeString = "TINYINT";
        private readonly byte _mValue;
        private readonly bool _isNull;
        public MySqlUByte( bool isNull ) {
            _isNull = isNull;
            _mValue = 0;
        }
        public MySqlUByte( byte val ) {
            _isNull = false;
            _mValue = val;
        }
        #region IMySqlValue Members
        public bool IsNull => _isNull;
        MySqlDbType IMySqlValue.MySqlDbType => MySqlDbType.UByte;
        object IMySqlValue.Value => _mValue;
        public byte Value => _mValue;
        Type IMySqlValue.SystemType => TByte;
        string IMySqlValue.MySqlTypeName => MySqlTypeString;
        void IMySqlValue.WriteValue( MySqlPacket packet, bool binary, object val, int length ) {
            var v = val as byte? ?? Convert.ToByte( val );
            if ( binary ) packet.WriteByte( v );
            else packet.WriteStringNoNull( v.InvariantToString() );
        }
        IMySqlValue IMySqlValue.ReadValue( MySqlPacket packet, long length, bool nullVal ) {
            if ( nullVal ) return new MySqlUByte( true );
            if ( length == -1 ) return new MySqlUByte( packet.ReadByte() );
            return new MySqlUByte( Byte.Parse( packet.ReadString( length ) ) );
        }
        void IMySqlValue.SkipValue( MySqlPacket packet ) { packet.ReadByte(); }
        #endregion
        internal static void SetDsInfo( MySqlSchemaCollection sc ) =>
            DsInfoHelper.FillRow( sc.AddRow(), MySqlTypeString, MySqlDbType.UByte, TByte, isAutoIncrementable: true,  isUnsigned: true  );
    }
}
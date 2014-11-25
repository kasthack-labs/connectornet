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
    internal struct MySqlInt64 : IMySqlValue {
        private const string MySqlTypeString = "BIGINT";
        private readonly long _mValue;
        private readonly bool _isNull;
        public MySqlInt64( bool isNull ) {
            _isNull = isNull;
            _mValue = 0;
        }
        public MySqlInt64( long val ) {
            _isNull = false;
            _mValue = val;
        }
        #region IMySqlValue Members
        public bool IsNull => _isNull;
        MySqlDbType IMySqlValue.MySqlDbType => MySqlDbType.Int64;
        object IMySqlValue.Value => _mValue;
        public long Value => _mValue;
        Type IMySqlValue.SystemType => TInt64;
        string IMySqlValue.MySqlTypeName => MySqlTypeString;
        void IMySqlValue.WriteValue( MySqlPacket packet, bool binary, object val, int length ) {
            var v = val as long? ?? Convert.ToInt64( val );
            if ( binary ) packet.WriteInteger( v, 8 );
            else packet.WriteStringNoNull( v.ToString() );
        }
        IMySqlValue IMySqlValue.ReadValue( MySqlPacket packet, long length, bool nullVal ) {
            if ( nullVal ) return new MySqlInt64( true );
            if ( length == -1 ) return new MySqlInt64( (long) packet.ReadULong( 8 ) );
            return new MySqlInt64( long.Parse( packet.ReadString( length ) ) );
        }
        void IMySqlValue.SkipValue( MySqlPacket packet ) => packet.Position += 8;
        #endregion
        internal static void SetDsInfo( MySqlSchemaCollection sc ) =>
            DsInfoHelper.FillRow( sc.AddRow(), MySqlTypeString, MySqlDbType.Int64, TInt64, 0, MySqlTypeString, true );
    }
}
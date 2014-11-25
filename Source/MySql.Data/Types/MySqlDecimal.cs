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
using MySql.Data.Constants.Types;

namespace MySql.Data.Types {
    public struct MySqlDecimal : IMySqlValue {
        private const string MySqlTypeString = "DECIMAL";
        private readonly string _mValue;
        private readonly bool _isNull;

        internal MySqlDecimal( bool isNull ) : this() {
            _isNull = isNull;
            _mValue = null;
            Precision = Scale = 0;
        }

        internal MySqlDecimal( string val ) : this() {
            _isNull = false;
            _mValue = val;
            Precision = Scale = 0;
        }

        #region IMySqlValue Members
        public bool IsNull => _isNull;

        MySqlDbType IMySqlValue.MySqlDbType => MySqlDbType.Decimal;

        public byte Precision { get; set; }

        public byte Scale { get; set; }

        object IMySqlValue.Value => Value;

        public decimal Value => Convert.ToDecimal( _mValue, CultureInfo.InvariantCulture );

        public double ToDouble() => double.Parse( _mValue );

        public override string ToString() => _mValue;

        Type IMySqlValue.SystemType => TDecimal;

        string IMySqlValue.MySqlTypeName => MySqlTypeString;

        void IMySqlValue.WriteValue( MySqlPacket packet, bool binary, object val, int length ) {
            var v = val as decimal? ?? Convert.ToDecimal( val );
            var valStr = v.InvariantToString();
            if ( binary ) packet.WriteLenString( valStr );
            else packet.WriteStringNoNull( valStr );
        }

        IMySqlValue IMySqlValue.ReadValue( MySqlPacket packet, long length, bool nullVal ) {
            if ( nullVal ) return new MySqlDecimal( true );
            return new MySqlDecimal( length == -1 ? packet.ReadLenString() : packet.ReadString( length ) );
        }

        void IMySqlValue.SkipValue( MySqlPacket packet ) {
            packet.Position += (int) packet.ReadFieldLength();
        }
        #endregion

        internal static void SetDsInfo( MySqlSchemaCollection sc ) =>
            DsInfoHelper.FillRow( sc.AddRow(), MySqlTypeString, MySqlDbType.NewDecimal, TDecimal, 0, "DECIMAL({0},{1})" );
    }
}
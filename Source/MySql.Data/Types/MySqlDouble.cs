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

namespace MySql.Data.Types {
    internal struct MySqlDouble : IMySqlValue {
        private const string MySqlTypeString = "DOUBLE";
        private readonly double _mValue;
        private readonly bool _isNull;

        public MySqlDouble( bool isNull ) {
            _isNull = isNull;
            _mValue = 0.0;
        }

        public MySqlDouble( double val ) {
            _isNull = false;
            _mValue = val;
        }

        #region IMySqlValue Members
        public bool IsNull => _isNull;

        MySqlDbType IMySqlValue.MySqlDbType => MySqlDbType.Double;

        object IMySqlValue.Value => _mValue;

        public double Value => _mValue;

        Type IMySqlValue.SystemType => Constants.Types.Double;

        string IMySqlValue.MySqlTypeName => MySqlTypeString;

        void IMySqlValue.WriteValue( MySqlPacket packet, bool binary, object val, int length ) {
            var v = val as double? ?? Convert.ToDouble( val );
            if ( binary ) packet.Write( BitConverter.GetBytes( v ) );
            else packet.WriteStringNoNull( v.ToString( "R", CultureInfo.InvariantCulture ) );
        }

        IMySqlValue IMySqlValue.ReadValue( MySqlPacket packet, long length, bool nullVal ) {
            if ( nullVal ) return new MySqlDouble( true );

            if ( length == -1 ) {
                var b = new byte[8];
                packet.Read( b, 0, 8 );
                return new MySqlDouble( BitConverter.ToDouble( b, 0 ) );
            }
            var s = packet.ReadString( length );
            double d;
            try {
                d = double.Parse( s, CultureInfo.InvariantCulture );
            }
            catch ( OverflowException ) {
                // MySQL server < 5.5 can return values not compatible with
                // double.Parse(), i.e out of range for double.
                d = s.InvariantStartsWith( "-" ) ? double.MinValue : double.MaxValue;
            }
            return new MySqlDouble( d );
        }

        void IMySqlValue.SkipValue( MySqlPacket packet ) => packet.Position += 8;
        #endregion

        internal static void SetDsInfo( MySqlSchemaCollection sc ) =>
            DsInfoHelper.FillRow( sc.AddRow(), MySqlTypeString, MySqlDbType.Double, Constants.Types.Double, 0, MySqlTypeString );
    }
}
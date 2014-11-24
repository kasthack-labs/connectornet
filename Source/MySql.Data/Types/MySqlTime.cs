// Copyright (c) 2004-2008 MySQL AB, 2008-2009 Sun Microsystems, Inc.,
// 2009, 2014 Oracle and/or its affiliates. All rights reserved.
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
    internal struct MySqlTimeSpan : IMySqlValue {
        private TimeSpan _mValue;
        private bool _isNull;

        public MySqlTimeSpan( bool isNull ) {
            this._isNull = isNull;
            _mValue = TimeSpan.MinValue;
        }

        public MySqlTimeSpan( TimeSpan val ) {
            _isNull = false;
            _mValue = val;
        }

        #region IMySqlValue Members
        public bool IsNull => _isNull;

        MySqlDbType IMySqlValue.MySqlDbType => MySqlDbType.Time;

        object IMySqlValue.Value => _mValue;

        public TimeSpan Value => _mValue;

        Type IMySqlValue.SystemType => Constants.Types.TimeSpan;

        string IMySqlValue.MySqlTypeName => "TIME";

        void IMySqlValue.WriteValue( MySqlPacket packet, bool binary, object val, int length ) {
            if ( !( val is TimeSpan ) ) throw new MySqlException( "Only TimeSpan objects can be serialized by MySqlTimeSpan" );

            var ts = (TimeSpan) val;
            var negative = ts.TotalMilliseconds < 0;
            ts = ts.Duration();

            if ( binary ) {
                packet.WriteByte( (byte) ( ts.Milliseconds > 0 ? 12 : 8 ) );

                packet.WriteByte( (byte) ( negative ? 1 : 0 ) );
                packet.WriteInteger( ts.Days, 4 );
                packet.WriteByte( (byte) ts.Hours );
                packet.WriteByte( (byte) ts.Minutes );
                packet.WriteByte( (byte) ts.Seconds );
                if ( ts.Milliseconds <= 0 ) return;
                var mval = ts.Milliseconds * 1000;
                packet.WriteInteger( mval, 4 );
            }
            else {
                var s = String.Format(
                    "'{0}{1} {2:00}:{3:00}:{4:00}.{5:000000}'",
                    negative ? "-" : "",
                    ts.Days,
                    ts.Hours,
                    ts.Minutes,
                    ts.Seconds,
                    ts.Ticks % 10000000 );

                packet.WriteStringNoNull( s );
            }
        }

        IMySqlValue IMySqlValue.ReadValue( MySqlPacket packet, long length, bool nullVal ) {
            if ( nullVal ) return new MySqlTimeSpan( true );

            if ( length >= 0 ) {
                var value = packet.ReadString( length );
                ParseMySql( value );
                return this;
            }

            var bufLength = packet.ReadByte();
            var negate = 0;
            if ( bufLength > 0 ) negate = packet.ReadByte();

            _isNull = false;
            switch ( bufLength ) {
                case 0:
                    _isNull = true;
                    break;
                case 5:
                    _mValue = new TimeSpan( packet.ReadInteger( 4 ), 0, 0, 0 );
                    break;
                case 8:
                    _mValue = new TimeSpan( packet.ReadInteger( 4 ), packet.ReadByte(), packet.ReadByte(), packet.ReadByte() );
                    break;
                default:
                    _mValue = new TimeSpan(
                        packet.ReadInteger( 4 ),
                        packet.ReadByte(),
                        packet.ReadByte(),
                        packet.ReadByte(),
                        packet.ReadInteger( 4 ) / 1000000 );
                    break;
            }

            if ( negate == 1 ) _mValue = _mValue.Negate();
            return this;
        }

        void IMySqlValue.SkipValue( MySqlPacket packet ) {
            var len = packet.ReadByte();
            packet.Position += len;
        }
        #endregion

        internal static void SetDsInfo( MySqlSchemaCollection sc ) {
            // we use name indexing because this method will only be called
            // when GetSchema is called for the DataSourceInformation 
            // collection and then it wil be cached.
            var row = sc.AddRow();
            row[ "TypeName" ] = "TIME";
            row[ "ProviderDbType" ] = MySqlDbType.Time;
            row[ "ColumnSize" ] = 0;
            row[ "CreateFormat" ] = "TIME";
            row[ "CreateParameters" ] = null;
            row[ "DataType" ] = "System.TimeSpan";
            row[ "IsAutoincrementable" ] = false;
            row[ "IsBestMatch" ] = true;
            row[ "IsCaseSensitive" ] = false;
            row[ "IsFixedLength" ] = true;
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
            row[ "LiteralPrefix" ] = null;
            row[ "LiteralSuffix" ] = null;
            row[ "NativeDataType" ] = null;
        }

        public override string ToString() => String.Format( "{0} {1:00}:{2:00}:{3:00}", _mValue.Days, _mValue.Hours, _mValue.Minutes, _mValue.Seconds );

        private void ParseMySql( string s ) {
            var parts = s.Split( ':', '.' );
            var hours = Int32.Parse( parts[ 0 ] );
            var mins = Int32.Parse( parts[ 1 ] );
            var secs = Int32.Parse( parts[ 2 ] );
            var nanoseconds = 0;

            if ( parts.Length > 3 ) {
                //if the data is saved in MySql as Time(3) the division by 1000 always returns 0, but handling the data as Time(6) the result is the expected
                parts[ 3 ] = parts[ 3 ].PadRight( 7, '0' );
                nanoseconds = int.Parse( parts[ 3 ] );
            }

            if ( hours < 0
                 || parts[ 0 ].InvariantStartsWith( "-" ) ) {
                mins *= -1;
                secs *= -1;
                nanoseconds *= -1;
            }
            var days = hours / 24;
            hours = hours - ( days * 24 );
            _mValue = new TimeSpan( days, hours, mins, secs ).Add( new TimeSpan( nanoseconds ) );
            _isNull = false;
        }
    }
}
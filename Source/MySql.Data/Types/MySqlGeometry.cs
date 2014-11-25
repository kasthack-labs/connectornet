// Copyright © 2013, 2014, Oracle and/or its affiliates. All rights reserved.
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
    //Bytes structure is:
    //SRID       [0 - 3]
    //Byte order [4]
    //WKB type   [5 - 8]
    //X          [9 - 16]
    //Y          [17 - 24]
    //The byte order may be either 1 or 0 to indicate little-endian or
    //big-endian storage. The little-endian and big-endian byte orders
    //are also known as Network Data Representation (NDR) and External
    //Data Representation (XDR), respectively.

    //The WKB type is a code that indicates the geometry type. Values
    //from 1 through 7 indicate Point, LineString, Polygon, MultiPoint,
    //MultiLineString, MultiPolygon, and GeometryCollection.

    public struct MySqlGeometry : IMySqlValue {
        private readonly MySqlDbType _type;
        private readonly double _xValue;
        private readonly double _yValue;
        private readonly int _srid;
        private readonly byte[] _valBinary;
        private readonly bool _isNull;

        private const int GeometryLength = 25;
        private const string MySqlTypeString = "GEOMETRY";

        public double? XCoordinate => _xValue;

        public double? YCoordinate => _yValue;

        public int? Srid => _srid;

        public MySqlGeometry( bool isNull ) : this( MySqlDbType.Geometry, isNull ) { }

        public MySqlGeometry( double xValue, double yValue ) : this( MySqlDbType.Geometry, xValue, yValue, 0 ) { }

        public MySqlGeometry( double xValue, double yValue, int srid ) : this( MySqlDbType.Geometry, xValue, yValue, srid ) { }

        internal MySqlGeometry( MySqlDbType type, bool isNull ) {
            _type = type;
            isNull = true;
            _xValue = 0;
            _yValue = 0;
            _srid = 0;
            _valBinary = null;
            _isNull = isNull;
        }

        internal MySqlGeometry( MySqlDbType type, double xValue, double yValue, int srid ) {
            _type = type;
            _xValue = xValue;
            _yValue = yValue;
            _isNull = false;
            _srid = srid;
            _valBinary = new byte[GeometryLength];

            var sridBinary = BitConverter.GetBytes( srid );

            for ( var i = 0; i < sridBinary.Length; i++ ) _valBinary[ i ] = sridBinary[ i ];

            var xVal = BitConverter.DoubleToInt64Bits( xValue );
            var yVal = BitConverter.DoubleToInt64Bits( yValue );

            _valBinary[ 4 ] = 1;
            _valBinary[ 5 ] = 1;

            for ( var i = 0; i < 8; i++ ) {
                _valBinary[ i + 9 ] = (byte) ( xVal & 0xff );
                xVal >>= 8;
            }

            for ( var i = 0; i < 8; i++ ) {
                _valBinary[ i + 17 ] = (byte) ( yVal & 0xff );
                yVal >>= 8;
            }
        }

        public MySqlGeometry( MySqlDbType type, byte[] val ) {
            if ( val == null ) throw new ArgumentNullException( "val" );

            var buffValue = new byte[val.Length];

            for ( var i = 0; i < val.Length; i++ ) buffValue[ i ] = val[ i ];

            var xIndex = val.Length == GeometryLength ? 9 : 5;
            var yIndex = val.Length == GeometryLength ? 17 : 13;

            _valBinary = buffValue;
            _xValue = BitConverter.ToDouble( val, xIndex );
            _yValue = BitConverter.ToDouble( val, yIndex );
            _srid = val.Length == GeometryLength ? BitConverter.ToInt32( val, 0 ) : 0;
            _isNull = false;
            _type = type;
        }

        #region IMySqlValue Members
        MySqlDbType IMySqlValue.MySqlDbType => _type;

        public bool IsNull => _isNull;

        object IMySqlValue.Value => _valBinary;

        public byte[] Value => _valBinary;

        Type IMySqlValue.SystemType => TByteArray;

        string IMySqlValue.MySqlTypeName => MySqlTypeString;

        void IMySqlValue.WriteValue( MySqlPacket packet, bool binary, object val, int length ) {
            byte[] buffToWrite;
            try {
                buffToWrite = ( (MySqlGeometry) val )._valBinary;
            }
            catch {
                buffToWrite = val as byte[];
            }

            if ( buffToWrite == null ) {
                MySqlGeometry v;
                TryParse( val.ToString(), out v );
                buffToWrite = v._valBinary;
            }

            var result = new byte[GeometryLength];

            for ( var i = 0; i < buffToWrite.Length; i++ )
                if ( buffToWrite.Length < GeometryLength ) result[ i + 4 ] = buffToWrite[ i ];
                else result[ i ] = buffToWrite[ i ];

            packet.WriteStringNoNull( "_binary " );
            packet.WriteByte( (byte) '\'' );
            EscapeByteArray( result, GeometryLength, packet );
            packet.WriteByte( (byte) '\'' );
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

        IMySqlValue IMySqlValue.ReadValue( MySqlPacket packet, long length, bool nullVal ) {
            MySqlGeometry g;
            if ( nullVal ) g = new MySqlGeometry( _type, true );
            else {
                if ( length == -1 ) length = packet.ReadFieldLength();

                var newBuff = new byte[length];
                packet.Read( newBuff, 0, (int) length );
                g = new MySqlGeometry( _type, newBuff );
            }
            return g;
        }

        void IMySqlValue.SkipValue( MySqlPacket packet ) => packet.Position += (int) packet.ReadFieldLength();
        #endregion

        /// <summary>Returns the Well-Known Text representation of this value</summary>
        /// POINT({0} {1})", longitude, latitude
        /// http://dev.mysql.com/doc/refman/4.1/en/gis-wkt-format.html
        public override string ToString() {
            if ( !_isNull )
                return _srid != 0
                           ? string.Format( CultureInfo.InvariantCulture.NumberFormat, "SRID={2};POINT({0} {1})", _xValue, _yValue, _srid )
                           : string.Format( CultureInfo.InvariantCulture.NumberFormat, "POINT({0} {1})", _xValue, _yValue );

            return String.Empty;
        }

        /// <summary>
        /// Get value from WKT format
        /// SRID=0;POINT (x y) or POINT (x y)
        /// </summary>
        /// <param name="value">WKT string format</param>    
        public static MySqlGeometry Parse( string value ) {
            if ( String.IsNullOrEmpty( value ) ) throw new ArgumentNullException( "value" );

            if ( !( value.Contains( "SRID" ) || value.Contains( "POINT(" ) || value.Contains( "POINT (" ) ) )
                throw new FormatException( "String does not contain a valid geometry value" );

            MySqlGeometry result;
            TryParse( value, out result );

            return result;
        }

        /// <summary>
        /// Try to get value from WKT format
        /// SRID=0;POINT (x y) or POINT (x y)
        /// </summary>
        /// <param name="value">WKT string format</param>    
        public static bool TryParse( string value, out MySqlGeometry mySqlGeometryValue ) {
            var arrayResult = new string[0];
            var strResult = string.Empty;
            var hasX = false;
            var hasY = false;
            double xVal = 0;
            double yVal = 0;
            var sridValue = 0;

            try {
                if ( value.Contains( ";" ) ) arrayResult = value.Split( ';' );
                else strResult = value;

                if ( arrayResult.Length > 1
                     || strResult != String.Empty ) {
                    var point = strResult != String.Empty ? strResult : arrayResult[ 1 ];
                    point = point.Replace( "POINT (", "" ).Replace( "POINT(", "" ).Replace( ")", "" );
                    var coord = point.Split( ' ' );
                    if ( coord.Length > 1 ) {
                        hasX = double.TryParse( coord[ 0 ], out xVal );
                        hasY = double.TryParse( coord[ 1 ], out yVal );
                    }
                    if ( arrayResult.Length >= 1 ) Int32.TryParse( arrayResult[ 0 ].Replace( "SRID=", "" ), out sridValue );
                }
                if ( hasX && hasY ) {
                    mySqlGeometryValue = new MySqlGeometry( xVal, yVal, sridValue );
                    return true;
                }
            }
            catch {}
            mySqlGeometryValue = new MySqlGeometry( true );
            return false;
        }

        // we use name indexing because this method will only be called
        // when GetSchema is called for the DataSourceInformation 
        // collection and then it wil be cached.
        public static void SetDsInfo( MySqlSchemaCollection dsTable ) =>
            DsInfoHelper.FillRow( dsTable.AddRow(), MySqlTypeString, MySqlDbType.Geometry, TByteArray, GeometryLength, MySqlTypeString, false, false );

        public string GetWkt() => _isNull ? String.Empty : string.Format( CultureInfo.InvariantCulture.NumberFormat, "POINT({0} {1})", _xValue, _yValue );
    }
}
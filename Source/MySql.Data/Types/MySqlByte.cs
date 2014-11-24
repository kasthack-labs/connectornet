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
    internal struct MySqlByte : IMySqlValue {
        private sbyte _mValue;
        private readonly bool _isNull;
        private bool _treatAsBool;

        public MySqlByte( bool isNull ) {
            this._isNull = isNull;
            _mValue = 0;
            _treatAsBool = false;
        }

        public MySqlByte( sbyte val ) {
            _isNull = false;
            _mValue = val;
            _treatAsBool = false;
        }

        #region IMySqlValue Members
        public bool IsNull => _isNull;

        MySqlDbType IMySqlValue.MySqlDbType => MySqlDbType.Byte;

        object IMySqlValue.Value {
            get {
                if ( TreatAsBoolean ) return Convert.ToBoolean( _mValue );
                return _mValue;
            }
        }

        public sbyte Value {
            get {
                return _mValue;
            }
            set {
                _mValue = value;
            }
        }

        Type IMySqlValue.SystemType {
            get {
                if ( TreatAsBoolean ) return typeof( Boolean );
                return typeof( sbyte );
            }
        }

        string IMySqlValue.MySqlTypeName => "TINYINT";

        void IMySqlValue.WriteValue( MySqlPacket packet, bool binary, object val, int length ) {
            var v = ( val is sbyte ) ? (sbyte) val : Convert.ToSByte( val );
            if ( binary ) packet.WriteByte( (byte) v );
            else packet.WriteStringNoNull( v.ToString() );
        }

        IMySqlValue IMySqlValue.ReadValue( MySqlPacket packet, long length, bool nullVal ) {
            if ( nullVal ) return new MySqlByte( true );

            if ( length == -1 ) return new MySqlByte( (sbyte) packet.ReadByte() );
            var s = packet.ReadString( length );
            var b = new MySqlByte( SByte.Parse( s, NumberStyles.Any, CultureInfo.InvariantCulture ) );
            b.TreatAsBoolean = TreatAsBoolean;
            return b;
        }

        void IMySqlValue.SkipValue( MySqlPacket packet ) { packet.ReadByte(); }
        #endregion

        internal bool TreatAsBoolean {
            get {
                return _treatAsBool;
            }
            set {
                _treatAsBool = value;
            }
        }

        internal static void SetDsInfo( MySqlSchemaCollection sc ) {
            // we use name indexing because this method will only be called
            // when GetSchema is called for the DataSourceInformation 
            // collection and then it wil be cached.
            var row = sc.AddRow();
            row[ "TypeName" ] = "TINYINT";
            row[ "ProviderDbType" ] = MySqlDbType.Byte;
            row[ "ColumnSize" ] = 0;
            row[ "CreateFormat" ] = "TINYINT";
            row[ "CreateParameters" ] = null;
            row[ "DataType" ] = "System.SByte";
            row[ "IsAutoincrementable" ] = true;
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
    }
}
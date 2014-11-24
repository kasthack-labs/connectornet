// Copyright © 2004, 2013, Oracle and/or its affiliates. All rights reserved.
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
using System.Collections.Generic;
using System.Globalization;
using System.Text;
using MySql.Data.Common;
using MySql.Data.Types;

namespace MySql.Data.MySqlClient {
    internal enum ColumnFlags {
        NotNull = 1,
        PrimaryKey = 2,
        UniqueKey = 4,
        MultipleKey = 8,
        Blob = 16,
        Unsigned = 32,
        ZeroFill = 64,
        Binary = 128,
        Enum = 256,
        AutoIncrement = 512,
        Timestamp = 1024,
        Set = 2048,
        Number = 32768
    };

    /// <summary>
    /// Summary description for Field.
    /// </summary>
    internal class MySqlField {
        #region Fields

        // public fields
        public string CatalogName;
        public int ColumnLength;
        public string ColumnName;
        public string OriginalColumnName;
        public string TableName;
        public string RealTableName;
        public string DatabaseName;
        public Encoding Encoding;
        public int maxLength;

        // protected fields
        protected ColumnFlags ColFlags;
        protected int CharSetIndex;
        protected byte precision;
        protected byte scale;
        protected MySqlDbType MySqlDbType;
        protected DbVersion ConnVersion;
        protected Driver Driver;
        protected bool BinaryOk;
        protected List<Type> typeConversions = new List<Type>();
        #endregion

        public MySqlField( Driver driver ) {
            this.Driver = driver;
            ConnVersion = driver.Version;
            maxLength = 1;
            BinaryOk = true;
        }

        #region Properties
        public int CharacterSetIndex {
            get {
                return CharSetIndex;
            }
            set {
                CharSetIndex = value;
                SetFieldEncoding();
            }
        }

        public MySqlDbType Type => MySqlDbType;

        public byte Precision {
            get {
                return precision;
            }
            set {
                precision = value;
            }
        }

        public byte Scale {
            get {
                return scale;
            }
            set {
                scale = value;
            }
        }

        public int MaxLength {
            get {
                return maxLength;
            }
            set {
                maxLength = value;
            }
        }

        public ColumnFlags Flags => ColFlags;

        public bool IsAutoIncrement => ( ColFlags & ColumnFlags.AutoIncrement ) > 0;

        public bool IsNumeric => ( ColFlags & ColumnFlags.Number ) > 0;

        public bool AllowsNull => ( ColFlags & ColumnFlags.NotNull ) == 0;

        public bool IsUnique => ( ColFlags & ColumnFlags.UniqueKey ) > 0;

        public bool IsPrimaryKey => ( ColFlags & ColumnFlags.PrimaryKey ) > 0;

        public bool IsBlob
            =>
                ( MySqlDbType >= MySqlDbType.TinyBlob && MySqlDbType <= MySqlDbType.Blob )
                || ( MySqlDbType >= MySqlDbType.TinyText && MySqlDbType <= MySqlDbType.Text ) || ( ColFlags & ColumnFlags.Blob ) > 0;

        public bool IsBinary => BinaryOk && ( CharacterSetIndex == 63 );

        public bool IsUnsigned => ( ColFlags & ColumnFlags.Unsigned ) > 0;

        public bool IsTextField
            => Type == MySqlDbType.VarString || Type == MySqlDbType.VarChar || Type == MySqlDbType.String || ( IsBlob && !IsBinary );

        public int CharacterLength => ColumnLength / MaxLength;

        public List<Type> TypeConversions => typeConversions;
        #endregion

        public void SetTypeAndFlags( MySqlDbType type, ColumnFlags flags ) {
            ColFlags = flags;
            MySqlDbType = type;

            if ( String.IsNullOrEmpty( TableName )
                 && String.IsNullOrEmpty( RealTableName )
                 && IsBinary
                 && Driver.Settings.FunctionsReturnString ) CharacterSetIndex = Driver.ConnectionCharSetIndex;

            // if our type is an unsigned number, then we need
            // to bump it up into our unsigned types
            // we're trusting that the server is not going to set the UNSIGNED
            // flag unless we are a number
            if ( IsUnsigned )
                switch ( type ) {
                    case MySqlDbType.Byte:
                        MySqlDbType = MySqlDbType.UByte;
                        return;
                    case MySqlDbType.Int16:
                        MySqlDbType = MySqlDbType.UInt16;
                        return;
                    case MySqlDbType.Int24:
                        MySqlDbType = MySqlDbType.UInt24;
                        return;
                    case MySqlDbType.Int32:
                        MySqlDbType = MySqlDbType.UInt32;
                        return;
                    case MySqlDbType.Int64:
                        MySqlDbType = MySqlDbType.UInt64;
                        return;
                }

            if ( IsBlob ) {
                // handle blob to UTF8 conversion if requested.  This is only activated
                // on binary blobs
                if ( IsBinary && Driver.Settings.TreatBlobsAsUtf8 ) {
                    var convertBlob = false;
                    var includeRegex = Driver.Settings.GetBlobAsUtf8IncludeRegex();
                    var excludeRegex = Driver.Settings.GetBlobAsUtf8ExcludeRegex();
                    if ( includeRegex != null
                         && includeRegex.IsMatch( ColumnName ) ) convertBlob = true;
                    else if ( includeRegex == null
                              && excludeRegex != null
                              && !excludeRegex.IsMatch( ColumnName ) ) convertBlob = true;

                    if ( convertBlob ) {
                        BinaryOk = false;
                        Encoding = Encoding.GetEncoding( "UTF-8" );
                        CharSetIndex = -1; // lets driver know we are in charge of encoding
                        maxLength = 4;
                    }
                }

                if ( !IsBinary )
                    if ( type == MySqlDbType.TinyBlob ) MySqlDbType = MySqlDbType.TinyText;
                    else if ( type == MySqlDbType.MediumBlob ) MySqlDbType = MySqlDbType.MediumText;
                    else if ( type == MySqlDbType.Blob ) MySqlDbType = MySqlDbType.Text;
                    else if ( type == MySqlDbType.LongBlob ) MySqlDbType = MySqlDbType.LongText;
            }

            // now determine if we really should be binary
            if ( Driver.Settings.RespectBinaryFlags ) CheckForExceptions();

            if ( Type == MySqlDbType.String
                 && CharacterLength == 36
                 && !Driver.Settings.OldGuids ) MySqlDbType = MySqlDbType.Guid;

            if ( !IsBinary ) return;

            if ( Driver.Settings.RespectBinaryFlags )
                if ( type == MySqlDbType.String ) MySqlDbType = MySqlDbType.Binary;
                else if ( type == MySqlDbType.VarChar
                          || type == MySqlDbType.VarString ) MySqlDbType = MySqlDbType.VarBinary;

            if ( CharacterSetIndex == 63 ) CharacterSetIndex = Driver.ConnectionCharSetIndex;

            if ( Type == MySqlDbType.Binary
                 && ColumnLength == 16
                 && Driver.Settings.OldGuids ) MySqlDbType = MySqlDbType.Guid;
        }

        public void AddTypeConversion( Type t ) {
            if ( TypeConversions.Contains( t ) ) return;
            TypeConversions.Add( t );
        }

        private void CheckForExceptions() {
            var colName = String.Empty;
            if ( OriginalColumnName != null ) colName = OriginalColumnName.InvariantToUpper();
            if ( colName.InvariantStartsWith( "CHAR(") ) BinaryOk = false;
        }

        public IMySqlValue GetValueObject() {
            var v = GetIMySqlValue( Type );
            if ( v is MySqlByte
                 && ColumnLength == 1
                 && Driver.Settings.TreatTinyAsBoolean ) {
                var b = (MySqlByte) v;
                b.TreatAsBoolean = true;
                v = b;
            }
            else if ( v is MySqlGuid ) {
                var g = (MySqlGuid) v;
                g.OldGuids = Driver.Settings.OldGuids;
                v = g;
            }
            return v;
        }

        public static IMySqlValue GetIMySqlValue( MySqlDbType type ) {
            switch ( type ) {
                case MySqlDbType.Byte:
                    return new MySqlByte();
                case MySqlDbType.UByte:
                    return new MySqlUByte();
                case MySqlDbType.Int16:
                    return new MySqlInt16();
                case MySqlDbType.UInt16:
                    return new MySqlUInt16();
                case MySqlDbType.Int24:
                case MySqlDbType.Int32:
                case MySqlDbType.Year:
                    return new MySqlInt32( type, true );
                case MySqlDbType.UInt24:
                case MySqlDbType.UInt32:
                    return new MySqlUInt32( type, true );
                case MySqlDbType.Bit:
                    return new MySqlBit();
                case MySqlDbType.Int64:
                    return new MySqlInt64();
                case MySqlDbType.UInt64:
                    return new MySqlUInt64();
                case MySqlDbType.Time:
                    return new MySqlTimeSpan();
                case MySqlDbType.Date:
                case MySqlDbType.DateTime:
                case MySqlDbType.Newdate:
                case MySqlDbType.Timestamp:
                    return new MySqlDateTime( type, true );
                case MySqlDbType.Decimal:
                case MySqlDbType.NewDecimal:
                    return new MySqlDecimal();
                case MySqlDbType.Float:
                    return new MySqlSingle();
                case MySqlDbType.Double:
                    return new MySqlDouble();
                case MySqlDbType.Set:
                case MySqlDbType.Enum:
                case MySqlDbType.String:
                case MySqlDbType.VarString:
                case MySqlDbType.VarChar:
                case MySqlDbType.Text:
                case MySqlDbType.TinyText:
                case MySqlDbType.MediumText:
                case MySqlDbType.LongText:
                case (MySqlDbType) FieldType.Null:
                    return new MySqlString( type, true );
                case MySqlDbType.Geometry:
                    return new MySqlGeometry( type, true );
                case MySqlDbType.Blob:
                case MySqlDbType.MediumBlob:
                case MySqlDbType.LongBlob:
                case MySqlDbType.TinyBlob:
                case MySqlDbType.Binary:
                case MySqlDbType.VarBinary:
                    return new MySqlBinary( type, true );
                case MySqlDbType.Guid:
                    return new MySqlGuid();
                default:
                    throw new MySqlException( "Unknown data type" );
            }
        }

        private void SetFieldEncoding() {
            var charSets = Driver.CharacterSets;
            var version = Driver.Version;

            if ( charSets == null
                 || charSets.Count == 0
                 || CharacterSetIndex == -1 ) return;
            if ( charSets[ CharacterSetIndex ] == null ) return;

            var cs = CharSetMap.GetCharacterSet( version, charSets[ CharacterSetIndex ] );
            // starting with 6.0.4 utf8 has a maxlen of 4 instead of 3.  The old
            // 3 byte utf8 is utf8mb3
            if ( cs.Name.InvariantToLower() == "utf-8"
                 && version.Major >= 6 ) MaxLength = 4;
            else MaxLength = cs.ByteCount;
            Encoding = CharSetMap.GetEncoding( version, charSets[ CharacterSetIndex ] );
        }
    }
}
// Copyright © 2004, 2011, Oracle and/or its affiliates. All rights reserved.
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
using System.Diagnostics;
using System.IO;
using System.Text;
using MySql.Data.Common;
using MySql.Data.MySqlClient.Properties;

namespace MySql.Data.MySqlClient {
    internal class MySqlPacket {
        private byte[] _tempBuffer = new byte[256];
        private Encoding _encoding;
        private readonly MemoryStream _buffer = new MemoryStream( 5 );

        private MySqlPacket() { Clear(); }

        public MySqlPacket( Encoding enc ) : this() { Encoding = enc; }

        public MySqlPacket( MemoryStream stream ) : this() { _buffer = stream; }

        #region Properties
        public Encoding Encoding {
            get {
                return _encoding;
            }
            set {
                Debug.Assert( value != null );
                _encoding = value;
            }
        }

        public bool HasMoreData => _buffer.Position < _buffer.Length;

        public int Position {
            get {
                return (int) _buffer.Position;
            }
            set {
                _buffer.Position = value;
            }
        }

        public int Length {
            get {
                return (int) _buffer.Length;
            }
            set {
                _buffer.SetLength( value );
            }
        }

        public bool IsLastPacket {
            get {
                var bits = _buffer.GetBuffer();
                return bits[ 0 ] == 0xfe && Length <= 5;
            }
        }

        public byte[] Buffer => _buffer.GetBuffer();

        public DbVersion Version { get; set; }
        #endregion

        public void Clear() { Position = 4; }

        #region Byte methods
        public byte ReadByte() => (byte) _buffer.ReadByte();

        public int Read( byte[] byteBuffer, int offset, int count ) => _buffer.Read( byteBuffer, offset, count );

        public void WriteByte( byte b ) { _buffer.WriteByte( b ); }

        public void Write( byte[] bytesToWrite ) { Write( bytesToWrite, 0, bytesToWrite.Length ); }

        public void Write( byte[] bytesToWrite, int offset, int countToWrite ) { _buffer.Write( bytesToWrite, offset, countToWrite ); }

        public int ReadNBytes() {
            var c = ReadByte();
            if ( c < 1
                 || c > 4 ) throw new MySqlException( Resources.IncorrectTransmission );
            return ReadInteger( c );
        }

        public void SetByte( long position, byte value ) {
            var currentPosition = _buffer.Position;
            _buffer.Position = position;
            _buffer.WriteByte( value );
            _buffer.Position = currentPosition;
        }
        #endregion

        #region Integer methods
        public long ReadFieldLength() {
            var c = ReadByte();

            switch ( c ) {
                case 251:
                    return -1;
                case 252:
                    return ReadInteger( 2 );
                case 253:
                    return ReadInteger( 3 );
                case 254:
                    return ReadLong( 8 );
                default:
                    return c;
            }
        }

        public ulong ReadBitValue( int numbytes ) {
            var value = 0UL;

            var pos = (int) _buffer.Position;
            var bits = _buffer.GetBuffer();
            var shift = 0;

            for ( var i = 0; i < numbytes; i++ ) {
                value <<= shift;
                value |= bits[ pos++ ];
                shift = 8;
            }
            _buffer.Position += numbytes;
            return value;
        }

        public long ReadLong( int numbytes ) {
            Debug.Assert( ( _buffer.Position + numbytes ) <= _buffer.Length );
            var bytes = _buffer.GetBuffer();
            var pos = (int) _buffer.Position;
            _buffer.Position += numbytes;
            switch ( numbytes ) {
                case 2:
                    return BitConverter.ToUInt16( bytes, pos );
                case 4:
                    return BitConverter.ToUInt32( bytes, pos );
                case 8:
                    return BitConverter.ToInt64( bytes, pos );
            }
            throw new NotSupportedException( "Only byte lengths of 2, 4, or 8 are supported" );
        }

        public ulong ReadULong( int numbytes ) {
            Debug.Assert( ( _buffer.Position + numbytes ) <= _buffer.Length );
            var bytes = _buffer.GetBuffer();
            var pos = (int) _buffer.Position;
            _buffer.Position += numbytes;
            switch ( numbytes ) {
                case 2:
                    return BitConverter.ToUInt16( bytes, pos );
                case 4:
                    return BitConverter.ToUInt32( bytes, pos );
                case 8:
                    return BitConverter.ToUInt64( bytes, pos );
            }
            throw new NotSupportedException( "Only byte lengths of 2, 4, or 8 are supported" );
        }

        public int Read3ByteInt() {
            var value = 0;

            var pos = (int) _buffer.Position;
            var bits = _buffer.GetBuffer();
            var shift = 0;

            for ( var i = 0; i < 3; i++ ) {
                value |= bits[ pos++ ] << shift;
                shift += 8;
            }
            _buffer.Position += 3;
            return value;
        }

        public int ReadInteger( int numbytes ) {
            if ( numbytes == 3 ) return Read3ByteInt();
            Debug.Assert( numbytes <= 4 );
            return (int) ReadLong( numbytes );
        }

        /// <summary>
        /// WriteInteger
        /// </summary>
        /// <param name="v"></param>
        /// <param name="numbytes"></param>
        public void WriteInteger( long v, int numbytes ) {
            var val = v;

            Debug.Assert( numbytes > 0 && numbytes < 9 );

            for ( var x = 0; x < numbytes; x++ ) {
                _tempBuffer[ x ] = (byte) ( val & 0xff );
                val >>= 8;
            }
            Write( _tempBuffer, 0, numbytes );
        }

        public int ReadPackedInteger() {
            var c = ReadByte();

            switch ( c ) {
                case 251:
                    return -1;
                case 252:
                    return ReadInteger( 2 );
                case 253:
                    return ReadInteger( 3 );
                case 254:
                    return ReadInteger( 4 );
                default:
                    return c;
            }
        }

        public void WriteLength( long length ) {
            if ( length < 251 ) WriteByte( (byte) length );
            else if ( length < 65536L ) {
                WriteByte( 252 );
                WriteInteger( length, 2 );
            }
            else if ( length < 16777216L ) {
                WriteByte( 253 );
                WriteInteger( length, 3 );
            }
            else {
                WriteByte( 254 );
                WriteInteger( length, 4 );
            }
        }
        #endregion

        #region String methods
        public void WriteLenString( string s ) {
            var bytes = _encoding.GetBytes( s );
            WriteLength( bytes.Length );
            Write( bytes, 0, bytes.Length );
        }

        public void WriteStringNoNull( string v ) {
            var bytes = _encoding.GetBytes( v );
            Write( bytes, 0, bytes.Length );
        }

        public void WriteString( string v ) {
            WriteStringNoNull( v );
            WriteByte( 0 );
        }

        public string ReadLenString() {
            var len = ReadPackedInteger();
            return ReadString( len );
        }

        public string ReadAsciiString( long length ) {
            if ( length == 0 ) return String.Empty;
            //            byte[] buf = new byte[length];
            Read( _tempBuffer, 0, (int) length );
            return Encoding.GetEncoding( "us-ascii" ).GetString( _tempBuffer, 0, (int) length );
            //return encoding.GetString(tempBuffer, 0, (int)length); //buf.Length);
        }

        public string ReadString( long length ) {
            if ( length == 0 ) return String.Empty;
            if ( _tempBuffer == null
                 || length > _tempBuffer.Length ) _tempBuffer = new byte[length];
            Read( _tempBuffer, 0, (int) length );
            return _encoding.GetString( _tempBuffer, 0, (int) length );
        }

        public string ReadString() => ReadString( _encoding );

        public string ReadString( Encoding theEncoding ) {
            var bits = _buffer.GetBuffer();
            var end = (int) _buffer.Position;

            while ( end < (int) _buffer.Length
                    && bits[ end ] != 0
                    && bits[ end ] != -1 ) end++;

            var s = theEncoding.GetString( bits, (int) _buffer.Position, end - (int) _buffer.Position );
            _buffer.Position = end + 1;
            return s;
        }
        #endregion
    }
}
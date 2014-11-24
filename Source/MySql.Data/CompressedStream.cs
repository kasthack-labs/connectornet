// Copyright © 2004, 2010, Oracle and/or its affiliates. All rights reserved.
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
using System.IO;
using System.IO.Compression;
using MySql.Data.Common;
using MySql.Data.MySqlClient.Properties;

namespace MySql.Data.MySqlClient {
    /// <summary>
    /// Summary description for CompressedStream.
    /// </summary>
    internal class CompressedStream : Stream {
        // writing fields
        private readonly Stream _baseStream;
        private readonly MemoryStream _cache;

        // reading fields
        private readonly byte[] _localByte;
        private byte[] _inBuffer;
        private readonly byte[] _lengthBytes;
        private WeakReference _inBufferRef;
        private int _inPos;
        private int _maxInPos;
        private GZipStream _zInStream;

        public CompressedStream( Stream baseStream ) {
            this._baseStream = baseStream;
            _localByte = new byte[1];
            _lengthBytes = new byte[7];
            _cache = new MemoryStream();
            _inBufferRef = new WeakReference( _inBuffer, false );
        }

        #region Properties
        public override bool CanRead => _baseStream.CanRead;

        public override bool CanWrite => _baseStream.CanWrite;

        public override bool CanSeek => _baseStream.CanSeek;

        public override long Length => _baseStream.Length;

        public override long Position {
            get {
                return _baseStream.Position;
            }
            set {
                _baseStream.Position = value;
            }
        }
        #endregion
        public override void Close() {
            base.Close();
            _baseStream.Close();
            _cache.Dispose();
        }

        public override void SetLength( long value ) { throw new NotSupportedException( Resources.CSNoSetLength ); }

        public override int ReadByte() {
            try {
                Read( _localByte, 0, 1 );
                return _localByte[ 0 ];
            }
            catch ( EndOfStreamException ) {
                return -1;
            }
        }

        public override bool CanTimeout => _baseStream.CanTimeout;

        public override int ReadTimeout {
            get {
                return _baseStream.ReadTimeout;
            }
            set {
                _baseStream.ReadTimeout = value;
            }
        }

        public override int WriteTimeout {
            get {
                return _baseStream.WriteTimeout;
            }
            set {
                _baseStream.WriteTimeout = value;
            }
        }

        public override int Read( byte[] buffer, int offset, int count ) {
            if ( buffer == null ) throw new ArgumentNullException( "buffer", Resources.BufferCannotBeNull );
            if ( offset < 0
                 || offset >= buffer.Length ) throw new ArgumentOutOfRangeException( "offset", Resources.OffsetMustBeValid );
            if ( ( offset + count ) > buffer.Length ) throw new ArgumentException( Resources.BufferNotLargeEnough, "buffer" );

            if ( _inPos == _maxInPos ) PrepareNextPacket();

            var countToRead = Math.Min( count, _maxInPos - _inPos );
            var countRead = (_zInStream ?? _baseStream).Read( buffer, offset, countToRead );
            _inPos += countRead;

            // release the weak reference

            if ( _inPos == _maxInPos ) {
                _zInStream = null;
                if ( Platform.IsMono() ) return countRead;
                _inBufferRef = new WeakReference( _inBuffer, false );
                _inBuffer = null;
            }

            return countRead;
        }

        private void PrepareNextPacket() {
            MySqlStream.ReadFully( _baseStream, _lengthBytes, 0, 7 );
            var compressedLength = _lengthBytes[ 0 ] + ( _lengthBytes[ 1 ] << 8 ) + ( _lengthBytes[ 2 ] << 16 );
            // lengthBytes[3] is seq
            var unCompressedLength = _lengthBytes[ 4 ] + ( _lengthBytes[ 5 ] << 8 ) + ( _lengthBytes[ 6 ] << 16 );

            if ( unCompressedLength == 0 ) {
                unCompressedLength = compressedLength;
                _zInStream = null;
            }
            else {
                ReadNextPacket( compressedLength );
                var ms = new MemoryStream( _inBuffer );
                _zInStream = new GZipStream( ms, CompressionMode.Decompress );
                //maxinput
                //zInStream. = compressedLength;
            }

            _inPos = 0;
            _maxInPos = unCompressedLength;
        }

        private void ReadNextPacket( int len ) {
            if ( !Platform.IsMono() ) _inBuffer = _inBufferRef.Target as byte[];

            if ( _inBuffer == null
                 || _inBuffer.Length < len ) _inBuffer = new byte[len];
            MySqlStream.ReadFully( _baseStream, _inBuffer, 0, len );
        }

        private MemoryStream CompressCache() {
            // small arrays almost never yeild a benefit from compressing
            if ( _cache.Length < 50 ) return null;

            var cacheBytes = _cache.GetBuffer();
            var compressedBuffer = new MemoryStream();
            var zos = new GZipStream( compressedBuffer, CompressionLevel.Optimal ); // zlibConst.Z_DEFAULT_COMPRESSION);
            zos.Flush();

            // if the compression hasn't helped, then just return null
            return compressedBuffer.Length >= _cache.Length ? null : compressedBuffer;
        }

        private void CompressAndSendCache() {
            long compressedLength, uncompressedLength;

            // we need to save the sequence byte that is written
            var cacheBuffer = _cache.GetBuffer();
            var seq = cacheBuffer[ 3 ];
            cacheBuffer[ 3 ] = 0;

            // first we compress our current cache
            var compressedBuffer = CompressCache();

            // now we set our compressed and uncompressed lengths
            // based on if our compression is going to help or not
            MemoryStream memStream;

            if ( compressedBuffer == null ) {
                compressedLength = _cache.Length;
                uncompressedLength = 0;
                memStream = _cache;
            }
            else {
                compressedLength = compressedBuffer.Length;
                uncompressedLength = _cache.Length;
                memStream = compressedBuffer;
            }

            // Make space for length prefix (7 bytes) at the start of output
            var dataLength = memStream.Length;
            var bytesToWrite = (int) dataLength + 7;
            memStream.SetLength( bytesToWrite );

            var buffer = memStream.GetBuffer();
            Array.Copy( buffer, 0, buffer, 7, (int) dataLength );

            // Write length prefix
            buffer[ 0 ] = (byte) ( compressedLength & 0xff );
            buffer[ 1 ] = (byte) ( ( compressedLength >> 8 ) & 0xff );
            buffer[ 2 ] = (byte) ( ( compressedLength >> 16 ) & 0xff );
            buffer[ 3 ] = seq;
            buffer[ 4 ] = (byte) ( uncompressedLength & 0xff );
            buffer[ 5 ] = (byte) ( ( uncompressedLength >> 8 ) & 0xff );
            buffer[ 6 ] = (byte) ( ( uncompressedLength >> 16 ) & 0xff );

            _baseStream.Write( buffer, 0, bytesToWrite );
            _baseStream.Flush();
            _cache.SetLength( 0 );
            compressedBuffer?.Dispose();
        }

        public override void Flush() {
            if ( !InputDone() ) return;

            CompressAndSendCache();
        }

        private bool InputDone() {
            // if we have not done so yet, see if we can calculate how many bytes we are expecting
            if ( _baseStream is TimedStream
                 && ( (TimedStream) _baseStream ).IsClosed ) return false;
            if ( _cache.Length < 4 ) return false;
            var buf = _cache.GetBuffer();
            var expectedLen = buf[ 0 ] + ( buf[ 1 ] << 8 ) + ( buf[ 2 ] << 16 );
            if ( _cache.Length < ( expectedLen + 4 ) ) return false;
            return true;
        }

        public override void WriteByte( byte value ) { _cache.WriteByte( value ); }

        public override void Write( byte[] buffer, int offset, int count ) { _cache.Write( buffer, offset, count ); }

        public override long Seek( long offset, SeekOrigin origin ) => _baseStream.Seek( offset, origin );
    }
}
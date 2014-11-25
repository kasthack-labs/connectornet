// Copyright (c) 2004-2008 MySQL AB, 2008-2009 Sun Microsystems, Inc. 2014, Oracle and/or its affiliates. All rights reserved.
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
using System.Runtime.InteropServices;
using System.Threading;
using MySql.Data.MySqlClient;

namespace MySql.Data.Common {
    /// <summary>
    /// Helper class to encapsulate shared memory functionality
    /// Also cares of proper cleanup of file mapping object and cew
    /// </summary>
    internal class SharedMemory : IDisposable {
        private const uint FileMapWrite = 0x0002;

        private IntPtr _fileMapping;

        public SharedMemory( string name, IntPtr size ) {
            _fileMapping = NativeMethods.OpenFileMapping( FileMapWrite, false, name );
            if ( _fileMapping == IntPtr.Zero ) throw new MySqlException( "Cannot open file mapping " + name );
            View = NativeMethods.MapViewOfFile( _fileMapping, FileMapWrite, 0, 0, size );
        }

        #region Destructor
        ~SharedMemory() { Dispose( false ); }
        #endregion

        public IntPtr View { get; private set; }

        public void Dispose() {
            Dispose( true );
            GC.SuppressFinalize( this );
        }

        protected virtual void Dispose( bool disposing ) {
            if ( !disposing ) return;
            if ( View != IntPtr.Zero ) {
                NativeMethods.UnmapViewOfFile( View );
                View = IntPtr.Zero;
            }
            if ( _fileMapping == IntPtr.Zero ) return;
            // Free the handle
            NativeMethods.CloseHandle( _fileMapping );
            _fileMapping = IntPtr.Zero;
        }
    }

    /// <summary>
    /// Summary description for SharedMemoryStream.
    /// </summary>
    internal class SharedMemoryStream : Stream {
        private string _memoryName;
        private EventWaitHandle _serverRead;
        private EventWaitHandle _serverWrote;
        private EventWaitHandle _clientRead;
        private EventWaitHandle _clientWrote;
        private EventWaitHandle _connectionClosed;
        private SharedMemory _data;
        private int _bytesLeft;
        private int _position;
        private int _connectNumber;

        private const int Bufferlength = 16004;

        private int _readTimeout = Timeout.Infinite;
        private int _writeTimeout = Timeout.Infinite;

        public SharedMemoryStream( string memName ) { _memoryName = memName; }

        public void Open( uint timeOut ) {
            if ( _connectionClosed != null ) Debug.Assert( false, "Connection is already open" );
            GetConnectNumber( timeOut );
            SetupEvents();
        }

        public override void Close() {
            if ( _connectionClosed == null ) return;
            var isClosed = _connectionClosed.WaitOne( 0 );
            if ( !isClosed ) {
                _connectionClosed.Set();
                _connectionClosed.Close();
            }
            _connectionClosed = null;
            EventWaitHandle[] handles = { _serverRead, _serverWrote, _clientRead, _clientWrote };

            foreach ( EventWaitHandle t in handles ) t?.Close();
            _data?.Dispose();
            _data = null;
        }

        private void GetConnectNumber( uint timeOut ) {
            EventWaitHandle connectRequest;
            try {
                connectRequest = EventWaitHandle.OpenExisting( _memoryName + "_CONNECT_REQUEST" );
            }
            catch ( Exception ) {
                // If server runs as service, its shared memory is global 
                // And if connector runs in user session, it needs to prefix
                // shared memory name with "Global\"
                var prefixedMemoryName = @"Global\" + _memoryName;
                connectRequest = EventWaitHandle.OpenExisting( prefixedMemoryName + "_CONNECT_REQUEST" );
                _memoryName = prefixedMemoryName;
            }
            var connectAnswer = EventWaitHandle.OpenExisting( _memoryName + "_CONNECT_ANSWER" );
            using ( var connectData = new SharedMemory( _memoryName + "_CONNECT_DATA", (IntPtr) 4 ) ) {
                // now start the connection
                if ( !connectRequest.Set() ) throw new MySqlException( "Failed to open shared memory connection" );
                if ( !connectAnswer.WaitOne( (int) ( timeOut * 1000 ), false ) ) throw new MySqlException( "Timeout during connection" );
                _connectNumber = Marshal.ReadInt32( connectData.View );
            }
        }

        private void SetupEvents() {
            var prefix = _memoryName + "_" + _connectNumber;
            _data = new SharedMemory( prefix + "_DATA", (IntPtr) Bufferlength );
            _serverWrote = EventWaitHandle.OpenExisting( prefix + "_SERVER_WROTE" );
            _serverRead = EventWaitHandle.OpenExisting( prefix + "_SERVER_READ" );
            _clientWrote = EventWaitHandle.OpenExisting( prefix + "_CLIENT_WROTE" );
            _clientRead = EventWaitHandle.OpenExisting( prefix + "_CLIENT_READ" );
            _connectionClosed = EventWaitHandle.OpenExisting( prefix + "_CONNECTION_CLOSED" );

            // tell the server we are ready
            _serverRead.Set();
        }

        #region Properties
        public override bool CanRead => true;

        public override bool CanSeek => false;

        public override bool CanWrite => true;

        public override long Length {
            get {
                throw new NotSupportedException( "SharedMemoryStream does not support seeking - length" );
            }
        }

        public override long Position {
            get {
                throw new NotSupportedException( "SharedMemoryStream does not support seeking - position" );
            }
            set {}
        }
        #endregion

        public override void Flush() {
            // No need to flush anything to disk ,as our shared memory is backed 
            // by the page file
        }

        public override int Read( byte[] buffer, int offset, int count ) {
            var timeLeft = _readTimeout;
            WaitHandle[] waitHandles = { _serverWrote, _connectionClosed };
            var stopwatch = new LowResolutionStopwatch();
            while ( _bytesLeft == 0 ) {
                stopwatch.Start();
                var index = WaitHandle.WaitAny( waitHandles, timeLeft );
                stopwatch.Stop();
                if ( index == WaitHandle.WaitTimeout ) throw new TimeoutException( "Timeout when reading from shared memory" );

                if ( waitHandles[ index ] == _connectionClosed ) throw new MySqlException( "Connection to server lost", true, null );

                if ( _readTimeout != Timeout.Infinite ) {
                    timeLeft = _readTimeout - (int) stopwatch.ElapsedMilliseconds;
                    if ( timeLeft < 0 ) throw new TimeoutException( "Timeout when reading from shared memory" );
                }

                _bytesLeft = Marshal.ReadInt32( _data.View );
                _position = 4;
            }

            var len = Math.Min( count, _bytesLeft );
            var baseMem = _data.View.ToInt64() + _position;

            for ( var i = 0; i < len; i++, _position++ ) buffer[ offset + i ] = Marshal.ReadByte( (IntPtr) ( baseMem + i ) );

            _bytesLeft -= len;
            if ( _bytesLeft == 0 ) _clientRead.Set();

            return len;
        }

        public override long Seek( long offset, SeekOrigin origin ) {
            throw new NotSupportedException( "SharedMemoryStream does not support seeking" );
        }

        public override void Write( byte[] buffer, int offset, int count ) {
            var leftToDo = count;
            var buffPos = offset;
            WaitHandle[] waitHandles = { _serverRead, _connectionClosed };
            var stopwatch = new LowResolutionStopwatch();
            var timeLeft = _writeTimeout;

            while ( leftToDo > 0 ) {
                stopwatch.Start();
                var index = WaitHandle.WaitAny( waitHandles, timeLeft );
                stopwatch.Stop();

                if ( waitHandles[ index ] == _connectionClosed ) throw new MySqlException( "Connection to server lost", true, null );

                if ( index == WaitHandle.WaitTimeout ) throw new TimeoutException( "Timeout when reading from shared memory" );

                if ( _writeTimeout != Timeout.Infinite ) {
                    timeLeft = _writeTimeout - (int) stopwatch.ElapsedMilliseconds;
                    if ( timeLeft < 0 ) throw new TimeoutException( "Timeout when writing to shared memory" );
                }
                var bytesToDo = Math.Min( leftToDo, Bufferlength );
                var baseMem = _data.View.ToInt64() + 4;
                Marshal.WriteInt32( _data.View, bytesToDo );
                Marshal.Copy( buffer, buffPos, (IntPtr) baseMem, bytesToDo );
                buffPos += bytesToDo;
                leftToDo -= bytesToDo;
                if ( !_clientWrote.Set() ) throw new MySqlException( "Writing to shared memory failed" );
            }
        }

        public override void SetLength( long value ) { throw new NotSupportedException( "SharedMemoryStream does not support seeking" ); }

        public override bool CanTimeout => true;

        public override int ReadTimeout {
            get {
                return _readTimeout;
            }
            set {
                _readTimeout = value;
            }
        }

        public override int WriteTimeout {
            get {
                return _writeTimeout;
            }
            set {
                _writeTimeout = value;
            }
        }
    }

}
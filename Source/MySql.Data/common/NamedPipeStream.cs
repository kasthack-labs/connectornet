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
using System.ComponentModel;
using System.IO;
using System.Runtime.InteropServices;
using System.Security;
using System.Threading;
using Microsoft.Win32.SafeHandles;
using MySql.Data.MySqlClient.Properties;

namespace MySql.Data.Common {
    /// <summary>
    /// Summary description for API.
    /// </summary>
#if !CF
    [SuppressUnmanagedCodeSecurity]
#endif
    internal class NamedPipeStream : Stream {
        private SafeFileHandle _handle;
        private Stream _fileStream;
        private int _readTimeout = Timeout.Infinite;
        private int _writeTimeout = Timeout.Infinite;
        private const int ErrorPipeBusy = 231;
        private const int ErrorSemTimeout = 121;

        public NamedPipeStream( string path, FileAccess mode, uint timeout ) { Open( path, mode, timeout ); }

        private void CancelIo() {
            var ok = NativeMethods.CancelIo( _handle.DangerousGetHandle() );
            if ( !ok ) throw new Win32Exception( Marshal.GetLastWin32Error() );
        }

        public void Open( string path, FileAccess mode, uint timeout ) {
            IntPtr nativeHandle;

            for ( ;; ) {
                var security = new NativeMethods.SecurityAttributes();
                security.inheritHandle = true;
                security.Length = Marshal.SizeOf( security );

                nativeHandle = NativeMethods.CreateFile(
                    path,
                    NativeMethods.GenericRead | NativeMethods.GenericWrite,
                    0,
                    security,
                    NativeMethods.OpenExisting,
                    NativeMethods.FileFlagOverlapped,
                    0 );

                if ( nativeHandle != IntPtr.Zero ) break;

                if ( Marshal.GetLastWin32Error() != ErrorPipeBusy ) throw new Win32Exception( Marshal.GetLastWin32Error(), "Error opening pipe" );
                var sw = LowResolutionStopwatch.StartNew();
                var success = NativeMethods.WaitNamedPipe( path, timeout );
                sw.Stop();
                if ( !success ) {
                    if ( timeout < sw.ElapsedMilliseconds
                         || Marshal.GetLastWin32Error() == ErrorSemTimeout ) throw new TimeoutException( "Timeout waiting for named pipe" );
                    throw new Win32Exception( Marshal.GetLastWin32Error(), "Error waiting for pipe" );
                }
                timeout -= (uint) sw.ElapsedMilliseconds;
            }
            _handle = new SafeFileHandle( nativeHandle, true );
            _fileStream = new FileStream( _handle, mode, 4096, true );
        }

        public override bool CanRead => _fileStream.CanRead;

        public override bool CanWrite => _fileStream.CanWrite;

        public override bool CanSeek {
            get {
                throw new NotSupportedException( Resources.NamedPipeNoSeek );
            }
        }

        public override long Length {
            get {
                throw new NotSupportedException( Resources.NamedPipeNoSeek );
            }
        }

        public override long Position {
            get {
                throw new NotSupportedException( Resources.NamedPipeNoSeek );
            }
            set {}
        }

        public override void Flush() { _fileStream.Flush(); }

        public override int Read( byte[] buffer, int offset, int count ) {
            if ( _readTimeout == Timeout.Infinite ) return _fileStream.Read( buffer, offset, count );
            var result = _fileStream.BeginRead( buffer, offset, count, null, null );
            if ( result.CompletedSynchronously ) return _fileStream.EndRead( result );

            if ( !result.AsyncWaitHandle.WaitOne( _readTimeout ) ) {
                CancelIo();
                throw new TimeoutException( "Timeout in named pipe read" );
            }
            return _fileStream.EndRead( result );
        }

        public override void Write( byte[] buffer, int offset, int count ) {
            if ( _writeTimeout == Timeout.Infinite ) {
                _fileStream.Write( buffer, offset, count );
                return;
            }
            var result = _fileStream.BeginWrite( buffer, offset, count, null, null );
            if ( result.CompletedSynchronously ) _fileStream.EndWrite( result );

            if ( !result.AsyncWaitHandle.WaitOne( _readTimeout ) ) {
                CancelIo();
                throw new TimeoutException( "Timeout in named pipe write" );
            }
            _fileStream.EndWrite( result );
        }

        public override void Close() {
            if ( _handle != null
                 && !_handle.IsInvalid
                 && !_handle.IsClosed ) {
                _fileStream.Close();
                try {
                    _handle.Close();
                }
                catch ( Exception ) {}
            }
        }

        public override void SetLength( long length ) { throw new NotSupportedException( Resources.NamedPipeNoSetLength ); }

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

        public override long Seek( long offset, SeekOrigin origin ) { throw new NotSupportedException( Resources.NamedPipeNoSeek ); }

        internal static Stream Create( string pipeName, string hostname, uint timeout ) {
            string pipePath;
            if ( 0 == String.Compare( hostname, "localhost", true ) ) pipePath = @"\\.\pipe\" + pipeName;
            else pipePath = String.Format( @"\\{0}\pipe\{1}", hostname, pipeName );
            return new NamedPipeStream( pipePath, FileAccess.ReadWrite, timeout );
        }
    }
}
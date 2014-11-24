// Copyright © 2012, Oracle and/or its affiliates. All rights reserved.
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
using System.Runtime.InteropServices;
using System.Security;
using System.Text;
using MySql.Data.Constants;
using MySql.Data.MySqlClient.Properties;

namespace MySql.Data.MySqlClient.Authentication {
    /// <summary>
    /// 
    /// </summary>
    [SuppressUnmanagedCodeSecurity]
    internal class MySqlWindowsAuthenticationPlugin : MySqlAuthenticationPlugin {
        private SecurityHandle _outboundCredentials = new SecurityHandle( 0 );
        private SecurityHandle _clientContext = new SecurityHandle( 0 );
        private SecurityInteger _lifetime = new SecurityInteger( 0 );
        private bool _continueProcessing;
        private string _targetName;

        protected override void CheckConstraints() {
            var platform = String.Empty;
            var p = (int) Environment.OSVersion.Platform;
            if ( ( p == 4 )
                 || ( p == 128 ) ) platform = "Unix";
            else if ( Environment.OSVersion.Platform == PlatformID.MacOSX ) platform = "Mac OS/X";

            if ( !String.IsNullOrEmpty( platform ) ) throw new MySqlException( String.Format( Resources.WinAuthNotSupportOnPlatform, platform ) );
            base.CheckConstraints();
        }

        public override string GetUsername() {
            var username = base.GetUsername();
            return String.IsNullOrEmpty( username ) ?"auth_windows" : username;
        }

        public override string PluginName => "authentication_windows_client";

        protected override byte[] MoreData( byte[] moreData ) {
            if ( moreData == null ) AcquireCredentials();

            byte[] clientBlob = null;

            if ( _continueProcessing ) InitializeClient( out clientBlob, moreData, out _continueProcessing );

            if ( !_continueProcessing
                 || clientBlob == null
                 || clientBlob.Length == 0 ) {
                FreeCredentialsHandle( ref _outboundCredentials );
                DeleteSecurityContext( ref _clientContext );
                return null;
            }
            return clientBlob;
        }

        private void InitializeClient( out byte[] clientBlob, byte[] serverBlob, out bool continueProcessing ) {
            clientBlob = null;
            continueProcessing = true;
            var clientBufferDesc = new SecBufferDesc( MaxTokenSize );
            var initLifetime = new SecurityInteger( 0 );
            var ss = -1;
            try {
                var contextAttributes = 0u;

                if ( serverBlob == null )
                    ss = InitializeSecurityContext(
                        ref _outboundCredentials,
                        IntPtr.Zero,
                        _targetName,
                        StandardContextAttributes,
                        0,
                        SecurityNetworkDrep,
                        IntPtr.Zero,
                        /* always zero first time around */
                        0,
                        out _clientContext,
                        out clientBufferDesc,
                        out contextAttributes,
                        out initLifetime );
                else {
                    var serverBufferDesc = new SecBufferDesc( serverBlob );

                    try {
                        ss = InitializeSecurityContext(
                            ref _outboundCredentials,
                            ref _clientContext,
                            _targetName,
                            StandardContextAttributes,
                            0,
                            SecurityNetworkDrep,
                            ref serverBufferDesc,
                            0,
                            out _clientContext,
                            out clientBufferDesc,
                            out contextAttributes,
                            out initLifetime );
                    }
                    finally {
                        serverBufferDesc.Dispose();
                    }
                }

                if ( ( SecICompleteNeeded == ss )
                     || ( SecICompleteAndContinue == ss ) ) CompleteAuthToken( ref _clientContext, ref clientBufferDesc );

                if ( ss != SecEOk
                     && ss != SecIContinueNeeded
                     && ss != SecICompleteNeeded
                     && ss != SecICompleteAndContinue ) throw new MySqlException( "InitializeSecurityContext() failed  with errorcode " + ss );

                clientBlob = clientBufferDesc.GetSecBufferByteArray();
            }
            finally {
                clientBufferDesc.Dispose();
            }
            continueProcessing = ( ss != SecEOk && ss != SecICompleteNeeded );
        }

        /// <summary>
        /// Currently this method is unused
        /// </summary>
        /// <returns></returns>
        private string GetTargetName() {
            //return null;
            if ( AuthenticationData == null ) return String.Empty;

            var index = -1;
            for ( var i = 0; i < AuthenticationData.Length; i++ ) {
                if ( AuthenticationData[ i ] != 0 ) continue;
                index = i;
                break;
            }
            if ( index == -1 )
                _targetName = Encoding.UTF8.GetString( AuthenticationData );
            _targetName = Encoding.UTF8.GetString( AuthenticationData, 0, index );
            return _targetName;
        }

        private void AcquireCredentials() {
            _continueProcessing = true;

            var ss = AcquireCredentialsHandle(
                null,
                "Negotiate",
                SecpkgCredOutbound,
                IntPtr.Zero,
                IntPtr.Zero,
                0,
                IntPtr.Zero,
                ref _outboundCredentials,
                ref _lifetime );
            if ( ss != SecEOk ) throw new MySqlException( "AcquireCredentialsHandle failed with errorcode" + ss );
        }

        #region SSPI Constants and Imports
        private const int SecEOk = 0;
        private const int SecIContinueNeeded = 0x90312;
        private const int SecICompleteNeeded = 0x1013;
        private const int SecICompleteAndContinue = 0x1014;

        private const int SecpkgCredOutbound = 2;
        private const int SecurityNetworkDrep = 0;
        private const int SecurityNativeDrep = 0x10;
        private const int SecpkgCredInbound = 1;
        private const int MaxTokenSize = 12288;
        private const int SecpkgAttrSizes = 0;
        private const int StandardContextAttributes = 0;

        [DllImport( "secur32", CharSet = CharSet.Unicode )]
        private static extern int AcquireCredentialsHandle(
            string pszPrincipal,
            string pszPackage,
            int fCredentialUse,
            IntPtr pAuthenticationId,
            IntPtr pAuthData,
            int pGetKeyFn,
            IntPtr pvGetKeyArgument,
            ref SecurityHandle phCredential,
            ref SecurityInteger ptsExpiry );

        [DllImport( "secur32", CharSet = CharSet.Unicode, SetLastError = true )]
        private static extern int InitializeSecurityContext(
            ref SecurityHandle phCredential,
            IntPtr phContext,
            string pszTargetName,
            int fContextReq,
            int reserved1,
            int targetDataRep,
            IntPtr pInput,
            int reserved2,
            out SecurityHandle phNewContext,
            out SecBufferDesc pOutput,
            out uint pfContextAttr,
            out SecurityInteger ptsExpiry );

        [DllImport( "secur32", CharSet = CharSet.Unicode, SetLastError = true )]
        private static extern int InitializeSecurityContext(
            ref SecurityHandle phCredential,
            ref SecurityHandle phContext,
            string pszTargetName,
            int fContextReq,
            int reserved1,
            int targetDataRep,
            ref SecBufferDesc secBufferDesc,
            int reserved2,
            out SecurityHandle phNewContext,
            out SecBufferDesc pOutput,
            out uint pfContextAttr,
            out SecurityInteger ptsExpiry );

        [DllImport( "secur32", CharSet = CharSet.Unicode, SetLastError = true )]
        private static extern int CompleteAuthToken( ref SecurityHandle phContext, ref SecBufferDesc pToken );

        [DllImport( "secur32.Dll", CharSet = CharSet.Unicode, SetLastError = false )]
        public static extern int QueryContextAttributes(
            ref SecurityHandle phContext,
            uint ulAttribute,
            out SecPkgContextSizes pContextAttributes );

        [DllImport( "secur32.Dll", CharSet = CharSet.Unicode, SetLastError = false )]
        public static extern int FreeCredentialsHandle( ref SecurityHandle pCred );

        [DllImport( "secur32.Dll", CharSet = CharSet.Unicode, SetLastError = false )]
        public static extern int DeleteSecurityContext( ref SecurityHandle pCred );
        #endregion
    }

    [StructLayout( LayoutKind.Sequential )]
    internal struct SecBufferDesc : IDisposable {
        public int ulVersion;
        public int cBuffers;
        public IntPtr pBuffers; //Point to SecBuffer

        public SecBufferDesc( int bufferSize ) {
            ulVersion = (int) SecBufferType.SecbufferVersion;
            cBuffers = 1;
            var secBuffer = new SecBuffer( bufferSize );
            pBuffers = Marshal.AllocHGlobal( Marshal.SizeOf( secBuffer ) );
            Marshal.StructureToPtr( secBuffer, pBuffers, false );
        }

        public SecBufferDesc( byte[] secBufferBytes ) {
            ulVersion = (int) SecBufferType.SecbufferVersion;
            cBuffers = 1;
            var thisSecBuffer = new SecBuffer( secBufferBytes );
            pBuffers = Marshal.AllocHGlobal( Marshal.SizeOf( thisSecBuffer ) );
            Marshal.StructureToPtr( thisSecBuffer, pBuffers, false );
        }

        public void Dispose() {
            if ( pBuffers != IntPtr.Zero ) {
                Debug.Assert( cBuffers == 1 );
                var thisSecBuffer = (SecBuffer)Marshal.PtrToStructure( pBuffers, Constants.Types.SecBuffer );
                thisSecBuffer.Dispose();
                Marshal.FreeHGlobal( pBuffers );
                pBuffers = IntPtr.Zero;
            }
        }

        public byte[] GetSecBufferByteArray() {
            byte[] buffer = null;

            if ( pBuffers == IntPtr.Zero ) throw new InvalidOperationException( "Object has already been disposed!!!" );
            Debug.Assert( cBuffers == 1 );
            var secBuffer = (SecBuffer)Marshal.PtrToStructure( pBuffers, Constants.Types.SecBuffer );
            if ( secBuffer.cbBuffer > 0 ) {
                buffer = new byte[secBuffer.cbBuffer];
                Marshal.Copy( secBuffer.pvBuffer, buffer, 0, secBuffer.cbBuffer );
            }
            return ( buffer );
        }
    }

    public enum SecBufferType {
        SecbufferVersion = 0,
        SecbufferEmpty = 0,
        SecbufferData = 1,
        SecbufferToken = 2
    }

    [StructLayout( LayoutKind.Sequential )]
    public struct SecHandle //=PCtxtHandle
    {
        private readonly IntPtr dwLower; // ULONG_PTR translates to IntPtr not to uint
        private readonly IntPtr dwUpper; // this is crucial for 64-Bit Platforms
    }

    [StructLayout( LayoutKind.Sequential )]
    public struct SecBuffer : IDisposable {
        public int cbBuffer;
        public int BufferType;
        public IntPtr pvBuffer;

        public SecBuffer( int bufferSize ) {
            cbBuffer = bufferSize;
            BufferType = (int) SecBufferType.SecbufferToken;
            pvBuffer = Marshal.AllocHGlobal( bufferSize );
        }

        public SecBuffer( byte[] secBufferBytes ) {
            cbBuffer = secBufferBytes.Length;
            BufferType = (int) SecBufferType.SecbufferToken;
            pvBuffer = Marshal.AllocHGlobal( cbBuffer );
            Marshal.Copy( secBufferBytes, 0, pvBuffer, cbBuffer );
        }

        public SecBuffer( byte[] secBufferBytes, SecBufferType bufferType ) {
            cbBuffer = secBufferBytes.Length;
            BufferType = (int) bufferType;
            pvBuffer = Marshal.AllocHGlobal( cbBuffer );
            Marshal.Copy( secBufferBytes, 0, pvBuffer, cbBuffer );
        }

        public void Dispose() {
            if ( pvBuffer != IntPtr.Zero ) {
                Marshal.FreeHGlobal( pvBuffer );
                pvBuffer = IntPtr.Zero;
            }
        }
    }

    [StructLayout( LayoutKind.Sequential )]
    public struct SecurityInteger {
        public uint LowPart;
        public int HighPart;

        public SecurityInteger( int dummy ) {
            LowPart = 0;
            HighPart = 0;
        }
    };

    [StructLayout( LayoutKind.Sequential )]
    public struct SecurityHandle {
        public IntPtr LowPart;
        public IntPtr HighPart;
        public SecurityHandle( int dummy ) { LowPart = HighPart = new IntPtr( 0 ); }
    };

    [StructLayout( LayoutKind.Sequential )]
    public struct SecPkgContextSizes {
        public uint cbMaxToken;
        public uint cbMaxSignature;
        public uint cbBlockSize;
        public uint cbSecurityTrailer;
    };
}
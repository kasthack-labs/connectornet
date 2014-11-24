// Copyright © 2004, 2011, 2013, Oracle and/or its affiliates. All rights reserved.
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

#if RT
using System.Linq;
#endif
using System;
using System.Collections;
using System.ComponentModel;
using System.IO;
using System.Security;
using System.Text;
using MySql.Data.Common;
using MySql.Data.MySqlClient.Authentication;
using MySql.Data.MySqlClient.Properties;
using MySql.Data.Types;
#if !CF && !RT
using System.Security.Cryptography.X509Certificates;
using System.Net.Security;
using System.Security.Authentication;

#endif

namespace MySql.Data.MySqlClient {
    /// <summary>
    /// Summary description for Driver.
    /// </summary>
    internal class NativeDriver : IDriver {
        private DbVersion _version;
        protected String EncryptionSeed;
        protected ServerStatusFlags serverStatus;
        protected MySqlStream Stream;
        protected Stream BaseStream;
        private BitArray _nullMap;
        private readonly Driver _owner;
        private MySqlAuthenticationPlugin _authPlugin;

        // Windows authentication method string, used by the protocol.
        // Also known as "client plugin name".
        private const string AuthenticationWindowsPlugin = "authentication_windows_client";

        // Predefined username for IntegratedSecurity
        private const string AuthenticationWindowsUser = "auth_windows";

        public NativeDriver( Driver owner ) {
            _owner = owner;
            ThreadId = -1;
        }

        public ClientFlags Flags { get; private set; }

        public int ThreadId { get; private set; }

        public DbVersion Version => _version;

        public ServerStatusFlags ServerStatus => serverStatus;

        public int WarningCount { get; private set; }

        public MySqlPacket Packet { get; private set; }

        internal MySqlConnectionStringBuilder Settings => _owner.Settings;

        internal Encoding Encoding => _owner.Encoding;

        private void HandleException( MySqlException ex ) { if ( ex.IsFatal ) _owner.Close(); }

        internal void SendPacket( MySqlPacket p ) { Stream.SendPacket( p ); }

        internal void SendEmptyPacket() {
            var buffer = new byte[4];
            Stream.SendEntirePacketDirectly( buffer, 0 );
        }

        internal MySqlPacket ReadPacket() { return Packet = Stream.ReadPacket(); }

        internal void ReadOk( bool read ) {
            try {
                if ( read ) Packet = Stream.ReadPacket();
                var marker = Packet.ReadByte();
                if ( marker != 0 ) throw new MySqlException( "Out of sync with server", true, null );

                Packet.ReadFieldLength(); /* affected rows */
                Packet.ReadFieldLength(); /* last insert id */
                if ( Packet.HasMoreData ) {
                    serverStatus = (ServerStatusFlags) Packet.ReadInteger( 2 );
                    Packet.ReadInteger( 2 ); /* warning count */
                    if ( Packet.HasMoreData ) Packet.ReadLenString(); /* message */
                }
            }
            catch ( MySqlException ex ) {
                HandleException( ex );
                throw;
            }
        }

        /// <summary>
        /// Sets the current database for the this connection
        /// </summary>
        /// <param name="dbName"></param>
        public void SetDatabase( string dbName ) {
            var dbNameBytes = Encoding.GetBytes( dbName );

            Packet.Clear();
            Packet.WriteByte( (byte) DbCmd.InitDb );
            Packet.Write( dbNameBytes );
            ExecutePacket( Packet );

            ReadOk( true );
        }

        public void Configure() {
            Stream.MaxPacketSize = (ulong) _owner.MaxPacketSize;
            Stream.Encoding = Encoding;
        }

        public void Open() {
            // connect to one of our specified hosts
            try {
                BaseStream = StreamCreator.GetStream( Settings );
#if !CF && !RT
                if ( Settings.IncludeSecurityAsserts ) MySqlSecurityPermission.CreatePermissionSet( false ).Assert();
#endif
            }
            catch ( SecurityException ) {
                throw;
            }
            catch ( Exception ex ) {
                throw new MySqlException( Resources.UnableToConnectToHost, (int) MySqlErrorCode.UnableToConnectToHost, ex );
            }
            if ( BaseStream == null ) throw new MySqlException( Resources.UnableToConnectToHost, (int) MySqlErrorCode.UnableToConnectToHost );

            var maxSinglePacket = 255 * 255 * 255;
            Stream = new MySqlStream( BaseStream, Encoding, false );

            Stream.ResetTimeout( (int) Settings.ConnectionTimeout * 1000 );

            // read off the welcome packet and parse out it's values
            Packet = Stream.ReadPacket();
            //todo:protocol???
            var protocol = Packet.ReadByte();
            var versionString = Packet.ReadString();
            _owner.IsFabric = versionString.EndsWith( "fabric", StringComparison.OrdinalIgnoreCase );
            _version = DbVersion.Parse( versionString );
            if ( !_owner.IsFabric
                 && !_version.IsAtLeast( 5, 0, 0 ) ) throw new NotSupportedException( Resources.ServerTooOld );
            ThreadId = Packet.ReadInteger( 4 );
            EncryptionSeed = Packet.ReadString();

            maxSinglePacket = ( 256 * 256 * 256 ) - 1;

            // read in Server capabilities if they are provided
            ClientFlags serverCaps = 0;
            if ( Packet.HasMoreData ) serverCaps = (ClientFlags) Packet.ReadInteger( 2 );

            /* New protocol with 16 bytes to describe server characteristics */
            _owner.ConnectionCharSetIndex = Packet.ReadByte();

            serverStatus = (ServerStatusFlags) Packet.ReadInteger( 2 );

            // Since 5.5, high bits of server caps are stored after status.
            // Previously, it was part of reserved always 0x00 13-byte filler.
            var serverCapsHigh = (uint) Packet.ReadInteger( 2 );
            serverCaps |= (ClientFlags) ( serverCapsHigh << 16 );

            Packet.Position += 11;
            var seedPart2 = Packet.ReadString();
            EncryptionSeed += seedPart2;

            var authenticationMethod = ( serverCaps & ClientFlags.PluginAuth ) != 0 ? Packet.ReadString() : "mysql_native_password";

            // based on our settings, set our connection flags
            SetConnectionFlags( serverCaps );

            Packet.Clear();
            Packet.WriteInteger( (int) Flags, 4 );

#if !CF && !RT
            if ( ( serverCaps & ClientFlags.Ssl ) == 0 ) {
                if ( ( Settings.SslMode != MySqlSslMode.None )
                     && ( Settings.SslMode != MySqlSslMode.Preferred ) ) {
                    // Client requires SSL connections.
                    var message = String.Format( Resources.NoServerSSLSupport, Settings.Server );
                    throw new MySqlException( message );
                }
            }
            else if ( Settings.SslMode != MySqlSslMode.None ) {
                Stream.SendPacket( Packet );
                StartSsl();
                Packet.Clear();
                Packet.WriteInteger( (int) Flags, 4 );
            }
#endif

#if RT
      if (Settings.SslMode != MySqlSslMode.None)
      {
        throw new NotImplementedException("SSL not supported in this WinRT release.");
      }
#endif

            Packet.WriteInteger( maxSinglePacket, 4 );
            Packet.WriteByte( 8 );
            Packet.Write( new byte[23] );

            Authenticate( authenticationMethod, false );

            // if we are using compression, then we use our CompressedStream class
            // to hide the ugliness of managing the compression
            if ( ( Flags & ClientFlags.Compress ) != 0 ) Stream = new MySqlStream( BaseStream, Encoding, true );

            // give our stream the server version we are connected to.  
            // We may have some fields that are read differently based 
            // on the version of the server we are connected to.
            Packet.Version = _version;
            Stream.MaxBlockSize = maxSinglePacket;
        }

#if !CF && !RT

        #region SSL
        /// <summary>
        /// Retrieve client SSL certificates. Dependent on connection string 
        /// settings we use either file or store based certificates.
        /// </summary>
        private X509CertificateCollection GetClientCertificates() {
            var certs = new X509CertificateCollection();

            // Check for file-based certificate
            if ( Settings.CertificateFile != null ) {
                if ( !Version.IsAtLeast( 5, 1, 0 ) ) throw new MySqlException( Resources.FileBasedCertificateNotSupported );

                var clientCert = new X509Certificate2( Settings.CertificateFile, Settings.CertificatePassword );
                certs.Add( clientCert );
                return certs;
            }

            if ( Settings.CertificateStoreLocation == MySqlCertificateStoreLocation.None ) return certs;

            var location = ( Settings.CertificateStoreLocation == MySqlCertificateStoreLocation.CurrentUser )
                               ? StoreLocation.CurrentUser
                               : StoreLocation.LocalMachine;

            // Check for store-based certificate
            var store = new X509Store( StoreName.My, location );
            store.Open( OpenFlags.ReadOnly | OpenFlags.OpenExistingOnly );

            if ( Settings.CertificateThumbprint == null ) {
                // Return all certificates from the store.
                certs.AddRange( store.Certificates );
                return certs;
            }

            // Find certificate with given thumbprint
            certs.AddRange( store.Certificates.Find( X509FindType.FindByThumbprint, Settings.CertificateThumbprint, true ) );

            if ( certs.Count == 0 ) throw new MySqlException( "Certificate with Thumbprint " + Settings.CertificateThumbprint + " not found" );
            return certs;
        }

        private void StartSsl() {
            var sslValidateCallback = new RemoteCertificateValidationCallback( ServerCheckValidation );
            var ss = new SslStream( BaseStream, true, sslValidateCallback, null );
            var certs = GetClientCertificates();
            ss.AuthenticateAsClient( Settings.Server, certs, SslProtocols.Default, false );
            BaseStream = ss;
            Stream = new MySqlStream( ss, Encoding, false ) { SequenceByte = 2 };
        }

        private bool ServerCheckValidation( object sender, X509Certificate certificate, X509Chain chain, SslPolicyErrors sslPolicyErrors ) {
            if ( sslPolicyErrors == SslPolicyErrors.None ) return true;

            if ( Settings.SslMode == MySqlSslMode.Preferred
                 || Settings.SslMode == MySqlSslMode.Required )
                //Tolerate all certificate errors.
                return true;

            if ( Settings.SslMode == MySqlSslMode.VerifyCa
                 && sslPolicyErrors == SslPolicyErrors.RemoteCertificateNameMismatch )
                // Tolerate name mismatch in certificate, if full validation is not requested.
                return true;

            return false;
        }
        #endregion

#endif

        #region Authentication
        /// <summary>
        /// Return the appropriate set of connection flags for our
        /// server capabilities and our user requested options.
        /// </summary>
        private void SetConnectionFlags( ClientFlags serverCaps ) {
            // allow load data local infile
            var flags = ClientFlags.LocalFiles;

            if ( !Settings.UseAffectedRows ) flags |= ClientFlags.FoundRows;

            flags |= ClientFlags.Protocol41;
            // Need this to get server status values
            flags |= ClientFlags.Transactions;

            // user allows/disallows batch statements
            if ( Settings.AllowBatch ) flags |= ClientFlags.MultiStatements;

            // We always allow multiple result sets
            flags |= ClientFlags.MultiResults;

            // if the server allows it, tell it that we want long column info
            if ( ( serverCaps & ClientFlags.LongFlag ) != 0 ) flags |= ClientFlags.LongFlag;

            // if the server supports it and it was requested, then turn on compression
            if ( ( serverCaps & ClientFlags.Compress ) != 0
                 && Settings.UseCompression ) flags |= ClientFlags.Compress;

            flags |= ClientFlags.LongPassword; // for long passwords

            // did the user request an interactive session?
            if ( Settings.InteractiveSession ) flags |= ClientFlags.Interactive;

            // if the server allows it and a database was specified, then indicate
            // that we will connect with a database name
            if ( ( serverCaps & ClientFlags.ConnectWithDb ) != 0
                 && !string.IsNullOrEmpty( Settings.Database ) ) flags |= ClientFlags.ConnectWithDb;

            // if the server is requesting a secure connection, then we oblige
            if ( ( serverCaps & ClientFlags.SecureConnection ) != 0 ) flags |= ClientFlags.SecureConnection;

#if !CF
            // if the server is capable of SSL and the user is requesting SSL
            if ( ( serverCaps & ClientFlags.Ssl ) != 0
                 && Settings.SslMode != MySqlSslMode.None ) flags |= ClientFlags.Ssl;
#endif

            // if the server supports output parameters, then we do too
            if ( ( serverCaps & ClientFlags.PsMultiResults ) != 0 ) flags |= ClientFlags.PsMultiResults;

            if ( ( serverCaps & ClientFlags.PluginAuth ) != 0 ) flags |= ClientFlags.PluginAuth;

            // if the server supports connection attributes
            if ( ( serverCaps & ClientFlags.ConnectAttrs ) != 0 ) flags |= ClientFlags.ConnectAttrs;

            if ( ( serverCaps & ClientFlags.CanHandleExpiredPassword ) != 0 ) flags |= ClientFlags.CanHandleExpiredPassword;

            Flags = flags;
        }

        public void Authenticate( string authMethod, bool reset ) {
            if ( authMethod != null ) {
                var seedBytes = Encoding.GetBytes( EncryptionSeed );

                // Integrated security is a shortcut for windows auth
                if ( Settings.IntegratedSecurity ) authMethod = "authentication_windows_client";

                _authPlugin = MySqlAuthenticationPlugin.GetPlugin( authMethod, this, seedBytes );
            }
            _authPlugin.Authenticate( reset );
        }
        #endregion

        public void Reset() {
            WarningCount = 0;
            Stream.Encoding = Encoding;
            Stream.SequenceByte = 0;
            Packet.Clear();
            Packet.WriteByte( (byte) DbCmd.ChangeUser );
            Authenticate( null, true );
        }

        /// <summary>
        /// Query is the method that is called to send all queries to the server
        /// </summary>
        public void SendQuery( MySqlPacket queryPacket ) {
            WarningCount = 0;
            queryPacket.SetByte( 4, (byte) DbCmd.Query );
            ExecutePacket( queryPacket );
            // the server will respond in one of several ways with the first byte indicating
            // the type of response.
            // 0 == ok packet.  This indicates non-select queries
            // 0xff == error packet.  This is handled in stream.OpenPacket
            // > 0 = number of columns in select query
            // We don't actually read the result here since a single query can generate
            // multiple resultsets and we don't want to duplicate code.  See ReadResult
            // Instead we set our internal server status flag to indicate that we have a query waiting.
            // This flag will be maintained by ReadResult
            serverStatus |= ServerStatusFlags.AnotherQuery;
        }

        public void Close( bool isOpen ) {
            try {
                if ( isOpen )
                    try {
                        Packet.Clear();
                        Packet.WriteByte( (byte) DbCmd.Quit );
                        ExecutePacket( Packet );
                    }
                    catch ( Exception ) {
                        // Eat exception here. We should try to closing 
                        // the stream anyway.
                    }

                Stream?.Close();
                Stream = null;
            }
            catch ( Exception ) {
                // we are just going to eat any exceptions
                // generated here
            }
        }

        public bool Ping() {
            try {
                Packet.Clear();
                Packet.WriteByte( (byte) DbCmd.Ping );
                ExecutePacket( Packet );
                ReadOk( true );
                return true;
            }
            catch ( Exception ) {
                _owner.Close();
                return false;
            }
        }

        public int GetResult( ref int affectedRow, ref long insertedId ) {
            try {
                Packet = Stream.ReadPacket();
            }
            catch ( TimeoutException ) {
                // Do not reset serverStatus, allow to reenter, e.g when
                // ResultSet is closed.
                throw;
            }
            catch ( Exception ) {
                serverStatus = 0;
                throw;
            }

            var fieldCount = (int) Packet.ReadFieldLength();
            if ( -1 == fieldCount ) {
                var filename = Packet.ReadString();
                SendFileToServer( filename );

                return GetResult( ref affectedRow, ref insertedId );
            }
            if ( fieldCount == 0 ) {
                // the code to read last packet will set these server status vars 
                // again if necessary.
                serverStatus &= ~( ServerStatusFlags.AnotherQuery | ServerStatusFlags.MoreResults );
                affectedRow = (int) Packet.ReadFieldLength();
                insertedId = Packet.ReadFieldLength();

                serverStatus = (ServerStatusFlags) Packet.ReadInteger( 2 );
                WarningCount += Packet.ReadInteger( 2 );
                if ( Packet.HasMoreData ) Packet.ReadLenString(); //TODO: server message
            }
            return fieldCount;
        }

        /// <summary>
        /// Sends the specified file to the server. 
        /// This supports the LOAD DATA LOCAL INFILE
        /// </summary>
        /// <param name="filename"></param>
        private void SendFileToServer( string filename ) {
            var buffer = new byte[8196];

            try {
                using ( var fs = new FileStream( filename, FileMode.Open, FileAccess.Read ) ) {
                    var len = fs.Length;
                    while ( len > 0 ) {
                        var count = fs.Read( buffer, 4, len > 8192 ? 8192 : (int)len );
                        Stream.SendEntirePacketDirectly( buffer, count );
                        len -= count;
                    }
                    Stream.SendEntirePacketDirectly( buffer, 0 );
                }
            }
            catch ( Exception ex ) {
                throw new MySqlException( "Error during LOAD DATA LOCAL INFILE", ex );
            }
        }

        private void ReadNullMap( int fieldCount ) {
            // if we are binary, then we need to load in our null bitmap
            _nullMap = null;
            var nullMapBytes = new byte[( fieldCount + 9 ) / 8];
            Packet.ReadByte();
            Packet.Read( nullMapBytes, 0, nullMapBytes.Length );
            _nullMap = new BitArray( nullMapBytes );
        }

        public IMySqlValue ReadColumnValue( int index, MySqlField field, IMySqlValue valObject ) {
            var length = -1L;
            bool isNull;

            if ( _nullMap != null ) isNull = _nullMap[ index + 2 ];
            else {
                length = Packet.ReadFieldLength();
                isNull = length == -1;
            }

            Packet.Encoding = field.Encoding;
            Packet.Version = _version;
            return valObject.ReadValue( Packet, length, isNull );
        }

        public void SkipColumnValue( IMySqlValue valObject ) {
            var length = -1;
            if ( _nullMap == null ) {
                length = (int) Packet.ReadFieldLength();
                if ( length == -1 ) return;
            }
            if ( length > -1 ) Packet.Position += length;
            else valObject.SkipValue( Packet );
        }

        public void GetColumnsData( MySqlField[] columns ) {
            foreach ( MySqlField t in columns ) GetColumnData( t );
            ReadEof();
        }

        private void GetColumnData( MySqlField field ) {
            Stream.Encoding = Encoding;
            Packet = Stream.ReadPacket();
            field.Encoding = Encoding;
            field.CatalogName = Packet.ReadLenString();
            field.DatabaseName = Packet.ReadLenString();
            field.TableName = Packet.ReadLenString();
            field.RealTableName = Packet.ReadLenString();
            field.ColumnName = Packet.ReadLenString();
            field.OriginalColumnName = Packet.ReadLenString();
            Packet.ReadByte();
            field.CharacterSetIndex = Packet.ReadInteger( 2 );
            field.ColumnLength = Packet.ReadInteger( 4 );
            var type = (MySqlDbType) Packet.ReadByte();
            ColumnFlags colFlags;
            if ( ( Flags & ClientFlags.LongFlag ) != 0 ) colFlags = (ColumnFlags) Packet.ReadInteger( 2 );
            else colFlags = (ColumnFlags) Packet.ReadByte();
            field.Scale = Packet.ReadByte();

            if ( Packet.HasMoreData ) Packet.ReadInteger( 2 ); // reserved

            if ( type == MySqlDbType.Decimal
                 || type == MySqlDbType.NewDecimal ) {
                field.Precision = (byte) ( field.ColumnLength - 2 );
                if ( ( colFlags & ColumnFlags.Unsigned ) != 0 ) field.Precision++;
            }

            field.SetTypeAndFlags( type, colFlags );
        }

        private void ExecutePacket( MySqlPacket packetToExecute ) {
            try {
                WarningCount = 0;
                Stream.SequenceByte = 0;
                Stream.SendPacket( packetToExecute );
            }
            catch ( MySqlException ex ) {
                HandleException( ex );
                throw;
            }
        }

        public void ExecuteStatement( MySqlPacket packetToExecute ) {
            WarningCount = 0;
            packetToExecute.SetByte( 4, (byte) DbCmd.Execute );
            ExecutePacket( packetToExecute );
            serverStatus |= ServerStatusFlags.AnotherQuery;
        }

        private void CheckEof() {
            if ( !Packet.IsLastPacket ) throw new MySqlException( "Expected end of data packet" );

            Packet.ReadByte(); // read off the 254

            if ( Packet.HasMoreData ) {
                WarningCount += Packet.ReadInteger( 2 );
                serverStatus = (ServerStatusFlags) Packet.ReadInteger( 2 );

                // if we are at the end of this cursor based resultset, then we remove
                // the last row sent status flag so our next fetch doesn't abort early
                // and we remove this command result from our list of active CommandResult objects.
                //                if ((serverStatus & ServerStatusFlags.LastRowSent) != 0)
                //              {
                //                serverStatus &= ~ServerStatusFlags.LastRowSent;
                //              commandResults.Remove(lastCommandResult);
                //        }
            }
        }

        private void ReadEof() {
            Packet = Stream.ReadPacket();
            CheckEof();
        }

        public int PrepareStatement( string sql, ref MySqlField[] parameters ) {
            //TODO: check this
            //ClearFetchedRow();

            Packet.Length = sql.Length * 4 + 5;
            var buffer = Packet.Buffer;
            var len = Encoding.GetBytes( sql, 0, sql.Length, Packet.Buffer, 5 );
            Packet.Position = len + 5;
            buffer[ 4 ] = (byte) DbCmd.Prepare;
            ExecutePacket( Packet );

            Packet = Stream.ReadPacket();

            var marker = Packet.ReadByte();
            if ( marker != 0 ) throw new MySqlException( "Expected prepared statement marker" );

            var statementId = Packet.ReadInteger( 4 );
            var numCols = Packet.ReadInteger( 2 );
            var numParams = Packet.ReadInteger( 2 );
            //TODO: find out what this is needed for
            Packet.ReadInteger( 3 );
            if ( numParams > 0 ) {
                parameters = _owner.GetColumns( numParams );
                // we set the encoding for each parameter back to our connection encoding
                // since we can't trust what is coming back from the server
                foreach ( MySqlField t in parameters ) t.Encoding = Encoding;
            }

            if ( numCols > 0 ) {
                while ( numCols-- > 0 ) Packet = Stream.ReadPacket();
                    //TODO: handle streaming packets

                ReadEof();
            }

            return statementId;
        }

        //		private void ClearFetchedRow() 
        //		{
        //			if (lastCommandResult == 0) return;

        //TODO
        /*			CommandResult result = (CommandResult)commandResults[lastCommandResult];
                result.ReadRemainingColumns();

                stream.OpenPacket();
                if (! stream.IsLastPacket)
                    throw new MySqlException("Cursor reading out of sync");

                ReadEOF(false);
                lastCommandResult = 0;*/
        //		}

        /// <summary>
        /// FetchDataRow is the method that the data reader calls to see if there is another 
        /// row to fetch.  In the non-prepared mode, it will simply read the next data packet.
        /// In the prepared mode (statementId > 0), it will 
        /// </summary>
        public bool FetchDataRow( int statementId, int columns ) {
            /*			ClearFetchedRow();

                  if (!commandResults.ContainsKey(statementId)) return false;

                  if ( (serverStatus & ServerStatusFlags.LastRowSent) != 0)
                      return false;

                  stream.StartPacket(9, true);
                  stream.WriteByte((byte)DBCmd.FETCH);
                  stream.WriteInteger(statementId, 4);
                  stream.WriteInteger(1, 4);
                  stream.Flush();

                  lastCommandResult = statementId;
                      */
            Packet = Stream.ReadPacket();
            if ( Packet.IsLastPacket ) {
                CheckEof();
                return false;
            }
            _nullMap = null;
            if ( statementId > 0 ) ReadNullMap( columns );

            return true;
        }

        public void CloseStatement( int statementId ) {
            Packet.Clear();
            Packet.WriteByte( (byte) DbCmd.CloseStmt );
            Packet.WriteInteger( statementId, 4 );
            Stream.SequenceByte = 0;
            Stream.SendPacket( Packet );
        }

        /// <summary>
        /// Execution timeout, in milliseconds. When the accumulated time for network IO exceeds this value
        /// TimeoutException is thrown. This timeout needs to be reset for every new command
        /// </summary>
        /// 
        public void ResetTimeout( int timeout ) {
            Stream?.ResetTimeout( timeout );
        }

        internal void SetConnectAttrs() {
            // Sets connect attributes
            if ( ( Flags & ClientFlags.ConnectAttrs ) == 0 ) return;
            var connectAttrs = string.Empty;
            var attrs = new MySqlConnectAttrs();
            foreach ( var property in attrs.GetType().GetProperties() ) {
                var name = property.Name;
#if RT
          object[] customAttrs = property.GetCustomAttributes(typeof(DisplayNameAttribute), false).ToArray<object>();
#else
                var customAttrs = property.GetCustomAttributes( typeof( DisplayNameAttribute ), false );
#endif
                if ( customAttrs.Length > 0 ) {
                    var displayNameAttribute = customAttrs[ 0 ] as DisplayNameAttribute;
                    if ( displayNameAttribute != null ) name = displayNameAttribute.DisplayName;
                }

                var value = (string) property.GetValue( attrs, null );
                connectAttrs += string.Format( "{0}{1}", (char) name.Length, name );
                connectAttrs += string.Format( "{0}{1}", (char) value.Length, value );
            }
            Packet.WriteLenString( connectAttrs );
        }
    }
}
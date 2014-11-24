// Copyright © 2004, 2013, 2014, Oracle and/or its affiliates. All rights reserved.
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
using System.Security;
using System.Text;
using MySql.Data.Common;
using MySql.Data.MySqlClient.Properties;
using MySql.Data.Types;

namespace MySql.Data.MySqlClient {
    /// <summary>
    /// Summary description for BaseDriver.
    /// </summary>
    internal class Driver : IDisposable {
        protected bool isOpen;
        protected DateTime CreationTime;
        protected string ServerCharSet;
        protected Dictionary<string, string> ServerProps;
        protected Dictionary<int, string> CharSets;
        protected long maxPacketSize;
        internal int TimeZoneOffset;
        private bool _firstResult;
        protected IDriver Handler;
        internal MySqlDataReader Reader;
        private bool _disposeInProgress;
        internal bool IsFabric;

        /// <summary>
        /// For pooled connections, time when the driver was
        /// put into idle queue
        /// </summary>
        public DateTime IdleSince { get; set; }

        public Driver( MySqlConnectionStringBuilder settings ) {
            Encoding = Encoding.GetEncoding( "Windows-1252" );
            if ( Encoding == null ) throw new MySqlException( Resources.DefaultEncodingNotFound );
            Settings = settings;
            ServerCharSet = "latin1";
            ConnectionCharSetIndex = -1;
            maxPacketSize = 1024;
            Handler = new NativeDriver( this );
        }

        ~Driver() { Dispose( false ); }

        #region Properties
        public int ThreadId => Handler.ThreadId;

        public DbVersion Version => Handler.Version;

        public MySqlConnectionStringBuilder Settings { get; set; }

        public Encoding Encoding { get; set; }

        public MySqlPromotableTransaction CurrentTransaction { get; set; }

        public bool IsInActiveUse { get; set; }

        public bool IsOpen => isOpen;

        public MySqlPool Pool { get; set; }

        public long MaxPacketSize => maxPacketSize;

        protected internal int ConnectionCharSetIndex { get; set; }

        internal Dictionary<int, string> CharacterSets => CharSets;

        public bool SupportsOutputParameters => Version.IsAtLeast( 5, 5, 0 );

        public bool SupportsBatch => ( Handler.Flags & ClientFlags.MultiStatements ) != 0;

        public bool SupportsConnectAttrs => ( Handler.Flags & ClientFlags.ConnectAttrs ) != 0;

        public bool SupportsPasswordExpiration => ( Handler.Flags & ClientFlags.CanHandleExpiredPassword ) != 0;

        public bool IsPasswordExpired { get; internal set; }
        #endregion

        public string Property( string key ) => ServerProps[ key ];

        public bool ConnectionLifetimeExpired() => Settings.ConnectionLifeTime != 0 && DateTime.Now.Subtract( CreationTime ).TotalSeconds > Settings.ConnectionLifeTime;

        public static Driver Create( MySqlConnectionStringBuilder settings ) {
            Driver d = null;
            try {
                if ( MySqlTrace.QueryAnalysisEnabled
                     || settings.Logging
                     || settings.UseUsageAdvisor ) d = new TracingDriver( settings );
            }
            catch ( TypeInitializationException ex ) {
                if ( !( ex.InnerException is SecurityException ) ) throw;
                //Only rethrow if InnerException is not a SecurityException. If it is a SecurityException then 
                //we couldn't initialize MySqlTrace because we don't have unmanaged code permissions. 
            }
            if ( d == null ) d = new Driver( settings );

            //this try was added as suggested fix submitted on MySql Bug 72025, socket connections are left in CLOSE_WAIT status when connector fails to open a new connection.
            //the bug is present when the client try to get more connections that the server support or has configured in the max_connections variable.
            try {
                d.Open();
            }
            catch {
                d.Dispose();
                throw;
            }
            return d;
        }

        public bool HasStatus( ServerStatusFlags flag ) => ( Handler.ServerStatus & flag ) != 0;

        public virtual void Open() {
            CreationTime = DateTime.Now;
            Handler.Open();
            isOpen = true;
        }

        public virtual void Close() { Dispose(); }

        public virtual void Configure( MySqlConnection connection ) {
            var firstConfigure = false;

            // if we have not already configured our server variables
            // then do so now
            if ( ServerProps == null ) {
                firstConfigure = true;

                // if we are in a pool and the user has said it's ok to cache the
                // properties, then grab it from the pool
                try {
                    if ( Pool != null
                         && Settings.CacheServerProperties ) {
                        if ( Pool.ServerProperties == null ) Pool.ServerProperties = LoadServerProperties( connection );
                        ServerProps = Pool.ServerProperties;
                    }
                    else ServerProps = LoadServerProperties( connection );

                    LoadCharacterSets( connection );
                }
                catch ( MySqlException ex ) {
                    // expired password capability
                    if ( ex.Number == 1820 ) {
                        IsPasswordExpired = true;
                        return;
                    }
                    throw;
                }
            }

#if AUTHENTICATED
      string licenseType = serverProps["license"];
      if (licenseType == null || licenseType.Length == 0 || 
        licenseType != "commercial") 
        throw new MySqlException( "This client library licensed only for use with commercially-licensed MySQL servers." );
#endif
            // if the user has indicated that we are not to reset
            // the connection and this is not our first time through,
            // then we are done.
            if ( !Settings.ConnectionReset
                 && !firstConfigure ) return;

            var charSet = Settings.CharacterSet;
            if ( string.IsNullOrEmpty( charSet ) )
                if ( ConnectionCharSetIndex >= 0
                     && CharSets.ContainsKey( ConnectionCharSetIndex ) ) charSet = CharSets[ ConnectionCharSetIndex ];
                else charSet = ServerCharSet;

            if ( ServerProps.ContainsKey( "max_allowed_packet" ) ) maxPacketSize = Convert.ToInt64( ServerProps[ "max_allowed_packet" ] );

            // now tell the server which character set we will send queries in and which charset we
            // want results in
            var charSetCmd = new MySqlCommand( "SET character_set_results=NULL", connection ) { InternallyCreated = true };

            string clientCharSet;
            ServerProps.TryGetValue( "character_set_client", out clientCharSet );
            string connCharSet;
            ServerProps.TryGetValue( "character_set_connection", out connCharSet );
            if ( ( clientCharSet != null && clientCharSet != charSet )
                 || ( connCharSet != null && connCharSet != charSet ) ) {
                var setNamesCmd = new MySqlCommand( "SET NAMES " + charSet, connection ) { InternallyCreated = true };
                setNamesCmd.ExecuteNonQuery();
            }
            charSetCmd.ExecuteNonQuery();

            Encoding = CharSetMap.GetEncoding( Version, charSet ?? "latin1" );

            Handler.Configure();
        }

        /// <summary>
        /// Loads the properties from the connected server into a hashtable
        /// </summary>
        /// <param name="connection"></param>
        /// <returns></returns>
        private Dictionary<string, string> LoadServerProperties( MySqlConnection connection ) {
            // load server properties
            var hash = new Dictionary<string, string>();
            var cmd = new MySqlCommand( "SHOW VARIABLES", connection );
            try {
                using ( var reader = cmd.ExecuteReader() )
                    while ( reader.Read() ) {
                        var key = reader.GetString( 0 );
                        var value = reader.GetString( 1 );
                        hash[ key ] = value;
                    }
                // Get time zone offset as numerical value
                TimeZoneOffset = GetTimeZoneOffset( connection );
                return hash;
            }
            catch ( Exception ex ) {
                MySqlTrace.LogError( ThreadId, ex.Message );
                throw;
            }
        }

        private int GetTimeZoneOffset( MySqlConnection con ) {
            var cmd = new MySqlCommand( "select timediff( curtime(), utc_time() )", con );
            var s = cmd.ExecuteScalar() as string ?? "0:00";

            return int.Parse( s.Substring( 0, s.IndexOf( ':' ) ) );
        }

        /// <summary>
        /// Loads all the current character set names and ids for this server 
        /// into the charSets hashtable
        /// </summary>
        private void LoadCharacterSets( MySqlConnection connection ) {
            var cmd = new MySqlCommand( "SHOW COLLATION", connection );

            // now we load all the currently active collations
            try {
                using ( var reader = cmd.ExecuteReader() ) {
                    CharSets = new Dictionary<int, string>();
                    while ( reader.Read() )
                        CharSets[ Convert.ToInt32( reader[ "id" ], NumberFormatInfo.InvariantInfo ) ] =
                            reader.GetString( reader.GetOrdinal( "charset" ) );
                }
            }
            catch ( Exception ex ) {
                MySqlTrace.LogError( ThreadId, ex.Message );
                throw;
            }
        }

        public virtual List<MySqlError> ReportWarnings( MySqlConnection connection ) {
            var warnings = new List<MySqlError>();

            var cmd = new MySqlCommand( "SHOW WARNINGS", connection ) { InternallyCreated = true };
            using ( var reader = cmd.ExecuteReader() ) while ( reader.Read() ) warnings.Add( new MySqlError( reader.GetString( 0 ), reader.GetInt32( 1 ), reader.GetString( 2 ) ) );

            var args = new MySqlInfoMessageEventArgs { Errors = warnings.ToArray() };
            connection?.OnInfoMessage( args );
            return warnings;
        }

        public virtual void SendQuery( MySqlPacket p ) {
            Handler.SendQuery( p );
            _firstResult = true;
        }

        public virtual ResultSet NextResult( int statementId, bool force ) {
            if ( !force
                 && !_firstResult
                 && !HasStatus( ServerStatusFlags.AnotherQuery | ServerStatusFlags.MoreResults ) ) return null;
            _firstResult = false;
            //todo:nu lv
            int affectedRows = -1, warnings = 0;

            var insertedId = -1L;
            var fieldCount = GetResult( statementId, ref affectedRows, ref insertedId );
            if ( fieldCount == -1 ) return null;
            return fieldCount > 0 ? new ResultSet( this, statementId, fieldCount ) : new ResultSet( affectedRows, insertedId );
        }

        protected virtual int GetResult( int statementId, ref int affectedRows, ref long insertedId ) => Handler.GetResult( ref affectedRows, ref insertedId );

        public virtual bool FetchDataRow( int statementId, int columns ) => Handler.FetchDataRow( statementId, columns );

        public virtual bool SkipDataRow() => FetchDataRow( -1, 0 );

        public virtual void ExecuteDirect( string sql ) {
            var p = new MySqlPacket( Encoding );
            p.WriteString( sql );
            SendQuery( p );
            NextResult( 0, false );
        }

        public MySqlField[] GetColumns( int count ) {
            var fields = new MySqlField[count];
            for ( var i = 0; i < count; i++ ) fields[ i ] = new MySqlField( this );
            Handler.GetColumnsData( fields );

            return fields;
        }

        public virtual int PrepareStatement( string sql, ref MySqlField[] parameters ) => Handler.PrepareStatement( sql, ref parameters );

        public IMySqlValue ReadColumnValue( int index, MySqlField field, IMySqlValue value ) => Handler.ReadColumnValue( index, field, value );

        public void SkipColumnValue( IMySqlValue valObject ) { Handler.SkipColumnValue( valObject ); }

        public void ResetTimeout( int timeoutMilliseconds ) { Handler.ResetTimeout( timeoutMilliseconds ); }

        public bool Ping() => Handler.Ping();

        public virtual void SetDatabase( string dbName ) { Handler.SetDatabase( dbName ); }

        public virtual void ExecuteStatement( MySqlPacket packetToExecute ) { Handler.ExecuteStatement( packetToExecute ); }

        public virtual void CloseStatement( int id ) { Handler.CloseStatement( id ); }

        public virtual void Reset() { Handler.Reset(); }

        public virtual void CloseQuery( MySqlConnection connection, int statementId ) {
            if ( Handler.WarningCount > 0 ) ReportWarnings( connection );
        }

        #region IDisposable Members
        protected virtual void Dispose( bool disposing ) {
            // Avoid cyclic calls to Dispose.
            if ( _disposeInProgress ) return;

            _disposeInProgress = true;

            try {
                ResetTimeout( 1000 );
                if ( disposing ) Handler.Close( isOpen );
                // if we are pooling, then release ourselves
                if ( Settings.Pooling ) MySqlPoolManager.RemoveConnection( this );
            }
            catch ( Exception ) {
                if ( disposing ) throw;
            }
            finally {
                Reader = null;
                isOpen = false;
                _disposeInProgress = false;
            }
        }

        public void Dispose() {
            Dispose( true );
            GC.SuppressFinalize( this );
        }
        #endregion
    }

    internal interface IDriver {
        int ThreadId { get; }
        DbVersion Version { get; }
        ServerStatusFlags ServerStatus { get; }
        ClientFlags Flags { get; }
        void Configure();
        void Open();
        void SendQuery( MySqlPacket packet );
        void Close( bool isOpen );
        bool Ping();
        int GetResult( ref int affectedRows, ref long insertedId );
        bool FetchDataRow( int statementId, int columns );
        int PrepareStatement( string sql, ref MySqlField[] parameters );
        void ExecuteStatement( MySqlPacket packet );
        void CloseStatement( int statementId );
        void SetDatabase( string dbName );
        void Reset();
        IMySqlValue ReadColumnValue( int index, MySqlField field, IMySqlValue valObject );
        void SkipColumnValue( IMySqlValue valueObject );
        void GetColumnsData( MySqlField[] columns );
        void ResetTimeout( int timeout );
        int WarningCount { get; }
    }
}
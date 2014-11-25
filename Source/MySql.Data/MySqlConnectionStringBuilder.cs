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
using System.Collections.Generic;
using System.ComponentModel;
using System.Globalization;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using MySql.Data.Common;
using MySql.Data.MySqlClient.Properties;
using MySql.Data.Constants.Types;

namespace MySql.Data.MySqlClient {
    public sealed partial class MySqlConnectionStringBuilder {
        internal Dictionary<string, object> values = new Dictionary<string, object>();
        //internal Dictionary<string, object> values
        //{
        //  get { lock (this) { return _values; } }
        //}

        private static readonly MySqlConnectionStringOptionCollection Options = new MySqlConnectionStringOptionCollection();

        static MySqlConnectionStringBuilder() {
            // Server options
            Options.Add( new MySqlConnectionStringOption( "server", "host,data source,datasource,address,addr,network address", TString, "" /*"localhost"*/, false ) );
            Options.Add( new MySqlConnectionStringOption( "database", "initial catalog", TString, string.Empty, false ) );
            Options.Add( new MySqlConnectionStringOption( "protocol", "connection protocol, connectionprotocol", typeof( MySqlConnectionProtocol ), MySqlConnectionProtocol.Sockets, false ) );
            Options.Add( new MySqlConnectionStringOption( "port", null, TUInt32, (uint) 3306, false ) );
            Options.Add( new MySqlConnectionStringOption( "pipe", "pipe name,pipename", TString, "MYSQL", false ) );
            Options.Add( new MySqlConnectionStringOption( "compress", "use compression,usecompression", TBoolean, false, false ) );
            Options.Add( new MySqlConnectionStringOption( "allowbatch", "allow batch", TBoolean, true, false ) );
            Options.Add( new MySqlConnectionStringOption( "logging", null, TBoolean, false, false ) );
            Options.Add( new MySqlConnectionStringOption( "sharedmemoryname", "shared memory name", TString, "MYSQL", false ) );
            Options.Add( new MySqlConnectionStringOption( "useoldsyntax", "old syntax,oldsyntax,use old syntax", TBoolean, false, true, ( msb, sender, value ) => { MySqlTrace.LogWarning( -1, "Use Old Syntax is now obsolete.  Please see documentation" ); msb.SetValue( "useoldsyntax", value ); }, ( msb, sender ) => (bool) msb.values[ "useoldsyntax" ] ) );
            Options.Add(
                new MySqlConnectionStringOption(
                    "connectiontimeout",
                    "connection timeout,connect timeout",
                    TUInt32,
                    (uint) 15,
                    false,
                    ( msb, sender, Value ) => {
                        var value = (uint) Convert.ChangeType( Value, sender.BaseType );
                        // Timeout in milliseconds should not exceed maximum for 32 bit
                        // signed integer (~24 days). We truncate the value if it exceeds 
                        // maximum (MySqlCommand.CommandTimeout uses the same technique
                        var timeout = Math.Min( value, Int32.MaxValue / 1000 );
                        if ( timeout != value )
                            MySqlTrace.LogWarning( -1, string.Format( "Connection timeout value too large ({0} seconds). Changed to max. possible value{1} seconds)", value, +timeout ) );
                        msb.SetValue( "connectiontimeout", timeout );
                    },
                    ( msb, sender ) => (uint) msb.values[ "connectiontimeout" ] ) );
            Options.Add(
                new MySqlConnectionStringOption(
                    "defaultcommandtimeout",
                    "command timeout,default command timeout",
                    TUInt32,
                    (uint) 30,
                    false ) );
            Options.Add(
                new MySqlConnectionStringOption(
                    "usedefaultcommandtimeoutforef",
                    "use default command timeout for ef",
                    TBoolean,
                    false,
                    false ) );

            // authentication options
            Options.Add( new MySqlConnectionStringOption( "user id", "uid,username,user name,user,userid", TString, "", false ) );
            Options.Add( new MySqlConnectionStringOption( "password", "pwd", TString, "", false ) );
            Options.Add( new MySqlConnectionStringOption( "persistsecurityinfo", "persist security info", TBoolean, false, false ) );
            Options.Add(
                new MySqlConnectionStringOption(
                    "encrypt",
                    null,
                    TBoolean,
                    false,
                    true,
                    ( msb, sender, value ) => {
                        // just for this case, reuse the logic to translate string to bool
                        sender.ValidateValue( ref value );
                        MySqlTrace.LogWarning( -1, "Encrypt is now obsolete. Use Ssl Mode instead" );
                        msb.SetValue( "Ssl Mode", (bool) value ? MySqlSslMode.Prefered : MySqlSslMode.None );
                    },
                    ( msb, sender ) => msb.SslMode != MySqlSslMode.None ) );
            Options.Add( new MySqlConnectionStringOption( "certificatefile", "certificate file", TString, null, false ) );
            Options.Add( new MySqlConnectionStringOption( "certificatepassword", "certificate password", TString, null, false ) );
            Options.Add(
                new MySqlConnectionStringOption(
                    "certificatestorelocation",
                    "certificate store location",
                    typeof( MySqlCertificateStoreLocation ),
                    MySqlCertificateStoreLocation.None,
                    false ) );
            Options.Add( new MySqlConnectionStringOption( "certificatethumbprint", "certificate thumb print", TString, null, false ) );
            Options.Add(
                new MySqlConnectionStringOption(
                    "integratedsecurity",
                    "integrated security",
                    TBoolean,
                    false,
                    false,
                    ( msb, sender, value ) => {
                        if ( !Platform.IsWindows() ) throw new MySqlException( "IntegratedSecurity is supported on Windows only" );
                        msb.SetValue( "Integrated Security", value );
                    },
                    ( msb, sender ) => (bool) msb.values[ "Integrated Security" ] ) );

            // Other properties
            Options.Add( new MySqlConnectionStringOption( "allowzerodatetime", "allow zero datetime", TBoolean, false, false ) );
            Options.Add( new MySqlConnectionStringOption( "convertzerodatetime", "convert zero datetime", TBoolean, false, false ) );
            Options.Add( new MySqlConnectionStringOption( "useusageadvisor", "use usage advisor,usage advisor", TBoolean, false, false ) );
            Options.Add( new MySqlConnectionStringOption( "procedurecachesize", "procedure cache size,procedure cache,procedurecache", TUInt32, (uint) 25, false ) );
            Options.Add( new MySqlConnectionStringOption( "useperformancemonitor", "use performance monitor,useperfmon,perfmon", TBoolean, false, false ) );
            Options.Add( new MySqlConnectionStringOption( "ignoreprepare", "ignore prepare", TBoolean, true, false ) );
            Options.Add( new MySqlConnectionStringOption(
                    "useprocedurebodies",
                    "use procedure bodies,procedure bodies",
                    TBoolean,
                    true,
                    true,
                    ( msb, sender, value ) => {
                        sender.ValidateValue( ref value );
                        MySqlTrace.LogWarning( -1, "Use Procedure Bodies is now obsolete.  Use Check Parameters instead" );
                        msb.SetValue( "checkparameters", value );
                        msb.SetValue( "useprocedurebodies", value );
                    },
                    ( msb, sender ) => (bool) msb.values[ "useprocedurebodies" ] ) );
            Options.Add( new MySqlConnectionStringOption( "autoenlist", "auto enlist", TBoolean, true, false ) );
            Options.Add( new MySqlConnectionStringOption( "respectbinaryflags", "respect binary flags", TBoolean, true, false ) );
            Options.Add( new MySqlConnectionStringOption( "treattinyasboolean", "treat tiny as boolean", TBoolean, true, false ) );
            Options.Add( new MySqlConnectionStringOption( "allowuservariables", "allow user variables", TBoolean, false, false ) );
            Options.Add( new MySqlConnectionStringOption( "interactivesession", "interactive session,interactive", TBoolean, false, false ) );
            Options.Add( new MySqlConnectionStringOption( "functionsreturnstring", "functions return string", TBoolean, false, false ) );
            Options.Add( new MySqlConnectionStringOption( "useaffectedrows", "use affected rows", TBoolean, false, false ) );
            Options.Add( new MySqlConnectionStringOption( "oldguids", "old guids", TBoolean, false, false ) );
            Options.Add( new MySqlConnectionStringOption( "keepalive", "keep alive", TUInt32, (uint) 0, false ) );
            Options.Add( new MySqlConnectionStringOption( "sqlservermode", "sql server mode", TBoolean, false, false ) );
            Options.Add( new MySqlConnectionStringOption( "tablecaching", "table cache,tablecache", TBoolean, false, false ) );
            Options.Add( new MySqlConnectionStringOption( "defaulttablecacheage", "default table cache age", TInt32, 60, false ) );
            Options.Add( new MySqlConnectionStringOption( "checkparameters", "check parameters", TBoolean, true, false ) );
            Options.Add( new MySqlConnectionStringOption( "replication", null, TBoolean, false, false ) );
            Options.Add( new MySqlConnectionStringOption( "exceptioninterceptors", "exception interceptors", TString, null, false ) );
            Options.Add( new MySqlConnectionStringOption( "commandinterceptors", "command interceptors", TString, null, false ) );
            Options.Add( new MySqlConnectionStringOption( "includesecurityasserts", "include security asserts", TBoolean, false, false ) );
            // pooling options
            Options.Add( new MySqlConnectionStringOption( "connectionlifetime", "connection lifetime", TUInt32, 0U, false ) );
            Options.Add( new MySqlConnectionStringOption( "pooling", null, TBoolean, true, false ) );
            Options.Add( new MySqlConnectionStringOption( "minpoolsize", "minimumpoolsize,min pool size,minimum pool size", TUInt32, 0U, false ) );
            Options.Add( new MySqlConnectionStringOption( "maxpoolsize", "maximumpoolsize,max pool size,maximum pool size", TUInt32, (uint) 100, false ) );
            Options.Add( new MySqlConnectionStringOption( "connectionreset", "connection reset", TBoolean, false, false ) );
            Options.Add( new MySqlConnectionStringOption( "cacheserverproperties", "cache server properties", TBoolean, false, false ) );

            // language and charset options
            Options.Add( new MySqlConnectionStringOption( "characterset", "character set,charset", TString, "", false ) );
            Options.Add( new MySqlConnectionStringOption( "treatblobsasutf8", "treat blobs as utf8", TBoolean, false, false ) );
            Options.Add( new MySqlConnectionStringOption( "blobasutf8includepattern", null, TString, "", false ) );
            Options.Add( new MySqlConnectionStringOption( "blobasutf8excludepattern", null, TString, "", false ) );
            Options.Add( new MySqlConnectionStringOption( "sslmode", "ssl mode", typeof( MySqlSslMode ), MySqlSslMode.None, false ) );
        }

        public MySqlConnectionStringBuilder() {
            HasProcAccess = true;
            // Populate initial values
            lock ( this ) {
                foreach ( MySqlConnectionStringOption t in Options.Options ) values[ t.Keyword ] = t.DefaultValue;
            }
        }

        public MySqlConnectionStringBuilder( string connStr ) {
            lock ( this ) {
                ConnectionString = connStr;
            }
        }

        #region Server Properties
        /// <summary>
        /// Gets or sets the name of the server.
        /// </summary>
        /// <value>The server.</value>
        [Category( "Connection" )]
        [Description( "Server to connect to" )]
        [RefreshProperties( RefreshProperties.All )]
        public string Server {
            get {
                return this[ "server" ] as string;
            }
            set {
                this[ "server" ] = value;
            }
        }

        /// <summary>
        /// Gets or sets the name of the database the connection should 
        /// initially connect to.
        /// </summary>
        [Category( "Connection" )]
        [Description( "Database to use initially" )]
        [RefreshProperties( RefreshProperties.All )]
        public string Database {
            get {
                return values[ "database" ] as string;
            }
            set {
                SetValue( "database", value );
            }
        }

        /// <summary>
        /// Gets or sets the protocol that should be used for communicating
        /// with MySQL.
        /// </summary>
        [Category( "Connection" )]
        [DisplayName( "Connection Protocol" )]
        [Description( "Protocol to use for connection to MySQL" )]
        [RefreshProperties( RefreshProperties.All )]
        public MySqlConnectionProtocol ConnectionProtocol {
            get {
                return (MySqlConnectionProtocol) values[ "protocol" ];
            }
            set {
                SetValue( "protocol", value );
            }
        }

        /// <summary>
        /// Gets or sets the name of the named pipe that should be used
        /// for communicating with MySQL.
        /// </summary>
        [Category( "Connection" )]
        [DisplayName( "Pipe Name" )]
        [Description( "Name of pipe to use when connecting with named pipes (Win32 only)" )]
        [RefreshProperties( RefreshProperties.All )]
        public string PipeName {
            get {
                return (string) values[ "pipe" ];
            }
            set {
                SetValue( "pipe", value );
            }
        }

        /// <summary>
        /// Gets or sets a boolean value that indicates whether this connection
        /// should use compression.
        /// </summary>
        [Category( "Connection" )]
        [DisplayName( "Use Compression" )]
        [Description( "Should the connection use compression" )]
        [RefreshProperties( RefreshProperties.All )]
        public bool UseCompression {
            get {
                return (bool) values[ "compress" ];
            }
            set {
                SetValue( "compress", value );
            }
        }

        /// <summary>
        /// Gets or sets a boolean value that indicates whether this connection will allow
        /// commands to send multiple SQL statements in one execution.
        /// </summary>
        [Category( "Connection" )]
        [DisplayName( "Allow Batch" )]
        [Description( "Allows execution of multiple SQL commands in a single statement" )]
        [RefreshProperties( RefreshProperties.All )]
        public bool AllowBatch {
            get {
                return (bool) values[ "allowbatch" ];
            }
            set {
                SetValue( "allowbatch", value );
            }
        }

        /// <summary>
        /// Gets or sets a boolean value that indicates whether logging is enabled.
        /// </summary>
        [Category( "Connection" )]
        [Description( "Enables output of diagnostic messages" )]
        [RefreshProperties( RefreshProperties.All )]
        public bool Logging {
            get {
                return (bool) values[ "logging" ];
            }
            set {
                SetValue( "logging", value );
            }
        }

        /// <summary>
        /// Gets or sets the base name of the shared memory objects used to 
        /// communicate with MySQL when the shared memory protocol is being used.
        /// </summary>
        [Category( "Connection" )]
        [DisplayName( "Shared Memory Name" )]
        [Description( "Name of the shared memory object to use" )]
        [RefreshProperties( RefreshProperties.All )]
        public string SharedMemoryName {
            get {
                return (string) values[ "sharedmemoryname" ];
            }
            set {
                SetValue( "sharedmemoryname", value );
            }
        }

        /// <summary>
        /// Gets or sets a boolean value that indicates whether this connection uses
        /// the old style (@) parameter markers or the new (?) style.
        /// </summary>
        [Category( "Connection" )]
        [DisplayName( "Use Old Syntax" )]
        [Description( "Allows the use of old style @ syntax for parameters" )]
        [RefreshProperties( RefreshProperties.All )]
        [Obsolete( "Use Old Syntax is no longer needed.  See documentation" )]
        public bool UseOldSyntax {
            get {
                return (bool) values[ "useoldsyntax" ];
            }
            set {
                SetValue( "useoldsyntax", value );
            }
        }

        /// <summary>
        /// Gets or sets the port number that is used when the socket
        /// protocol is being used.
        /// </summary>
        [Category( "Connection" )]
        [Description( "Port to use for TCP/IP connections" )]
        [RefreshProperties( RefreshProperties.All )]
        public uint Port {
            get {
                return (uint) values[ "port" ];
            }
            set {
                SetValue( "port", value );
            }
        }

        /// <summary>
        /// Gets or sets the connection timeout.
        /// </summary>
        [Category( "Connection" )]
        [DisplayName( "Connect Timeout" )]
        [Description(
            "The length of time (in seconds) to wait for a connection "
            + "to the server before terminating the attempt and generating an error." )]
        [RefreshProperties( RefreshProperties.All )]
        public uint ConnectionTimeout {
            get {
                return (uint) values[ "connectiontimeout" ];
            }

            set {
                // Timeout in milliseconds should not exceed maximum for 32 bit
                // signed integer (~24 days). We truncate the value if it exceeds 
                // maximum (MySqlCommand.CommandTimeout uses the same technique
                var timeout = Math.Min( value, Int32.MaxValue / 1000 );
                if ( timeout != value )
                    MySqlTrace.LogWarning( -1, string.Format( "Connection timeout value too large ({0} seconds). Changed to max. possible value{1} seconds)", value, timeout ) );
                SetValue( "connectiontimeout", timeout );
            }
        }

        /// <summary>
        /// Gets or sets the default command timeout.
        /// </summary>
        [Category( "Connection" )]
        [DisplayName( "Default Command Timeout" )]
        [Description( @"The default timeout that MySqlCommand objects will use
                     unless changed." )]
        [RefreshProperties( RefreshProperties.All )]
        public uint DefaultCommandTimeout {
            get {
                return (uint) values[ "defaultcommandtimeout" ];
            }
            set {
                SetValue( "defaultcommandtimeout", value );
            }
        }
        #endregion

        #region Authentication Properties
        /// <summary>
        /// Gets or sets the user id that should be used to connect with.
        /// </summary>
        [Category( "Security" )]
        [DisplayName( "User Id" )]
        [Description( "Indicates the user ID to be used when connecting to the data source." )]
        [RefreshProperties( RefreshProperties.All )]
        public string UserId {
            get {
                return (string) values[ "user id" ];
            }
            set {
                SetValue( "user id", value );
            }
        }

        /// <summary>
        /// Gets or sets the password that should be used to connect with.
        /// </summary>
        [Category( "Security" )]
        [Description( "Indicates the password to be used when connecting to the data source." )]
        [PasswordPropertyText( true )]
        [RefreshProperties( RefreshProperties.All )]
        public string Password {
            get {
                return (string) values[ "password" ];
            }
            set {
                SetValue( "password", value );
            }
        }

        /// <summary>
        /// Gets or sets a boolean value that indicates if the password should be persisted
        /// in the connection string.
        /// </summary>
        [Category( "Security" )]
        [DisplayName( "Persist Security Info" )]
        [Description(
            "When false, security-sensitive information, such as the password, "
            + "is not returned as part of the connection if the connection is open or " + "has ever been in an open state." )]
        [RefreshProperties( RefreshProperties.All )]
        public bool PersistSecurityInfo {
            get {
                return (bool) values[ "persistsecurityinfo" ];
            }
            set {
                SetValue( "persistsecurityinfo", value );
            }
        }

        [Category( "Authentication" )]
        [Description( "Should the connection use SSL." )]
        [Obsolete( "Use Ssl Mode instead." )]
        internal bool Encrypt {
            get {
                return SslMode != MySqlSslMode.None;
            }
            set {
                SetValue( "Ssl Mode", value ? MySqlSslMode.Prefered : MySqlSslMode.None );
            }
        }

        [Category( "Authentication" )]
        [DisplayName( "Certificate File" )]
        [Description( "Certificate file in PKCS#12 format (.pfx)" )]
        public string CertificateFile {
            get {
                return (string) values[ "certificatefile" ];
            }
            set {
                SetValue( "certificatefile", value );
            }
        }

        [Category( "Authentication" )]
        [DisplayName( "Certificate Password" )]
        [Description( "Password for certificate file" )]
        public string CertificatePassword {
            get {
                return (string) values[ "certificatepassword" ];
            }
            set {
                SetValue( "certificatepassword", value );
            }
        }

        [Category( "Authentication" )]
        [DisplayName( "Certificate Store Location" )]
        [Description( "Certificate Store Location for client certificates" )]
        [DefaultValue( MySqlCertificateStoreLocation.None )]
        public MySqlCertificateStoreLocation CertificateStoreLocation {
            get {
                return (MySqlCertificateStoreLocation) values[ "certificatestorelocation" ];
            }
            set {
                SetValue( "certificatestorelocation", value );
            }
        }

        [Category( "Authentication" )]
        [DisplayName( "Certificate Thumbprint" )]
        [Description(
            "Certificate thumbprint. Can be used together with Certificate "
            + "Store Location parameter to uniquely identify certificate to be used " + "for SSL authentication." )]
        public string CertificateThumbprint {
            get {
                return (string) values[ "certificatethumbprint" ];
            }
            set {
                SetValue( "certificatethumbprint", value );
            }
        }


        [Category( "Authentication" )]
        [DisplayName( "Integrated Security" )]
        [Description( "Use windows authentication when connecting to server" )]
        [DefaultValue( false )]
        public bool IntegratedSecurity {
            get {
                return (bool) values[ "integratedsecurity" ];
            }
            set {
                if ( !Platform.IsWindows() ) throw new MySqlException( "IntegratedSecurity is supported on Windows only" );

                SetValue( "integratedsecurity", value );
            }
        }
        #endregion

        #region Other Properties
        /// <summary>
        /// Gets or sets a boolean value that indicates if zero date time values are supported.
        /// </summary>
        [Category( "Advanced" )]
        [DisplayName( "Allow Zero Datetime" )]
        [Description( "Should zero datetimes be supported" )]
        [DefaultValue( false )]
        [RefreshProperties( RefreshProperties.All )]
        public bool AllowZeroDateTime {
            get {
                return (bool) values[ "allowzerodatetime" ];
            }
            set {
                SetValue( "allowzerodatetime", value );
            }
        }

        /// <summary>
        /// Gets or sets a boolean value indicating if zero datetime values should be 
        /// converted to DateTime.MinValue.
        /// </summary>
        [Category( "Advanced" )]
        [DisplayName( "Convert Zero Datetime" )]
        [Description( "Should illegal datetime values be converted to DateTime.MinValue" )]
        [DefaultValue( false )]
        [RefreshProperties( RefreshProperties.All )]
        public bool ConvertZeroDateTime {
            get {
                return (bool) values[ "convertzerodatetime" ];
            }
            set {
                SetValue( "convertzerodatetime", value );
            }
        }

        /// <summary>
        /// Gets or sets a boolean value indicating if the Usage Advisor should be enabled.
        /// </summary>
        [Category( "Advanced" )]
        [DisplayName( "Use Usage Advisor" )]
        [Description( "Logs inefficient database operations" )]
        [DefaultValue( false )]
        [RefreshProperties( RefreshProperties.All )]
        public bool UseUsageAdvisor {
            get {
                return (bool) values[ "useusageadvisor" ];
            }
            set {
                SetValue( "useusageadvisor", value );
            }
        }

        /// <summary>
        /// Gets or sets the size of the stored procedure cache.
        /// </summary>
        [Category( "Advanced" )]
        [DisplayName( "Procedure Cache Size" )]
        [Description(
            "Indicates how many stored procedures can be cached at one time. " + "A value of 0 effectively disables the procedure cache." )]
        [DefaultValue( 25 )]
        [RefreshProperties( RefreshProperties.All )]
        public uint ProcedureCacheSize {
            get {
                return (uint) values[ "procedurecachesize" ];
            }
            set {
                SetValue( "procedurecachesize", value );
            }
        }

        /// <summary>
        /// Gets or sets a boolean value indicating if the permon hooks should be enabled.
        /// </summary>
        [Category( "Advanced" )]
        [DisplayName( "Use Performance Monitor" )]
        [Description( "Indicates that performance counters should be updated during execution." )]
        [DefaultValue( false )]
        [RefreshProperties( RefreshProperties.All )]
        public bool UsePerformanceMonitor {
            get {
                return (bool) values[ "useperformancemonitor" ];
            }
            set {
                SetValue( "useperformancemonitor", value );
            }
        }

        /// <summary>
        /// Gets or sets a boolean value indicating if calls to Prepare() should be ignored.
        /// </summary>
        [Category( "Advanced" )]
        [DisplayName( "Ignore Prepare" )]
        [Description( "Instructs the provider to ignore any attempts to prepare a command." )]
        [DefaultValue( true )]
        [RefreshProperties( RefreshProperties.All )]
        public bool IgnorePrepare {
            get {
                return (bool) values[ "ignoreprepare" ];
            }
            set {
                SetValue( "ignoreprepare", value );
            }
        }

        [Category( "Advanced" )]
        [DisplayName( "Use Procedure Bodies" )]
        [Description( "Indicates if stored procedure bodies will be available for parameter detection." )]
        [DefaultValue( true )]
        [Obsolete( "Use CheckParameters instead" )]
        public bool UseProcedureBodies {
            get {
                return (bool) values[ "useprocedurebodies" ];
            }
            set {
                SetValue( "useprocedurebodies", value );
            }
        }

        [Category( "Advanced" )]
        [DisplayName( "Auto Enlist" )]
        [Description( "Should the connetion automatically enlist in the active connection, if there are any." )]
        [DefaultValue( true )]
        [RefreshProperties( RefreshProperties.All )]
        public bool AutoEnlist {
            get {
                return (bool) values[ "autoenlist" ];
            }
            set {
                SetValue( "autoenlist", value );
            }
        }

        [Category( "Advanced" )]
        [DisplayName( "Respect Binary Flags" )]
        [Description( "Should binary flags on column metadata be respected." )]
        [DefaultValue( true )]
        [RefreshProperties( RefreshProperties.All )]
        public bool RespectBinaryFlags {
            get {
                return (bool) values[ "respectbinaryflags" ];
            }
            set {
                SetValue( "respectbinaryflags", value );
            }
        }

        [Category( "Advanced" )]
        [DisplayName( "Treat Tiny As Boolean" )]
        [Description( "Should the provider treat TINYINT(1) columns as boolean." )]
        [DefaultValue( true )]
        [RefreshProperties( RefreshProperties.All )]
        public bool TreatTinyAsBoolean {
            get {
                return (bool) values[ "treattinyasboolean" ];
            }
            set {
                SetValue( "treattinyasboolean", value );
            }
        }

        [Category( "Advanced" )]
        [DisplayName( "Allow User Variables" )]
        [Description( "Should the provider expect user variables to appear in the SQL." )]
        [DefaultValue( false )]
        [RefreshProperties( RefreshProperties.All )]
        public bool AllowUserVariables {
            get {
                return (bool) values[ "allowuservariables" ];
            }
            set {
                SetValue( "allowuservariables", value );
            }
        }

        [Category( "Advanced" )]
        [DisplayName( "Interactive Session" )]
        [Description( "Should this session be considered interactive?" )]
        [DefaultValue( false )]
        [RefreshProperties( RefreshProperties.All )]
        public bool InteractiveSession {
            get {
                return (bool) values[ "interactivesession" ];
            }
            set {
                SetValue( "interactivesession", value );
            }
        }

        [Category( "Advanced" )]
        [DisplayName( "Functions Return String" )]
        [Description( "Should all server functions be treated as returning string?" )]
        [DefaultValue( false )]
        public bool FunctionsReturnString {
            get {
                return (bool) values[ "functionsreturnstring" ];
            }
            set {
                SetValue( "functionsreturnstring", value );
            }
        }

        [Category( "Advanced" )]
        [DisplayName( "Use Affected Rows" )]
        [Description( "Should the returned affected row count reflect affected rows instead of found rows?" )]
        [DefaultValue( false )]
        public bool UseAffectedRows {
            get {
                return (bool) values[ "useaffectedrows" ];
            }
            set {
                SetValue( "useaffectedrows", value );
            }
        }

        [Category( "Advanced" )]
        [DisplayName( "Old Guids" )]
        [Description( "Treat binary(16) columns as guids" )]
        [DefaultValue( false )]
        public bool OldGuids {
            get {
                return (bool) values[ "oldguids" ];
            }
            set {
                SetValue( "oldguids", value );
            }
        }

        [DisplayName( "Keep Alive" )]
        [Description(
            "For TCP connections, idle connection time measured in seconds, before the first keepalive packet is sent."
            + "A value of 0 indicates that keepalive is not used." )]
        [DefaultValue( 0 )]
        public uint Keepalive {
            get {
                return (uint) values[ "keepalive" ];
            }
            set {
                SetValue( "keepalive", value );
            }
        }

        [Category( "Advanced" )]
        [DisplayName( "Sql Server Mode" )]
        [Description(
            "Allow Sql Server syntax.  " + "A value of yes allows symbols to be enclosed with [] instead of ``.  This does incur "
            + "a performance hit so only use when necessary." )]
        [DefaultValue( false )]
        public bool SqlServerMode {
            get {
                return (bool) values[ "sqlservermode" ];
            }
            set {
                SetValue( "sqlservermode", value );
            }
        }

        [Category( "Advanced" )]
        [DisplayName( "Table Cache" )]
        [Description( @"Enables or disables caching of TableDirect command.  
            A value of yes enables the cache while no disables it." )]
        [DefaultValue( false )]
        public bool TableCaching {
            get {
                return (bool) values[ "tablecaching" ];
            }
            set {
                SetValue( "tablecachig", value );
            }
        }

        [Category( "Advanced" )]
        [DisplayName( "Default Table Cache Age" )]
        [Description( @"Specifies how long a TableDirect result should be cached in seconds." )]
        [DefaultValue( 60 )]
        public int DefaultTableCacheAge {
            get {
                return (int) values[ "defaulttablecacheage" ];
            }
            set {
                SetValue( "defaulttablecacheage", value );
            }
        }

        [Category( "Advanced" )]
        [DisplayName( "Check Parameters" )]
        [Description( "Indicates if stored routine parameters should be checked against the server." )]
        [DefaultValue( true )]
        public bool CheckParameters {
            get {
                return (bool) values[ "checkparameters" ];
            }
            set {
                SetValue( "checkparameters", value );
            }
        }

        [Category( "Advanced" )]
        [DisplayName( "Replication" )]
        [Description( "Indicates if this connection is to use replicated servers." )]
        [DefaultValue( false )]
        public bool Replication {
            get {
                return (bool) values[ "replication" ];
            }
            set {
                SetValue( "replication", value );
            }
        }

        [Category( "Advanced" )]
        [DisplayName( "Exception Interceptors" )]
        [Description( "The list of interceptors that can triage thrown MySqlExceptions." )]
        public string ExceptionInterceptors {
            get {
                return (string) values[ "exceptioninterceptors" ];
            }
            set {
                SetValue( "exceptioninterceptors", value );
            }
        }

        [Category( "Advanced" )]
        [DisplayName( "Command Interceptors" )]
        [Description( "The list of interceptors that can intercept command operations." )]
        public string CommandInterceptors {
            get {
                return (string) values[ "commandinterceptors" ];
            }
            set {
                SetValue( "commandinterceptors", value );
            }
        }

        [Category( "Advanced" )]
        [DisplayName( "Include Security Asserts" )]
        [Description( "Include security asserts to support Medium Trust" )]
        [DefaultValue( false )]
        public bool IncludeSecurityAsserts {
            get {
                return (bool) values[ "includesecurityasserts" ];
            }
            set {
                SetValue( "includesecurityasserts", value );
            }
        }
        #endregion

        #region Pooling Properties
        /// <summary>
        /// Gets or sets the lifetime of a pooled connection.
        /// </summary>
        [Category( "Pooling" )]
        [DisplayName( "Connection Lifetime" )]
        [Description( "The minimum amount of time (in seconds) for this connection to " + "live in the pool before being destroyed." )]
        [DefaultValue( 0 )]
        [RefreshProperties( RefreshProperties.All )]
        public uint ConnectionLifeTime {
            get {
                return (uint) values[ "connectionlifetime" ];
            }
            set {
                SetValue( "connectionlifetime", value );
            }
        }

        /// <summary>
        /// Gets or sets a boolean value indicating if connection pooling is enabled.
        /// </summary>
        [Category( "Pooling" )]
        [Description(
            "When true, the connection object is drawn from the appropriate "
            + "pool, or if necessary, is created and added to the appropriate pool." )]
        [DefaultValue( true )]
        [RefreshProperties( RefreshProperties.All )]
        public bool Pooling {
            get {
                return (bool) values[ "pooling" ];
            }
            set {
                SetValue( "pooling", value );
            }
        }

        /// <summary>
        /// Gets the minimum connection pool size.
        /// </summary>
        [Category( "Pooling" )]
        [DisplayName( "Minimum Pool Size" )]
        [Description( "The minimum number of connections allowed in the pool." )]
        [DefaultValue( 0 )]
        [RefreshProperties( RefreshProperties.All )]
        public uint MinimumPoolSize {
            get {
                return (uint) values[ "minpoolsize" ];
            }
            set {
                SetValue( "minpoolsize", value );
            }
        }

        /// <summary>
        /// Gets or sets the maximum connection pool setting.
        /// </summary>
        [Category( "Pooling" )]
        [DisplayName( "Maximum Pool Size" )]
        [Description( "The maximum number of connections allowed in the pool." )]
        [DefaultValue( 100 )]
        [RefreshProperties( RefreshProperties.All )]
        public uint MaximumPoolSize {
            get {
                return (uint) values[ "maxpoolsize" ];
            }
            set {
                SetValue( "maxpoolsize", value );
            }
        }

        /// <summary>
        /// Gets or sets a boolean value indicating if the connection should be reset when retrieved
        /// from the pool.
        /// </summary>
        [Category( "Pooling" )]
        [DisplayName( "Connection Reset" )]
        [Description( "When true, indicates the connection state is reset when removed from the pool." )]
        [DefaultValue( false )]
        [RefreshProperties( RefreshProperties.All )]
        public bool ConnectionReset {
            get {
                return (bool) values[ "connectionreset" ];
            }
            set {
                SetValue( "connectionreset", value );
            }
        }

        [Category( "Pooling" )]
        [DisplayName( "Cache Server Properties" )]
        [Description( "When true, server properties will be cached after the first server in the pool is created" )]
        [DefaultValue( false )]
        [RefreshProperties( RefreshProperties.All )]
        public bool CacheServerProperties {
            get {
                return (bool) values[ "cacheserverproperties" ];
            }
            set {
                SetValue( "cacheserverproperties", value );
            }
        }
        #endregion

        #region Language and Character Set Properties
        /// <summary>
        /// Gets or sets the character set that should be used for sending queries to the server.
        /// </summary>
        [DisplayName( "Character Set" )]
        [Category( "Advanced" )]
        [Description( "Character set this connection should use" )]
        [DefaultValue( "" )]
        [RefreshProperties( RefreshProperties.All )]
        public string CharacterSet {
            get {
                return (string) values[ "characterset" ];
            }
            set {
                SetValue( "characterset", value );
            }
        }

        /// <summary>
        /// Indicates whether the driver should treat binary blobs as UTF8
        /// </summary>
        [DisplayName( "Treat Blobs As UTF8" )]
        [Category( "Advanced" )]
        [Description( "Should binary blobs be treated as UTF8" )]
        [DefaultValue( false )]
        [RefreshProperties( RefreshProperties.All )]
        public bool TreatBlobsAsUtf8 {
            get {
                return (bool) values[ "treatblobsasutf8" ];
            }
            set {
                SetValue( "treatblobsasutf8", value );
            }
        }

        /// <summary>
        /// Gets or sets the pattern that matches the columns that should be treated as UTF8
        /// </summary>
        [Category( "Advanced" )]
        [Description( "Pattern that matches columns that should be treated as UTF8" )]
        [RefreshProperties( RefreshProperties.All )]
        public string BlobAsUtf8IncludePattern {
            get {
                return (string) values[ "blobasutf8includepattern" ];
            }
            set {
                SetValue( "blobasutf8includepattern", value );
            }
        }

        /// <summary>
        /// Gets or sets the pattern that matches the columns that should not be treated as UTF8
        /// </summary>
        [Category( "Advanced" )]
        [Description( "Pattern that matches columns that should not be treated as UTF8" )]
        [RefreshProperties( RefreshProperties.All )]
        public string BlobAsUtf8ExcludePattern {
            get {
                return (string) values[ "blobasutf8excludepattern" ];
            }
            set {
                SetValue( "blobasutf8excludepattern", value );
            }
        }

        /// <summary>
        /// Indicates whether to use SSL connections and how to handle server certificate errors.
        /// </summary>
        [DisplayName( "Ssl Mode" )]
        [Category( "Security" )]
        [Description( "SSL properties for connection" )]
        [DefaultValue( MySqlSslMode.None )]
        public MySqlSslMode SslMode {
            get {
                return (MySqlSslMode) values[ "sslmode" ];
            }
            set {
                SetValue( "sslmode", value );
            }
        }
        #endregion

        #region Backwards compatibility properties
        [DisplayName( "Use Default Command Timeout For EF" )]
        [Category( "Backwards Compatibility" )]
        [Description( "Enforces the command timeout of EFMySqlCommand to the value provided in 'DefaultCommandTimeout' property" )]
        [DefaultValue( false )]
        public bool UseDefaultCommandTimeoutForEf {
            get {
                return (bool) values[ "usedefaultcommandtimeoutforef" ];
            }
            set {
                SetValue( "usedefaultcommandtimeoutforef", value );
            }
        }
        #endregion

        #region Fabric Properties
        public string FabricGroup { get; internal set; }

        public string ShardingTable { get; internal set; }

        public object ShardingKey { get; internal set; }

        public int? FabricServerMode { get; internal set; }

        public int? FabricScope { get; internal set; }
        #endregion

        internal bool HasProcAccess { get; set; }

        public override object this[ string keyword ] {
            get {
                var opt = GetOption( keyword );
                return opt.Getter( this, opt );
            }
            set {
                var opt = GetOption( keyword );
                opt.Setter( this, opt, value );
            }
        }

        internal Regex GetBlobAsUtf8IncludeRegex() {
            if ( String.IsNullOrEmpty( BlobAsUtf8IncludePattern ) ) return null;
            return new Regex( BlobAsUtf8IncludePattern );
        }

        internal Regex GetBlobAsUtf8ExcludeRegex() {
            return String.IsNullOrEmpty( BlobAsUtf8ExcludePattern ) ? null : new Regex( BlobAsUtf8ExcludePattern );
        }

        public override void Clear() {
            base.Clear();
            lock ( this ) {
                foreach ( var option in Options.Options )
                    if ( option.DefaultValue != null ) values[ option.Keyword ] = option.DefaultValue;
                    else values[ option.Keyword ] = null;
            }
        }

        internal void SetValue( string keyword, object value ) {
            var option = GetOption( keyword );
            option.ValidateValue( ref value );

            // remove all related keywords
            option.Clean( this );

            if ( value == null ) return;
            lock ( this ) {
                // set value for the given keyword
                values[ option.Keyword ] = value;
                base[ keyword ] = value;
            }
        }

        private MySqlConnectionStringOption GetOption( string key ) {
            var option = Options.Get( key );
            if ( option == null ) throw new ArgumentException( Resources.KeywordNotSupported, key );
            return option;
        }

        public override bool ContainsKey( string keyword ) {
            var option = Options.Get( keyword );
            return option != null;
        }

        public override bool Remove( string keyword ) {
            bool removed;
            lock ( this ) {
                removed = base.Remove( keyword );
            }
            if ( !removed ) return false;
            var option = GetOption( keyword );
            lock ( this ) {
                values[ option.Keyword ] = option.DefaultValue;
            }
            return true;
        }

        public string GetConnectionString( bool includePass ) {
            if ( includePass ) return ConnectionString;

            var conn = new StringBuilder();
            var delimiter = "";
            foreach ( String key in Keys ) {
                if ( key == "password" || key == "pwd" ) continue;
                conn.AppendFormat( CultureInfo.CurrentCulture, "{0}{1}={2}", delimiter, key, this[ key ] );
                delimiter = ";";
            }
            return conn.ToString();
        }

        public override bool Equals( object obj ) {
            var other = obj as MySqlConnectionStringBuilder;
            if ( obj == null ) return false;

            if ( values.Count != other.values.Count ) return false;

            foreach ( var kvp in values )
                if ( other.values.ContainsKey( kvp.Key ) ) {
                    var v = other.values[ kvp.Key ];
                    if ( v == null
                         && kvp.Value != null ) return false;
                    if ( kvp.Value == null
                         && v != null ) return false;
                    if ( kvp.Value == null
                         && v == null ) return true;
                    if ( !v.Equals( kvp.Value ) ) return false;
                }
                else return false;

            return true;
        }
    }

    internal class MySqlConnectionStringOption {
        public MySqlConnectionStringOption(
            string keyword,
            string synonyms,
            Type baseType,
            object defaultValue,
            bool obsolete,
            SetterDelegate setter,
            GetterDelegate getter ) {
            Keyword = keyword.InvariantToLower();
            if ( synonyms != null ) Synonyms = synonyms.InvariantToLower().Split( ',' );
            BaseType = baseType;
            Obsolete = obsolete;
            DefaultValue = defaultValue;
            Setter = setter;
            Getter = getter;
        }

        public MySqlConnectionStringOption( string keyword, string synonyms, Type baseType, object defaultValue, bool obsolete )
            : this(
                keyword,
                synonyms,
                baseType,
                defaultValue,
                obsolete,
                ( msb, sender, value ) => {
                    sender.ValidateValue( ref value );
                    msb.SetValue( sender.Keyword, Convert.ChangeType( value, sender.BaseType ) );
                },
                ( msb, sender ) => msb.values[ sender.Keyword ] ) {}

        public string[] Synonyms { get; }
        public bool Obsolete { get; private set; }
        public Type BaseType { get; }
        public string Keyword { get; }
        public object DefaultValue { get; }
        public SetterDelegate Setter { get; }
        public GetterDelegate Getter { get; }

        public delegate void SetterDelegate( MySqlConnectionStringBuilder msb, MySqlConnectionStringOption sender, object value );

        public delegate object GetterDelegate( MySqlConnectionStringBuilder msb, MySqlConnectionStringOption sender );

        public bool HasKeyword( string key ) => Keyword == key || Synonyms != null && Synonyms.Any( syn => syn == key );

        public void Clean( MySqlConnectionStringBuilder builder ) {
            builder.Remove( Keyword );
            if ( Synonyms == null ) return;
            foreach ( var syn in Synonyms ) builder.Remove( syn );
        }

        public void ValidateValue( ref object value ) {
            if ( value == null ) return;
            var typeName = Type.GetTypeCode( BaseType );
            var s = value as string;
            if ( s != null ) {
                if ( BaseType == TString) return;
                if ( BaseType == TBoolean ) {
                    switch ( s ) {
                        case "yes":
                            value = true;
                            break;
                        case "no":
                            value = false;
                            break;
                        default:
                            bool b;
                            if ( Boolean.TryParse( value.ToString(), out b ) ) value = b;
                            else throw new ArgumentException( String.Format( Resources.ValueNotCorrectType, value ) );
                            break;
                    }
                }
            }
            //todo: fix shitcode.
            switch ( typeName ) {
                case TypeCode.Boolean:
                    bool b;
                    if ( Boolean.TryParse( value.ToString(), out b ) ) {
                        value = b;
                        return;
                    }
                    break;
                case TypeCode.UInt64:
                    ulong uintVal;
                    if ( UInt64.TryParse( value.ToString(), out uintVal ) ) {
                        value = uintVal;
                        return;
                    }
                    break;
                case TypeCode.UInt32:
                    uint uintVal32;
                    if ( UInt32.TryParse( value.ToString(), out uintVal32 ) ) {
                        value = uintVal32;
                        return;
                    }
                    break;
                case TypeCode.Int64:
                    long intVal;
                    if ( long.TryParse( value.ToString(), out intVal ) ) {
                        value = intVal;
                        return;
                    }
                    break;
                case TypeCode.Int32:
                    int intVal32;
                    if ( Int32.TryParse( value.ToString(), out intVal32 ) ) {
                        value = intVal32;
                        return;
                    }
                    break;
            }

            object objValue;
            if ( BaseType.IsEnum && ParseEnum( value.ToString(), out objValue ) ) {
                value = objValue;
                return;
            }

            throw new ArgumentException( String.Format( Resources.ValueNotCorrectType, value ) );
        }

        private bool ParseEnum( string requestedValue, out object value ) {
            value = null;
            try {
                value = Enum.Parse( BaseType, requestedValue, true );
                return true;
            }
            catch ( ArgumentException ) {
                return false;
            }
        }
    }

    internal class MySqlConnectionStringOptionCollection : Dictionary<string, MySqlConnectionStringOption> {
        internal List<MySqlConnectionStringOption> Options { get; }

        internal MySqlConnectionStringOptionCollection() : base( StringComparer.OrdinalIgnoreCase ) {
            Options = new List<MySqlConnectionStringOption>();
        }

        internal void Add( MySqlConnectionStringOption option ) {
            Options.Add( option );
            // Register the option with all the keywords.
            Add( option.Keyword, option );
            if ( option.Synonyms == null ) return;
            foreach ( string t in option.Synonyms ) Add( t, option );
        }

        internal MySqlConnectionStringOption Get( string keyword ) {
            MySqlConnectionStringOption option;
            TryGetValue( keyword, out option );
            return option;
        }
    }
}
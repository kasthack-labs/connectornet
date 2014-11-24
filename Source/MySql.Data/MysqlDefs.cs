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

using System;
using System.ComponentModel;
using System.Diagnostics;
using System.IO;
using System.Management;
using System.Reflection;

namespace MySql.Data.MySqlClient {
    /// <summary>
    /// Summary description for ClientParam.
    /// </summary>
    [Flags]
    internal enum ClientFlags : ulong {
        LongPassword = 1, // new more secure passwords
        FoundRows = 2, // found instead of affected rows
        LongFlag = 4, // Get all column flags
        ConnectWithDb = 8, // One can specify db on connect
        NoSchema = 16, // Don't allow db.table.column
        Compress = 32, // Client can use compression protocol
        Odbc = 64, // ODBC client
        LocalFiles = 128, // Can use LOAD DATA LOCAL
        IgnoreSpace = 256, // Ignore spaces before '('
        Protocol41 = 512, // Support new 4.1 protocol
        Interactive = 1024, // This is an interactive client
#if !CF
        Ssl = 2048, // Switch to SSL after handshake
#endif
        IgnoreSigpipe = 4096, // IGNORE sigpipes
        Transactions = 8192, // Client knows about transactions
        Reserved = 16384, // old 4.1 protocol flag
        SecureConnection = 32768, // new 4.1 authentication
        MultiStatements = 65536, // Allow multi-stmt support
        MultiResults = 131072, // Allow multiple resultsets
        PsMultiResults = 1UL << 18, // allow multi results using PS protocol
        PluginAuth = ( 1UL << 19 ), //Client supports plugin authentication
        ConnectAttrs = ( 1UL << 20 ), // Allows client connection attributes
        CanHandleExpiredPassword = ( 1UL << 22 ), // Support for password expiration > 5.6.6
        ClientSslVerifyServerCert = ( 1UL << 30 ),
        ClientRememberOptions = ( 1UL << 31 )
    }

    [Flags]
    internal enum ServerStatusFlags {
        InTransaction = 1, // Transaction has started
        AutoCommitMode = 2, // Server in auto_commit mode 
        MoreResults = 4, // More results on server
        AnotherQuery = 8, // Multi query - next query exists
        BadIndex = 16,
        NoIndex = 32,
        CursorExists = 64,
        LastRowSent = 128,
        OutputParameters = 4096
    }

    /// <summary>
    /// DB Operations Code
    /// </summary>
    internal enum DbCmd : byte {
        Sleep = 0,
        Quit = 1,
        InitDb = 2,
        Query = 3,
        FieldList = 4,
        CreateDb = 5,
        DropDb = 6,
        Reload = 7,
        Shutdown = 8,
        Statistics = 9,
        ProcessInfo = 10,
        Connect = 11,
        ProcessKill = 12,
        Debug = 13,
        Ping = 14,
        Time = 15,
        DelayedInsert = 16,
        ChangeUser = 17,
        BinlogDump = 18,
        TableDump = 19,
        ConnectOut = 20,
        RegisterSlave = 21,
        Prepare = 22,
        Execute = 23,
        LongData = 24,
        CloseStmt = 25,
        ResetStmt = 26,
        SetOption = 27,
        Fetch = 28
    }

    /// <summary>
    /// Specifies MySQL specific data type of a field, property, for use in a <see cref="MySqlParameter"/>.
    /// </summary>
    public enum MySqlDbType {
        /// <summary>
        /// <see cref="Decimal"/>
        /// <para>A fixed precision and scale numeric value between -1038 
        /// -1 and 10 38 -1.</para>
        /// </summary>
        Decimal = 0,

        /// <summary>
        /// <see cref="Byte"/><para>The signed range is -128 to 127. The unsigned 
        /// range is 0 to 255.</para>
        /// </summary>
        Byte = 1,

        /// <summary>
        /// <see cref="Int16"/><para>A 16-bit signed integer. The signed range is 
        /// -32768 to 32767. The unsigned range is 0 to 65535</para>
        /// </summary>
        Int16 = 2,

        /// <summary>
        /// Specifies a 24 (3 byte) signed or unsigned value.
        /// </summary>
        Int24 = 9,

        /// <summary>
        /// <see cref="Int32"/><para>A 32-bit signed integer</para>
        /// </summary>
        Int32 = 3,

        /// <summary>
        /// <see cref="long"/><para>A 64-bit signed integer.</para>
        /// </summary>
        Int64 = 8,

        /// <summary>
        /// <see cref="float"/><para>A small (single-precision) floating-point 
        /// number. Allowable values are -3.402823466E+38 to -1.175494351E-38, 
        /// 0, and 1.175494351E-38 to 3.402823466E+38.</para>
        /// </summary>
        Float = 4,

        /// <summary>
        /// <see cref="double"/><para>A normal-size (double-precision) 
        /// floating-point number. Allowable values are -1.7976931348623157E+308 
        /// to -2.2250738585072014E-308, 0, and 2.2250738585072014E-308 to 
        /// 1.7976931348623157E+308.</para>
        /// </summary>
        Double = 5,

        /// <summary>
        /// A timestamp. The range is '1970-01-01 00:00:00' to sometime in the 
        /// year 2037
        /// </summary>
        Timestamp = 7,

        ///<summary>
        ///Date The supported range is '1000-01-01' to '9999-12-31'.
        ///</summary>
        Date = 10,

        /// <summary>
        /// Time <para>The range is '-838:59:59' to '838:59:59'.</para>
        /// </summary>
        Time = 11,

        ///<summary>
        ///DateTime The supported range is '1000-01-01 00:00:00' to 
        ///'9999-12-31 23:59:59'.
        ///</summary>
        DateTime = 12,

        ///<summary>
        ///Datetime The supported range is '1000-01-01 00:00:00' to 
        ///'9999-12-31 23:59:59'.
        ///</summary>
        [Obsolete( "The Datetime enum value is obsolete.  Please use DateTime." )]
        Datetime = 12,

        /// <summary>
        /// A year in 2- or 4-digit format (default is 4-digit). The 
        /// allowable values are 1901 to 2155, 0000 in the 4-digit year 
        /// format, and 1970-2069 if you use the 2-digit format (70-69).
        /// </summary>
        Year = 13,

        /// <summary>
        /// <b>Obsolete</b>  Use Datetime or Date type
        /// </summary>
        Newdate = 14,

        /// <summary>
        /// A variable-length string containing 0 to 65535 characters
        /// </summary>
        VarString = 15,

        /// <summary>
        /// Bit-field data type
        /// </summary>
        Bit = 16,

        /// <summary>
        /// New Decimal
        /// </summary>
        NewDecimal = 246,

        /// <summary>
        /// An enumeration. A string object that can have only one value, 
        /// chosen from the list of values 'value1', 'value2', ..., NULL 
        /// or the special "" error value. An ENUM can have a maximum of 
        /// 65535 distinct values
        /// </summary>
        Enum = 247,

        /// <summary>
        /// A set. A string object that can have zero or more values, each 
        /// of which must be chosen from the list of values 'value1', 'value2', 
        /// ... A SET can have a maximum of 64 members.
        /// </summary>
        Set = 248,

        /// <summary>
        /// A binary column with a maximum length of 255 (2^8 - 1) 
        /// characters
        /// </summary>
        TinyBlob = 249,

        /// <summary>
        /// A binary column with a maximum length of 16777215 (2^24 - 1) bytes.
        /// </summary>
        MediumBlob = 250,

        /// <summary>
        /// A binary column with a maximum length of 4294967295 or 
        /// 4G (2^32 - 1) bytes.
        /// </summary>
        LongBlob = 251,

        /// <summary>
        /// A binary column with a maximum length of 65535 (2^16 - 1) bytes.
        /// </summary>
        Blob = 252,

        /// <summary>
        /// A variable-length string containing 0 to 255 bytes.
        /// </summary>
        VarChar = 253,

        /// <summary>
        /// A fixed-length string.
        /// </summary>
        String = 254,

        /// <summary>
        /// Geometric (GIS) data type.
        /// </summary>
        Geometry = 255,

        /// <summary>
        /// Unsigned 8-bit value.
        /// </summary>
        UByte = 501,

        /// <summary>
        /// Unsigned 16-bit value.
        /// </summary>
        UInt16 = 502,

        /// <summary>
        /// Unsigned 24-bit value.
        /// </summary>
        UInt24 = 509,

        /// <summary>
        /// Unsigned 32-bit value.
        /// </summary>
        UInt32 = 503,

        /// <summary>
        /// Unsigned 64-bit value.
        /// </summary>
        UInt64 = 508,

        /// <summary>
        /// Fixed length binary string.
        /// </summary>
        Binary = 600,

        /// <summary>
        /// Variable length binary string.
        /// </summary>
        VarBinary = 601,

        /// <summary>
        /// A text column with a maximum length of 255 (2^8 - 1) characters.
        /// </summary>
        TinyText = 749,

        /// <summary>
        /// A text column with a maximum length of 16777215 (2^24 - 1) characters.
        /// </summary>
        MediumText = 750,

        /// <summary>
        /// A text column with a maximum length of 4294967295 or 
        /// 4G (2^32 - 1) characters.
        /// </summary>
        LongText = 751,

        /// <summary>
        /// A text column with a maximum length of 65535 (2^16 - 1) characters.
        /// </summary>
        Text = 752,

        /// <summary>
        /// A guid column
        /// </summary>
        Guid = 800
    };

    internal enum FieldType : byte {
        Decimal = 0,
        Byte = 1,
        Short = 2,
        Long = 3,
        Float = 4,
        Double = 5,
        Null = 6,
        Timestamp = 7,
        Longlong = 8,
        Int24 = 9,
        Date = 10,
        Time = 11,
        Datetime = 12,
        Year = 13,
        Newdate = 14,
        Enum = 247,
        Set = 248,
        TinyBlob = 249,
        MediumBlob = 250,
        LongBlob = 251,
        Blob = 252,
        VarString = 253,
        String = 254
    }

    /// <summary>
    /// Allows the user to specify the type of connection that should
    /// be used.
    /// </summary>
    public enum MySqlConnectionProtocol {
        /// <summary>
        /// TCP/IP style connection.  Works everywhere.
        /// </summary>
        Sockets = 1,
        Socket = 1,
        Tcp = 1,

        /// <summary>
        /// Named pipe connection.  Works only on Windows systems.
        /// </summary>
        Pipe = 2,
        NamedPipe = 2,

        /// <summary>
        /// Unix domain socket connection.  Works only with Unix systems.
        /// </summary>
        UnixSocket = 3,
        Unix = 3,

        /// <summary>
        /// Shared memory connection.  Currently works only with Windows systems.
        /// </summary>
        SharedMemory = 4,
        Memory = 4
    }

    /// <summary>
    /// SSL options for connection.
    /// </summary>
    public enum MySqlSslMode {
        /// <summary>
        /// Do not use SSL.
        /// </summary>
        None,

        /// <summary>
        /// Use SSL, if server supports it.
        /// </summary>
        Preferred,
        Prefered = Preferred,

        /// <summary>
        /// Always use SSL. Deny connection if server does not support SSL.
        /// Do not perform server certificate validation. 
        /// </summary>
        Required,

        /// <summary>
        /// Always use SSL. Validate server SSL certificate, but different host name mismatch.
        /// </summary>
        VerifyCa,

        /// <summary>
        /// Always use SSL and perform full certificate validation.
        /// </summary>
        VerifyFull
    }

    /// <summary>
    /// Specifies the connection types supported
    /// </summary>
    public enum MySqlDriverType {
        /// <summary>
        /// Use TCP/IP sockets.
        /// </summary>
        Native,

        /// <summary>
        /// Use client library.
        /// </summary>
        Client,

        /// <summary>
        /// Use MySQL embedded server.
        /// </summary>
        Embedded
    }

    public enum MySqlCertificateStoreLocation {
        /// <summary>
        /// Do not use certificate store
        /// </summary>
        None,

        /// <summary>
        /// Use certificate store for the current user
        /// </summary>
        CurrentUser,

        /// <summary>
        /// User certificate store for the machine
        /// </summary>
        LocalMachine
    }

    internal class MySqlConnectAttrs {
        [DisplayName( "_client_name" )]
        public string ClientName => "MySql Connector/NET";

#if !RT
        [DisplayName( "_pid" )]
        public string Pid {
            get {
                var pid = string.Empty;
                try {
                    pid = Process.GetCurrentProcess().Id.ToString();
                }
                catch ( Exception ex ) {
                    Debug.WriteLine( ex.ToString() );
                }

                return pid;
            }
        }

#if !CF

        [DisplayName( "_client_version" )]
        public string ClientVersion {
            get {
                var version = string.Empty;
                try {
                    version = Assembly.GetAssembly( typeof( MySqlConnectAttrs ) ).GetName().Version.ToString();
                }
                catch ( Exception ex ) {
                    Debug.WriteLine( ex.ToString() );
                }
                return version;
            }
        }

        [DisplayName( "_platform" )]
        public string Platform => Is64BitOs() ? "x86_64" : "x86_32";

        [DisplayName( "program_name" )]
        public string ProgramName {
            get {
                var name = Environment.CommandLine;
                try {
                    var path = Environment.CommandLine.Substring( 0, Environment.CommandLine.IndexOf( "\" " ) ).Trim( '"' );
                    name = Path.GetFileName( path );
                    if ( Assembly.GetEntryAssembly() != null ) name = Assembly.GetEntryAssembly().ManifestModule.Name;
                }
                catch ( Exception ex ) {
                    name = string.Empty;
                    Debug.WriteLine( ex.ToString() );
                }
                return name;
            }
        }

        [DisplayName( "_os" )]
        public string Os {
            get {
                var os = string.Empty;
                try {
                    os = Environment.OSVersion.Platform.ToString();
                    if ( os == "Win32NT" ) {
                        os = "Win";
                        os += Is64BitOs() ? "64" : "32";
                    }
                }
                catch ( Exception ex ) {
                    Debug.WriteLine( ex.ToString() );
                }

                return os;
            }
        }

        [DisplayName( "_os_details" )]
        public string OsDetails {
            get {
                var os = string.Empty;
                try {
                    var searcher = new ManagementObjectSearcher( "SELECT * FROM Win32_OperatingSystem" );
                    var collection = searcher.Get();
                    foreach ( var mgtObj in collection ) {
                        os = mgtObj.GetPropertyValue( "Caption" ).ToString();
                        break;
                    }
                }
                catch ( Exception ex ) {
                    Debug.WriteLine( ex.ToString() );
                }

                return os;
            }
        }

        [DisplayName( "_thread" )]
        public string Thread {
            get {
                var thread = string.Empty;
                try {
                    thread = Process.GetCurrentProcess().Threads[ 0 ].Id.ToString();
                }
                catch ( Exception ex ) {
                    Debug.WriteLine( ex.ToString() );
                }

                return thread;
            }
        }

        private bool Is64BitOs() {
#if CLR4
      return Environment.Is64BitOperatingSystem;
#else
            return Environment.GetEnvironmentVariable( "PROCESSOR_ARCHITECTURE" ) == "AMD64";
#endif
        }

#endif
#endif
    }
}
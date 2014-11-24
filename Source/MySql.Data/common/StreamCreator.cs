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
using System.IO;
using MySql.Data.MySqlClient;
using MySql.Data.MySqlClient.Properties;

namespace MySql.Data.Common {
    /// <summary>
    /// Summary description for StreamCreator.
    /// </summary>
    internal class StreamCreator {
        private readonly string _hostList;
        private uint _port;
        private string _pipeName;
        private uint _timeOut;
        private uint _keepalive;
        private DbVersion _driverVersion;

        public StreamCreator( string hosts, uint port, string pipeName, uint keepalive, DbVersion driverVersion ) {
            _hostList = hosts;
            if ( string.IsNullOrEmpty( _hostList ) ) _hostList = "localhost";
            _port = port;
            _pipeName = pipeName;
            _keepalive = keepalive;
            _driverVersion = driverVersion;
        }

        public static Stream GetStream( string server, uint port, string pipename, uint keepalive, DbVersion v, uint timeout ) {
            var settings = new MySqlConnectionStringBuilder {
                Server = server,
                Port = port,
                PipeName = pipename,
                Keepalive = keepalive,
                ConnectionTimeout = timeout
            };
            return GetStream( settings );
        }

        public static Stream GetStream( MySqlConnectionStringBuilder settings ) {
            switch ( settings.ConnectionProtocol ) {
                case MySqlConnectionProtocol.Tcp:
                    return GetTcpStream( settings );
                case MySqlConnectionProtocol.UnixSocket:
                    return GetUnixSocketStream( settings );
                case MySqlConnectionProtocol.SharedMemory:
                    return GetSharedMemoryStream( settings );
                case MySqlConnectionProtocol.NamedPipe:
                    return GetNamedPipeStream( settings );
            }
            throw new InvalidOperationException( Resources.UnknownConnectionProtocol );
        }

        private static Stream GetTcpStream( MySqlConnectionStringBuilder settings ) {
            return MyNetworkStream.CreateStream( settings, false );
        }

        private static Stream GetUnixSocketStream( MySqlConnectionStringBuilder settings ) {
            if ( Platform.IsWindows() ) throw new InvalidOperationException( Resources.NoUnixSocketsOnWindows );
            return MyNetworkStream.CreateStream( settings, true );
        }

        private static Stream GetSharedMemoryStream( MySqlConnectionStringBuilder settings ) {
            var str = new SharedMemoryStream( settings.SharedMemoryName );
            str.Open( settings.ConnectionTimeout );
            return str;
        }

        private static Stream GetNamedPipeStream( MySqlConnectionStringBuilder settings ) {
            return NamedPipeStream.Create( settings.PipeName, settings.Server, settings.ConnectionTimeout );
        }
    }
}
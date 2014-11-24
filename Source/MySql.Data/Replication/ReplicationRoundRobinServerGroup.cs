// Copyright © 2013, Oracle and/or its affiliates. All rights reserved.
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

namespace MySql.Data.MySqlClient.Replication {
    /// <summary>
    /// Class that implements Round Robing Load Balancing technique
    /// </summary>
    public class ReplicationRoundRobinServerGroup : ReplicationServerGroup {
        private int _nextServer;

        public ReplicationRoundRobinServerGroup( string name, int retryTime ) : base( name, retryTime ) { _nextServer = -1; }

        /// <summary>
        /// Gets an available server based on Round Robin load balancing
        /// </summary>
        /// <param name="isMaster">True if the server to return must be a master</param>
        /// <returns>Next available server</returns>
        protected internal override ReplicationServer GetServer( bool isMaster ) {
            for ( var i = 0; i < Servers.Count; i++ ) {
                _nextServer++;
                if ( _nextServer == Servers.Count ) _nextServer = 0;
                var s = Servers[ _nextServer ];
                if ( !s.IsAvailable ) continue;
                if ( isMaster && !s.IsMaster ) continue;
                return s;
            }
            return null;
        }
    }
}
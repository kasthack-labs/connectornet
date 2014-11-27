// Copyright © 2014, Oracle and/or its affiliates. All rights reserved.
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
using System.Linq;
using MySql.Data.MySqlClient.Properties;

namespace MySql.Data.MySqlClient.Replication {
    /// <summary>
    /// Manager for Replication & Load Balancing features
    /// </summary>
    internal static class ReplicationManager {
        private static readonly List<ReplicationServerGroup> _groups = new List<ReplicationServerGroup>();
        private static readonly Object ThisLock = new Object();
        //private static Dictionary<string, ReplicationServerSelector> selectors = new Dictionary<string, ReplicationServerSelector>();

        static ReplicationManager() {
            Groups = _groups;
            // load up our selectors
            if ( MySqlConfiguration.Settings == null ) return;

            foreach ( var group in MySqlConfiguration.Settings.Replication.ServerGroups ) {
                var g = AddGroup( group.Name, group.GroupType, group.RetryTime );
                foreach ( var server in group.Servers ) g.AddServer( server.Name, server.IsMaster, server.ConnectionString );
            }
        }

        /// <summary>
        /// Returns Replication Server Group List
        /// </summary>
        internal static IList<ReplicationServerGroup> Groups { get; private set; }

        /// <summary>
        /// Adds a Default Server Group to the list
        /// </summary>
        /// <param name="name">Group name</param>
        /// <param name="retryTime">Time between reconnections for failed servers</param>
        /// <returns>Replication Server Group added</returns>
        internal static ReplicationServerGroup AddGroup( string name, int retryTime ) => AddGroup( name, null, retryTime );

        /// <summary>
        /// Adds a Server Group to the list
        /// </summary>
        /// <param name="name">Group name</param>
        /// <param name="groupType">ServerGroup type reference</param>
        /// <param name="retryTime">Time between reconnections for failed servers</param>
        /// <returns>Server Group added</returns>
        internal static ReplicationServerGroup AddGroup( string name, string groupType, int retryTime ) {
            if ( string.IsNullOrEmpty( groupType ) ) groupType = "MySql.Data.MySqlClient.Replication.ReplicationRoundRobinServerGroup";
            var g = (ReplicationServerGroup) Activator.CreateInstance( Type.GetType( groupType ), name, retryTime );
            _groups.Add( g );
            return g;
        }

        /// <summary>
        /// Gets the next server from a replication group
        /// </summary>
        /// <param name="groupName">Group name</param>
        /// <param name="isMaster">True if the server to return must be a master</param>
        /// <returns>Replication Server defined by the Load Balancing plugin</returns>
        internal static ReplicationServer GetServer( string groupName, bool isMaster ) {
            return GetGroup( groupName ).GetServer( isMaster );
        }

        /// <summary>
        /// Gets a Server Group by name
        /// </summary>
        /// <param name="groupName">Group name</param>
        /// <returns>Server Group if found, otherwise throws an MySqlException</returns>
        internal static ReplicationServerGroup GetGroup( string groupName ) {
            var group = _groups.FirstOrDefault( g => g.Name.IgnoreCaseEquals( groupName ));
            if ( group == null ) throw new MySqlException( String.Format( Resources.ReplicationGroupNotFound, groupName ) );
            return group;
        }

        /// <summary>
        /// Validates if the replication group name exists
        /// </summary>
        /// <param name="groupName">Group name to validate</param>
        /// <returns>True if replication group name is found, otherwise false</returns>
        internal static bool IsReplicationGroup( string groupName ) => _groups.Any( g =>  g.Name.IgnoreCaseEquals( groupName ));

        /// <summary>
        /// Assigns a new server driver to the connection object
        /// </summary>
        /// <param name="groupName">Group name</param>
        /// <param name="master">True if the server connection to assign must be a master</param>
        /// <param name="connection">MySqlConnection object where the new driver will be assigned</param>
        internal static void GetNewConnection( string groupName, bool master, MySqlConnection connection ) {
            do {
                lock ( ThisLock ) {
                    if ( !IsReplicationGroup( groupName ) ) return;

                    var group = GetGroup( groupName );
                    var server = group.GetServer( master, connection.Settings );

                    if ( server == null ) throw new MySqlException( Resources.Replication_NoAvailableServer );

                    try {
                        var isNewServer = false;
                        if ( connection.Driver == null
                             || !connection.Driver.IsOpen ) isNewServer = true;
                        else {
                            if ( !new MySqlConnectionStringBuilder( server.ConnectionString ).Equals( connection.Driver.Settings ) ) isNewServer = true;
                        }
                        if ( isNewServer ) {
                            connection.Driver = Driver.Create( new MySqlConnectionStringBuilder( server.ConnectionString ) );
                        }
                        return;
                    }
                    catch ( MySqlException ex ) {
                        connection.Driver = null;
                        server.IsAvailable = false;
                        MySqlTrace.LogError( ex.Number, ex.ToString() );
                        if ( ex.Number == 1042 )
                            // retry to open a failed connection and update its status
                            group.HandleFailover( server, ex );
                        else throw;
                    }
                }
            } while ( true );
        }
    }
}
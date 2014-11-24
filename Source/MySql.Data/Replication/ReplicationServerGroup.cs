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
using System.ComponentModel;
using System.Linq;
using System.Timers;
using MySql.Data.MySqlClient.Properties;

namespace MySql.Data.MySqlClient.Replication {
    /// <summary>
    /// Base class used to implement load balancing features
    /// </summary>
    public abstract class ReplicationServerGroup {
        protected List<ReplicationServer> servers = new List<ReplicationServer>();

        /// <param name="name">Group name</param>
        /// <param name="retryTime"></param>
        public ReplicationServerGroup( string name, int retryTime ) {
            Servers = servers;
            Name = name;
            RetryTime = retryTime;
        }

        /// <summary>
        /// Group name
        /// </summary>
        public string Name { get; protected set; }

        /// <summary>
        /// Retry time between connections to failed servers
        /// </summary>
        public int RetryTime { get; protected set; }

        /// <summary>
        /// Servers list in the group
        /// </summary>
        protected IList<ReplicationServer> Servers { get; private set; }

        /// <summary>
        /// Adds a server into the group
        /// </summary>
        /// <param name="name">Server name</param>
        /// <param name="isMaster">True if the server to add is master, False for slave server</param>
        /// <param name="connectionString">Connection string used by this server</param>
        /// <returns></returns>
        protected internal ReplicationServer AddServer( string name, bool isMaster, string connectionString ) {
            var server = new ReplicationServer( name, isMaster, connectionString );
            servers.Add( server );
            return server;
        }

        /// <summary>
        /// Removes a server from group
        /// </summary>
        /// <param name="name">Server name</param>
        protected internal void RemoveServer( string name ) {
            var serverToRemove = GetServer( name );
            if ( serverToRemove == null ) throw new MySqlException( String.Format( Resources.ReplicationServerNotFound, name ) );
            servers.Remove( serverToRemove );
        }

        /// <summary>
        /// Gets a server by name
        /// </summary>
        /// <param name="name">Server name</param>
        /// <returns>Replication server</returns>
        protected internal ReplicationServer GetServer( string name ) => servers.FirstOrDefault( server => server.Name == name);

        /// <summary>
        /// Must be implemented. Defines the next server for a custom load balancing implementation.
        /// </summary>
        /// <param name="isMaster">Defines if the server to return is a master or any</param>
        /// <returns>Next server based on the load balancing implementation.
        ///   Null if no available server is found.
        /// </returns>
        protected internal abstract ReplicationServer GetServer( bool isMaster );

        protected internal virtual ReplicationServer GetServer( bool isMaster, MySqlConnectionStringBuilder settings ) => GetServer( isMaster );

        /// <summary>
        /// Handles a failed connection to a server.
        /// This method can be overrided to implement a custom failover handling
        /// </summary>
        /// <param name="server">The failed server</param>
        protected internal virtual void HandleFailover( ReplicationServer server ) {
            var worker = new BackgroundWorker();
            worker.DoWork += ( sender, e ) => {
                var isRunning = false;
                var server1 = e.Argument as ReplicationServer;
                var timer = new Timer( RetryTime * 1000.0 );
                ElapsedEventHandler elapsedEvent = ( o, args ) => {
                    if ( isRunning ) return;
                    try {
                        isRunning = true;
                        using ( var connectionFailed = new MySqlConnection( server.ConnectionString ) ) {
                            connectionFailed.Open();
                            server1.IsAvailable = true;
                            timer.Stop();
                        }
                    }
                    catch {
                        MySqlTrace.LogWarning( 0, string.Format( Resources.Replication_ConnectionAttemptFailed, server1.Name ) );
                    }
                    finally {
                        isRunning = false;
                    }
                };
                timer.Elapsed += elapsedEvent;
                timer.Start();
                elapsedEvent( sender, null );
            };

            worker.RunWorkerAsync( server );
        }

        /// <summary>
        /// Handles a failed connection to a server.
        /// </summary>
        /// <param name="server">The failed server</param>
        /// <param name="exception">Exception that caused the failover</param>
        protected internal virtual void HandleFailover( ReplicationServer server, Exception exception ) {
            HandleFailover( server );
        }
    }
}
// Copyright (c) 2004-2008 MySQL AB, 2008-2009 Sun Microsystems, Inc. 2014 Oracle and/or its affiliates. All rights reserved.
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
#if !RT
using System.Data;

#endif

namespace MySql.Data.MySqlClient {
    /// <include file='docs/MySqlTransaction.xml' path='docs/Class/*'/>
    public sealed partial class MySqlTransaction : IDisposable {
        private bool _open;

        internal MySqlTransaction( MySqlConnection c, IsolationLevel il ) {
            Connection = c;
            IsolationLevel = il;
            _open = true;
        }

        #region Destructor
        ~MySqlTransaction() { Dispose( false ); }
        #endregion

        #region Properties
        /// <summary>
        /// Gets the <see cref="MySqlConnection"/> object associated with the transaction, or a null reference (Nothing in Visual Basic) if the transaction is no longer valid.
        /// </summary>
        /// <value>The <see cref="MySqlConnection"/> object associated with this transaction.</value>
        /// <remarks>
        /// A single application may have multiple database connections, each 
        /// with zero or more transactions. This property enables you to 
        /// determine the connection object associated with a particular 
        /// transaction created by <see cref="MySqlConnection.BeginTransaction()"/>.
        /// </remarks>
        public new MySqlConnection Connection { get; }

        /// <summary>
        /// Specifies the <see cref="IsolationLevel"/> for this transaction.
        /// </summary>
        /// <value>
        /// The <see cref="IsolationLevel"/> for this transaction. The default is <b>ReadCommitted</b>.
        /// </value>
        /// <remarks>
        /// Parallel transactions are not supported. Therefore, the IsolationLevel 
        /// applies to the entire transaction.
        /// </remarks>
        public override IsolationLevel IsolationLevel { get; }
        #endregion

        public void Dispose() {
            Dispose( true );
            GC.SuppressFinalize( this );
        }

        internal void Dispose( bool disposing ) {
            if ( disposing ) if ( ( Connection != null && Connection.State == ConnectionState.Open || Connection.SoftClosed ) && _open ) Rollback();
        }

        /// <include file='docs/MySqlTransaction.xml' path='docs/Commit/*'/>
        public override void Commit() {
            if ( Connection == null
                 || ( Connection.State != ConnectionState.Open && !Connection.SoftClosed ) ) throw new InvalidOperationException( "Connection must be valid and open to commit transaction" );
            if ( !_open ) throw new InvalidOperationException( "Transaction has already been committed or is not pending" );
            var cmd = new MySqlCommand( "COMMIT", Connection );
            cmd.ExecuteNonQuery();
            _open = false;
        }

        /// <include file='docs/MySqlTransaction.xml' path='docs/Rollback/*'/>
        public override void Rollback() {
            if ( Connection == null
                 || ( Connection.State != ConnectionState.Open && !Connection.SoftClosed ) ) throw new InvalidOperationException( "Connection must be valid and open to rollback transaction" );
            if ( !_open ) throw new InvalidOperationException( "Transaction has already been rolled back or is not pending" );
            var cmd = new MySqlCommand( "ROLLBACK", Connection );
            cmd.ExecuteNonQuery();
            _open = false;
        }
    }
}
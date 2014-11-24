// Copyright © 2004, 2010, Oracle and/or its affiliates. All rights reserved.
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
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Threading;
using System.Transactions;
using MySql.Data.Constants;
using IsolationLevel = System.Transactions.IsolationLevel;

namespace MySql.Data.MySqlClient {
    /// <summary>
    /// Represents a single(not nested) TransactionScope
    /// </summary>
    internal class MySqlTransactionScope {
        public MySqlConnection Connection;
        public Transaction BaseTransaction;
        public MySqlTransaction SimpleTransaction;
        public int RollbackThreadId;

        public MySqlTransactionScope( MySqlConnection con, Transaction trans, MySqlTransaction simpleTransaction ) {
            Connection = con;
            BaseTransaction = trans;
            this.SimpleTransaction = simpleTransaction;
        }

        public void Rollback( SinglePhaseEnlistment singlePhaseEnlistment ) {
            // prevent commands in main thread to run concurrently
            var driver = Connection.Driver;
            lock ( driver ) {
                RollbackThreadId = Thread.CurrentThread.ManagedThreadId;
                while ( Connection.Reader != null )
                    // wait for reader to finish. Maybe we should not wait 
                    // forever and cancel it after some time?
                    Thread.Sleep( 100 );
                SimpleTransaction.Rollback();
                singlePhaseEnlistment.Aborted();
                DriverTransactionManager.RemoveDriverInTransaction( BaseTransaction );

                driver.CurrentTransaction = null;

                if ( Connection.State == ConnectionState.Closed ) Connection.CloseFully();
                RollbackThreadId = 0;
            }
        }

        public void SinglePhaseCommit( SinglePhaseEnlistment singlePhaseEnlistment ) {
            SimpleTransaction.Commit();
            singlePhaseEnlistment.Committed();
            DriverTransactionManager.RemoveDriverInTransaction( BaseTransaction );
            Connection.Driver.CurrentTransaction = null;

            if ( Connection.State == ConnectionState.Closed ) Connection.CloseFully();
        }
    }

    internal sealed class MySqlPromotableTransaction : IPromotableSinglePhaseNotification {
        // Per-thread stack to manage nested transaction scopes
        [ThreadStatic]
        private static Stack<MySqlTransactionScope> _globalScopeStack;

        private readonly MySqlConnection _connection;
        private readonly Transaction _baseTransaction;
        private Stack<MySqlTransactionScope> _scopeStack;

        public MySqlPromotableTransaction( MySqlConnection connection, Transaction baseTransaction ) {
            _connection = connection;
            _baseTransaction = baseTransaction;
        }

        public Transaction BaseTransaction => _scopeStack.Count > 0 ? _scopeStack.Peek().BaseTransaction : null;

        public bool InRollback => _scopeStack.Count > 0 && _scopeStack.Peek().RollbackThreadId == Thread.CurrentThread.ManagedThreadId;

        void IPromotableSinglePhaseNotification.Initialize() {
            var valueName = Enum.GetName( Constants.Types.IsolationLevel, _baseTransaction.IsolationLevel );
            var dataLevel = (System.Data.IsolationLevel)Enum.Parse( Constants.Types.IsolationLevel, valueName );
            var simpleTransaction = _connection.BeginTransaction( dataLevel );

            // We need to save the per-thread scope stack locally.
            // We cannot always use thread static variable in rollback: when scope
            // times out, rollback is issued by another thread.
            if ( _globalScopeStack == null ) _globalScopeStack = new Stack<MySqlTransactionScope>();

            _scopeStack = _globalScopeStack;
            _scopeStack.Push( new MySqlTransactionScope( _connection, _baseTransaction, simpleTransaction ) );
        }

        void IPromotableSinglePhaseNotification.Rollback( SinglePhaseEnlistment singlePhaseEnlistment ) {
            var current = _scopeStack.Peek();
            current.Rollback( singlePhaseEnlistment );
            _scopeStack.Pop();
        }

        void IPromotableSinglePhaseNotification.SinglePhaseCommit( SinglePhaseEnlistment singlePhaseEnlistment ) {
            _scopeStack.Pop().SinglePhaseCommit( singlePhaseEnlistment );
        }

        byte[] ITransactionPromoter.Promote() { throw new NotSupportedException(); }
    }

    internal class DriverTransactionManager {
        private static readonly Hashtable DriversInUse = new Hashtable();

        public static Driver GetDriverInTransaction( Transaction transaction ) {
            lock ( DriversInUse.SyncRoot ) {
                var d = (Driver) DriversInUse[ transaction.GetHashCode() ];
                return d;
            }
        }

        public static void SetDriverInTransaction( Driver driver ) {
            lock ( DriversInUse.SyncRoot ) {
                DriversInUse[ driver.CurrentTransaction.BaseTransaction.GetHashCode() ] = driver;
            }
        }

        public static void RemoveDriverInTransaction( Transaction transaction ) {
            lock ( DriversInUse.SyncRoot ) {
                DriversInUse.Remove( transaction.GetHashCode() );
            }
        }
    }
}
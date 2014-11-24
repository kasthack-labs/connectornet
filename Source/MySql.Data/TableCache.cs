// Copyright © 2011, Oracle and/or its affiliates. All rights reserved.
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

namespace MySql.Data.MySqlClient {
    internal class TableCache {
        private static readonly BaseTableCache Cache;

        static TableCache() { Cache = new BaseTableCache( 480 /* 8 hour max by default */ ); }

        public static void AddToCache( string commandText, ResultSet resultSet ) { Cache.AddToCache( commandText, resultSet ); }

        public static ResultSet RetrieveFromCache( string commandText, int cacheAge ) {
            return (ResultSet) Cache.RetrieveFromCache( commandText, cacheAge );
        }

        public static void RemoveFromCache( string commandText ) { Cache.RemoveFromCache( commandText ); }

        public static void DumpCache() { Cache.Dump(); }
    }

    public class BaseTableCache {
        protected int MaxCacheAge;
        private readonly Dictionary<string, CacheEntry> _cache = new Dictionary<string, CacheEntry>();

        public BaseTableCache( int maxCacheAge ) { MaxCacheAge = maxCacheAge; }

        public virtual void AddToCache( string commandText, object resultSet ) {
            CleanCache();
            var entry = new CacheEntry();
            entry.CacheTime = DateTime.Now;
            entry.CacheElement = resultSet;
            lock ( _cache ) {
                if ( _cache.ContainsKey( commandText ) ) return;
                _cache.Add( commandText, entry );
            }
        }

        public virtual object RetrieveFromCache( string commandText, int cacheAge ) {
            CleanCache();
            lock ( _cache ) {
                if ( !_cache.ContainsKey( commandText ) ) return null;
                var entry = _cache[ commandText ];
                if ( DateTime.Now.Subtract( entry.CacheTime ).TotalSeconds > cacheAge ) return null;
                return entry.CacheElement;
            }
        }

        public void RemoveFromCache( string commandText ) {
            lock ( _cache ) {
                if ( !_cache.ContainsKey( commandText ) ) return;
                _cache.Remove( commandText );
            }
        }

        public virtual void Dump() { lock ( _cache ) _cache.Clear(); }

        protected virtual void CleanCache() {
            var now = DateTime.Now;
            var keysToRemove = new List<string>();

            lock ( _cache ) {
                foreach ( var key in _cache.Keys ) {
                    var diff = now.Subtract( _cache[ key ].CacheTime );
                    if ( diff.TotalSeconds > MaxCacheAge ) keysToRemove.Add( key );
                }

                foreach ( var key in keysToRemove ) _cache.Remove( key );
            }
        }

        private struct CacheEntry {
            public DateTime CacheTime;
            public object CacheElement;
        }
    }
}
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
using System.Collections;
using System.Text;
using MySql.Data.Constants.Types;

namespace MySql.Data.Common {
    internal class ContextString {
        private readonly bool _escapeBackslash;
        // Create a private ctor so the compiler doesn't give us a default one
        public ContextString( string contextMarkers, bool escapeBackslash ) {
            ContextMarkers = contextMarkers;
            _escapeBackslash = escapeBackslash;
        }
        public string ContextMarkers { get; set; }
        public int IndexOf( string src, string target ) => IndexOf( src, target, 0 );
        public int IndexOf( string src, string target, int startIndex ) {
            var index = src.IndexOf(target, startIndex, StringComparison.Ordinal);
            while ( index != -1 ) {
                if ( !IndexInQuotes( src, index, startIndex ) ) break;
                index = src.IndexOf(target, index + 1, StringComparison.Ordinal);
            }
            return index;
        }
        private bool IndexInQuotes( string src, int index, int startIndex ) {
            var contextMarker = Char.MinValue;
            var escaped = false;
            for ( var i = startIndex; i < index; i++ ) {
                var c = src[ i ];
                var contextIndex = ContextMarkers.IndexOf( c );
                // if we have found the closing marker for our open marker, then close the context
                if ( contextIndex <= -1 || escaped ) continue;
                if ( contextMarker == ContextMarkers[ contextIndex ] ) contextMarker = Char.MinValue;
                // if we have found a context marker and we are not in a context yet, then start one
                else if ( contextMarker == Char.MinValue ) contextMarker = c;
                else if ( c == '\\' && _escapeBackslash ) escaped = true;
            }
            return contextMarker != Char.MinValue || escaped;
        }

        public int IndexOf( string src, char target ) {
            var contextMarker = Char.MinValue;
            var escaped = false;
            var pos = 0;

            foreach ( var c in src ) {
                var contextIndex = ContextMarkers.IndexOf( c );

                // if we have found the closing marker for our open marker, then close the context
                if ( contextIndex > -1 && contextMarker == ContextMarkers[ contextIndex ] && !escaped )
                    contextMarker = Char.MinValue;
                // if we have found a context marker and we are not in a context yet, then start one
                else if ( contextMarker == Char.MinValue && contextIndex > -1 && !escaped ) contextMarker = c;
                else if ( contextMarker == Char.MinValue && c == target ) return pos;
                else if ( c == '\\' && _escapeBackslash ) escaped = !escaped;
                pos++;
            }
            return -1;
        }

        public string[] Split( string src, string delimiters ) {
            var parts = new ArrayList();
            var sb = new StringBuilder();
            var escaped = false;

            var contextMarker = Char.MinValue;

            foreach ( var c in src )
                if ( delimiters.IndexOf( c ) != -1
                     && !escaped ) {
                    if ( contextMarker != Char.MinValue ) sb.Append( c );
                    else if ( sb.Length > 0 ) {
                        parts.Add( sb.ToString() );
                        sb.Remove( 0, sb.Length );
                    }
                }
                else if ( c == '\\' && _escapeBackslash ) escaped = !escaped;
                else {
                    var contextIndex = ContextMarkers.IndexOf( c );
                    if ( !escaped
                         && contextIndex != -1 )
                        // if we have found the closing marker for our open 
                        // marker, then close the context
                        if ( ( contextIndex % 2 ) == 1 ) {
                            if ( contextMarker == ContextMarkers[ contextIndex - 1 ] ) contextMarker = Char.MinValue;
                        }
                        else
                        // if the opening and closing context markers are 
                        // the same then we will always find the opening
                        // marker.
                            if ( contextMarker == ContextMarkers[ contextIndex + 1 ] ) contextMarker = Char.MinValue;
                            else if ( contextMarker == Char.MinValue ) contextMarker = c;

                    sb.Append( c );
                }
            if ( sb.Length > 0 ) parts.Add( sb.ToString() );
            return (string[]) parts.ToArray( TString );
        }
    }
}
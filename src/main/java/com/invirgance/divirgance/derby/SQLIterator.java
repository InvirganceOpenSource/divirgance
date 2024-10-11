/*
 * Copyright 2024 INVIRGANCE LLC

Permission is hereby granted, free of charge, to any person obtaining a copy 
of this software and associated documentation files (the “Software”), to deal 
in the Software without restriction, including without limitation the rights to 
use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies 
of the Software, and to permit persons to whom the Software is furnished to do 
so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all 
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED “AS IS”, WITHOUT WARRANTY OF ANY KIND, EXPRESS OR 
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, 
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE 
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER 
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, 
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE 
SOFTWARE.
 */
package com.invirgance.divirgance.derby;

import com.invirgance.convirgance.CloseableIterator;
import com.invirgance.convirgance.ConvirganceException;
import com.invirgance.convirgance.json.JSONObject;
import java.sql.*;

/**
 *
 * @author jbanes
 */
public class SQLIterator implements CloseableIterator<JSONObject>
{
        private final Connection connection;
        private final ResultSet set;
        
        private boolean next;
        private String[] columns;

        public SQLIterator(Connection connection, ResultSet set) throws SQLException
        {
            this.connection = connection;
            this.set = set;
            this.next = set.next();
            
            set.setFetchSize(1000);
        }

        @Override
        public boolean hasNext()
        {
            return next;
        }
        
        private void loadColumns(ResultSetMetaData meta) throws SQLException
        {
            columns = new String[meta.getColumnCount()];

            for(int i=0; i<columns.length; i++)
            {
                columns[i] = meta.getColumnLabel(i+1);
            }
        }

        @Override
        public JSONObject next()
        {
            JSONObject result = new JSONObject(true);
            
            try
            {
                if(columns == null) loadColumns(set.getMetaData());

                for(int i=0; i<columns.length; i++)
                {
                    result.put(columns[i], set.getObject(i+1));
                }
                
                this.next = set.next();
                
                if(!next) close();
                
                return result;
            }
            catch(SQLException e)
            {
                throw new ConvirganceException(e);
            }
        }

        @Override
        public void close() throws SQLException
        {
            try { set.close(); } catch(SQLException e) { e.printStackTrace(); }
            
            connection.close();
        }
    }
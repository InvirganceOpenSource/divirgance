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
package com.invirgance.divirgance.sql;

import com.invirgance.divirgance.Table;
import java.sql.SQLException;
import java.util.ArrayList;

/**
 *
 * @author jbanes
 */
public class TableDefinition implements SQLAction
{
    private SQLParser.Token token;
    private boolean closed;

    private ArrayList<ColumnDefinition> columns = new ArrayList<>();
    private ColumnDefinition column;
    
    private Table table;
    
    public TableDefinition(SQLParser.Token token)
    {
        this.token = token;
    }

    public boolean isClosed()
    {
        return closed;
    }

    public void setClosed(boolean closed)
    {
        this.closed = closed;
    }
    
    public ColumnDefinition[] getColumns()
    {
        return columns.toArray(ColumnDefinition[]::new); 
    }

    public Table getTable()
    {
        return table;
    }

    public void setTable(Table table)
    {
        this.table = table;
    }
    
    @Override
    public SQLAction parseToken(SQLParser.Token token) throws SQLException
    {
        if(closed)
        {
            token.getParser().parseError("Attempt to insert token [" + token.token + "] into a closed table description.");
        }
        
        if(token.token.equals(")") && (column == null || !column.isOpen()))
        {
            closed = true;
            
            if(column != null) 
            {
                columns.add(column);
                column = null;
            }
            
            return this;
        }
        
        if(token.token.equals(",") && (column == null || !column.isOpen()))
        {
            if(column != null) 
            {
                columns.add(column);
                column = null;
            }
            
            return this;
        }
        
        if(column == null)
        {
            column = new ColumnDefinition(token);
            return this;
        }
        
        if(column != null)
        {
            return column.parseToken(token);
        }
        
        token.getParser().parseError("Unknown token: " + token.token);
        
        return null;
    }

    @Override
    public void execute() throws SQLException
    {
        for(ColumnDefinition definition : this.columns)
        {
            definition.setTable(table);
            definition.execute();
        }
    }
    
}

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

/**
 *
 * @author jbanes
 */
public class ColumnDefinition implements SQLAction
{
    private SQLParser.Token name;
    private SQLParser.Token type;
    
    private boolean open;
    private Integer length1;
    private Integer length2;
    private boolean close;
    
    private Table table;

    public ColumnDefinition(SQLParser.Token token)
    {
        this.name = token;
    }
    
    public boolean isOpen()
    {
        return (open && !close);
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
        if(name == null)
        {
            this.name = token;
            return this;
        }
        
        if(type == null)
        {
            this.type = token;
            return this;
        }
        
        if(token.token.equals("(") && !open)
        {
            open = true;
            return this;
        }
        
        if(token.token.equals(")") && !close)
        {
            close = true;
            return this;
        }
        
        if(token.token.equals(",") && open && !close && length1 != null)
        {
            return this;
        }
        
        if(open && !close && length1 == null)
        {
            length1 = Integer.valueOf(token.token);
            return this;
        }
        
        if(open && !close && length2 == null)
        {
            length2 = Integer.valueOf(token.token);
            return this;
        }
        
        token.getParser().parseError("Unexpected token: " + token.token);
        
        return null;
    }

    @Override
    public void execute() throws SQLException
    {
        String type = this.type.token;
        
        if(length2 != null) type += "(" + length1 + "," + length2 + ")";
        else if(length1 != null) type += "(" + length1 + ")";

        table.addColumn(name.token, type);
    }
    
}

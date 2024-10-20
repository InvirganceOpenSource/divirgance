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

import java.sql.SQLException;
import java.util.ArrayList;

/**
 *
 * @author jbanes
 */
public class Select implements SQLAction
{
    private SQLParser.Token token;
    private From from;
    
    private ArrayList<SelectColumn> columns = new ArrayList<>();
    private SelectColumn column;

    public Select(SQLParser.Token token)
    {
        this.token = token;
    }

    public SQLParser.Token getToken()
    {
        return token;
    }
    
    public SelectColumn[] getColumns()
    {
        if(column != null)
        {
            columns.add(column);
            
            column = null;
        }
        
        return this.columns.toArray(SelectColumn[]::new);
    }
    
    public From getFrom()
    {
        return from;
    }

    @Override
    public SQLAction parseToken(SQLParser.Token token) throws SQLException
    {
        if(from != null)
        {
            return from.parseToken(token);
        }
        
        if(token.type == SQLParser.STATE_TOKEN && token.token.equalsIgnoreCase("from"))
        {
            from = new From(token);
            
            return from;
        }
        
        if(from == null && token.token.equals(","))
        {
            if(column != null) columns.add(column);
            
            column = null;
            
            return this;
        }
        
        if(from == null)
        {
            if(column == null) column = new SelectColumn(token);
            else column.parseToken(token);
            
            return this;
        }
        
        token.getParser().parseError("Unexpected token: " + token.token);
        
        return null;
    }

    @Override
    public void execute() throws SQLException
    {
        throw new UnsupportedOperationException("Not supported yet."); // Generated from nbfs://nbhost/SystemFileSystem/Templates/Classes/Code/GeneratedMethodBody
    }
    
}

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

/**
 *
 * @author jbanes
 */
public class From implements SQLAction
{
    private SQLParser.Token token;
    private SQLParser.Token table;
    private SQLParser.Token alias;

    public From(SQLParser.Token token)
    {
        this.token = token;
    }

    public SQLParser.Token getToken()
    {
        return token;
    }

    public String getTable()
    {
        return table.token;
    }

    public String getAlias()
    {
        if(alias == null) return table.token;
        
        return alias.token;
    }

    @Override
    public SQLAction parseToken(SQLParser.Token token) throws SQLException
    {
        if(table == null)
        {
            table = token;
            
            return this;
        }
        
        if(token.is("as") && alias == null)
        {
            return this;
        }
        
        if(alias == null)
        {
            alias = token;
            
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

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
public class Create implements SQLAction
{
    private SQLParser.Token token;
    private SQLAction action;

    public Create(SQLParser.Token token)
    {
        this.token = token;
    }
    
    @Override
    public SQLAction parseToken(SQLParser.Token token) throws SQLException
    {
        if(this.action != null)
        {
            return this.action.parseToken(token);
        }
        
        if(token.token.equalsIgnoreCase("table")) 
        {
            this.action = new CreateTable(token);
            
            return this.action;
        }
        
        token.getParser().parseError("Unknown token: " + token.token);
        
        return null;
    }

    @Override
    public void execute() throws SQLException
    {
        this.action.execute();
    }
    
}

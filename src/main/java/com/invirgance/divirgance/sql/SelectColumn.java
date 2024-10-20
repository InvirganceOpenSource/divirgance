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
public class SelectColumn implements SQLAction
{
    private SQLParser.Token token;
    private SQLParser.Token resultName;
    private boolean as;

    public SelectColumn(SQLParser.Token token)
    {
        this.token = token;
    }
    
    public String getName()
    {
        return token.token;
    }

    public String getResultName()
    {
        if(resultName == null) return token.token;
        
        return resultName.token;
    }

    @Override
    public SQLAction parseToken(SQLParser.Token token) throws SQLException
    {
        if(token.token.equalsIgnoreCase("as"))
        {
            as = true;
            return this;
        }
        
        if(as && resultName == null)
        {
            this.resultName = token;
            return this;
        }
        
        token.getParser().parseError("Unknown token: " + token.token);
        
        return null;
    }

    @Override
    public void execute() throws SQLException
    {
        throw new UnsupportedOperationException("Not supported yet."); // Generated from nbfs://nbhost/SystemFileSystem/Templates/Classes/Code/GeneratedMethodBody
    }
}

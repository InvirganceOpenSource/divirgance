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

import com.invirgance.divirgance.Database;
import com.invirgance.divirgance.Table;
import java.sql.SQLException;

/**
 *
 * @author jbanes
 */
public class CreateTable implements SQLAction
{
    private SQLParser.Token token;
    private SQLAction action;
    
    private Database database;
    private String tableName;

    public CreateTable(SQLParser.Token token)
    {
        this.token = token;
        this.database = token.getParser().getContext().getDatabase();
    }
    
    @Override
    public SQLAction parseToken(SQLParser.Token token) throws SQLException
    {
        if(this.tableName == null)
        {
            this.tableName = token.token;
            
            if(database.getTable(tableName) != null)
            {
                token.getParser().parseError("Table " + tableName + " already exists!");
            }
            
            return this;
        }
        
        token.getParser().parseError("Unknown token: " + token.token);
        
        return null;
    }

    @Override
    public void execute() throws SQLException
    {
        this.database.createTable(this.tableName);
    }
    
}

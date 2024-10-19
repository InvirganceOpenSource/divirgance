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

import com.invirgance.divirgance.Divirgance;
import java.sql.SQLException;
import java.util.ArrayList;

/**
 *
 * @author jbanes
 */
public class SQLParser
{
    public static final int STATE_WHITESPACE = 0;
    public static final int STATE_TOKEN = 1;
    public static final int STATE_IDENTIFIER = 2;
    public static final int STATE_STRING = 3;
    public static final int STATE_OPERATOR = 4;
    
    private Token root;
    private SQLAction action;
    private ExecutionContext context;
    
    private String sql;
    int offset = 1;
    int line = 1;

    public SQLParser(Divirgance divirgance, String sql)
    {
        this.context = new ExecutionContext(divirgance);
        this.sql = sql;
    }

    public ExecutionContext getContext()
    {
        return context;
    }
    
    public SQLAction parse() throws SQLException
    {
        int index = 0;
        int last = STATE_WHITESPACE;
        int state = STATE_WHITESPACE;
        char c;
        
        StringBuffer buffer = new StringBuffer();
        ArrayList<Token> tokens = new ArrayList<>();
        Token token;
        
        this.offset = 1;
        this.line = 1;
        
        while(index < sql.length())
        {
            c = sql.charAt(index);
            
            if(Character.isLetterOrDigit(c) || c == '_' || c == '-')
            {
                state = STATE_TOKEN;
                
                buffer.append(c);
            }
            else if(Character.isWhitespace(c))
            {
                state = STATE_WHITESPACE;
                
                if(buffer.length() > 0)
                {
                    token = new Token(buffer.toString(), last);
                    action = parseToken(token);
                    
                    if(root == null) root = token;
                    
                    buffer.setLength(0);
                }
            }
            else if("(),+.".indexOf(c) >= 0)
            {
                state = STATE_OPERATOR;
                
                if(buffer.length() > 0)
                {
                    token = new Token(buffer.toString(), last);
                    action = parseToken(token);
                    
                    if(root == null) root = token;
                    
                    buffer.setLength(0);
                }
                
                token = new Token(c + "", STATE_OPERATOR);
                action = parseToken(token);

                if(root == null) root = token;

                buffer.setLength(0);
            }
            
            if(c == '\n')
            {
                line++;
                offset = 0;
            }
            
            last = state;
            
            offset++;
            index++;
        }
        
        if(buffer.length() > 0)
        {
            token = new Token(buffer.toString(), last);
            action = parseToken(token);

            if(root == null) root = token;

            buffer.setLength(0);
        }
        
        if(action == null) parseError("No query provided");
        
        return action;
    }
    
    public SQLAction parseToken(Token token) throws SQLException
    {
        if(action != null)
        {
            action.parseToken(token);
            
            return action;
        }
        
        if(token.type != STATE_TOKEN) parseError("SQL did not start with a token");
        
        if(token.token.equalsIgnoreCase("create")) return new Create(token);
        if(token.token.equalsIgnoreCase("select")) return new Select(token);
        
        parseError("Unrecognized token " + token.token);
        
        return null;
    }
    
    public void parseError(String reason) throws SQLException
    {
        throw new SQLException("Parsing error at Line " + line + ":" + offset + "\n" +
                               "Reason: " + reason + "\n" +
                               "SQL: " + sql);
    }
    
    public class Token
    {
        public int start;
        public String token;
        public int type;

        public Token(String token, int type)
        {
            this.start = offset - token.length();
            this.token = token;
            this.type = type;
        }
        
        public SQLParser getParser()
        {
            return SQLParser.this;
        }
        
        public boolean is(String token)
        {
            return this.token.equalsIgnoreCase(token);
        }
    }
}

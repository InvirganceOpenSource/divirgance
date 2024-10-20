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
package com.invirgance.divirgance.planner;

import com.invirgance.convirgance.ConvirganceException;
import com.invirgance.convirgance.json.JSONObject;
import com.invirgance.convirgance.transform.IdentityTransformer;
import com.invirgance.divirgance.Database;
import com.invirgance.divirgance.Table;
import com.invirgance.divirgance.sql.Select;
import com.invirgance.divirgance.sql.SelectColumn;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;

/**
 *
 * @author jbanes
 */
public class QueryPlan
{
    private Select select;
    
    private HashMap<String,Table> tableAliases = new HashMap();

    public QueryPlan(Select select)
    {
        this.select = select;
    }
    
    private String[] getColumns(Table table)
    {
        ArrayList<String> names = new ArrayList<>();
        
        for(SelectColumn column : select.getColumns())
        {
            if(column.getName().equals("*"))
            {
                names.addAll(Arrays.asList(table.getColumns()));
            }
            else
            {
                names.add(column.getName());
            }
        }
        
        return names.toArray(String[]::new);
    }
    
    private String[] getAliases(Table table)
    {
        ArrayList<String> aliases = new ArrayList<>();
        String alias;
        int index;
        
        for(SelectColumn column : select.getColumns())
        {
            if(column.getName().equals("*"))
            {
                for(String name : table.getColumns())
                {
                    index = 1;
                    alias = name;
                    
                    while(aliases.contains(alias))
                    {
                        alias = name + "_" + index;
                        
                        index++;
                    }
                    
                    aliases.add(alias);
                }
            }
            else
            {
                index = 1;
                alias = column.getResultName();

                while(aliases.contains(alias))
                {
                    alias = column.getName() + "_" + index;

                    index++;
                }

                aliases.add(alias);
            }
        }
        
        return aliases.toArray(String[]::new);
    }
    
    public Iterable<JSONObject> execute()
    {
        Database database = select.getToken().getParser().getContext().getDatabase();
        Table<JSONObject> table = database.getTable(select.getFrom().getTable());
        
        final String[] columns = getColumns(table);
        final String[] aliases = getAliases(table);
        
        tableAliases.put(select.getFrom().getAlias().toLowerCase(), table);
        
        return new IdentityTransformer() {
            @Override
            public JSONObject transform(JSONObject record) throws ConvirganceException
            {
                JSONObject result = new JSONObject(true);
                int index = 0;
                
                for(String column : columns)
                {
                    result.put(aliases[index++], record.get(column));
                }
                
                return result;
            }
        }.transform(table);
    }
}

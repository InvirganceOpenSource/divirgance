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

import com.invirgance.convirgance.json.JSONObject;
import com.invirgance.divirgance.Database;
import com.invirgance.divirgance.Divirgance;
import com.invirgance.divirgance.derby.DerbyAnalyticDatabase;
import com.invirgance.divirgance.sql.SQLParser;
import com.invirgance.divirgance.sql.Select;
import java.io.File;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

/**
 *
 * @author jbanes
 */
public class QueryPlanTest
{
    public void delete(File directory)
    {
        if(!directory.exists()) return;
        
        for(File file : directory.listFiles())
        {
            if(file.isDirectory()) delete(file);
            else file.delete();
        }
        
        directory.delete();
    }
    
    @Test
    public void testBasicSelect() throws Exception
    {
        File directory = new File("target/testing/sql/select");
        
        delete(directory);
        
        Divirgance divirgance = new Divirgance(directory);
        SQLParser parser = new SQLParser(divirgance, "select Column1, Column2, Column3 from TestTable");
        QueryPlan plan;
        Database database = new DerbyAnalyticDatabase();
        
        divirgance.addDatabase("testdb", database);
        database.createTable("TestTable");
        database.getTable("TestTable").addColumn("Column1", "Integer");
        database.getTable("TestTable").addColumn("Column2", "Integer");
        database.getTable("TestTable").addColumn("Column3", "Integer");
        
        database.getTable("TestTable").insert(new JSONObject("{\"Column1\": 3, \"Column2\": 5, \"Column3\": 8}"));
        
        parser.getContext().setDatabase(database);
        
        plan = new QueryPlan((Select)parser.parse());
        
        for(JSONObject record : plan.execute())
        {
            System.out.println(record);
            
            assertEquals("{\"Column1\":3,\"Column2\":5,\"Column3\":8}", record.toString());
        }
        
        parser = new SQLParser(divirgance, "select * from TestTable test");
        plan = new QueryPlan((Select)parser.parse());
        
        for(JSONObject record : plan.execute())
        {
            System.out.println(record);
            
            assertEquals("{\"Column1\":3,\"Column2\":5,\"Column3\":8}", record.toString());
        }
        
        parser = new SQLParser(divirgance, "select Column1 as col1, Column2 as Two, Column3 as OnePlusTwo from TestTable test");
        plan = new QueryPlan((Select)parser.parse());
        
        for(JSONObject record : plan.execute())
        {
            System.out.println(record);
            
            assertEquals("{\"col1\":3,\"Two\":5,\"OnePlusTwo\":8}", record.toString());
        }
        
        parser = new SQLParser(divirgance, "select Column2 as Two, Column3 as OnePlusTwo, Column1 as col1, *, Column1, Column1, *  from TestTable test");
        plan = new QueryPlan((Select)parser.parse());
        
        for(JSONObject record : plan.execute())
        {
            System.out.println(record);
            
            assertEquals("{\"Two\":5,\"OnePlusTwo\":8,\"col1\":3,\"Column1\":3,\"Column2\":5,\"Column3\":8,\"Column1_1\":3,\"Column1_2\":3,\"Column1_3\":3,\"Column2_1\":5,\"Column3_1\":8}", record.toString());
        }
    }
    
}

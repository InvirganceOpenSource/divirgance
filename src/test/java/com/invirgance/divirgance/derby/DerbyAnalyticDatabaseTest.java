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
package com.invirgance.divirgance.derby;

import com.invirgance.convirgance.json.JSONObject;
import com.invirgance.divirgance.Table;
import java.io.File;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

/**
 *
 * @author jbanes
 */
public class DerbyAnalyticDatabaseTest
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
    public void testInitialization() throws Exception
    {
        File directory = new File("target/testing/derby/initialize");
        
        delete(directory);
            
        try(DerbyAnalyticDatabase derby = new DerbyAnalyticDatabase())
        {
            derby.initialize(directory);
        }
        
        try(DerbyAnalyticDatabase derby = new DerbyAnalyticDatabase())
        {
            derby.load(directory);
            
            derby.createTable("TestTable");
            
            assertEquals("TestTable", derby.getTable("TestTable").getName());
            assertTrue(derby.getTables().iterator().hasNext());
            assertEquals("TestTable", derby.getTables().iterator().next().getName());
        }
        
        assertTrue(directory.exists());
        assertTrue(new File(directory, "config.json").exists());
    }
    
    
    @Test
    public void testCRUD() throws Exception
    {
        File directory = new File("target/testing/derby/crud");
        DerbyAnalyticTable table;
        int count = 0;
        
        delete(directory);
        
        try(DerbyAnalyticDatabase derby = new DerbyAnalyticDatabase())
        {
            derby.initialize(directory);
            
            table = (DerbyAnalyticTable)derby.createTable("TestTable");
            
            assertFalse(table.iterator().hasNext());
            
            table.addColumn("BrandName", "VARCHAR");
            table.insert(new JSONObject("{\"BrandName\": \"Bob's Products\"}"));
            
            for(JSONObject record : table)
            {
                assertEquals(36, record.getString("_primarykey").length());
System.out.println(record);
                count++;
            }
        }
    }
    
}

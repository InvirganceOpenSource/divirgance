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
import com.invirgance.divirgance.Divirgance;
import com.invirgance.divirgance.derby.DerbyAnalyticDatabase;
import java.io.File;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

/**
 *
 * @author jbanes
 */
public class SQLParserTest
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
    public void testCreate() throws Exception
    {
        File directory = new File("target/testing/sql/create");
        
        delete(directory);
        
        Divirgance divirgance = new Divirgance(directory);
        SQLParser parser = new SQLParser(divirgance, "create database testdb");
        Database database;
        
        // Create Database "testdb"
        parser.parse().execute();

        database = divirgance.getDatabase("testdb");

        assertNotNull(database);

        // Create Table "Test"
        parser = new SQLParser(divirgance, "create table Test");
        
        parser.getContext().setDatabase(database);
        parser.parse().execute();
        
        assertNotNull(database.getTable("Test"));
        
        // Create Table "Test2"
        parser = new SQLParser(divirgance, "create table Test2 ()");
        
        parser.getContext().setDatabase(database);
        parser.parse().execute();
        
        assertNotNull(database.getTable("Test2"));
        
        // Create Table "Test3"
        parser = new SQLParser(divirgance, "create table Test3 (Column1 Integer, Column2 VARCHAR(64), Column3 NUMERIC(24,12))");
        
        parser.getContext().setDatabase(database);
        parser.parse().execute();
        
        assertNotNull(database.getTable("Test3"));
        assertEquals(3, database.getTable("Test3").getColumns().length);
        assertEquals("Column1", database.getTable("Test3").getColumns()[0]);
        assertEquals("Column2", database.getTable("Test3").getColumns()[1]);
        assertEquals("Column3", database.getTable("Test3").getColumns()[2]);
        assertEquals("Integer", database.getTable("Test3").getType("Column1"));
        assertEquals("VARCHAR(64)", database.getTable("Test3").getType("Column2"));
        assertEquals("NUMERIC(24,12)", database.getTable("Test3").getType("Column3"));
    }
    
    @Test
    public void testSelect() throws Exception
    {
        File directory = new File("target/testing/sql/select");
        
        delete(directory);
        
        Divirgance divirgance = new Divirgance(directory);
        SQLParser parser = new SQLParser(divirgance, "select Column1, Column2, Column3 from TestTable");
        Database database = new DerbyAnalyticDatabase();
        
        divirgance.addDatabase("testdb", database);
        database.createTable("TestTable");
        database.getTable("TestTable").addColumn("Column1", "Integer");
        database.getTable("TestTable").addColumn("Column2", "Integer");
        database.getTable("TestTable").addColumn("Column3", "Integer");
        
        parser.parse();
    }
}

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
package com.invirgance.divirgance;

import com.invirgance.divirgance.derby.DerbyAnalyticDatabase;
import java.io.File;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

/**
 *
 * @author jbanes
 */
public class DivirganceTest
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
        File directory = new File("target/testing/system/initialization");
        
        delete(directory);
        
        Divirgance divirgance = new Divirgance(directory);
        
        try(DerbyAnalyticDatabase derby = new DerbyAnalyticDatabase())
        {
            divirgance.addDatabase("TestDatabase", derby);
        }
        
        assertTrue(directory.exists());
        assertTrue(new File(directory, "config.json").exists());
        
        assertTrue(new File(directory, "TestDatabase").exists());
        assertTrue(new File(new File(directory, "TestDatabase"), "config.json").exists());
    }
    
}

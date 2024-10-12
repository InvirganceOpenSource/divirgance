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

import com.invirgance.convirgance.ConvirganceException;
import com.invirgance.convirgance.json.JSONArray;
import com.invirgance.convirgance.json.JSONObject;
import com.invirgance.convirgance.json.JSONParser;
import java.io.*;

/**
 *
 * @author jbanes
 */
public class Divirgance
{
    private File directory;
    private JSONObject config;
    
    public Divirgance() throws IOException
    {
        this(new File("."));
    }
    
    public Divirgance(File directory) throws IOException
    {
        this.directory = directory;
        
        if(new File(this.directory, "config.json").exists()) 
        {
            loadConfig();
        }
        else
        {
            this.config = new JSONObject();
            
            this.config.put("databases", new JSONArray<>());
            
            saveConfig();
        }
    }
    
    private synchronized void loadConfig() throws IOException
    {
        try(FileInputStream in = new FileInputStream(new File(directory, "config.json")))
        {
            this.config = new JSONParser(new InputStreamReader(in)).parseObject();
        }
    }
    
    private synchronized void saveConfig() throws ConvirganceException
    {
        this.directory.mkdirs();
        
        try(PrintWriter writer = new PrintWriter(new FileOutputStream(new File(this.directory, "config.json"))))
        {
            writer.println(this.config.toString(4));
        }
        catch(IOException e)
        {
            throw new ConvirganceException(e);
        }
    }
    
    public Iterable<JSONObject> getDatabaseConfigs()
    {
        return new JSONArray(this.config.getJSONArray("databases"));
    }
    
    public Database getDatabase(String name)
    {
        File directory;
        Database database;
        
        for(JSONObject record : (JSONArray<JSONObject>)this.config.getJSONArray("databases"))
        {
            if(!record.getString("name").equals(name)) continue;
            
            try
            {
System.out.println(record);
                directory = new File(record.getString("directory"));
                database = (Database)Class.forName(record.getString("type")).newInstance();
                
                database.load(directory);
                
                return database;
            }
            catch(ClassNotFoundException | InstantiationException | IllegalAccessException e)
            {
                throw new ConvirganceException(e);
            }
        }
        
        return null;
    }
    
    public void addDatabase(String name, Database database)
    {
        JSONObject descriptor = new JSONObject();
        File location = new File(this.directory, name);
        
        descriptor.put("name", name);
        descriptor.put("type", database.getClass().getName());
        descriptor.put("directory", location.getAbsolutePath());
        
        this.config.getJSONArray("databases").add(descriptor);
        database.initialize(location);
        saveConfig();
    }
}

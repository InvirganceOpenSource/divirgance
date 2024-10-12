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

import com.invirgance.convirgance.ConvirganceException;
import com.invirgance.convirgance.json.JSONArray;
import com.invirgance.convirgance.json.JSONObject;
import com.invirgance.convirgance.json.JSONParser;
import com.invirgance.divirgance.Database;
import com.invirgance.divirgance.Table;
import java.io.*;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Iterator;
import org.apache.derby.jdbc.EmbeddedDataSource;

/**
 *
 * @author jbanes
 */
public class DerbyAnalyticDatabase implements Database
{
    private String name;
    private File directory;
    
    private EmbeddedDataSource source;
    private JSONObject config;

    public DerbyAnalyticDatabase()
    {
    }
    
    @Override
    public String getName()
    {
        return this.name;
    }
    
    public void setName(String name)
    {
        this.name = name;
    }

    public File getDirectory()
    {
        return directory;
    }

    public JSONObject getConfig()
    {
        return new JSONObject(config);
    }
    
    private void initDriver(boolean create) throws ConvirganceException
    {
        File derby = new File(directory, "derby");
        
        this.source = new EmbeddedDataSource();
        
        this.source.setDatabaseName(derby.getAbsolutePath());
        this.source.setCreateDatabase(create ? "create" : null);
        
        try
        {
            this.source.getConnection().close(); // Ensure database is created and working
        }
        catch(SQLException e)
        {
            throw new ConvirganceException(e);
        }
    }
    
    private synchronized void loadConfig(File file) throws ConvirganceException
    {
        try(FileInputStream in = new FileInputStream(file))
        {
            this.config = new JSONParser(new InputStreamReader(in)).parseObject();
            this.name = this.config.getString(name);
            this.directory = new File(this.config.getString("directory"));
        }
        catch(IOException e)
        {
            throw new ConvirganceException(e);
        }
    }
    
    private synchronized void saveConfig() throws ConvirganceException
    {
        try(PrintWriter writer = new PrintWriter(new FileOutputStream(new File(this.directory, "config.json"))))
        {
            this.config.put("name", this.name);
            this.config.put("directory", this.directory.getAbsolutePath());
            this.config.put("type", this.getClass().getName());
        
            writer.println(this.config.toString(4));
        }
        catch(IOException e)
        {
            throw new ConvirganceException(e);
        }
    }
    
    public void initialize(File directory) throws ConvirganceException
    {
        if(directory.exists() && directory.list() != null && directory.list().length > 0) 
        {
            throw new ConvirganceException("Database already exists in " + directory);
        }
        
        this.name = directory.getName();
        this.directory = directory;
        this.config = new JSONObject();
        this.config.put("tables", new JSONArray());
        
        initDriver(true);
        saveConfig();
    }
    
    public void load(File directory) throws ConvirganceException
    {
        loadConfig(new File(directory, "config.json"));
        initDriver(false);
    }
    
    public Table createTable(String name)
    {
        Statement statement;
        DerbyAnalyticTable table;
        
        try(Connection connection = source.getConnection())
        {
            statement = connection.createStatement();
            table = new DerbyAnalyticTable(this.source, name);
            
            statement.execute("create table \"" + name + "\" (\"_primarykey\" CHAR(36) PRIMARY KEY)");
            table.initialize(new File(this.directory, "table_" + name));
            
            config.getJSONArray("tables").add(name);
            saveConfig();
            
            return table;
        }
        catch(SQLException e)
        {
            throw new ConvirganceException(e);
        }
    }
    
    public Iterable<Table> getTables()
    {   
        return new Iterable<Table>() {
            
            private JSONArray<String> tables = config.getJSONArray("tables");
            
            @Override
            public Iterator<Table> iterator()
            {
                return new Iterator<Table>() {
                    private int index = 0;
                    
                    @Override
                    public boolean hasNext()
                    {
                        return (index < tables.size());
                    }

                    @Override
                    public Table next()
                    {
                        return getTable(tables.get(index++));
                    }
                };
            }
        };
    }
    
    public Table getTable(String name)
    {
        DerbyAnalyticTable table = new DerbyAnalyticTable(this.source, name);
        
        if(!this.config.getJSONArray("tables").contains(name)) return null;
        
        table.load(new File(this.directory, "table_" + name));
        
        return table;
    }

    @Override
    public void close() throws Exception
    {
        this.source.setShutdownDatabase("shutdown");
    }
}

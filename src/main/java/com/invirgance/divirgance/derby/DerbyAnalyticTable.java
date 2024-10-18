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

import com.invirgance.convirgance.CloseableIterator;
import com.invirgance.convirgance.ConvirganceException;
import com.invirgance.convirgance.dbms.DBMS;
import com.invirgance.convirgance.dbms.Query;
import com.invirgance.convirgance.dbms.QueryOperation;
import com.invirgance.convirgance.json.JSONArray;
import com.invirgance.convirgance.json.JSONObject;
import com.invirgance.convirgance.json.JSONParser;
import com.invirgance.divirgance.Table;
import java.io.*;
import java.util.UUID;
import javax.sql.DataSource;

/**
 *
 * @author jbanes
 */
public class DerbyAnalyticTable implements Table<JSONObject>
{
    private JSONObject config;
    private File directory;
    
    private DataSource source;
    private String name;
    

    public DerbyAnalyticTable(DataSource source, String name)
    {
        this.source = source;
        this.name = name;
    }

    @Override
    public String getName()
    { 
        return this.name;
    }

    public File getDirectory()
    {
        return directory;
    }

    public JSONObject getConfig()
    {
        return new JSONObject(config);
    }
    
    private synchronized void loadConfig(File file) throws ConvirganceException
    {
        try(FileInputStream in = new FileInputStream(file))
        {
            this.config = new JSONParser(new InputStreamReader(in)).parseObject();
            this.name = this.config.getString("name");
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
            throw new ConvirganceException("Table already exists in " + directory);
        }
        
        directory.mkdirs();
        
        this.directory = directory;
        this.config = new JSONObject();
        this.config.put("columns", new JSONArray());
        
        saveConfig();
    }
    
    public void load(File directory)
    {
        loadConfig(new File(directory, "config.json"));
    }

    @Override
    public CloseableIterator<JSONObject> iterator()
    {
        DBMS dbms = new DBMS(source);
        String sql = "select \"_primarykey\"";
        Query query;
        
        for(JSONObject column : (JSONArray<JSONObject>)this.config.getJSONArray("columns"))
        {
            sql += ", \"col_" + column.getString("name") + "\" as \"" + column.getString("name") + "\"";
        }
        
        sql += " from \"" + this.name + "\"";
        
        query = new Query(sql);
        
        return (CloseableIterator<JSONObject>)dbms.query(query).iterator();
    }
    
    @Override
    public void insert(JSONObject record)
    {
        UUID uuid = UUID.randomUUID();
        String sql = "insert into \"" + name + "\" (\"_primarykey\"";
        Query query;
        
        for(String key : record.keySet())
        {
            sql += ", \"col_" + key + "\"";
        }
        
        sql += ") VALUES (:_primarykey";
        
        for(String key : record.keySet())
        {
            sql += ", :" + key;
        }
        
        sql += ")";
        
        query = new Query(sql);

        query.setBindings(record);
        query.setBinding("_primarykey", uuid.toString());
        
        new DBMS(source).update(new QueryOperation(query));
    }
    
    @Override
    public void addColumn(String name, String type)
    {
        String sql = "alter table \"" + this.name + "\" add column \"col_" + name + "\" " + type + (type.equalsIgnoreCase("varchar") ? "(32672)" : "");
        Query query = new Query(sql);
        JSONObject record = new JSONObject();
        
        for(JSONObject column : (JSONArray<JSONObject>)this.config.getJSONArray("columns"))
        {
            if(column.getString("name").equalsIgnoreCase(name))
            {
                throw new ConvirganceException("Column " + name + " already exists in table " + this.name);
            }
        }

        new DBMS(source).update(new QueryOperation(query));
        
        record.put("name", name);
        record.put("type", type);
        record.put("tableName", "col_" + name);
        
        this.config.getJSONArray("columns").add(record);
        
        saveConfig();
    }
    
    public String[] getColumns()
    {
        String[] columns = new String[this.config.getJSONArray("columns").size()];
        int index = 0;
        
        for(JSONObject column : (JSONArray<JSONObject>)this.config.getJSONArray("columns"))
        {
            columns[index++] = column.getString("name");
        }
        
        return columns;
    }
    
    public String getType(String name)
    {
        for(JSONObject column : (JSONArray<JSONObject>)this.config.getJSONArray("columns"))
        {
            if(column.getString("name").equalsIgnoreCase(name)) return column.getString("type");
        }
        
        return null;
    }
}

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
import com.invirgance.convirgance.json.JSONObject;
import com.invirgance.convirgance.output.BSONOutput;
import com.invirgance.convirgance.output.OutputCursor;
import com.invirgance.convirgance.target.OutputStreamTarget;
import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;

/**
 *
 * @author jbanes
 */
public class DivirganceServer implements Runnable
{
    public static final int COMMAND_LIST = 0x01;
    
    public static final int SUB_COMMAND_DATABASES = 0x01;
    public static final int SUB_COMMAND_TABLES = 0x02;
    
    private static final int RESPONSE_ERROR = 0xFF;
    private static final int RESPONSE_BSON = 0x01;
    
    private static Divirgance divirgance;
    private Socket socket;

    public DivirganceServer(Socket socket)
    {
        this.socket = socket;
    }
    
    public static void main(String[] args) throws Exception
    {
        ServerSocket server = new ServerSocket(2999);
        Socket socket;
        
        Thread thread;
        
        divirgance = new Divirgance(new File(args.length > 0 ? args[0] : "."));
        
        while(true)
        {
            socket = server.accept();
            thread = new Thread(new DivirganceServer(socket));
            
            thread.start();
        }
    }
    
    private void list(InputStream in, OutputStream out) throws IOException
    {
        BSONOutput output = new BSONOutput();
        Database database;
        
        DataInputStream data;
        JSONObject record;
        
        int subcommand = in.read();
        
        switch(subcommand)
        {
            case SUB_COMMAND_DATABASES:
                out.write(RESPONSE_BSON);
                output.write(new OutputStreamTarget(new NoCloseOutputStream(out)), divirgance.getDatabaseConfigs());
                break;
                
            case SUB_COMMAND_TABLES:
                data = new DataInputStream(in);
                database = divirgance.getDatabase(data.readUTF());
                
                out.write(RESPONSE_BSON);
                
                try(OutputCursor cursor = output.write(new OutputStreamTarget(new NoCloseOutputStream(out))))
                {
                    for(Table table : database.getTables())
                    {
                        record = new JSONObject();
                        
                        record.put("name", table.getName());
                        cursor.write(record);
                    }
                }
                catch(Exception e)
                {
                    throw new ConvirganceException(e);
                }
                
                break;
                
            default:
                throw new ConvirganceException("Unknown list sub-command 0x" + Integer.toHexString(0xFF & subcommand));
        }
    }
    
    public void serve(InputStream in, OutputStream out) throws IOException
    {
        int command;
        
        while(true)
        {
            command = in.read();
            
            if(command <= 0) return;
            
            switch(command)
            {
                case COMMAND_LIST:
                    list(in, out);
                    break;
                    
                default:
                    throw new ConvirganceException("Unknown command 0x" + Integer.toHexString(0xFF & command));
            }
        }
    }

    @Override
    public void run()
    {
        InputStream in;
        OutputStream out;
        
        try
        {
            in = this.socket.getInputStream();
            out = this.socket.getOutputStream();
            
            out.write("DIVIRGANCE".getBytes("UTF-8"));
            out.write(0); // Major version
            out.write(1); // Minor version
            
            serve(in, out);
        }
        catch(Exception e)
        {
            throw new ConvirganceException(e);
        }
        
    }
    
    private class NoCloseOutputStream extends OutputStream
    {
        private OutputStream out;

        public NoCloseOutputStream(OutputStream out)
        {
            this.out = out;
        }
        
        @Override
        public void write(int b) throws IOException
        {
            out.write(b);
        }

        @Override
        public void write(byte[] b) throws IOException
        {
            out.write(b);
        }

        @Override
        public void write(byte[] b, int off, int len) throws IOException
        {
            out.write(b, off, len);
        }
        
        @Override
        public void close() throws IOException
        {
            // Don't close
        }
    }
}

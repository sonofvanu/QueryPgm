/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package QueryClassess;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;

/**
 *
 * @author elixir
 */
public class FileHandler  {
    static String p,c1,c2;
       static String alldata[],str,finaldata[],withoutheader[],conditions[],hdr,headerdata[];
       static Map<String,Integer> header=new HashMap<String, Integer>();
       static Map<Integer,String> rd=new TreeMap<>();
   
    
    public void filehandl(String p,String c1,String c2)
    {
        FileHandler.p=p;
        FileHandler.c1=c1;
        FileHandler.c2=c2;
        System.out.println("the obtained path is"+p);
         try{
           getdatabyrow();
        }
        catch(Exception e)
        {
         System.out.println(e);   
        }
         headerfilesep();
         obtainedheaders();
         rowdata();
         rowdatas();
         columndata();
    }
    
        

   
   
   
   
    public static void getdatabyrow()throws IOException
    {
          FileReader f=new FileReader("g:\\"+p);
            BufferedReader br = new BufferedReader(f);
            List<String> list = new ArrayList<String>();
            while((str = br.readLine()) != null)
            {
                list.add(str);
            }
          String[] stringArr = list.toArray(new String[0]);

          finaldata=Arrays.copyOf(stringArr, stringArr.length-1);
            System.out.println("no. of elements"+finaldata.length);
            for(String p:finaldata)
                {
                System.out.println(p);
                }
       
    }
    
    
    public static Map<String,Integer> headerfilesep()
    {
        
            hdr=finaldata[0];
        headerdata=hdr.split(",");
        
        for(int i=0;i<headerdata.length;i++)
        {
            header.put(headerdata[i], i);
        }
        return header;
    }
    
    public static void obtainedheaders()
    {
        System.out.println(header);
    }
    
     public static Map<Integer,String> rowdata()
    {
        withoutheader=Arrays.copyOfRange(finaldata, 1, finaldata.length);
        for(int i=0;i<withoutheader.length;i++)
        {
            rd.put(i+1,withoutheader[i]);
        }
        return rd;
    }
   
     
     public static void rowdatas()
     {
     
         System.out.println(rd);
         
     }
     
     public static void columndata()
     {
         int position;
         conditions=c1.split(",");
         System.out.println(conditions.length);
        
         
         Set entrySet = header.entrySet();
 
    // Obtaining an iterator for the entry set
    Iterator it = entrySet.iterator();
 
    // Iterate through HashMap entries(Key-Value pairs)
    
    while(it.hasNext())
    {
    for(int i=0;i<conditions.length;i++)
    {
       Map.Entry me = (Map.Entry)it.next();
      
      String value=(String) me.getKey();
    if(value.contains(conditions[i]))
    {
        position=(int) me.getValue();
        
        for(int s=0;s<finaldata.length;s++)
        {
            String[] temp=finaldata[s].split(",");
            System.out.println(temp[position]);
            
        }
        
    }
    
   }

        }
     } 
}




     
    



package QueryOpr;


/** this class is used in order to perform string manipulation with the provided 
string**/
public class StringParser {
    String fragquery[],classpath,firstcond,secondcond;
    boolean astrexist,whereexist,sortbyexist,groupbyexist;
    
    public void strarray()
    {
         StringInput s2=new StringInput();
               String query=s2.sendquery();
               fragquery=query.split(" ");

    }
    
   
public void getscvparams()
{
    for(int i=0;i<fragquery.length;i++)
    {
        if(fragquery[i].contains(".csv"))
        {
            classpath=fragquery[i];
        }  
        if(fragquery[i].contains("where"))
        {
            secondcond=fragquery[i+1];
        }
    }
    firstcond=fragquery[1];  
}
   
    
    public void passparams()
    {
        
        CustObj c=new CustObj(classpath,firstcond,secondcond); 
        c.disp();
        
    }
    
    public void booleancheck()
    {
         for(int i=0;i<fragquery.length;i++)
    {
        if(fragquery[i].contains("*"))
        {
            astrexist=true;
        }  
        if(fragquery[i].contains("where"))
        {
            whereexist=true;
        }
        if(fragquery[i].contains("sortby"))
        {
            sortbyexist=true;
        }
        if(fragquery[i].contains("groupby"))
        {
            groupbyexist=true;
        }
    }
    }
    
    
   
    
}


package QueryClassess;


import java.util.Arrays;


/** this class is used in order to perform string manipulation with the provided 
string**/
public class StringParser {
    String fragquery[],classpath,firstcond,secondcond,query;
    boolean astrexist,whereexist,sortexist,groupexist,selectexist,fieldexist,byexists,orderexists,orderbyexists,sortbyexists,groupbyexists,sumexists,countexists;
    int queryarrlength,afterorder,aftersort,aftergroup,querylength,csvfound;
    
    CustomObject c=new CustomObject();
    
    public void strarray()
    {
         StringInput s2=new StringInput();
               query=s2.sendquery();
               fragquery=query.split(" ");
               querylength=query.length();
               queryarrlength=fragquery.length;
               

    }
    
   
public void getcsvparams()
{
    for(int i=0;i<queryarrlength;i++)
    {
        if(fragquery[i].contains(".csv"))
        {
            classpath=fragquery[i];            
        }
        
    }   
    
}
   
    
    public void passparams()
    {
       // c.custobj(classpath,firstcond,secondcond);
        c.parametersetter(query);
        c.disp();  
    }
    
    public void booleancheck()
    {
         for(int i=0;i<queryarrlength;i++)
    {
        if(fragquery[0].contains("select"))
        {
            selectexist=true;
        }
        if(fragquery[1].contains("*"))
        {
            astrexist=true;
        }  
        if(fragquery[i].contains("where"))
        {
            whereexist=true;
        }
            orderbyexists=query.contains("order by");
            sortbyexists=query.contains("sort by");
            groupbyexists=query.contains("group by");
        
        
        if(fragquery[i].contains("sum"))
        {
            sumexists=true;
        }
        if(fragquery[i].contains("count"))
        {
            countexists=true;
        }

    }
    }
    

    
    public boolean queryvalidator(String query)
    {
        if(query.contains("select")&& query.contains("from")||query.contains("*")||query.contains("where")||query.contains("group by")||query.contains("sort by")||query.contains("order by"))
        {
            return true;
        }    
        else{
        return false;    
        }
        
    }
   
    public void Queryexecutor(String query)
    {
        if(queryvalidator(query))
        {
         CustomObject custobj=new CustomObject();
         custobj.parametersetter(query);
        }
        else
        {
        System.out.println("Improper query format");    
        }
    }
    
}

package QueryClassess;


import java.util.*;


//This class is used to get the input query from the user//
    

public class StringInput {

    static StringParser s1=new StringParser();
    
    public static void main(String[] args) 
    {
        
        s1.strarray();
        s1.getcsvparams();
        s1.booleancheck();
        s1.passparams();
        
        
        
        
        
        
   
    }
    
    public String sendquery()
        {
        String inpquery;
        Scanner s=new Scanner(System.in);
        System.out.println("Kindly insert the query: ");
        inpquery=s.nextLine();
        s1.Queryexecutor(inpquery);
        return inpquery;
        }
}

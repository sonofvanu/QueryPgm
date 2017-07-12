/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package QueryClassess;

/**
 *
 * @author elixir
 */
public class CustomObject {
    
    String orderbycol,sortbycol,groupbycol,selectcolumn;
    boolean hasAllColumn,hasorderbyfield,hasgroupbyfiled,haswhere;
    public ParserAndController parseandcontrol=new ParserAndController();
    OtherConditions othercond=new OtherConditions();
    public void disp()
    {
        
    }
    
    public void parametersetter(String userquery)
    {
        String firstcondition=null,secondcondition=null;
        if(userquery.contains("order by"))
        {
            firstcondition=userquery.split("order by")[0].trim();
            orderbycol=userquery.split("order by")[1].trim();
            firstcondition=firstcondition.split("from")[0].trim();
            selectcolumn=secondcondition.split("select")[1].trim();
            this.processingoffields(selectcolumn);
            hasorderbyfield=true;
        }
   
        if(userquery.contains("group by"))
        {
            firstcondition=userquery.split("group by")[0].trim();
            orderbycol=userquery.split("group by")[1].trim();
            if(firstcondition.contains("where"))
            {
                secondcondition=firstcondition.split("where")[1].trim();
				String relationalqry=secondcondition.split("and|or")[0].trim();
				this.restofexpressionprocessing(relationalqry);
				firstcondition=firstcondition.split("where")[0].trim();	
            }
            
			firstcondition=firstcondition.split("from")[0].trim();
			secondcondition=firstcondition.split("select")[1].trim();
			this.processingoffields(groupbycol);
			hasgroupbyfiled=true;
        }
        
        else if(userquery.contains("where"))
		{
			firstcondition=userquery.split("where")[0];
			secondcondition=userquery.split("where")[1];
			secondcondition=secondcondition.trim();
			String relationalqry=secondcondition.split("and|or")[0].trim();
			System.out.println(relationalqry);
			this.restofexpressionprocessing(relationalqry);
			selectcolumn=firstcondition.split("select")[1].trim();
			this.processingoffields(selectcolumn);
			haswhere=true;
		}
		
        
        else
		{
			firstcondition=userquery.split("from")[0].trim();
			selectcolumn=firstcondition.split("select")[1].trim();
			this.processingoffields(selectcolumn);
		}
		
    }
    
    
    public void processingoffields(String onecolumn)
    {
        if(onecolumn.trim().contains("*") && onecolumn.length()==1)
		{
			hasAllColumn=true;
		}
		if(onecolumn.trim().contains(","))
		{
			String columnlist[]=onecolumn.split(",");
			
			int i=0;
			for(String column:columnlist)
			{
				
                                parseandcontrol.colnames.put(column, i);
				i++;
			}
			
		}
    }
    
    
    
    private void restofexpressionprocessing(String relationqry)
	{
		String oper[]={">","<",">=","<=","=","!="};
		
		for(String operator:oper)
		{
			if(relationqry.contains(operator))
			{
			othercond.setColumn(relationqry.split(operator)[0].trim());
                        othercond.setValue(relationqry.split(operator)[1].trim());
			othercond.setOperator(operator);
			break;
			}
		}
	}
}
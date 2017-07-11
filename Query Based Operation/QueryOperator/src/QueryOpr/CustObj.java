/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package QueryOpr;

/**
 *
 * @author elixir
 */
public class CustObj {
    public String cond1;
    String cond2;

    public String path;
    

    public CustObj(String path, String cond1, String cond2) {
        this.path = path;
        this.cond1 = cond1;
        this.cond2 = cond2;
    }

    public String getPath() {
        return path;
    }

    public void setPath(String path) {
        this.path = path;
    }

    public String getCond1() {
        return cond1;
    }

    public void setCond1(String cond1) {
        this.cond1 = cond1;
    }
    
    
    public void disp()
    {
        
        System.out.println(path+cond1+cond2);
        FileHandl f=new FileHandl(path,cond1,cond2);
       
    }
}
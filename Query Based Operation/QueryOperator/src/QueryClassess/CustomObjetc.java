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
public class CustomObjetc {
    public String cond1,cond2,path,whereconditions[],orderbyconditions[],sortbyconditions[],groupbyconditions[];

    public String[] getGroupbyconditions() {
        return groupbyconditions;
    }

    public void setGroupbyconditions(String[] groupbyconditions) {
        this.groupbyconditions = groupbyconditions;
        
    }

    public String[] getOrderbyconditions() {
        return orderbyconditions;
    }

    public void setOrderbyconditions(String[] orderbyconditions) {
        this.orderbyconditions = orderbyconditions;
    }

    public String[] getSortbyconditions() {
        return sortbyconditions;
    }

    public void setSortbyconditions(String[] sortbyconditions) {
        this.sortbyconditions = sortbyconditions;
    }

    public String[] getWhereconditions() {
        return whereconditions;
    }

    public void setWhereconditions(String[] whereconditions) {
        this.whereconditions = whereconditions;
    }

    public String getCond2() {
        return cond2;
    }

    public void setCond2(String cond2) {
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
    
     public void custobj(String path, String cond1, String cond2) {
        this.setPath(path);
        this.setCond1(cond1);
        this.setCond2(cond2);
    }

    public void disp()
    {
        
        System.out.println(this.getPath()+this.getCond1()+this.getCond2());
        
        
       
    }
}
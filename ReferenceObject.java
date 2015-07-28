import java.util.*;

public class ReferenceObject {
    private ArrayList<String> gene;
    private ArrayList<String> chr;
    private ArrayList<Integer> start;
    private ArrayList<Integer> end;
    
    public ReferenceObject(){
	set(new ArrayList<String>(),new ArrayList<String>(),new ArrayList<Integer>(),new ArrayList<Integer>());
    }
    
    public ReferenceObject(ArrayList<String> gene, ArrayList<String> chr, ArrayList<Integer> start, ArrayList<Integer> end){
        set(gene,chr,start,end);
    }

    public void set(ArrayList<String> gene, ArrayList<String> chr, ArrayList<Integer> start, ArrayList<Integer> end){
	this.gene=gene;
	this.chr=chr;
	this.start=start;
	this.end=end;
    }
    
    public void addContent(String geneValue, String chrValue, Integer startValue, Integer endValue){
	gene.add(geneValue);
	chr.add(chrValue);
	start.add(startValue);
	end.add(endValue);
    }

    public ArrayList<String> getGeneArrayList(){
	return gene;
    }

    public ArrayList<String> getChrArrayList(){
	return chr;
    }

    public ArrayList<Integer> getStartArrayList(){
	return start;
    }
    
    public ArrayList<Integer> getEndArrayList(){
	return end;
    }
}

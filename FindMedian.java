import java.io.*;
import java.util.*;

public class FindMedian {
    
    public static int countLines(String filename){
        int TotalNumberLines = 0;
        String line = null;
        
        try {
            // FileReader reads text files in the default encoding.
            FileReader fileReader = new FileReader(filename);
            // Always wrap FileReader in BufferedReader.
            BufferedReader bufferedReader = new BufferedReader(fileReader);
            while((line = bufferedReader.readLine()) != null) {
                TotalNumberLines++;
            }
            // Always close files.
            bufferedReader.close();
        }
        catch(FileNotFoundException ex) {
            System.out.println("Unable to open file '" + filename + "'");
        }
        catch(IOException ex) {
            System.out.println("Error reading file '" + filename + "'");
            // Or we could just do this:
            // ex.printStackTrace();
        }
        return TotalNumberLines;
    }
    
    public static ArrayList<String> getExonContigID(String filename, int cnt){
        ArrayList<String> exonContigList = new ArrayList<String>();
        String line = null;
        try {
            // FileReader reads text files in the default encoding.
            FileReader fileReader = new FileReader(filename);
            // Always wrap FileReader in BufferedReader.
            BufferedReader bufferedReader = new BufferedReader(fileReader);
            while((line = bufferedReader.readLine()) != null) {
                String lineHolder = line.toString();
                String delimiter = "\\t";
                String [] strArray = lineHolder.split(delimiter);
                if(strArray.length < 4){
                    // Skip. This is not a valid entry
                }else{
                    exonContigList.add(strArray[0]);
                }
            }
            // Always close files.
            bufferedReader.close();
        }
        catch(FileNotFoundException ex) {
            System.out.println("Unable to open file '" + filename + "'");
        }
        catch(IOException ex) {
            System.out.println("Error reading file '" + filename + "'");
            // Or we could just do this:
            // ex.printStackTrace();
        }
        return exonContigList;
    }
    
    public static ArrayList<Integer> getCoverage(String filename, int cnt){
        ArrayList<Integer> coverageList = new ArrayList<Integer>();
        String line = null;
        try {
            // FileReader reads text files in the default encoding.
            FileReader fileReader = new FileReader(filename);
            // Always wrap FileReader in BufferedReader.
            BufferedReader bufferedReader = new BufferedReader(fileReader);
            while((line = bufferedReader.readLine()) != null) {
                String lineHolder = line.toString();
                String delimiter = "\\t";
                String [] strArray = lineHolder.split(delimiter);
                if(strArray.length < 4){
                    // Skip. This is not a valid entry
                }else{
                    coverageList.add(Integer.parseInt(strArray[3]));
                }
                
            }
            // Always close files.
            bufferedReader.close();
        }
        catch(FileNotFoundException ex) {
            System.out.println("Unable to open file '" + filename + "'");
        }
        catch(IOException ex) {
            System.out.println("Error reading file '" + filename + "'");
            // Or we could just do this:
            // ex.printStackTrace();
        }
        return coverageList;
    }
    
    
    public static Set<String> getUniqueExonContigID(ArrayList<String> exonContigList){
        Set<String> uniqueContig = new HashSet<String>(exonContigList);
        return uniqueContig;
    }
    
    
    private static void getMedian(Set<String> uniqueContig, ArrayList<String>  exonContig, ArrayList<Integer> exonCoverage, String output){
        
        if (exonContig.size() != exonCoverage.size()){
            System.err.println("Contig list and Coverage List must have the same length");
            System.exit(-1);
        }
        
        try {
            // Assume default encoding.
            FileWriter fileWriter = new FileWriter(output);
            
            // Always wrap FileWriter in BufferedWriter.
            BufferedWriter bufferedWriter = new BufferedWriter(fileWriter);
            
            for (String ref : uniqueContig) {
                int cnt = 0;
                ArrayList<Integer> coverageList = new ArrayList<Integer>();
                for (String exon : exonContig) {
                    if(ref.equals(exon)){
                        coverageList.add(exonCoverage.get(cnt));
                    }
                    cnt++;
                }
                Collections.sort(coverageList);
            
                int middle = coverageList.size()/2;
                int medianValue = 0; //declare variable
                if (coverageList.size()%2 == 1){
                    medianValue = coverageList.get(middle);
                }else{
                    // For two middle that add up to more than int max cases
                    medianValue = coverageList.get(middle-1) + Math.abs(coverageList.get(middle-1) - coverageList.get(middle))/2;
                }
            
                StringBuilder outputSB = new StringBuilder();
                outputSB.append(ref);
                outputSB.append(",");
                outputSB.append(medianValue);
                outputSB.append("\n");
                
                bufferedWriter.write(outputSB.toString());
            }
            // Always close files.
            bufferedWriter.close();
        }
        catch(IOException ex) {
                System.out.println("Error writing to file '" + output + "'");
        }
    }
    
    private static void printStringArrayList(ArrayList<String> strings){
        for (String string : strings) {
            System.out.println(string);
        }
    }
    
    
    private static void printIntArrayList(ArrayList<Integer> ints){
        for (Integer value : ints) {
            System.out.println(value);
        }
    }

    public static void main(String [] args) {
		if (args.length != 2){
			System.err.println("Usage FindMedian <input file> <output file>");
			System.exit(-1);
		}
        
        
        // The name of the file to open.
        String fileName = args[0];
		// Name of output open.
        String outputFileName = args[1];
		
        int totalLine = countLines(fileName);
        
        // Get exon_contigs
        ArrayList<String>  exonContig = getExonContigID(fileName, totalLine);
        
        // Get coverages corresponding to contigs above
        ArrayList<Integer> exonCoverage = getCoverage(fileName, totalLine);
        
        // Get unique contigs
        Set<String> uniqueContig = getUniqueExonContigID(exonContig);
        
        // Find median coverage
        getMedian(uniqueContig, exonContig, exonCoverage, outputFileName);
        
	}
}

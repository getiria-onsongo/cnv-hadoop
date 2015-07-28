import java.io.*;
import java.util.*;

public class GetRandomReferences {
	
	public static ArrayList<String> getGeneSymbols(String filename){
		// This will reference one line at a time
        String line = null;		
		// This will contain the gene symbols
		ArrayList<String> geneSymbols = new ArrayList<String>();
        try {
            // FileReader reads text files in the default encoding.
            FileReader fileReader = new FileReader(filename);
            // Always wrap FileReader in BufferedReader.
            BufferedReader bufferedReader = new BufferedReader(fileReader);
            while((line = bufferedReader.readLine()) != null) {
				String lineHolder = line.toString();
				String delimiter = "\\t";
				String [] strArray = lineHolder.split(delimiter);
                geneSymbols.add(strArray[0]);
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
		return geneSymbols;
	}

	public static ArrayList<String> randomSubset(ArrayList<String> geneArrayList, int cnt){
		if (cnt > geneArrayList.size()){
			System.err.println("Size of the subset (" + cnt + ") must be less than " + geneArrayList.size());
			System.exit(-1);
		}
		// shuffle list of gene symbols
		Collections.shuffle(geneArrayList);
		// cnt number of genes to target
		ArrayList<String> targetList = new ArrayList<String>();
		for (int i = 0; i < cnt; i++) {
			targetList.add(geneArrayList.get(i)); 
		}
		return targetList;
	}
	
	
	public static ReferenceObject getReferenceObject(ArrayList<String> geneArrayList, String filename){
		ReferenceObject result = new ReferenceObject();
		
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
				if(geneArrayList.contains(strArray[0])){
					result.addContent(strArray[0],strArray[2],Integer.valueOf(strArray[3]),Integer.valueOf(strArray[4]));
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
		return result;
	}
	
	public static void createReferencePileup(ReferenceObject refObj, String output){	
        try {
            // Assume default encoding.
            FileWriter fileWriter = new FileWriter(output);
			
            // Always wrap FileWriter in BufferedWriter.
            BufferedWriter bufferedWriter = new BufferedWriter(fileWriter);
			
			int numGenes = refObj.getGeneArrayList().size();
			StringBuilder outputSB = new StringBuilder();
			
			for(int i=0; i<numGenes; i++){
				String gene = refObj.getGeneArrayList().get(i);
				String chr = refObj.getChrArrayList().get(i);
				int start = refObj.getStartArrayList().get(i).intValue();
				int end = refObj.getEndArrayList().get(i).intValue();
			
				for(int j=start; j<(end+1); j++){
					// Create exon contig
					outputSB.append(chr); 
					outputSB.append(":");
					outputSB.append(start);
					outputSB.append("-");
					outputSB.append(end);
					
					outputSB.append("\t");
					outputSB.append(gene);
					
					outputSB.append("\t");
					outputSB.append(chr);
					
					outputSB.append("\t");
					outputSB.append(j);
					outputSB.append("\n");
				}
			}
            bufferedWriter.write(outputSB.toString());
			
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
	
	private static void printReferenceObject(ReferenceObject refObj){
		int cnt = refObj.getGeneArrayList().size();
		
		for(int i=0; i<cnt; i++){			
			System.out.print(refObj.getGeneArrayList().get(i));
			System.out.print("\t");
			System.out.print(refObj.getChrArrayList().get(i));
			System.out.print("\t");
			System.out.print(refObj.getStartArrayList().get(i));
			System.out.print("\t");
			System.out.print(refObj.getEndArrayList().get(i));
			System.out.print("\n");
		}
		
	}
	
    public static void main(String [] args) {
		if (args.length != 3){
			System.err.println("Usage Get3randomReferences <input file> <output file> <number of references>");
			System.exit(-1);
		}
        // The name of the file to open.
        String fileName = args[0];
		// Name of output open.
        String outputFileName = args[1];
		// Number of references to select
		int numberReferences = Integer.parseInt(args[2]);

		// Get just gene symbols from input file
		ArrayList<String> geneSymbolArray = getGeneSymbols(fileName);
		// Randomly select X genes where X = numberReferences
		ArrayList<String> referencesArray = randomSubset(geneSymbolArray, numberReferences);
		// Create exon reference object that contains <gene_symbol, chr, start, end>
		ReferenceObject randomSubsetObjects = getReferenceObject(referencesArray,fileName);
		
		// Generate pileup
		createReferencePileup(randomSubsetObjects,outputFileName);
		
	}
}

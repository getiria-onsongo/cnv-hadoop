/**
* reducerKey=<chrID>;<analysis_id>
*   where chrID=1, 2, ..., 22, 23 (23 includes chrM, chrX, chrY)
*/
public static void mergeAllChromosomesAndPartition(String reducerKey)
    throws Exception {
    // split the line: each line has two fields (fields are separated by ";")
    String[] tokens = reducerKey.split(";");
    String chrID = tokens[0];
    String analysisID = tokens[1];
    Map<String, String> templateMap = new HashMap<String, String>();
    templateMap.put("chr_id", chrID);
    templateMap.put("analysis_id", analysisID);
    mergeAllChromosomesBamFiles(templateMap);
    partitionSingleChromosomeBam(templateMap);
}

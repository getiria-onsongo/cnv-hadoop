/**
* This method will merge the following files and create a single chr<i>.bam file
* where i is in {1, 2, ..., 23}:
*
*    HDFS: /.../chr<i>/chr<i>.bam.0000
*    HDFS: /.../chr<i>/chr<i>.bam.0001
*    ...
    HDFS: /.../chr<i>/chr<i>.bam.0437
*
* Then merge all these (.0000, .0001, ..., .0437) files and save the result in
* /data/tmp/<analysis_id>/chr<i>/chr<i>.bam
*
* Once chr<i>.bam is created, then we partition it into small .bam files,
* which will be fed to RecalibrationDriver (step 2 of DNA sequencing)
*
**/

public static void mergeAllChromosomesBamFiles(Map<String, String> templateMap)
throws Exception {
    TemplateEngine.initTemplatEngine();
    String templateFileName = <freemarker-template-file-as-a-bash-script>;
    // create the actual script from a template file
    String chrID = templateMap.get("chr_id");
    String analysisID = templateMap.get("analysis_id");
    String scriptFileName = createScriptFileName(chrID, analysisID);
    String logFileName = createLogFileName(chrID, analysisID);

    File scriptFile = TemplateEngine.createDynamicContentAsFile(templateFileName,templateMap,scriptFileName);
    if (scriptFile != null) {
	ShellScriptUtil.callProcess(scriptFileName, logFileName);
    }
}

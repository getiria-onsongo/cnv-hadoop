import java.io.File;
import java.io.Writer;
import java.io.FileWriter;
import java.io.BufferedWriter;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import freemarker.template.Template;
import freemarker.template.Configuration;
import freemarker.template.DefaultObjectWrapper;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.logging.LogManager;

/**
  * This class uses FreeMarker (http://freemarker.sourceforge.net/).
  * FreeMarker is a template engine, a generic tool to generate text
  * output (anything from shell scripts to autogenerated source code)
  * based on templates. It's a Java package, a class library for Java
  * programmers. It's not an application for end users in itself, but
  * something that programmers can embed into their products.
  *
  * @author Mahmoud Parsian (Modified by Getiria Onsongo)
  *
  **/
public class TemplateEngine {
    // you usually do it only once in the whole application life cycle
    
    private static Configuration TEMPLATE_CONFIGURATION = null;
    private static AtomicBoolean initialized = new AtomicBoolean(false);
    // the following template directories will be loaded from configuration file
    private static String TEMPLATE_DIRECTORY = "/home/dnaseq/template";
    
    private static Logger theLogger = Logger.getLogger(TemplateEngine.class.getName());
    
    public static void init() throws Exception {
        if (initialized.get()) {
            // it is already initialized and returning...
            return;
        }
        initConfiguration();
        initialized.compareAndSet(false, true);
    }

    static {
        if (!initialized.get()) {
            try {
                init();
            }
            catch(Exception e) {
                theLogger.severe("CHANGE TO ERROR MSG: TemplateEngine init failed at initialization.");
                e.printStackTrace();
            }
        }
    }

    // this suppports a single template directory
    private static void initConfiguration() throws Exception {
        TEMPLATE_CONFIGURATION = new Configuration();
        TEMPLATE_CONFIGURATION.setDirectoryForTemplateLoading(new File(TEMPLATE_DIRECTORY));
        TEMPLATE_CONFIGURATION.setObjectWrapper(new DefaultObjectWrapper());
        TEMPLATE_CONFIGURATION.setWhitespaceStripping(true);
        // if the following is set, then undefined keys will be set to ""
        TEMPLATE_CONFIGURATION.setClassicCompatible(true);
    }
    
/**
 * @param templateFile is a template filename such as script.sh.template
 * @param keyValuePairs is a set of (K,V) pairs
 * @param outputFileName is a generated filename from templateFile
 **/

    public static File createDynamicContentAsFile(String templateFile,Map<String,String> keyValuePairs,String outputFileName)
    throws Exception {
        if ((templateFile == null) || (templateFile.length() == 0)) {
            return null;
        }

        Writer writer = null;
        try {
            // create a template: example "cb_stage1.sh.template2"
            Template template = TEMPLATE_CONFIGURATION.getTemplate(templateFile);
            // merge data model with template
            File outputFile = new File(outputFileName);
            writer = new BufferedWriter(new FileWriter(outputFile));
            template.process(keyValuePairs, writer);
            writer.flush();
            return outputFile;
        }
        finally {
            if (writer != null) {
                writer.close();
            }
        }
    }
}

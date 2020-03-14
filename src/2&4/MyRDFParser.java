import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import org.semanticweb.yars.nx.Node;
import org.semanticweb.yars.nx.parser.NxParser;

/**
 *
 * @author biar
 */
public class MyRDFParser {

    private Map<Integer, String> PoS = new HashMap<Integer, String>();
    
    public HashMap<String, String> parseLine(String line) {
        PoS.put(0, "Subject"); PoS.put(1, "Predicate"); PoS.put(2, "Object"); PoS.put(3, "Context");
        
        NxParser nxp = new NxParser();
        Iterator<Node[]> parsedLine = nxp.parse(new ByteArrayInputStream(line.getBytes(StandardCharsets.UTF_8)));
        
	HashMap<String, String> result = new HashMap<String, String>();
        Node[] lineNodes = parsedLine.next();
        for (int i = 0; i < lineNodes.length; i++) 
               result.put(PoS.get(i),(lineNodes[i].toString().substring(0, 2).equals("_:")? "blankNode" : lineNodes[i].toString()));
	return result;
    }
}


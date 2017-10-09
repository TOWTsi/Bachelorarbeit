package eu.stratosphere.nephele.api;

import static org.junit.Assert.fail;

//import java.io.IOException;
//import java.net.InetSocketAddress;

/**Zum Aufbau:
 * Potentiell alle Projekte mit Ausnahme der Visualisierung sollen hier importiert
 * und genutzt werden. Die Visualisierung wird dann später die API benutzen.
 * Die API wird später in einer ordentlich benannten Klasse geschrieben. Hier können
 * aber alle Funktionen schonmal im Vorfeld getestet werden. Nur vorraussichtlich 
 * Fehlerfreie und getestete Funktionen sollen in die API aufgenommen werden.
 * 
 */
//import eu.stratosphere.nephele.client.AbstractJobResult;

import eu.stratosphere.nephele.client.JobClient;
import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.nephele.jobgraph.JobGraph;
import eu.stratosphere.nephele.util.StringUtils;


public class FirstTests {
	
	
	public void createAndLaunchJob(JobGraph jobGraph, Configuration conf) {
		JobClient jobClient = null;
		try {
			jobClient = new JobClient(jobGraph, conf);
			jobClient.submitJobAndWait();
		} catch (Exception e) {
			fail(StringUtils.stringifyException(e));
		} finally {
			if (jobClient != null) {
				jobClient.close();
			}
		}
	}

}

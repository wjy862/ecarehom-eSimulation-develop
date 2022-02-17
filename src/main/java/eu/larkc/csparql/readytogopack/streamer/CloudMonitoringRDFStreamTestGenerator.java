/*******************************************************************************
 * Copyright 2014 Davide Barbieri, Emanuele Della Valle, Marco Balduini
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * 
 * Acknowledgements:
 * 
 * This work was partially supported by the European project LarKC (FP7-215535) 
 * and by the European project MODAClouds (FP7-318484)
 ******************************************************************************/
package eu.larkc.csparql.readytogopack.streamer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.larkc.csparql.cep.api.RdfQuadruple;
import eu.larkc.csparql.cep.api.RdfStream;

public class CloudMonitoringRDFStreamTestGenerator extends RdfStream implements Runnable {

	/** The logger. */
	protected final Logger logger = LoggerFactory
			.getLogger(CloudMonitoringRDFStreamTestGenerator.class);	
	
	private String streamIri = "http://www.modaclouds.eu/ontologies/2013/2/monitoring#";

	public CloudMonitoringRDFStreamTestGenerator(final String iri) {
		super(iri);
	}
	

	@Override
	public void run() {
		try{

			Thread.sleep(700);

			RdfQuadruple q;
			long tempTS;
			
			Thread.sleep(1000);

			//First Observation
			tempTS = System.currentTimeMillis();
			q = new RdfQuadruple(streamIri + "dc1", streamIri + "observes", streamIri + "o1", tempTS);
			this.put(q);
			q = new RdfQuadruple(streamIri + "o1", streamIri + "isAbout", streamIri + "ms", tempTS);
			this.put(q);
			q = new RdfQuadruple(streamIri + "o1", streamIri + "hasValue", 10 + "^^http://www.w3.org/2001/XMLSchema#integer", tempTS);
			this.put(q);
			q = new RdfQuadruple(streamIri + "o1", streamIri + "hasMonitoredMetric", streamIri + "CPUUtilization", tempTS);
			this.put(q);
			System.out.println("First Observation: " + System.currentTimeMillis());

			Thread.sleep(1000);
			
			//Second Observation
			tempTS = System.currentTimeMillis();
			q = new RdfQuadruple(streamIri + "dc2", streamIri + "observes", streamIri + "o2", tempTS);
			this.put(q);
			q = new RdfQuadruple(streamIri + "o2", streamIri + "isAbout", streamIri + "ms", tempTS);
			this.put(q);
			q = new RdfQuadruple(streamIri + "o2", streamIri + "hasValue", 30 + "^^http://www.w3.org/2001/XMLSchema#integer", tempTS);
			this.put(q);
			q = new RdfQuadruple(streamIri + "o2", streamIri + "hasMonitoredMetric", streamIri + "CPUUtilization", tempTS);
			this.put(q);
			System.out.println("Second Observation: " + System.currentTimeMillis());

			Thread.sleep(4000);
			
			//Third Observation
			tempTS = System.currentTimeMillis();
			q = new RdfQuadruple(streamIri + "dc1", streamIri + "observes", streamIri + "o3", tempTS);
			this.put(q);
			q = new RdfQuadruple(streamIri + "o3", streamIri + "isAbout", streamIri + "ms", tempTS);
			this.put(q);
			q = new RdfQuadruple(streamIri + "o3", streamIri + "hasValue", 50 + "^^http://www.w3.org/2001/XMLSchema#integer", tempTS);
			this.put(q);
			q = new RdfQuadruple(streamIri + "o3", streamIri + "hasMonitoredMetric", streamIri + "CPUUtilization", tempTS);
			this.put(q);
			System.out.println("Third Observation: " + System.currentTimeMillis());

			Thread.sleep(6000);
			
			//Fourth Observation
			tempTS = System.currentTimeMillis();
			q = new RdfQuadruple(streamIri + "dc2", streamIri + "observes", streamIri + "o4", tempTS);
			this.put(q);
			q = new RdfQuadruple(streamIri + "o4", streamIri + "isAbout", streamIri + "ms", tempTS);
			this.put(q);
			q = new RdfQuadruple(streamIri + "o4", streamIri + "hasValue", 60 + "^^http://www.w3.org/2001/XMLSchema#integer", tempTS);
			this.put(q);
			q = new RdfQuadruple(streamIri + "o4", streamIri + "hasMonitoredMetric", streamIri + "CPUUtilization", tempTS);
			this.put(q);
			System.out.println("Fourth Observation: " + System.currentTimeMillis());

			Thread.sleep(5000);

			//Fifth Observation
			tempTS = System.currentTimeMillis();
			q = new RdfQuadruple(streamIri + "dc1", streamIri + "observes", streamIri + "o5", tempTS);
			this.put(q);
			q = new RdfQuadruple(streamIri + "o5", streamIri + "isAbout", streamIri + "ms", tempTS);
			this.put(q);
			q = new RdfQuadruple(streamIri + "o5", streamIri + "hasValue", 70 + "^^http://www.w3.org/2001/XMLSchema#integer", tempTS);
			this.put(q);
			q = new RdfQuadruple(streamIri + "o5", streamIri + "hasMonitoredMetric", streamIri + "CPUUtilization", tempTS);
			this.put(q);
			System.out.println("Fifth Observation: " + System.currentTimeMillis());
	
		} catch(Exception e){
			e.printStackTrace();
		}
	}
}

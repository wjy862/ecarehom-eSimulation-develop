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
package eu.larkc.csparql.eu.tsp.test.streamer;

import eu.larkc.csparql.cep.api.RdfQuadruple;
import eu.larkc.csparql.cep.api.RdfStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Random;
import java.util.UUID;
import java.util.stream.Stream;

import static eu.larkc.csparql.eu.tsp.test.Utils.*;

public class TvStreamer extends RdfStream implements Runnable  {

	private long sleepTime;
	private String baseUri;
	private static Logger logger = LoggerFactory.getLogger(TvStreamer.class);

	public TvStreamer(String iri, String baseUri, long sleepTime) {
		super(iri);
		this.sleepTime = sleepTime;
		this.baseUri = baseUri;
	}

	public void run() {
		Random random = new Random();
		int sensorIndex;
		int valeurSensor;
		int roomIndex;
		int observationIndex;
		//int numberOfPerson;

		while (true) {
			try {

				valeurSensor = random.nextInt(100);

				observationIndex = random.nextInt(Integer.MAX_VALUE);
				String observationTime = observationIndex + "time";

				RdfQuadruple q = new RdfQuadruple("_:" + observationIndex, "http://www.ontologydesignpatterns.org/ont/dul/DUL.owl#isObservableAt", observationTime, System.currentTimeMillis());
				this.put(q);
				q = new RdfQuadruple("_:" + observationIndex, "http://www.w3.org/ns/sosa#isObservedBy", "tv_sensor", System.currentTimeMillis());
				this.put(q);
				q = new RdfQuadruple("_:" + observationIndex, "http://www.w3.org/ns/sosa#hasSimpleResult", "\"" + valeurSensor + "\"^^http://www.w3.org/2001/XMLSchema#integer", System.currentTimeMillis());
				this.put(q);

				SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss+SSS");

				q = new RdfQuadruple(observationTime, "http://w3id.org/ecareathome/patterns/timeinterval.owl#hasUpperTimeStampValue", sdf.format(new Date()) + "^^http://www.w3.org/2001/XMLSchema#dateTime", System.currentTimeMillis());
				this.put(q);
				q = new RdfQuadruple(observationTime, "http://w3id.org/ecareathome/patterns/timeinterval.owl#hasLowerTimeStampValue", sdf.format(new Date()) + "^^http://www.w3.org/2001/XMLSchema#dateTime", System.currentTimeMillis());
				this.put(q);


				Thread.sleep(sleepTime);

			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

}

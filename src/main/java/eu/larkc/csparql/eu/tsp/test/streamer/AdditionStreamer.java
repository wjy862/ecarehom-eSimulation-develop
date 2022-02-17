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

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Random;

public class AdditionStreamer extends RdfStream implements Runnable  {

	private long sleepTime;
	private String baseUri;

	public AdditionStreamer(String iri, String baseUri, long sleepTime) {
		super(iri);
		this.sleepTime = sleepTime;
		this.baseUri = baseUri;
	}

	public void run() {


		while(true){
			try{

				Thread.sleep(sleepTime);
				
			} catch(Exception e){
				e.printStackTrace();
			}
		}

	}

}

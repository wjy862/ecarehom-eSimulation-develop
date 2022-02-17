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
package eu.larkc.csparql.sr4ld2014;

import org.apache.log4j.PropertyConfigurator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.larkc.csparql.common.utils.CsparqlUtils;
import eu.larkc.csparql.core.engine.ConsoleFormatter;
import eu.larkc.csparql.core.engine.CsparqlEngineImpl;
import eu.larkc.csparql.core.engine.CsparqlQueryResultProxy;
import eu.larkc.csparql.sr4ld2014.streamer.FacebookStreamer4RoomConnection;

public class SR4LD2014_Ex7 {

	private static Logger logger = LoggerFactory.getLogger(SR4LD2014_Ex7.class);
	
	/*
	 * Example 7: Access and usage of static knowledge example
	 */

	public static void main(String[] args) {

		try{
			
			//Configure log4j logger for the csparql engine
			PropertyConfigurator.configure("log4j_configuration/csparql_readyToGoPack_log4j.properties");
				
			//Create csparql engine instance
			CsparqlEngineImpl engine = new CsparqlEngineImpl();
			//Initialize the engine instance
			//The initialization creates the static engine (SPARQL) and the stream engine (CEP)
			engine.initialize();
			
			String queryBody = "REGISTER QUERY staticKnowledge AS "
					+ "PREFIX :<http://www.streamreasoning.org/ontologies/sr4ld2014-onto#> "
					+ "SELECT ?p1 ?r1 "
					+ "FROM STREAM <http://streamreasoning.org/streams/fb> [RANGE 1s STEP 1s] "
					+ "FROM <http://streamreasoning.org/roomConnection> "
					+ "WHERE { "
					+ "?p :posts [ :who ?p1 ; :where ?r ] . "
					+ "?r :isConnectedTo ?r1 . "
					+ "} ";
			
			FacebookStreamer4RoomConnection fb = new FacebookStreamer4RoomConnection("http://streamreasoning.org/streams/fb", "http://www.streamreasoning.org/ontologies/sr4ld2014-onto#", 1000L);
			
			//Register new streams in the engine
			engine.registerStream(fb);
			Thread fbThread = new Thread(fb);
			
			engine.putStaticNamedModel("http://streamreasoning.org/roomConnection", CsparqlUtils.serializeRDFFile("examples_files/roomConnection.rdf"));

			//Register new query in the engine
			CsparqlQueryResultProxy c = engine.registerQuery(queryBody, false);	
			
			//Attach a result consumer to the query result proxy to print the results on the console
			c.addObserver(new ConsoleFormatter());

			fbThread.start();
			
			String updateQuery = "PREFIX : <http://www.streamreasoning.org/ontologies/sr4ld2014-onto#> "
					+ "INSERT DATA "
					+ "{ GRAPH <http://streamreasoning.org/roomConnection> { :room  :isConnectedTo  :room2 } }";
			//Update the static knowledge
			engine.execUpdateQueryOverDatasource(updateQuery);

			updateQuery = "PREFIX : <http://www.streamreasoning.org/ontologies/sr4ld2014-onto#> "
					+ "DELETE DATA "
					+ "{ GRAPH <http://streamreasoning.org/roomConnection> { :room  :isConnectedTo  :room1 } }";
			//Update the static knowledge
			engine.execUpdateQueryOverDatasource(updateQuery);

		}catch (Exception e) {
			logger.error(e.getMessage(), e);
		}

	}

}

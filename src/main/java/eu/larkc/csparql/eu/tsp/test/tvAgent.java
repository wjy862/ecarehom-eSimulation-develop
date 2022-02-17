package eu.larkc.csparql.eu.tsp.test;

import eu.larkc.csparql.core.engine.ConsoleFormatter;
import eu.larkc.csparql.core.engine.CsparqlEngineImpl;
import eu.larkc.csparql.core.engine.CsparqlQueryResultProxy;
import eu.larkc.csparql.eu.tsp.test.streamer.TvStreamer;

import org.apache.log4j.PropertyConfigurator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class tvAgent {

    private static Logger logger = LoggerFactory.getLogger(tvAgent.class);

   /*
     * ECARE-HOME Reasoning example
     */

    //  startTraitement
    public static void main(String[] args) {
        try{

           //Configure log4j logger for the csparql engine
            PropertyConfigurator.configure("log4j_configuration/csparql_readyToGoPack_log4j.properties");

           //Create csparql engine instance
            final CsparqlEngineImpl engine = new CsparqlEngineImpl();
           //Initialize the engine instance
           //The initialization creates the static engine (SPARQL) and the stream engine (CEP)
            engine.initialize();

            String streamCleanTv = "REGISTER STREAM cleantv AS " +
                    "PREFIX :<http://ecareathome.org/stream#> " +
                    "PREFIX rdf:<http://www.w3.org/1999/02/22-rdf-syntax-ns#> " +
                    "PREFIX xsd:<http://www.w3.org/2001/XMLSchema#> " +
                    "PREFIX dul:<http://www.ontologydesignpatterns.org/ont/dul/DUL.owl#> " +
                    "PREFIX time:<http://w3id.org/ecareathome/patterns/timeinterval.owl#> " +
                    "PREFIX event:<http://w3id.org/ecareathome/patterns/event.owl#> " +
                    "PREFIX sosa:<http://www.w3.org/ns/sosa#> " +
                    "CONSTRUCT " +
                    "{ " +
                    "_:c0 rdf:type event:ComplexEvent . " +
                    "_:c0 dul:isObservableAt _:c1 . " +
                    "_:c0 sosa:isObservedBy ?sensor . " +
                    "_:c0 sosa:hasSimpleResult ?value . " +
                    "_:c1 rdf:type dul:TimeInterval . " +
                    "_:c1 time:hasUpperTimeStampValue ?maxTime . " +
                    "_:c1 time:hasLowerTimeStampValue ?minTime . " +
                    "} " +
                    "FROM STREAM <http://ecareathome.org/stream#tv> [ RANGE 10s STEP 10s] " +
                    "WHERE " +
                    "{ { SELECT ?sensor ( AVG (?value ) AS ?avg ) ( MAX (?upper ) AS ?maxTime ) " +
                    " ( MIN (?lower ) AS ?minTime ) " +
                    "WHERE { " +
                    "_:b0 sosa:isObservedBy ?sensor ; " +
                    "sosa:hasSimpleResult ?value ; " +
                    "dul:isObservableAt _:b1 . " +
                    "_:b1 time:hasUpperTimeStampValue ?upper ; " +
                    "time:hasLowerTimeStampValue ?lower . " +
                    "} " +
                    "GROUP BY ?sensor " +
                    "} " +
                    "BIND (if (( ?avg <20 ) , false , true ) AS ?value ) " +
                    "BIND ( now() AS ?time ) " +
                    "FILTER bound (?sensor ) " +
                    "}";

            TvStreamer streamTV = new TvStreamer("http://ecareathome.org/stream#tv", "", 1000L);

            //Register new streams in the engine
            engine.registerStream(streamTV);

            Thread TvThread = new Thread(streamTV);

            CsparqlQueryResultProxy streamQ2 = engine.registerQuery(streamCleanTv, false);

            streamQ2.addObserver(new ConsoleFormatter());

            //Start streaming data
            TvThread.start();


        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
    }

}

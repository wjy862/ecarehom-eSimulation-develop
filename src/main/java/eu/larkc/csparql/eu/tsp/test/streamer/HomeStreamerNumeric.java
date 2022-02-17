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
import java.util.UUID;
import java.util.stream.Stream;

import static eu.larkc.csparql.eu.tsp.test.Utils.*;

public class HomeStreamerNumeric extends RdfStream implements Runnable  {

    private long sleepTime;
    private String baseUri;
    private static Logger logger = LoggerFactory.getLogger(TvStreamer.class);

    public HomeStreamerNumeric(String iri, String baseUri, long sleepTime) {
        super(iri);
        this.sleepTime = sleepTime;
        this.baseUri = baseUri;
    }

    public void run() {


        String fileName = "D:/CSPARQL-ReadyToGoPack/examples_files/ann.txt";

        //read file into stream, try-with-resources
        try (Stream<String> stream = Files.lines(Paths.get(fileName))) {

            stream.forEach(line -> {
                try {

                    String typeCapteur ;
                    String[] split = line.split("\t");
                    String acrTypeCapteur;
                    String room = split[1].substring(split[1].length()-3, split[1].length());

                    switch (split[1].substring(0,1)) {
                        case "D":
                            typeCapteur = "magnetic_door_sensors";
                            acrTypeCapteur = split[1].substring(0,1);
                            break;
                        case "L":
                            typeCapteur = "light_sensors";
                            acrTypeCapteur = split[1].substring(0,1);
                            switch (split[1].substring(0,2)) {
                                case "LS":
                                    typeCapteur = "light_sensors";
                                    acrTypeCapteur = split[1].substring(0,2);
                                    break;
                                case "LA":
                                    typeCapteur = "light_switches";
                                    acrTypeCapteur = split[1].substring(0,2);
                                    break;
                            }
                            break;
                        case "M":
                            if(split[1].substring(0,2).equals("MA")){
                                typeCapteur = "wide_area_infrared_motion_sensors";
                                acrTypeCapteur = split[1].substring(0,2);
                            }
                            else {
                                typeCapteur = "infrared_motion_sensors";
                                acrTypeCapteur = split[1].substring(0, 1);
                            }
                            break;
                        case "T":
                            typeCapteur = "temperature_sensors";
                            acrTypeCapteur = split[1].substring(0,1);
                            break;
                        default:
                            switch (split[1].substring(0,2)) {
                                case "BA":
                                    typeCapteur = "sensor_battery_levels";
                                    acrTypeCapteur = split[1].substring(0,2);
                                    break;
                                default:
                                    typeCapteur = "unknown_sensors";
                                    acrTypeCapteur = split[1].substring(0,1);
                                    break;
                            }
                    }

                    String valeurCapteur = split[2];

                    //String valeurCapteur = split[3];
                    String valeurCapteurNormalize;

                    if (isInteger(valeurCapteur) && typeCapteur != "unknown_sensors") {
                        valeurCapteurNormalize = getTypeValeur(valeurCapteur);

                        //String typeCapteur = split[0];

                        UUID observationIndex = UUID.randomUUID(); //random.nextInt(Integer.MAX_VALUE);
                        String observationTime = observationIndex + "time";

                        RdfQuadruple q = new RdfQuadruple("_:" + observationIndex, "http://www.ontologydesignpatterns.org/ont/dul/DUL.owl#isObservableAt", observationTime, System.currentTimeMillis());
                        this.put(q);
                        q = new RdfQuadruple("_:" + observationIndex, "http://www.w3.org/ns/sosa/isObservedBy", split[1], System.currentTimeMillis());
                        this.put(q);

                        q = new RdfQuadruple("_:" + observationIndex, "http://www.w3.org/ns/sosa/madeBySensor", typeCapteur, System.currentTimeMillis());
                        this.put(q);

                        q = new RdfQuadruple("_:" + observationIndex, "http://www.ontologydesignpatterns.org/ont/dul/DUL.owl#hasLocation", room, System.currentTimeMillis());
                        this.put(q);


                        q = new RdfQuadruple("_:" + observationIndex, "http://www.w3.org/ns/sosa/hasSimpleResult", valeurCapteurNormalize, System.currentTimeMillis());
                        this.put(q);

                        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss+SSS");

                        q = new RdfQuadruple(observationTime, "http://w3id.org/ecareathome/patterns/timeinterval.owl#hasUpperTimeStampValue", sdf.format(new Date()) + "^^http://www.w3.org/2001/XMLSchema#dateTime", System.currentTimeMillis());
                        this.put(q);
                        q = new RdfQuadruple(observationTime, "http://w3id.org/ecareathome/patterns/timeinterval.owl#hasLowerTimeStampValue", sdf.format(new Date()) + "^^http://www.w3.org/2001/XMLSchema#dateTime", System.currentTimeMillis());
                        this.put(q);

                        logger.info("TV Data sent => {}",line);
                        Thread.sleep(sleepTime);
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }

            });
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


}
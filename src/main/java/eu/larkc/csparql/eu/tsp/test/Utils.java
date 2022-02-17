package eu.larkc.csparql.eu.tsp.test;

public class Utils {

    public static boolean isInteger(String s) {
        try {
            Integer.parseInt(s);
        } catch(NumberFormatException e) {
            return false;
        } catch(NullPointerException e) {
            return false;
        }
        // only got here if we didn't return false
        return true;
    }

    public static boolean isBoolean(String s) {
        try {
            Boolean.parseBoolean(s);
        } catch(Exception e) {
            return false;
        }
        // only got here if we didn't return false
        return true;
    }

    public static boolean isBooleanHerit(String s){
        if (s.equals("ON") || s.equals("OFF") || s.equals("OPEN") || s.equals("CLOSE") || s.equals("TRUE") || s.equals("FALSE")){
            return true;
        }
        return false;
    }

    public static int getBooleanFromString(String s){
        return s.equals("ON") || s.equals("OPEN") || s.equals("TRUE") ? 100 : 0;
    }

    public static boolean getBooleanFromBooleanValue(String s){
        return s.equals("ON") || s.equals("OPEN") || s.equals("TRUE") ? true : false;
    }

    public static String getTypeValeur(String valeurCapteur){
        String valeurCapteurNormalize;
        /** Determiner le type de la valeur */
        if (isInteger(valeurCapteur)){
            valeurCapteurNormalize = "\""+ valeurCapteur + "\"^^http://www.w3.org/2001/XMLSchema#integer";
        }
        else if (isBooleanHerit(valeurCapteur)){
            valeurCapteurNormalize = "\""+ getBooleanFromString(valeurCapteur) + "\"^^http://www.w3.org/2001/XMLSchema#integer";
        }
        //else if (isBoolean(valeurCapteur)){
          //  valeurCapteurNormalize = "\""+ valeurCapteur + "\"^^http://www.w3.org/2001/XMLSchema#boolean";
        //}
        else {
            valeurCapteurNormalize = "\""+ valeurCapteur + "\"^^http://www.w3.org/2001/XMLSchema#string";
        }
        return valeurCapteurNormalize;

    }

    public static String getTypeCapteur(String s) {
        String typeCapteur;
        String acrTypeCapteur;
        switch (s.substring(0,1)) {
            case "D":
                typeCapteur = "magnetic_door_sensors";
                acrTypeCapteur = s.substring(0,1);
                break;
            case "L":
                typeCapteur = "light_sensors";
                acrTypeCapteur = s.substring(0,1);
                switch (s.substring(0,2)) {
                    case "LS":
                        typeCapteur = "light_sensors";
                        acrTypeCapteur = s.substring(0,2);
                        break;
                    case "LA":
                        typeCapteur = "light_switches";
                        acrTypeCapteur = s.substring(0,2);
                        break;
                }
                break;
            case "M":
                if(s.substring(0,2).equals("MA")){
                    typeCapteur = "wide_area_infrared_motion_sensors";
                    acrTypeCapteur = s.substring(0,2);
                }
                else {
                    typeCapteur = "infrared_motion_sensors";
                    acrTypeCapteur = s.substring(0, 1);
                }
                break;
            case "T":
                typeCapteur = "temperature_sensors";
                acrTypeCapteur = s.substring(0,1);
                break;
            default:
                switch (s.substring(0,2)) {
                    case "BA":
                        typeCapteur = "sensor_battery_levels";
                        acrTypeCapteur = s.substring(0,2);
                        break;
                    default:
                        typeCapteur = "unknown_sensors";
                        acrTypeCapteur = s.substring(0,1);
                        break;
                }
        }
        return typeCapteur;
    }
}

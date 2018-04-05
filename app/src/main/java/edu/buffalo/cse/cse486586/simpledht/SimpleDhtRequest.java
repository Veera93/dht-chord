package edu.buffalo.cse.cse486586.simpledht;


/**
 * Created by veera on 4/2/18.
 */

class SimpleDhtRequest {

    /*********

        type == join                : originator; join; null
        type == insert               : originator; insert; id+type+key
        type == query               : originator; query; id+type+key
        type == delete              : originator; delete; type+key
        type == changePredecessor   : originator; changePredecessor; newPredecessor

    **********/

    private String originator;
    private String type;
    private String arguments;

    public SimpleDhtRequest() {

    }

    public SimpleDhtRequest(String sender, String type, String arguments) {
        this.originator = sender;
        this.type = type;
        this.arguments = arguments;
    }

    @Override
    public String toString() {
        return originator + SimpleDhtConfiguration.DELIMITER + type + SimpleDhtConfiguration.DELIMITER + arguments;
    }

    static final class Type {
        static final String JOIN="requestJoin";
        static final String QUERY="requestQuery";
        static final String DELETE="requestDelete";
        static final String CHNG_PRE="requestChangePredecessor";
        static final String INSERT="insertQuery";
    }
}



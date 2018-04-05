package edu.buffalo.cse.cse486586.simpledht;

/**
 * Created by veera on 4/2/18.
 */

public class SimpleDhtResponse {
    /*********
     type == join    : sender; join; predecessor
     type == query   : sender; query; content
     **********/

    private String sender;
    private String type;
    private String arguments;

    public SimpleDhtResponse(String sender, String type, String arguments) {
        this.sender = sender;
        this.type = type;
        this.arguments = arguments;
    }

    @Override
    public String toString() {
        return sender + SimpleDhtConfiguration.DELIMITER + type + SimpleDhtConfiguration.DELIMITER + arguments;
    }

    static final class Type {
        static final String JOIN="respondJoin";
        static final String QUERY="respondQuery";
        static final String DELETE="respondDelete";

    }
}

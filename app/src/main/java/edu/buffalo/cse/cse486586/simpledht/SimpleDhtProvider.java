package edu.buffalo.cse.cse486586.simpledht;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Formatter;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.sqlite.SQLiteDatabase;
import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;

import edu.buffalo.cse.cse486586.simpledht.SimpleDhtSchema.*;


public class SimpleDhtProvider extends ContentProvider {

    boolean running;
    String myPort;
    String myId;
    static final String TAG = SimpleDhtProvider.class.getSimpleName();
    SimpleDhtDbHelper dbHelper;
    SQLiteDatabase db;
    Node node;

    @Override
    public int delete(Uri uri, String selection, String[] selectionArgs) {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public String getType(Uri uri) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public boolean onCreate() {
        // TODO Auto-generated method stub
        running = true;
        /*
         * Calculate the port number that this AVD listens on.
         * It is just a hack that I came up with to get around the networking limitations of AVDs.
         */

        TelephonyManager tel = (TelephonyManager) getContext().getSystemService(Context.TELEPHONY_SERVICE);
        myId = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
        Log.v(TAG, myId);
        Log.v(TAG, "------------");
        myPort = String.valueOf((Integer.parseInt(myId) * 2));
        String hashedId = null;
        try {
            //First node
            hashedId = genHash(myId);
            node = new Node();
            node.myHashedId = hashedId;
            node.successor = myId;
            node.predecessor = myId;
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }

        try {
            /*
             * Create a server socket as well as a thread (AsyncTask) that listens on the server
             * port.
             *
             * AsyncTask is a simplified thread construct that Android provides.
             */
            ServerSocket serverSocket = new ServerSocket(SimpleDhtConfiguration.SERVER_PORT);
            new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
        } catch (IOException e) {
            Log.e(TAG, "Can't create a ServerSocket");
            return true;
        }

        if(hashedId != null) {
            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, SimpleDhtRequest.Type.JOIN , SimpleDhtConfiguration.FIRST_PORT.toString() ,myId);
        }


        return true;
    }

    @Override
    public Uri insert(Uri uri, ContentValues values) {
        /*
         * values will have two columns (a key
         * column and a value column) and one row that contains the actual (key, value) pair to be
         * inserted.
         *
         * Used SQLite for database
         */
        Log.v(TAG,"insert "+values.toString());
        String key = (String) values.get(SimpleDhtDataEntry.COLUMN_NAME_KEY);
        String value = (String) values.get(SimpleDhtDataEntry.COLUMN_NAME_VALUE);
        if(!isOwner(key)) {
            //ToDo: Send the request to successor
            Log.v(TAG,"insert calling successor");
            Integer successorPort = Integer.parseInt(node.successor) * 2;
            String args = myId+SimpleDhtConfiguration.ARG_DELIMITER+key+SimpleDhtConfiguration.ARG_DELIMITER+value;
            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, SimpleDhtRequest.Type.INSERT , successorPort.toString() , args);
        } else {
            Log.v(TAG,"insert handled");
            dbHelper = new SimpleDhtDbHelper(this.getContext());
            Cursor cursor = query(uri,null, key, null, null);
            db = dbHelper.getWritableDatabase();

            if(cursor.getCount() == 0 ) {
                db.insert(SimpleDhtDataEntry.TABLE_NAME, null, values);
            } else {
                update(uri, values, key, null);
            }
        }

        return uri;
    }

    @Override
    public Cursor query(Uri uri, String[] projection, String selection, String[] selectionArgs,
            String sortOrder) {

        /*
        ToDo: Implement Query for * and @ and also handle for my keyspace
         */

        dbHelper = new SimpleDhtDbHelper(this.getContext());
        db = dbHelper.getReadableDatabase();
        Cursor cursor = null;

        try{
            //Querying for all the messages
            if(selection == null) {
                cursor = db.query(
                    SimpleDhtDataEntry.TABLE_NAME,     // The table to query
                    projection,                       // The columns to return
                    null,                   // The columns for the WHERE clause
                    null,               // The values for the WHERE clause
                    null,                  // don't group the rows
                    null,                   // don't filter by row groups
                    sortOrder                      // The sort order
                );
            } else {
                Log.v("query", selection);
                String mSelection = SimpleDhtDataEntry.COLUMN_NAME_KEY + " = ?";
                String[] mSelectArg = { selection };

                cursor = db.query(
                        SimpleDhtDataEntry.TABLE_NAME,   // The table to query
                        projection,                     // The columns to return
                        mSelection,                     // The columns for the WHERE clause
                        mSelectArg,                     // The values for the WHERE clause
                        null,                  // don't group the rows
                        null,                   // don't filter by row groups
                        sortOrder                      // The sort order
                );
            }
        } catch (Exception e) {
            Log.e(TAG,e.getMessage());
        }
        finally {
            return cursor;
        }
    }

    @Override
    public int update(Uri uri, ContentValues values, String selection, String[] selectionArgs) {
        // TODO Auto-generated method stub
        // Which row to update, based on the title
        String mSelection = SimpleDhtDataEntry.COLUMN_NAME_KEY + " LIKE ?";
        String key = (String) values.get(SimpleDhtDataEntry.COLUMN_NAME_KEY);
        String[] mSelectionArgs = { key };

        int count = db.update(
                SimpleDhtDataEntry.TABLE_NAME,
                values,
                mSelection,
                mSelectionArgs);

        return count;
    }

    /*
 * ServerTask is an AsyncTask that should handle incoming messages. It is created by
 * ServerTask.executeOnExecutor() call.
 */
    private class ServerTask extends AsyncTask<ServerSocket, String, Void> {

        @Override
        protected Void doInBackground(ServerSocket... sockets) {
            ServerSocket serverSocket = sockets[0];
            Socket server = null;
            while(running) {
                try {
                    server = serverSocket.accept();
                    DataInputStream in = new DataInputStream(server.getInputStream());
                    DataOutputStream out = new DataOutputStream(server.getOutputStream());
                    String msg = in.readUTF();

                    if(msg != null) {
                        String[] paresedMsg = msg.split(SimpleDhtConfiguration.DELIMITER, 3);
                        String type = paresedMsg[1];

                        if(type.compareTo(SimpleDhtRequest.Type.JOIN) == 0) {
                            String originatorId = paresedMsg[0];
                            if(isOwner(originatorId)) {
                                Log.v(TAG,"I am your successor "+myId);
                                //I am your successor and you are my predecessor
                                //Notify my current predecessor to change his successor
                                Integer originatorIdPort = 2 * Integer.parseInt(originatorId);
                                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, SimpleDhtResponse.Type.JOIN , originatorIdPort.toString(), node.predecessor);
                                node.predecessor = originatorId;
                                Log.v(TAG,"Successor"+node.successor);
                                Log.v(TAG,"Predecessor"+node.predecessor);
                            } else {
                                Log.v(TAG,"I am your not your successor "+myId);
                                Integer succPort = Integer.parseInt(node.successor) * 2;
                                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, SimpleDhtRequest.Type.JOIN , succPort.toString() ,originatorId);
                            }
                        } else if(type.compareTo(SimpleDhtResponse.Type.JOIN) == 0) {
                            String succ = paresedMsg[0];
                            String pred = paresedMsg[2];
                            Log.v(TAG,"Node joined at"+myId);
                            Log.v(TAG,"Successor"+succ);
                            Log.v(TAG,"Predecessor"+pred);
                            node.successor = succ;
                            node.predecessor = pred;
                        } else if(type.compareTo(SimpleDhtRequest.Type.CHNG_PRE) == 0) {
                            //ToDO: Can check if my successor is in paresedMsg[0]
                            Log.v(TAG,"Successor updated "+myId);
                            node.successor = paresedMsg[2];
//                            if(node.predecessor.compareTo(myId) == 0) {
//                                //I am the only node
//                                node.predecessor = paresedMsg[2];
//                            }

                            Log.v(TAG,"Successor"+node.successor);
                            Log.v(TAG,"Predecessor"+node.predecessor);
                        } else if(type.compareTo(SimpleDhtRequest.Type.INSERT) == 0) {
                            String argument = paresedMsg[2];
                            String[] pair = argument.split(SimpleDhtConfiguration.ARG_DELIMITER);
                            if(isOwner(pair[0])) {
                                customInsert(pair[0],pair[1]);
                            } else {
                                Integer successorPort = Integer.parseInt(node.successor) * 2;
                                String originatorId = paresedMsg[0];
                                String arg = originatorId+SimpleDhtConfiguration.ARG_DELIMITER+paresedMsg[2];
                                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, SimpleDhtRequest.Type.INSERT , successorPort.toString() ,arg);
                            }

                        }
                    }
                    server.close();
                } catch (IOException e) {
                    Log.e(TAG, "Exception in server");
                }
            }
            return null;
        }

        protected void onProgressUpdate(String...strings) {
            /*
             * The following code displays what is received in doInBackground()
             */
        }
    }

    /***
     * ClientTask is an AsyncTask that should send a string over the network.
     * It is created by ClientTask.executeOnExecutor() call
     */
    private class ClientTask extends AsyncTask<String, Void, Void> {

        @Override
        protected Void doInBackground(String... msgs) {
            String type = msgs[0];
            String toSendPort = msgs[1];
            if(type.compareTo(SimpleDhtRequest.Type.JOIN ) == 0) {
                String originator = msgs[2];
                sendRequest(type, toSendPort, originator);
            } else if(type.compareTo(SimpleDhtResponse.Type.JOIN) == 0) {
                //Send Join response :  join ; toSendPort ; Predecessor Info
                //Send Change successor message to predecessor
                String predecessor = msgs[2];
                sendResponse(type, toSendPort, predecessor);
                Integer predecessorPort = Integer.parseInt(predecessor) * 2;
                Integer newSuccessor = Integer.parseInt(toSendPort) / 2;
                sendRequest(SimpleDhtRequest.Type.CHNG_PRE, predecessorPort.toString(), newSuccessor.toString());
            } else if(type.compareTo(SimpleDhtRequest.Type.INSERT) == 0) {
                sendRequest(type, toSendPort.toString(), msgs[2]);
            }
           return null;
        }
    }

    private void sendRequest(String type, String toSendPort, String argument) {
        if(type.compareTo(SimpleDhtRequest.Type.JOIN) == 0) {
            String originator = argument;
            //ToDo: check if sending null causes an issue
            SimpleDhtRequest request = new SimpleDhtRequest(originator, SimpleDhtRequest.Type.JOIN, null);
            String msgToSend = request.toString();
            Log.v(TAG,"Request  "+msgToSend + " to "+toSendPort+" at "+myId);
            sendMessage(toSendPort, msgToSend);
        } else if(type.compareTo(SimpleDhtRequest.Type.CHNG_PRE) == 0) {
            String newSucessor = argument;
            SimpleDhtRequest request = new SimpleDhtRequest(myId, SimpleDhtRequest.Type.CHNG_PRE, newSucessor);
            String msgToSend = request.toString();
            Log.v(TAG,"Request  "+msgToSend + " to "+toSendPort+" at "+myId);
            sendMessage(toSendPort, msgToSend);
        } else if(type.compareTo(SimpleDhtRequest.Type.INSERT) == 0) {
            String[] args = argument.split(SimpleDhtConfiguration.ARG_DELIMITER);
            String pair = args[1]+SimpleDhtConfiguration.ARG_DELIMITER+args[2];
            SimpleDhtRequest request = new SimpleDhtRequest(args[0], SimpleDhtRequest.Type.INSERT, pair);
            String msgToSend = request.toString();
            sendMessage(toSendPort, msgToSend);
        }
    }

    private void sendResponse(String type, String toSendPort, String argument) {

        if(type.compareTo(SimpleDhtResponse.Type.JOIN) == 0) {
            String predecessor = argument;
            SimpleDhtResponse response = new SimpleDhtResponse(myId, SimpleDhtResponse.Type.JOIN, predecessor);
            String msgToSend = response.toString();
            Log.v(TAG,"Response  "+msgToSend + " to "+toSendPort+" at "+myId);
            sendMessage(toSendPort, msgToSend);
        }

    }

    private void sendMessage(String port, String message) {
        try {
            Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(port));
            //socket.setSoTimeout(SimpleDhtConfiguration.TIMEOUT_TIME);
            //socket.setTcpNoDelay(false);
            DataOutputStream out = new DataOutputStream(socket.getOutputStream());
            //Send data to server
            out.writeUTF(message);
            socket.close();
        } catch (IOException e) {
            Log.e(TAG, "Exception in server");
        }
    }


    @Override
    public void shutdown() {
        super.shutdown();
        running = false;
    }

    private String genHash(String input) throws NoSuchAlgorithmException {
        MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
        byte[] sha1Hash = sha1.digest(input.getBytes());
        Formatter formatter = new Formatter();
        for (byte b : sha1Hash) {
            formatter.format("%02x", b);
        }
        return formatter.toString();
    }

    private boolean isOwner(String id) {
        String hashedId = null;
        String hashedSuccessor = null;
        String hashedPredecessor = null;

        try {
            hashedId = genHash(id);
            hashedSuccessor = genHash(node.successor);
            hashedPredecessor = genHash(node.predecessor);
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }

        if(hashedId.compareTo(node.myHashedId) == 0) {
            return true;
        } else if(hashedSuccessor.compareTo(hashedPredecessor) == 0 && node.myHashedId.compareTo(hashedPredecessor) == 0) {
            return true;
        } else if(hashedPredecessor.compareTo(node.myHashedId) > 0) {
            if(hashedId.compareTo(hashedPredecessor) > 0 && hashedId.compareTo(node.myHashedId) > 0) {
                return true;
            } else if(hashedId.compareTo(hashedPredecessor) < 0 && hashedId.compareTo(node.myHashedId) < 0) {
                return true;
            }
        } else if(hashedId.compareTo(hashedPredecessor) > 0 && hashedId.compareTo(node.myHashedId) < 0) {
            return true;
        }

        return false;
    }

    public void customInsert(String key, String value) {
        Log.v(TAG,"insert "+key+" "+value);
        if(!isOwner(key)) {
            //ToDo: Send the request to successor
            Log.v(TAG,"insert calling successor");
            Integer successorPort = Integer.parseInt(node.successor) * 2;
            String pair = key+SimpleDhtConfiguration.ARG_DELIMITER+value;
            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, SimpleDhtRequest.Type.INSERT , successorPort.toString() ,pair);
        } else {
            Log.v(TAG,"insert handled in custom");
            dbHelper = new SimpleDhtDbHelper(this.getContext());
            db = dbHelper.getWritableDatabase();
            //String sql = "INSERT INTO message VALUES (?,?)";
            //db.execSQL(sql, new String[] {key,value});
            String sql = "INSERT INTO message ("+SimpleDhtDataEntry.COLUMN_NAME_KEY+", " +SimpleDhtDataEntry.COLUMN_NAME_VALUE + ") values('"+key+"', '"+value+"');";
            Log.v(TAG,sql);
            db.execSQL(sql);
            db.close();
        }
    }

}

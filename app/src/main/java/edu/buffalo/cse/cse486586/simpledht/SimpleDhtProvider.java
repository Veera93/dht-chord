package edu.buffalo.cse.cse486586.simpledht;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.Formatter;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.DatabaseUtils;
import android.database.MatrixCursor;
import android.database.sqlite.SQLiteDatabase;
import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;

import edu.buffalo.cse.cse486586.simpledht.SimpleDhtSchema.*;


public class SimpleDhtProvider extends ContentProvider {

    private  boolean running;
    private  String myPort;
    private  String myId;
    private  static final String TAG = SimpleDhtProvider.class.getSimpleName();
    private  SimpleDhtDbHelper dbHelper;
    private  SQLiteDatabase db;
    private  Node node;
    private  QueryData sharedData;
    private  Object lock;

    @Override
    public String getType(Uri uri) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public boolean onCreate() {
        running = true;
        /*
         * Calculate the port number that this AVD listens on.
         * It is just a hack that I came up with to get around the networking limitations of AVDs.
         */
        TelephonyManager tel = (TelephonyManager) getContext().getSystemService(Context.TELEPHONY_SERVICE);
        myId = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
        myPort = String.valueOf((Integer.parseInt(myId) * 2));
        String hashedId = null;
        sharedData = new QueryData();
        lock = new Object();
        try {
            //When node starts up store its metadata
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
        try {
            Log.v(TAG,"Content provider insert called "+values.toString());
            String key = (String) values.get(SimpleDhtDataEntry.COLUMN_NAME_KEY);
            String value = (String) values.get(SimpleDhtDataEntry.COLUMN_NAME_VALUE);
            if(!isOwner(key)) {
                Log.v(TAG,"insert calling successor");
                Integer successorPort = Integer.parseInt(node.successor) * 2;
                String args = myId+SimpleDhtConfiguration.ARG_DELIMITER+key+SimpleDhtConfiguration.ARG_DELIMITER+value;
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, SimpleDhtRequest.Type.INSERT , successorPort.toString() , args);
            } else {
                Log.v(TAG,"insert handled in content provider");
                dbHelper = new SimpleDhtDbHelper(this.getContext());
                db = dbHelper.getReadableDatabase();
                Cursor cursor = query(uri,null, key, null, null);
                db = dbHelper.getWritableDatabase();
                if(cursor.getCount() == 0 ) {
                    db.insert(SimpleDhtDataEntry.TABLE_NAME, null, values);
                } else {
                    update(uri, values, key, null);
                }
            }
        } catch (Exception e) {
            Log.e(TAG, e.getMessage());
        } finally {
            return uri;
        }
    }
    /*
        Reference for Synchronization https://docs.oracle.com/javase/tutorial/essential/concurrency/guardmeth.html
     */
    @Override
    public Cursor query(Uri uri, String[] projection, String selection, String[] selectionArgs,
            String sortOrder) {
        Log.v(TAG, "Content provider query called "+ selection);
        dbHelper = new SimpleDhtDbHelper(this.getContext());
        db = dbHelper.getReadableDatabase();
        Cursor cursor = null;
        try {
            if(selection.compareTo(SimpleDhtConfiguration.GLOBAL) == 0) {
                Cursor tcursor = db.query(
                        SimpleDhtDataEntry.TABLE_NAME,     // The table to query
                        projection,                       // The columns to return
                        null,                   // The columns for the WHERE clause
                        null,               // The values for the WHERE clause
                        null,                  // don't group the rows
                        null,                   // don't filter by row groups
                        sortOrder                      // The sort order
                );
                if(node.successor.compareTo(myId) != 0) {
                    Integer successorPort = Integer.parseInt(node.successor) * 2;
                    SimpleDhtRequest request = new SimpleDhtRequest(myId, SimpleDhtRequest.Type.QUERY, selection);
                    String msgTosend = request.toString();
                    sharedData.temp.add(tcursor);
                    synchronized (lock) {
                        Log.v(TAG, "Inside Lock");
                        sharedData.isLocked = true;
                        sharedData.key = selection;
                        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,  SimpleDhtRequest.Type.QUERY , successorPort.toString() , msgTosend);
                        while(sharedData.isLocked) {
                            lock.wait();
                        }
                        Log.v(TAG, "Lock Released");
                        MatrixCursor matrixCursor = new MatrixCursor(new String[]{SimpleDhtDataEntry.COLUMN_NAME_KEY, SimpleDhtDataEntry.COLUMN_NAME_VALUE});
                        for(Cursor c : sharedData.temp) {
                            while (c.moveToNext()) {
                                int valueIndex = c.getColumnIndex(SimpleDhtDataEntry.COLUMN_NAME_VALUE);
                                int keyIndex = c.getColumnIndex(SimpleDhtDataEntry.COLUMN_NAME_KEY);
                                matrixCursor.addRow(new String[] {c.getString(keyIndex), c.getString(valueIndex)});
                            }
                        }
                        cursor = matrixCursor;
                        sharedData = new QueryData();
                    }
                } else {
                    cursor = tcursor;
                }

            } else if(selection.compareTo(SimpleDhtConfiguration.LOCAL) == 0) {
                cursor = db.query(
                        SimpleDhtDataEntry.TABLE_NAME,     // The table to query
                        projection,                       // The columns to return
                        null,                   // The columns for the WHERE clause
                        null,               // The values for the WHERE clause
                        null,                  // don't group the rows
                        null,                   // don't filter by row groups
                        sortOrder                      // The sort order
                );
            } else if(selection != null) {
                if(isOwner(selection)) {
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
                } else {
                    Integer successorPort = Integer.parseInt(node.successor) * 2;
                    SimpleDhtRequest request = new SimpleDhtRequest(myId, SimpleDhtRequest.Type.QUERY, selection);
                    String msgTosend = request.toString();
                    synchronized (lock) {
                        sharedData.isLocked = true;
                        sharedData.key = selection;
                        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,  SimpleDhtRequest.Type.QUERY , successorPort.toString() , msgTosend);
                        while(sharedData.isLocked) {
                            lock.wait();
                        }
                        Log.v(TAG, "Lock Released");
                        cursor = sharedData.value;
                        sharedData = new QueryData();
                    }
                    Log.v(TAG, "Outside Lock");
                }
            }
        } catch (Exception e) {
            Log.e(TAG,e.getMessage());
        } finally {
            Log.v(TAG, "Returning");
            return cursor;
        }
    }

    @Override
    public int update(Uri uri, ContentValues values, String selection, String[] selectionArgs) {
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

    @Override
    public int delete(Uri uri, String selection, String[] selectionArgs) {
        Log.v(TAG, "Content provider delete called "+ selection);
        int deletedRows = 0;
        dbHelper = new SimpleDhtDbHelper(this.getContext());
        db = dbHelper.getWritableDatabase();
        if(selection.compareTo(SimpleDhtConfiguration.GLOBAL) == 0) {
            deletedRows = customDelete(selection);
            if(node.successor.compareTo(myId) != 0) {
                forwardDelete(selection);
            }
        } else if(selection.compareTo(SimpleDhtConfiguration.LOCAL) == 0) {
            deletedRows = customDelete(selection);
        } else if(selection != null) {
            if(isOwner(selection)) {
                deletedRows = customDelete(selection);
            } else {
                forwardDelete(selection);
            }
        }
        return deletedRows;
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
                    Log.v(TAG, "Incoming message" + msg);
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
                                if(node.predecessor.compareTo(myId) != 0)
                                    handleKeyReinsert();
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
                            Log.v(TAG,"Successor"+node.successor);
                            Log.v(TAG,"Predecessor"+node.predecessor);
                        } else if(type.compareTo(SimpleDhtRequest.Type.INSERT) == 0) {
                            String argument = paresedMsg[2];
                            Log.v(TAG, "Got Insert"+ paresedMsg[2]);
                            String[] pair = argument.split(SimpleDhtConfiguration.ARG_DELIMITER);
                            if(isOwner(pair[0])) {
                                customInsert(pair[0],pair[1]);
                            } else {
                                Integer successorPort = Integer.parseInt(node.successor) * 2;
                                String originatorId = paresedMsg[0];
                                String arg = originatorId+SimpleDhtConfiguration.ARG_DELIMITER+paresedMsg[2];
                                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, SimpleDhtRequest.Type.INSERT , successorPort.toString() ,arg);
                            }

                        } else if(type.compareTo(SimpleDhtRequest.Type.QUERY) == 0) {
                            String query = paresedMsg[2];

                            if(query.compareTo(SimpleDhtConfiguration.LOCAL) == 0) {

                            } else if (query.compareTo(SimpleDhtConfiguration.GLOBAL) == 0) {
                                //Check if the originator is not me forward
                                if(paresedMsg[0].compareTo(myId) != 0) {
                                    Integer originatorPort = Integer.parseInt(paresedMsg[0]) * 2;
                                    String[] returnValue = customQuery(null, query);
                                    Integer successorPort = Integer.parseInt(node.successor) * 2;
                                    //Send reply to originator
                                    StringBuilder values = new StringBuilder();
                                    for(int i = 0; i < returnValue.length - 1 ; i++) {
                                        values.append(returnValue[i]);
                                        values.append(SimpleDhtConfiguration.ARG_DELIMITER);
                                        values.append(returnValue[i+1]);
                                        values.append(SimpleDhtConfiguration.ARG_DELIMITER);
                                        i++;
                                    }
                                    if(values.length() > 0)
                                        values.deleteCharAt(values.length() - 1);

                                    String arg = myId+SimpleDhtConfiguration.ARG_DELIMITER+query+SimpleDhtConfiguration.ARG_DELIMITER+values.toString();
                                    SimpleDhtResponse response = new SimpleDhtResponse(paresedMsg[0], SimpleDhtResponse.Type.QUERY, arg);
                                    String responseMsg = response.toString();
                                    new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, SimpleDhtResponse.Type.QUERY , originatorPort.toString() , responseMsg);
                                    //Sending message to successor
                                    new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,  SimpleDhtRequest.Type.QUERY , successorPort.toString() , msg);
                                }

                            } else {
                                if(isOwner(query)) {
                                    Integer originatorPort = Integer.parseInt(paresedMsg[0]) * 2;
                                    String[] returnValue = customQuery(query, "u");
                                    String value = null;
                                    if(returnValue != null)
                                        value = returnValue[1];
                                    String arg = myId+SimpleDhtConfiguration.ARG_DELIMITER+query+SimpleDhtConfiguration.ARG_DELIMITER+value;
                                    SimpleDhtResponse response = new SimpleDhtResponse(paresedMsg[0], SimpleDhtResponse.Type.QUERY, arg);
                                    String responseMsg = response.toString();
                                    new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, SimpleDhtResponse.Type.QUERY , originatorPort.toString() , responseMsg);
                                } else {
                                    Integer successorPort = Integer.parseInt(node.successor) * 2;
                                    new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,  SimpleDhtRequest.Type.QUERY , successorPort.toString() , msg);
                                }
                            }
                        } else if(type.compareTo(SimpleDhtResponse.Type.QUERY) == 0) {
                            String[] response = paresedMsg[2].split(SimpleDhtConfiguration.ARG_DELIMITER);
                            Log.v(TAG, Arrays.toString(response));
                            String queryType = response[1];
                            if(queryType.compareTo(SimpleDhtConfiguration.GLOBAL) == 0) {
                                // Find if key value pair is not null
                                Log.v(TAG, "Got response for *");
                                if(response.length > 2) {
                                    MatrixCursor matrixCursor = new MatrixCursor(new String[]{SimpleDhtDataEntry.COLUMN_NAME_KEY, SimpleDhtDataEntry.COLUMN_NAME_VALUE});
                                    for(int i = 2; i < response.length - 1 ; i++) {
                                        matrixCursor.addRow(new String[] {response[i], response[i+1]});
                                        i++;
                                    }
                                    sharedData.temp.add(matrixCursor);
                                }

                                //Message sender is my previous and I am the originator of the messsage. Release lock, I have received all messages
                                if(response[0].compareTo(node.predecessor) == 0 && paresedMsg[0].compareTo(myId) == 0) {
                                    synchronized (lock) {
                                        sharedData.isLocked = false;
                                        lock.notify();
                                    }
                                }
                            } else {
                                if(sharedData.key.compareTo(response[1]) == 0) {
                                    synchronized (lock) {
                                        MatrixCursor matrixCursor = new MatrixCursor(new String[]{SimpleDhtDataEntry.COLUMN_NAME_KEY, SimpleDhtDataEntry.COLUMN_NAME_VALUE}, 1);
                                        matrixCursor.addRow(new String[] {response[1], response[2]});
                                        sharedData.value = matrixCursor;
                                        sharedData.isLocked = false;
                                        lock.notify();
                                    }
                                }
                            }
                        } else if(type.compareTo(SimpleDhtRequest.Type.DELETE) == 0) {
                            String delete = paresedMsg[2];
                            if(delete.compareTo(SimpleDhtConfiguration.LOCAL) == 0) {

                            } else if (delete.compareTo(SimpleDhtConfiguration.GLOBAL) == 0) {
                                //Forward to successor only if originator is not me
                                if(paresedMsg[0].compareTo(myId) != 0) {
                                    Integer successorPort = Integer.parseInt(node.successor) * 2;
                                    customDelete(delete);
                                    //Sending message to successor
                                    new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,  SimpleDhtRequest.Type.DELETE , successorPort.toString() , msg);
                                }
                            } else {
                                if(isOwner(delete)) {
                                    customDelete(delete);
                                } else {
                                    Integer successorPort = Integer.parseInt(node.successor) * 2;
                                    new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,  SimpleDhtRequest.Type.DELETE , successorPort.toString() , msg);
                                }
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
            } else if(type.compareTo(SimpleDhtRequest.Type.QUERY) == 0) {
                sendRequest(type, toSendPort.toString(), msgs[2]);
            } else if(type.compareTo(SimpleDhtResponse.Type.QUERY) == 0) {
                sendResponse(type, toSendPort.toString(), msgs[2]);
            } else if(type.compareTo(SimpleDhtRequest.Type.DELETE) == 0) {
                sendRequest(type, toSendPort.toString(), msgs[2]);
            }
           return null;
        }
    }

    private void sendRequest(String type, String toSendPort, String argument) {
        if(type.compareTo(SimpleDhtRequest.Type.JOIN) == 0) {
            String originator = argument;
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
        } else if(type.compareTo(SimpleDhtRequest.Type.QUERY) == 0) {
            sendMessage(toSendPort, argument);
        } else if(type.compareTo(SimpleDhtRequest.Type.DELETE) == 0) {
            sendMessage(toSendPort, argument);
        }
    }

    private void sendResponse(String type, String toSendPort, String argument) {

        if(type.compareTo(SimpleDhtResponse.Type.JOIN) == 0) {
            String predecessor = argument;
            SimpleDhtResponse response = new SimpleDhtResponse(myId, SimpleDhtResponse.Type.JOIN, predecessor);
            String msgToSend = response.toString();
            Log.v(TAG,"Response  "+msgToSend + " to "+toSendPort+" at "+myId);
            sendMessage(toSendPort, msgToSend);
        } else if(type.compareTo(SimpleDhtResponse.Type.QUERY) == 0) {
            sendMessage(toSendPort, argument);
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

        int nodeKey = hashedId.compareTo(node.myHashedId);
        int succPred = hashedSuccessor.compareTo(hashedPredecessor);
        int nodePred = node.myHashedId.compareTo(hashedPredecessor);
        int keyPred = hashedId.compareTo(hashedPredecessor);

        if(nodeKey == 0) {
            return true;
        } else if(succPred == 0 && nodePred == 0) {
            return true;
        } else if(nodePred < 0) {
            if(keyPred > 0 && nodeKey > 0) {
                return true;
            } else if(keyPred < 0 && nodeKey < 0) {
                return true;
            }
        } else if(keyPred > 0 && nodeKey < 0) {
            return true;
        }

        return false;
    }

    public void customInsert(String key, String value) {
        try{
            Log.v(TAG,"insert handled in custom");
            dbHelper = new SimpleDhtDbHelper(this.getContext());
            db = dbHelper.getWritableDatabase();
            String sql = "INSERT INTO message ("+SimpleDhtDataEntry.COLUMN_NAME_KEY+", " +SimpleDhtDataEntry.COLUMN_NAME_VALUE + ") values('"+key+"', '"+value+"');";
            db.execSQL(sql);
        } catch (Exception e) {
            Log.e(TAG, e.getMessage());
        }

    }

    public String[] customQuery(String key, String type) {
        //ToDo: remove type
        Log.v(TAG,"query "+key);
        Cursor mCursor = null;
        dbHelper = new SimpleDhtDbHelper(this.getContext());
        db = dbHelper.getReadableDatabase();
        String[] messages = null;
        try {
            if (type.compareTo(SimpleDhtConfiguration.GLOBAL) == 0) {
                String sql = "SELECT * FROM message";
                mCursor = db.rawQuery(sql, null);
            } else if (type.compareTo("u") == 0) {
                    Log.v(TAG, "query handled in custom");
                    String sql = "SELECT * FROM message WHERE key = ?";
                    String[] selectionArgs = {key};
                    mCursor = db.rawQuery(sql, selectionArgs);
            }

            if (mCursor != null) {
                Log.v(TAG, DatabaseUtils.dumpCursorToString(mCursor));
                messages = new String[mCursor.getCount() * 2];
                int counter = 0;
                while (mCursor.moveToNext()) {
                    int valueIndex = mCursor.getColumnIndex(SimpleDhtDataEntry.COLUMN_NAME_VALUE);
                    int keyIndex = mCursor.getColumnIndex(SimpleDhtDataEntry.COLUMN_NAME_KEY);
                    String k = mCursor.getString(keyIndex);
                    String v = mCursor.getString(valueIndex);
                    messages[counter] = k;
                    ++counter;
                    messages[counter] = v;
                    ++counter;
                }
            }
        } catch (Exception e) {
                Log.e(TAG, e.getMessage());
        } finally {
            mCursor.close();
            return messages;
        }
    }

    public int customDelete(String selection) {
        Log.v(TAG, "Deleting Locally");
        int rowsAffected = 0;
        dbHelper = new SimpleDhtDbHelper(this.getContext());
        db = dbHelper.getWritableDatabase();
        if(selection.compareTo(SimpleDhtConfiguration.GLOBAL) == 0 || selection.compareTo(SimpleDhtConfiguration.LOCAL) == 0) {
            rowsAffected = db.delete(SimpleDhtDataEntry.TABLE_NAME, null, null);
        } else {
            String mselection = SimpleDhtDataEntry.COLUMN_NAME_KEY + " LIKE ?";
            String[] mselectionArgs = { selection };
            // Issue SQL statement.
            rowsAffected = db.delete(SimpleDhtDataEntry.TABLE_NAME, mselection, mselectionArgs);
        }
        return rowsAffected;
    }

    public void forwardDelete(String selection) {
        Log.v(TAG, "Forwarding Delete");
        Integer successorPort = Integer.parseInt(node.successor) * 2;
        SimpleDhtRequest request = new SimpleDhtRequest(myId, SimpleDhtRequest.Type.DELETE, selection);
        String msgTosend = request.toString();
        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, SimpleDhtRequest.Type.DELETE, successorPort.toString() , msgTosend);
    }

    public void handleKeyReinsert() {
        Log.v(TAG, "Handling key reinsert");
        dbHelper = new SimpleDhtDbHelper(this.getContext());
        db = dbHelper.getReadableDatabase();
        String[] projection = { SimpleDhtDataEntry.COLUMN_NAME_KEY, SimpleDhtDataEntry.COLUMN_NAME_VALUE};
        Cursor cursor = null;
        try {
            cursor = db.query(
                    SimpleDhtDataEntry.TABLE_NAME,     // The table to query
                    projection,                       // The columns to return
                    null,                   // The columns for the WHERE clause
                    null,               // The values for the WHERE clause
                    null,                  // don't group the rows
                    null,                   // don't filter by row groups
                    null                      // The sort order
            );
            String hashKey = null;
            String value = null;
            String key = null;
            Integer predecessorPort = Integer.parseInt(node.predecessor) * 2;
            if(cursor  != null) {
                while (cursor.moveToNext()) {
                    int valueIndex = cursor.getColumnIndex(SimpleDhtDataEntry.COLUMN_NAME_VALUE);
                    int keyIndex = cursor.getColumnIndex(SimpleDhtDataEntry.COLUMN_NAME_KEY);
                    key = cursor.getString(keyIndex);
                    value = cursor.getString(valueIndex);
                    if(!isOwner(key)) {
                        String arg = myId+SimpleDhtConfiguration.ARG_DELIMITER+key+SimpleDhtConfiguration.ARG_DELIMITER+value;
                        customDelete(key);
                        Log.v(TAG, "Sending key value:"+ key + value);
                        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, SimpleDhtRequest.Type.INSERT , predecessorPort.toString() ,arg);
                    }
                }
                hashKey = null;
                value = null;
                key = null;
            }
        } catch (Exception e) {
            Log.e(TAG, e.getMessage());
        }
    }
}

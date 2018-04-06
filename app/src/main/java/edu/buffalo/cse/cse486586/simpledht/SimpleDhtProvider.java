package edu.buffalo.cse.cse486586.simpledht;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Formatter;
import java.util.Iterator;
import java.util.List;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.content.ContextWrapper;
import android.database.Cursor;
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteOpenHelper;
import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;
import android.widget.TextView;

import org.json.JSONException;
import org.json.JSONObject;

public class SimpleDhtProvider extends ContentProvider {

    //Code Source: Project 2b
    static final String TAG = SimpleDhtProvider.class.getSimpleName();
    static final String[] Ports_Array = {"11108","11112","11116","11120","11124"};
    static final List<String> REMOTE_PORTS = new ArrayList<String>(Arrays.asList(Ports_Array));
    static final int SERVER_PORT = 10000;
    static final String KEY = "key";
    static final String VALUE = "value";
    static final String MSG_TYPE = "type";
    static final String MSG_FROM = "from";
    static final String MSG_NEWNODE = "newnode";
    static final String MSG_PREDECESSOR = "pre";
    static final String MSG_SUCCESSOR = "suc";
    static final String MSG_EMPTY = "#";
    static final String TYPE_INSERT = "insert"; // type to handle insert of (key,value) pair
    static final String TYPE_JOIN = "join"; // type to change the predecessor and successor port
                                            // in case of a new node join
    static final String TYPE_JOIN_HANDLE = "joinhandle"; // type to handle new node join in the chord
    static final String JOIN_HANDLE_PORT = "11108"; // port number that will be handling the node join request
    //Code Source Projecr 2b
    public static String DB_NAME = "GroupMessenger.db";
    public static String TABLE_NAME = "MessageHistory";
    public static String Create_Query = "CREATE TABLE " + TABLE_NAME +
            "(key TEXT PRIMARY KEY, value TEXT);";
    public SQLiteDatabase db;
    public static final String[] projections = {"key","value"};

    public static NodeObject Node;

    private static Uri buildUri(String scheme, String authority) {
        Uri.Builder uriBuilder = new Uri.Builder();
        uriBuilder.authority(authority);
        uriBuilder.scheme(scheme);
        return uriBuilder.build();
    }
    static final Uri mUri = buildUri("content", "edu.buffalo.cse.cse486586.simpledht.SimpleDhtActivity");

    // dbHelper class. Reference: https://developer.android.com/reference/android/database/sqlite/SQLiteOpenHelper.html
    public static class dbHelper extends SQLiteOpenHelper {
        dbHelper(Context context){
            super(context,DB_NAME,null,1);
        }

        @Override
        public void onCreate(SQLiteDatabase db){
            db.execSQL(Create_Query);
        }

        @Override
        public void onUpgrade(SQLiteDatabase db, int old_version, int new_version){
            db.execSQL("DROP TABLE IF EXISTS "+TABLE_NAME);
            onCreate(db);
        }
    }

    @Override
    public boolean onCreate() {
        try {
            // Code Source: project 2b
            TelephonyManager tel = (TelephonyManager) this.getContext().getSystemService(Context.TELEPHONY_SERVICE);
            String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
            final String myPort = String.valueOf((Integer.parseInt(portStr) * 2));
            Node = new NodeObject(myPort,this.genHash(myPort),myPort,this.genHash(myPort),myPort,this.genHash(myPort));
            try {
                ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
                new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
            }
            catch (IOException e) {
                Log.e(TAG, "Can't create a ServerSocket");
                //return;
            }

            // Message triggered to emulator 5554 when the new node enters the chord-DHT
            if(Node.getMyPort().compareTo(JOIN_HANDLE_PORT) != 0) {
                String msg = (new JSONObject()
                        .put(MSG_TYPE, TYPE_JOIN_HANDLE)
                        .put(MSG_NEWNODE, Node.getMyPort())
                        .put(MSG_FROM, Node.getMyPort())).toString();
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg, JOIN_HANDLE_PORT);
            }

        }
        catch (NoSuchAlgorithmException e){
            Log.e(TAG, "OnCreate - NoSuchAlgorithmException");

        }
        catch (JSONException e){
            Log.e(TAG, "OnCreate - JSONException");
        }
        // Code Source: Project 2b
        dbHelper help = new dbHelper(getContext());
        db = help.getWritableDatabase();
        return (db != null);
    }

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
    public Uri insert(Uri uri, ContentValues values) {
        this.insertHelper(values);
        return uri;
    }

    public void insertHelper(ContentValues values){
        try {
            long tempVal;
            String key = values.get(KEY).toString();
            String keyHash = this.genHash(key);

            // Insert in current AVD
            if ((keyHash.compareTo(Node.getMyPort_hash()) == 0) ||
                    (Node.getPrePort().compareTo(Node.getMyPort()) == 0)||
                    (keyHash.compareTo(Node.getMyPort_hash()) < 0 && keyHash.compareTo(Node.getPrePort_hash()) > 0) ||
                    (keyHash.compareTo(Node.getMyPort_hash()) < 0 && Node.getPrePort_hash().compareTo(Node.getMyPort_hash()) > 0) ||
                    (keyHash.compareTo(Node.getMyPort_hash()) > 0 && Node.getPrePort_hash().compareTo(Node.getMyPort_hash()) > 0
                            && keyHash.compareTo(Node.getPrePort_hash()) > 0)) {
                tempVal = db.insert(TABLE_NAME, null, values);
            }
            // move to successor
            else if (keyHash.compareTo(Node.getMyPort_hash()) > 0) {
                String msg = (new JSONObject().put(MSG_TYPE, TYPE_INSERT)
                                .put(KEY, key)
                                .put(VALUE, values.get(VALUE).toString())
                                .put(MSG_FROM,Node.getMyPort())).toString();
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg, Node.getSucPort());
            }
            //move to predecessor
            else if (keyHash.compareTo(Node.getMyPort_hash()) < 0) {
                String msg = (new JSONObject().put(MSG_TYPE, TYPE_INSERT)
                        .put(KEY, key)
                        .put(VALUE, values.get(VALUE).toString())
                        .put(MSG_FROM,Node.getMyPort())).toString();
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg, Node.getPrePort());
            }

            Log.v("insert", values.toString());
        }
        catch (NoSuchAlgorithmException e){
            Log.e(TAG, "Insert - NoSuchAlgorithmException");
        }
        catch (JSONException e){
            Log.e(TAG, "Insert - JSON Exception");
        }
    }

    @Override
    public Cursor query(Uri uri, String[] projection, String selection, String[] selectionArgs,
            String sortOrder) {
        // Code Source Project 2b
        Cursor cursor = db.query(TABLE_NAME,projection,"key = '" +selection + "'",selectionArgs,null,null,sortOrder);
        Log.v("query", selection);
        return cursor;
    }

    @Override
    public int update(Uri uri, ContentValues values, String selection, String[] selectionArgs) {
        // Code Source Project 2b
        long temp = db.update(TABLE_NAME,values,selection+ " = ?",selectionArgs);
        return 0;
    }

    // Hash Function
    private String genHash(String input) throws NoSuchAlgorithmException {
        MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
        byte[] sha1Hash = sha1.digest(input.getBytes());
        Formatter formatter = new Formatter();
        for (byte b : sha1Hash) {
            formatter.format("%02x", b);
        }
        return formatter.toString();
    }

    // Node Join Request Handler
    public void nodeJoinHandler(JSONObject obj){
        try {
            String new_node = (String) obj.get(MSG_NEWNODE);
            String new_node_hash = genHash(new_node);
            // if the current node is the only node in the chord
            if(Node.getMyPort().compareTo(Node.getPrePort()) == 0){
                Node.setPrePort(new_node);
                Node.setPrePort_hash(new_node_hash);
                Node.setSucPort(new_node);
                Node.setSucPort_hash(new_node_hash);
                String msg = (new JSONObject()
                            .put(MSG_TYPE,TYPE_JOIN)
                            .put(MSG_PREDECESSOR,Node.getMyPort())
                            .put(MSG_SUCCESSOR,Node.getMyPort())
                            .put(MSG_FROM,Node.getMyPort())).toString();

                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg, new_node);
            }

            else if(new_node_hash.compareTo(Node.getMyPort_hash()) > 0 &&
                   (new_node_hash.compareTo(Node.getSucPort_hash()) < 0 ||
                    Node.getSucPort_hash().compareTo(Node.getMyPort_hash()) < 0)){
                String temp = Node.getSucPort();
                Node.setSucPort(new_node);
                Node.setSucPort_hash(new_node_hash);
                String msg1 = (new JSONObject()
                        .put(MSG_TYPE,TYPE_JOIN)
                        .put(MSG_PREDECESSOR,Node.getMyPort())
                        .put(MSG_SUCCESSOR,temp)
                        .put(MSG_FROM,Node.getMyPort())).toString();
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg1, new_node);

                String msg2 = (new JSONObject()
                        .put(MSG_TYPE,TYPE_JOIN)
                        .put(MSG_PREDECESSOR,new_node)
                        .put(MSG_SUCCESSOR,MSG_EMPTY)
                        .put(MSG_FROM,Node.getMyPort())).toString();
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg2, temp);
            }

            else if(new_node_hash.compareTo(Node.getMyPort_hash()) < 0 &&
                    (new_node_hash.compareTo(Node.getPrePort_hash()) > 0 ||
                    Node.getPrePort_hash().compareTo(Node.getMyPort_hash()) > 0)){
                String temp = Node.getPrePort();
                Node.setPrePort(new_node);
                Node.setPrePort_hash(new_node_hash);
                String msg1 = (new JSONObject()
                        .put(MSG_TYPE,TYPE_JOIN)
                        .put(MSG_PREDECESSOR,temp)
                        .put(MSG_SUCCESSOR,Node.getMyPort())
                        .put(MSG_FROM,Node.getMyPort())).toString();
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg1, new_node);

                String msg2 = (new JSONObject()
                        .put(MSG_TYPE,TYPE_JOIN)
                        .put(MSG_PREDECESSOR,MSG_EMPTY)
                        .put(MSG_SUCCESSOR,new_node)
                        .put(MSG_FROM,Node.getMyPort())).toString();
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg2, temp);
            }

            else if(new_node_hash.compareTo(Node.getMyPort_hash()) > 0){
                String msg = (new JSONObject()
                        .put(MSG_TYPE,TYPE_JOIN_HANDLE)
                        .put(MSG_NEWNODE,new_node)
                        .put(MSG_FROM,Node.getMyPort())).toString();
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg, Node.getSucPort());
            }

            else if(new_node_hash.compareTo(Node.getMyPort_hash()) < 0){
                String msg = (new JSONObject()
                        .put(MSG_TYPE,TYPE_JOIN_HANDLE)
                        .put(MSG_NEWNODE,new_node)
                        .put(MSG_FROM,Node.getMyPort())).toString();
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg, Node.getPrePort());
            }
        }
        catch (JSONException e){
            Log.e(TAG, "nodeJoinHandler - JSON Exception");
        }
        catch (NoSuchAlgorithmException e){
            Log.e(TAG, "nodeJoinHandler - NoSuchAlgorithmException");
        }
    }

    // Server Class
    // Code Source Project 2b
    private class ServerTask extends AsyncTask<ServerSocket, String, Void> {

        @Override
        protected Void doInBackground(ServerSocket... sockets) {

            try {
                ServerSocket serverSocket = sockets[0];

                while (true) {
                    Socket newSocket = serverSocket.accept();
                    DataInputStream inputStream = new DataInputStream(newSocket.getInputStream());
                    String strReceived = inputStream.readUTF().trim();
                    //JSONObject obj = new JSONObject(strReceived);
                    publishProgress(strReceived);
                    inputStream.close();
                    newSocket.close();
                }
                //serverSocket.close();
            }
            catch (IOException e) {
                Log.e(TAG, "Server Socket IOException");
                e.printStackTrace();
            }
            /*catch (JSONException e){
                Log.e(TAG, "Server Task JSON Exception");
            }*/

            return null;
        }

        protected void onProgressUpdate(String...strings) {
            try {
            /*
             * The following code displays what is received in doInBackground().
             */
                String strReceived = strings[0].trim();
                JSONObject obj = new JSONObject(strReceived);
                String msgType = (String) obj.get(MSG_TYPE);

                // Handling message of type 'insert'
                if(msgType.trim().equals(TYPE_INSERT)){
                    ContentValues cv = new ContentValues();
                    cv.put(KEY, (String)obj.get(KEY));
                    cv.put(VALUE, (String)obj.get(VALUE));
                    insertHelper(cv);
                }

                // Handling message of type 'joinhandle'
                if(msgType.trim().equals(TYPE_JOIN_HANDLE)){
                    nodeJoinHandler(obj);
                }

                // Handling message of type 'join'
                if(msgType.trim().equals(TYPE_JOIN)){

                }

            }
            /*catch (JSONException e){
                Log.e(TAG, "failed in onProgressUpdate - JSON Exception");
            }*/
            catch(Exception e){
                Log.e(TAG, "failed in onProgressUpdate ");
                e.printStackTrace();
            }

            return;
        }

    }

    // Client Class
    private class ClientTask extends AsyncTask<String, Void, Void> {

        @Override
        protected Void doInBackground(String... msgs) {
            try {
                Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                        Integer.parseInt(msgs[1]));
                String msgToSend = msgs[0];
                DataOutputStream outputStream = new DataOutputStream(socket.getOutputStream());
                outputStream.writeUTF(msgToSend);
                outputStream.flush();
                //socket.close();
            } catch (UnknownHostException e) {
                Log.e(TAG, "ClientTask UnknownHostException");
            } catch (IOException e) {
                Log.e(TAG, "ClientTask socket IOException");
            }

            return null;
        }
    }
}

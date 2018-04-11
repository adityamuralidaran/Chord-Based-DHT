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
import android.database.MatrixCursor;
import android.database.MergeCursor;
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteOpenHelper;
import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;
import android.widget.TextView;

import org.json.JSONException;
import org.json.JSONObject;

import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

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
    static final String MSG_COUNT = "count";
    static final String MSG_QUERY_RES_COUNT = "rescount";
    static final String MSG_QUERY_RES_KEY = "k";
    static final String MSG_QUERY_RES_VALUE = "v";
    static final String TYPE_INSERT = "insert"; // type to handle insert of (key,value) pair
    static final String TYPE_JOIN = "join"; // type to change the predecessor and successor port
                                            // in case of a new node join
    static final String TYPE_JOIN_HANDLE = "joinhandle"; // type to handle new node join in the chord
    static final String TYPE_QUERY_ALL = "queryall";
    static final String TYPE_QUERY_ALL_RESPONSE = "queryallres";
    static final String TYPE_QUERY_KEY = "querykey";
    static final String TYPE_QUERY_KEY_RESPONSE = "querykeyres";

    static final String JOIN_HANDLE_PORT = "5554"; // port number that will be handling the node join request
    //Code Source Projecr 2b
    public static String DB_NAME = "GroupMessenger.db";
    public static String TABLE_NAME = "MessageHistory";
    public static String Create_Query = "CREATE TABLE " + TABLE_NAME +
            "(key TEXT PRIMARY KEY, value TEXT);";
    public SQLiteDatabase db;
    public static final String[] projections = {"key","value"};

    public static NodeObject Node;


    // Lock for Query All Statement
    public static Lock queryAllLock = new ReentrantLock();
    public static MatrixCursor queryAllCursor;
    public static int queryAllCount = 0;
    public static int queryAllTotalCount = -1;

    // Lock for Query Key Statement
    public static Lock queryKeyLock = new ReentrantLock();
    public static MatrixCursor queryKeyCursor;


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
            Log.v(TAG, "into on create");
            TelephonyManager tel = (TelephonyManager) this.getContext().getSystemService(Context.TELEPHONY_SERVICE);
            String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
            final String myPort = String.valueOf((Integer.parseInt(portStr)));
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

            Log.v(TAG, "node : "+Node.getMyPort()+" node hash : "+Node.getMyPort_hash());
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
        if(selection.equals("@")){
            return db.delete(TABLE_NAME,null,selectionArgs);
        }
        if(selection.equals("*")){
            // TODO: Required Change
            return db.delete(TABLE_NAME,null,selectionArgs);
        }
        Log.v("delete", selection);
        return db.delete(TABLE_NAME,"key = '" +selection + "'",selectionArgs);
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
                Log.v("insert", values.toString());
                Log.v("insert vals","key= "+key+" , key hash= "+keyHash);
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

        Log.v("query", selection);

        if (selection.equals("@")) {
            Cursor cursor = db.query(TABLE_NAME, projection, null, selectionArgs, null, null, sortOrder);
            return cursor;
        }
        if (selection.equals("*")) {
            // TODO: change required
            Cursor cursor = db.query(TABLE_NAME, projection, null, selectionArgs, null, null, sortOrder);
            queryAllCursor = new MatrixCursor(new String[]{KEY, VALUE});
            while (cursor.moveToNext()) {
                String k = cursor.getString(cursor.getColumnIndex(KEY));
                String v = cursor.getString(cursor.getColumnIndex(VALUE));
                queryAllCursor.addRow(new Object[]{k, v});
            }
            if(!Node.getPrePort().equals(Node.getMyPort())) {
                try {
                    String msg = (new JSONObject().put(MSG_TYPE, TYPE_QUERY_ALL)
                            .put(MSG_COUNT, "0")
                            .put(MSG_FROM, Node.getMyPort())).toString();
                    new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg, Node.getSucPort());
                    synchronized (queryAllLock) {
                        queryAllLock.wait();
                    }

                } catch (JSONException e) {
                    Log.e(TAG, "query all - JSON Exception");
                } catch (InterruptedException e) {
                    Log.e(TAG, "query all - Interrupted Exception");
                }
            }
            queryAllCount = 0;
            queryAllTotalCount = -1;
            return queryAllCursor;
        }
        // query key handler
        else {
            return queryHelper(projection,selection,selectionArgs,sortOrder);
        }

    }


    public Cursor queryHelper(String[] projection, String selection, String[] selectionArgs,
                              String sortOrder){
        try {
            String key = selection;
            String keyHash = this.genHash(key);
            if ((keyHash.compareTo(Node.getMyPort_hash()) == 0) ||
                    (Node.getPrePort().compareTo(Node.getMyPort()) == 0) ||
                    (keyHash.compareTo(Node.getMyPort_hash()) < 0 && keyHash.compareTo(Node.getPrePort_hash()) > 0) ||
                    (keyHash.compareTo(Node.getMyPort_hash()) < 0 && Node.getPrePort_hash().compareTo(Node.getMyPort_hash()) > 0) ||
                    (keyHash.compareTo(Node.getMyPort_hash()) > 0 && Node.getPrePort_hash().compareTo(Node.getMyPort_hash()) > 0
                            && keyHash.compareTo(Node.getPrePort_hash()) > 0)) {

                Cursor cursor = db.query(TABLE_NAME, projection, "key = '" + selection + "'", selectionArgs, null, null, sortOrder);
                return cursor;
            }
            // Move to successor
            else if (keyHash.compareTo(Node.getMyPort_hash()) > 0) {
                queryKeyCursor = new MatrixCursor(new String[]{KEY, VALUE});
                String msg = (new JSONObject()
                        .put(MSG_TYPE,TYPE_QUERY_KEY)
                        .put(MSG_FROM,Node.getMyPort())
                        .put(KEY,key)).toString();
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg, Node.getSucPort());
                synchronized (queryKeyLock) {
                    queryKeyLock.wait();
                }

                return queryKeyCursor;
            }
            // Move to my predecessor
            else {
                queryKeyCursor = new MatrixCursor(new String[]{KEY, VALUE});
                String msg = (new JSONObject()
                        .put(MSG_TYPE,TYPE_QUERY_KEY)
                        .put(MSG_FROM,Node.getMyPort())
                        .put(KEY,key)).toString();
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg, Node.getPrePort());
                synchronized (queryKeyLock) {
                    queryKeyLock.wait();
                }
                return queryKeyCursor;
            }

            //return cursor;
        }
        catch (NoSuchAlgorithmException e){
            Log.e(TAG, "Insert Helper - NoSuchAlgorithmException");
            return new MatrixCursor(new String[]{KEY, VALUE});
        }
        catch (JSONException e){
            Log.e(TAG, "Insert Helper - JSON Exception");
            return new MatrixCursor(new String[]{KEY, VALUE});
        }
        catch (InterruptedException e){
            Log.e(TAG, "Insert Helper - Interrupted Exception");
            return new MatrixCursor(new String[]{KEY, VALUE});
        }
    }

    public void queryKeyMessageHelper(JSONObject strMsgReceived){
        try {
            String key = (String) strMsgReceived.get(KEY);
            String keyHash = this.genHash(key);
            String from = (String) strMsgReceived.get(MSG_FROM);
            if ((keyHash.compareTo(Node.getMyPort_hash()) == 0) ||
                    (Node.getPrePort().compareTo(Node.getMyPort()) == 0) ||
                    (keyHash.compareTo(Node.getMyPort_hash()) < 0 && keyHash.compareTo(Node.getPrePort_hash()) > 0) ||
                    (keyHash.compareTo(Node.getMyPort_hash()) < 0 && Node.getPrePort_hash().compareTo(Node.getMyPort_hash()) > 0) ||
                    (keyHash.compareTo(Node.getMyPort_hash()) > 0 && Node.getPrePort_hash().compareTo(Node.getMyPort_hash()) > 0
                            && keyHash.compareTo(Node.getPrePort_hash()) > 0)) {

                Cursor cursor = db.query(TABLE_NAME, null, "key = '" + key + "'", null, null, null, null);
                String keyRes = "";
                String valueRes = "";
                while (cursor.moveToNext()) {
                    keyRes = cursor.getString(cursor.getColumnIndex(KEY));
                    valueRes = cursor.getString(cursor.getColumnIndex(VALUE));
                }
                String msg = (new JSONObject()
                        .put(MSG_TYPE,TYPE_QUERY_KEY_RESPONSE)
                        .put(MSG_FROM,Node.getMyPort())
                        .put(KEY,keyRes)
                        .put(VALUE,valueRes)).toString();
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg, from);
            }

            // Move to successor
            else if (keyHash.compareTo(Node.getMyPort_hash()) > 0) {
                String msg = (new JSONObject()
                        .put(MSG_TYPE,TYPE_QUERY_KEY)
                        .put(MSG_FROM,from)
                        .put(KEY,key)).toString();
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg, Node.getSucPort());
            }
            // Move to my predecessor
            else {
                String msg = (new JSONObject()
                        .put(MSG_TYPE,TYPE_QUERY_KEY)
                        .put(MSG_FROM,from)
                        .put(KEY,key)).toString();
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg, Node.getPrePort());
            }
        }
        catch (NoSuchAlgorithmException e){
            Log.e(TAG, "query key message Helper - NoSuchAlgorithmException");
        }
        catch (JSONException e){
            Log.e(TAG, "query key message Helper - JSON Exception");
        }
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

            Log.v(TAG, "successor node : "+Node.getSucPort());
            Log.v(TAG, "predecessor node : "+Node.getPrePort());
        }
        catch (JSONException e){
            Log.e(TAG, "nodeJoinHandler - JSON Exception");
        }
        catch (NoSuchAlgorithmException e){
            Log.e(TAG, "nodeJoinHandler - NoSuchAlgorithmException");
        }
    }

    // Handles message type 'join' that helps the node join by changing the
    // successor and predecessor in chord
    public void joinNodes(JSONObject obj){
        try {
            String predecessor = (String) obj.get(MSG_PREDECESSOR);
            String successor = (String) obj.get(MSG_SUCCESSOR);

            if(!predecessor.equals(MSG_EMPTY)){
                Node.setPrePort(predecessor);
                Node.setPrePort_hash(genHash(predecessor));
            }
            if(!successor.equals(MSG_EMPTY)){
                Node.setSucPort(successor);
                Node.setSucPort_hash(genHash(successor));
            }
            Log.v(TAG, "successor node : "+Node.getSucPort());
            Log.v(TAG, "predecessor node : "+Node.getPrePort());
         }
        catch(JSONException e){
            Log.e(TAG, "joinNodes - JSON Exception");
        }
        catch(NoSuchAlgorithmException e){
            Log.e(TAG, "joinNodes - NoSuchAlgorithmException");
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
                    JSONObject obj = new JSONObject(strReceived);
                    String msgType = (String) obj.get(MSG_TYPE);

                    // Handling message of type 'queryallres'
                    if(msgType.trim().equals(TYPE_QUERY_ALL_RESPONSE)){
                        int resultCount = Integer.parseInt((String)obj.get(MSG_QUERY_RES_COUNT));
                        for(int i = 1;i<=resultCount;i++){
                            String keyName = MSG_QUERY_RES_KEY + Integer.toString(i);
                            String valueName = MSG_QUERY_RES_VALUE + Integer.toString(i);
                            String ki = (String) obj.get(keyName);
                            String vi = (String) obj.get(valueName);
                            queryAllCursor.addRow(new Object[]{ki,vi});
                        }
                        queryAllCount ++;
                        if(((String)obj.get(MSG_FROM)).equals(Node.getPrePort()))
                            queryAllTotalCount = Integer.parseInt((String)obj.get(MSG_COUNT));

                        if(queryAllTotalCount == queryAllCount) {
                            synchronized (queryAllLock) {
                                queryAllLock.notify();
                            }
                        }
                    }

                    // Handling message of type 'queryall'
                    if(msgType.trim().equals(TYPE_QUERY_ALL)){
                        String from = (String) obj.get(MSG_FROM);
                        if(!from.equals(Node.getMyPort())){
                            int cnt = Integer.parseInt((String)obj.get(MSG_COUNT));
                            Cursor cursor = db.query(TABLE_NAME,null,null,null,null,null,null);
                            JSONObject obj1 = new JSONObject()
                                    .put(MSG_TYPE,TYPE_QUERY_ALL_RESPONSE)
                                    .put(MSG_COUNT,Integer.toString(cnt+1))
                                    .put(MSG_FROM,Node.getMyPort());
                            int i = 1;
                            while (cursor.moveToNext()) {
                                String k = cursor.getString(cursor.getColumnIndex(KEY));
                                String v = cursor.getString(cursor.getColumnIndex(VALUE));
                                String keyName = MSG_QUERY_RES_KEY + Integer.toString(i);
                                String valueName = MSG_QUERY_RES_VALUE + Integer.toString(i);
                                obj1.put(keyName,k);
                                obj1.put(valueName,v);
                                i += 1;
                            }

                            obj1.put(MSG_QUERY_RES_COUNT,Integer.toString(i-1));
                            String msg1 = obj1.toString();
                            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg1, from);

                            String msg2 = (new JSONObject()
                                    .put(MSG_TYPE,TYPE_QUERY_ALL)
                                    .put(MSG_FROM,from)
                                    .put(MSG_COUNT,Integer.toString(cnt+1))).toString();
                            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg2, Node.getSucPort());
                        }
                    }

                    // Handling message of type 'querykey'
                    if(msgType.trim().equals(TYPE_QUERY_KEY)){
                        String from = (String) obj.get(MSG_FROM);
                        if(from.equals(Node.getMyPort())){
                            synchronized (queryKeyLock) {
                                queryKeyLock.notify();
                            }
                        }
                        else{
                            queryKeyMessageHelper(obj);
                        }
                    }

                    // Handling message of type 'querykeyres'
                    if(msgType.trim().equals(TYPE_QUERY_KEY_RESPONSE)){
                        String keyRes = (String) obj.get(KEY);
                        String valueRes = (String) obj.get(VALUE);
                        queryKeyCursor.addRow(new Object[]{keyRes,valueRes});
                        synchronized (queryKeyLock) {
                            queryKeyLock.notify();
                        }
                    }

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
                        joinNodes(obj);
                    }

                    //publishProgress(strReceived);

                    inputStream.close();
                    newSocket.close();
                }
                //serverSocket.close();
            }
            catch (IOException e) {
                Log.e(TAG, "Server Socket IOException");
                e.printStackTrace();
            }
            catch (JSONException e){
                Log.e(TAG, "Server Task JSON Exception");
            }

            return null;
        }

        protected void onProgressUpdate(String...strings) {
            try {
            /*
             * The following code displays what is received in doInBackground().
             */
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
                String port = String.valueOf((Integer.parseInt(msgs[1]) * 2));
                Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                        Integer.parseInt(port));
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

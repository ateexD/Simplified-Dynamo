package edu.buffalo.cse.cse486586.simpledynamo;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Formatter;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;

import android.database.Cursor;
import android.database.MatrixCursor;
import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;

import org.json.JSONException;
import org.json.JSONObject;

import static android.content.Context.MODE_PRIVATE;

public class SimpleDynamoProvider extends ContentProvider {

    // HashMaps
    public TreeMap<String, Integer> allNodeHashMap = new TreeMap<String, Integer>();
    public HashMap<Integer, Socket> clientNodeHashMap = new HashMap<Integer, Socket>();
    public ConcurrentHashMap<Integer, Queue<Message>> messageQueueHashMap = new ConcurrentHashMap<Integer, Queue<Message>>();
    public ConcurrentLinkedQueue<Message> cacheQueue = new ConcurrentLinkedQueue<Message>();
    AtomicBoolean recoveryMode = new AtomicBoolean(true);
    Integer latestRecovery = null;
    // Telephone number - Port number hack
    TelephonyManager tel;
    String portStr;
    Integer myPort;

    // Print boolean for StackTrace
    boolean print = true;

    // Timeout variable
    Integer TIMEOUT = 2000;

    // All ports
    final int[] portsToSend = {11108, 11112, 11116, 11120, 11124};

    enum Operation {
        INSERT, DELETE, QUERY, RECOVER
    }

    private synchronized MatrixCursor operationHandler(Operation operationType, String key, String value){
        JSONObject jsonObject = null;
        if (operationType == Operation.RECOVER) {
            while(!cacheQueue.isEmpty()) {
                Message m = cacheQueue.poll();
                Log.d("Recovery cache", m.toString());
                operationHandler(Operation.INSERT, m.key, m.value);
            }
        }
        try {
         jsonObject = new JSONObject(getJSONAsString());
            switch (operationType){
                case QUERY:
                    MatrixCursor matrixCursor = new MatrixCursor(new String[]{"key", "value"});
                    if(key.equals("@"))
                        return jsonToCursor(jsonObject.toString(), null);
                    else {
                        if (jsonObject.has(key)) {
                            Object[] row = new Object[]{key, jsonObject.get(key)};
                            matrixCursor.addRow(row);
                        }
                        return matrixCursor;
                    }
                case INSERT:
                    jsonObject.put(key, value);
                    break;
                case DELETE:
                    jsonObject.remove(key);
                    break;
                default:
                    return null;
            }
        } catch (JSONException e) {
            e.printStackTrace();
        }
        writeToJSON(jsonObject);
        return null;
    }

    public String getJSONAsString() {
        // Convert JSON file to string
        int i;

        FileInputStream fileInputStream;
        StringBuilder sb = new StringBuilder();

        try {
            fileInputStream = getContext().openFileInput("myJSON.json");

            if (fileInputStream != null)
                while ((i = fileInputStream.read()) != -1)
                    sb.append((char) i);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return sb.toString();
    }

    public void writeToJSON(JSONObject jsonObject) {
        // Dump JsonObject to myJSON.json
        try {
            FileOutputStream fileOutputStream = getContext().openFileOutput("myJSON.json", MODE_PRIVATE);
            if (fileOutputStream != null) {
                OutputStreamWriter outputStreamWriter = new OutputStreamWriter(fileOutputStream);
                outputStreamWriter.write(jsonObject.toString());
                outputStreamWriter.flush();
                outputStreamWriter.close();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public synchronized Socket addOrGetSocket(int portNum) {
        try {
            Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), portNum);
            socket.setSoTimeout(TIMEOUT);
            return socket;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    public Message deserialize(String messageString) {

        String[] split = messageString.split("\\;");
        int port = Integer.parseInt(split[0]);
        String status = split[1];
        String key = split[2];
        String value = split[3];
        long timestamp = Long.valueOf(split[4]);
        return new Message(port, status, key, value, timestamp);
    }


    @Override
    public int delete(Uri uri, String selection, String[] selectionArgs) {
        Message m = new Message(myPort, "DELETE", selection, " ", 0);
        try {
            int[] portsToDel = getNext(genHash(selection));

            for (int port : portsToDel) {
                if (port == myPort) {
                    operationHandler(Operation.DELETE, selection, null);
                } else {
                    try {
                        Socket socket = addOrGetSocket(port * 2);
                        DataOutputStream dos = new DataOutputStream(socket.getOutputStream());
                        dos.writeUTF(m.toString());
                        dos.flush();
                    } catch (Exception e) {
                        m.timestamp = System.currentTimeMillis() / 1000L;
                        messageQueueHashMap.get(port).offer(m);
                    }
                }
            }
        } catch (Exception e) {
            if (print)
                e.printStackTrace();
        }
        return 0;
    }

    @Override
    public String getType(Uri uri) {
        // TODO Auto-generated method stub
        return null;
    }

    public String nextVal(String val) {

        // Returns next key of given hash
        String next;

        if (allNodeHashMap.containsKey(val))
            next = allNodeHashMap.higherKey(val);
        else
            next = allNodeHashMap.ceilingKey(val);

        if (next == null)
            next = allNodeHashMap.firstKey();
        return next;
    }

    public int[] getNext(String keyHash) {
        // This function returns the strings of all 3 ports that the
        // value needs to inserted to

        int[] ports = {-1, -1, -1};

        String next = nextVal(keyHash);
        ports[0] = allNodeHashMap.get(next);
        ports[1] = allNodeHashMap.get(nextVal(next));
        ports[2] = allNodeHashMap.get(nextVal(nextVal(next)));
        return ports;
    }

    @Override
    public Uri insert(Uri uri, ContentValues values) {
        // Get node that the new thing belongs to
        String key = (String) values.get("key");
        String value = (String) values.get("value");

        Message m = new Message(myPort, "INSERT", key, value, 0);

        String keyHash = "";
        try {
            keyHash = genHash(key);
        } catch (Exception e) { }

        int[] portsToInsert = getNext(keyHash);

        Log.e("This key", keyHash);
        Log.e("Port", portsToInsert[0] + ";" + portsToInsert[1] + ";" + portsToInsert[2]);

        for (int port : portsToInsert) {
            if (port == myPort) {
                Log.e("Insert", m.toString() + " to " + port + " ");
                operationHandler(Operation.INSERT, key, value);
            } else {
                try {
                    Log.d("port", port * 2 + "");
                    Socket insertSocket = addOrGetSocket(port * 2);

                    DataOutputStream dataOutputStream = new DataOutputStream(insertSocket.getOutputStream());
                    dataOutputStream.writeUTF(m.toString());

                    if (latestRecovery != null &&  latestRecovery == port)
                        Thread.sleep(100);

                    dataOutputStream.flush();
                    new DataInputStream(insertSocket.getInputStream()).readUTF();
                    Log.e("Insert", m.toString() + " to " + port + " ");
                } catch (Exception e) {
                    e.printStackTrace();
                    m.timestamp = System.currentTimeMillis() / 1000L;
                    Log.e("Caching", m.toString());
                    messageQueueHashMap.get(port).offer(m);
                }
            }
        }
        return uri;
    }

    @Override
    public boolean onCreate() {
        // TODO Auto-generated method stub
        Log.e("..", "Creation");
        try {
            tel = (TelephonyManager) this.getContext().getSystemService(Context.TELEPHONY_SERVICE);
            portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
            myPort = Integer.parseInt(portStr);
            for (int port : portsToSend) {
                String nodeHash = genHash(Integer.toString(port >> 1));
                allNodeHashMap.put(nodeHash, port >> 1);
                messageQueueHashMap.put(port >> 1, new LinkedList<Message>());
            }

            ServerSocket serverSocket = new ServerSocket(10000);
            new Server().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);

            Log.e("Ring status", "------");

            for (String key : allNodeHashMap.keySet())
                Log.e(key, allNodeHashMap.get(key) + "");

            Log.e("Ring status", "------");

            boolean check = new File(getContext().getFilesDir(), "myJSON.json").exists();
            JSONObject jsonObject;

            if (check)
                jsonObject = new JSONObject(getJSONAsString());
            else
                jsonObject = new JSONObject();

            new Recovery().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, jsonObject).get();
            operationHandler(Operation.RECOVER, null, null);
            recoveryMode.set(false);

        } catch (Exception e) {
            if (print)
                e.printStackTrace();
        }

        return true;
    }

    public void recover(JSONObject jsonObject) {
        Message m = new Message(myPort, "RECOVER", " ", " ", 0);
        ArrayList<Message> messageArrayList = new ArrayList<Message>();
        for (int port : portsToSend) {
            try {
                if (port == myPort * 2)
                    continue;

                Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), port);
                socket.setSoTimeout(TIMEOUT);
                DataOutputStream dataOutputStream = new DataOutputStream(socket.getOutputStream());
                dataOutputStream.writeUTF(m.toString());
                dataOutputStream.flush();

                DataInputStream dataInputStream = new DataInputStream(socket.getInputStream());
                String reply = dataInputStream.readUTF();

                if (reply.equals(""))
                    continue;

                String[] messages = reply.split("\\|");

                for (String message : messages) {
                    messageArrayList.add(deserialize(message));
                }

            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        Collections.sort(messageArrayList, new Comparator<Message>() {
            @Override
            public int compare(Message lhs, Message rhs) {
                return (int) (lhs.timestamp - rhs.timestamp);
            }
        });
        try {
            for (Message message : messageArrayList) {
                Log.e("Recovery msgs", message.toString());
                if (message.status.equals("INSERT"))
                    jsonObject.put(message.key, message.value);

                else if (message.status.equals("DELETE"))
                    jsonObject.remove(message.key);
            }
        }
        catch (Exception e) {e.printStackTrace();}


        writeToJSON(jsonObject);
        return;
    }

    private MatrixCursor jsonToCursor(String jsonString, MatrixCursor matrixCursor){
        JSONObject jsonObject = null;
        if(matrixCursor == null)
            matrixCursor = new MatrixCursor(new String[]{"key", "value"});

        try {
            jsonObject = new JSONObject(jsonString);
        } catch (JSONException e) {
            e.printStackTrace();
        }
        Iterator<String> keys = jsonObject.keys();
        while (keys.hasNext()) {
            String currKey = keys.next();
            Object[] row = null;
            try {
                row = new Object[]{currKey, jsonObject.get(currKey)};
            } catch (JSONException e) {
                e.printStackTrace();
            }
            matrixCursor.addRow(row);
        }
        return matrixCursor;
    }

    @Override
    public Cursor query(Uri uri, String[] projection, String selection,
                        String[] selectionArgs, String sortOrder) {
        try {
            if(selection.equals("@")){
                return operationHandler(Operation.QUERY, selection, null);
            } else if (selection.equals("*")) {

                List<Cursor> cursorList = new LinkedList<Cursor>();
                for (int port : portsToSend) {
                    if (port >> 1 == myPort) {
                        cursorList.add(operationHandler(Operation.QUERY, "@", null));
                    }
                    else {
                        Socket socket = addOrGetSocket(port);
                        socket.setSoTimeout(TIMEOUT);

                        DataOutputStream dos = new DataOutputStream(socket.getOutputStream());
                        DataInputStream dis = new DataInputStream(socket.getInputStream());
                        try {
                            Message m = new Message(myPort, "QUERY", "*", " ", 0);
                            dos.writeUTF(m.toString());
                            dos.flush();
                            String jsonReply = dis.readUTF();
                            cursorList.add(jsonToCursor(jsonReply, null));

                        }
                        catch (Exception e) {}
                    }
                }

                MatrixCursor matrixCursor = new MatrixCursor(new String[]{"key", "value"});

                for (Cursor cursor: cursorList) {
                    cursor.moveToFirst();
                    do {
                        if(cursor.getCount() == 0)
                            break;
                        String key = cursor.getString(cursor.getColumnIndex("key"));
                        String val = cursor.getString(cursor.getColumnIndex("value"));
                        matrixCursor.addRow(new Object[]{key, val});
                    }
                    while(cursor.moveToNext());
                }
                return matrixCursor;

            } else {
                int[] portsToQuery = {0, 0, 0};
                try {portsToQuery = getNext(genHash(selection));} catch (Exception e){e.printStackTrace();}

                MatrixCursor matrixCursor = operationHandler(Operation.QUERY, selection, null);

                if (matrixCursor.getCount() != 0)
                    return matrixCursor;

                Message m = new Message(myPort, "QUERY", selection, " ", 0);

                for (int port: portsToQuery) {
                    if (port == myPort)
                        continue;
                    String res = "";
                    try {
                        Socket socket = addOrGetSocket(port * 2);
                        socket.setSoTimeout(TIMEOUT);
                        DataOutputStream dos = new DataOutputStream(socket.getOutputStream());
                        DataInputStream dis = new DataInputStream(socket.getInputStream());
                        Log.e("gonna query", m.toString() + " " + port);
                        dos.writeUTF(m.toString());
                        dos.flush();
                        res = dis.readUTF();
                    }
                    catch (Exception e) {e.printStackTrace();}
                    Log.e("Query one", m.toString() + res);
                    if (!res.equals(" ") && !res.equals("")) {
                        Object[] row = {selection, res};
                        matrixCursor.addRow(row);
                    }
                }
                return matrixCursor;
            }
        } catch (Exception e) {
            if (print)
                e.printStackTrace();
        }
        return null;
    }
    public String matrixCursorToString(MatrixCursor matrixCursor) {
        JSONObject jsonObject = new JSONObject();
        matrixCursor.moveToFirst();
        do {
            String key = matrixCursor.getString(matrixCursor.getColumnIndex("key"));
            String value = matrixCursor.getString(matrixCursor.getColumnIndex("value"));
            try {
                jsonObject.put(key, value);
            }
            catch (Exception j) {j.printStackTrace();}
        } while (matrixCursor.moveToNext());
        return jsonObject.toString();
    }
    public void handleMessage(Message m, DataOutputStream dos) {
        try {
            if (m.status.equals("INSERT")) {
                if (recoveryMode.get())
                    cacheQueue.offer(m);
                else
                    operationHandler(Operation.INSERT, m.key, m.value);
                dos.writeUTF(" ");
                dos.flush();
            }

            else if (m.status.equals("QUERY")) {
                Log.e("query", m.key);
                String toSend = matrixCursorToString(operationHandler(Operation.QUERY, "@", null));
                if (m.key.equals("*")) {
                    dos.writeUTF(toSend);
                    dos.flush();
                } else {
                    JSONObject jsonObject = new JSONObject(toSend);
                    if (jsonObject.has(m.key))
                        dos.writeUTF((String) jsonObject.get(m.key));
                    else
                        dos.writeUTF(" ");
                    dos.flush();
                }
            }

            else if (m.status.equals("DELETE"))
                operationHandler(Operation.DELETE, m.key, null);

            else if (m.status.equals("RECOVER")) {
                StringBuilder toSend = new StringBuilder();
                Queue<Message> messageQueue = messageQueueHashMap.get(m.myPort);
                latestRecovery = m.myPort;
                while (!messageQueue.isEmpty()) {
                    Message head = messageQueue.poll();
                    toSend.append(head.toString());

                    if (!messageQueue.isEmpty())
                        toSend.append("|");
                }
                Log.e("Recovery for", m.myPort + " " + toSend.toString());
                dos.writeUTF(toSend.toString());
                dos.flush();
            }
        }
        catch (Exception e) {e.printStackTrace();}
    }

    @Override
    public int update(Uri uri, ContentValues values, String selection,
                      String[] selectionArgs) {
        // TODO Auto-generated method stub
        return 0;
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

    private class Server extends AsyncTask<ServerSocket, Void, Void> {

        private class ServerThread implements Runnable {
            Socket socket;

            public ServerThread(Socket socket) {
                this.socket = socket;
            }

            @Override
            public void run() {
                DataInputStream dis = null;
                InputStream is;
                Message m;
                DataOutputStream dos = null;
                OutputStream os;

                try {
                    os = socket.getOutputStream();
                    is = socket.getInputStream();
                    dis = new DataInputStream(is);
                    dos = new DataOutputStream(os);
                } catch (Exception e) {
                    if (print)
                        e.printStackTrace();
                }

                try {
                    m = deserialize(dis.readUTF());
                    handleMessage(m, dos);
                } catch (Exception e) {
                    if (print)
                        e.printStackTrace();
                }

            }
        }

        @Override
        protected Void doInBackground(ServerSocket... serverSockets) {

            ServerSocket serverSocket = serverSockets[0];
            try {
                while (true) {
                    Socket socket = serverSocket.accept();
                    new Thread(new ServerThread(socket)).start();
                }

            } catch (Exception e) {
                if (print)
                    e.printStackTrace();
            }
            return null;
        }
    }

    private class Recovery extends AsyncTask<JSONObject, Void, Void> {
        @Override
        protected Void doInBackground(JSONObject... jsonObjects) {
            recover(jsonObjects[0]);
            return null;
        }
    }
}

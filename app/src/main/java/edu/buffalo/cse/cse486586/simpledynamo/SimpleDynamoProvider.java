package edu.buffalo.cse.cse486586.simpledynamo;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.database.SQLException;
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteOpenHelper;
import android.net.Uri;
import android.telephony.TelephonyManager;
import android.util.Log;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Formatter;
import java.util.HashMap;
import java.util.concurrent.Semaphore;

public class SimpleDynamoProvider extends ContentProvider {

    static final String TAG = SimpleDynamoActivity.class.getSimpleName();
    static final int TIMEOUT = 1500;
    static final int SERVER_PORT = 10000;
    static final int[] nodes = {5554, 5556, 5558, 5560, 5562};
    static final String selectAllGlobal = "\"*\"";
    static final String selectAllLocal = "\"@\"";
    private static final String dbName = "keyValue.db";
    private final Semaphore operationComplete = new Semaphore(1, true);
    private final Semaphore startUpSync = new Semaphore(1, true);
    private HashMap<String, String> queryResult = null;
    private HashMap<String,String> local= null;
    private HashMap<String,String> replicas1=null;
    private HashMap<String,String> replicas2=null;
    private ArrayList<DynamoNode> dynamoNodes = null;
    private DynamoNode me, successor, successor2, predecessor;
    private MainDatabaseHelper mOpenHelper;
    private SQLiteDatabase db;
    private int myPortInt;
    private boolean inRecovery = false;

    @Override
    public int delete(Uri uri, String selection, String[] selectionArgs) {
        try {
            startUpSync.acquire();
        } catch (InterruptedException e) {
            Log.e(TAG, "Sync Exception");
        }
        //long startTime = System.currentTimeMillis();
        //Log.v(TAG, "deleteRequest Key = " + selection);
        String sqlLiteSelection;
        if (selection.equals(selectAllGlobal) || selection.equals(selectAllLocal)) {
            sqlLiteSelection = null;
        } else {
            sqlLiteSelection = " key = '" + selection + "'";
        }
        //Log.v(TAG, "delete=" + sqlLiteSelection);

        if ((sqlLiteSelection != null && isInMyRange(selection) == true) || (sqlLiteSelection == null && selection.equals(selectAllLocal))) {
            try {
                operationComplete.acquire();
            } catch (InterruptedException e) {
                Log.e(TAG, "Sync Exception", e);
            }
            try {
                db.delete("chats", sqlLiteSelection, null);
                SimpleDynamoMessage delete_replica_1 = new SimpleDynamoMessage(MessageType.DELETE_REPLICA_1);
                delete_replica_1.setDestinationNode(successor);
                delete_replica_1.setSelection(selection);
                delete_replica_1.setKey("Source = " + me.toString());
                sendTo(delete_replica_1);
            } catch (SQLException e) {
                Log.e(TAG, "query Error", e);
            }
            try {
                operationComplete.acquire();
            } catch (InterruptedException e) {
                Log.e(TAG, "Sync Exception", e);
            }
            try {
                SimpleDynamoMessage delete_replica_2 = new SimpleDynamoMessage(MessageType.DELETE_REPLICA_2);
                delete_replica_2.setDestinationNode(successor2);
                delete_replica_2.setSelection(selection);
                delete_replica_2.setKey("Source = " + me.toString());
                sendTo(delete_replica_2);
            } catch (SQLException e) {
                Log.e(TAG, "query Error", e);
            }
            try {
                operationComplete.acquire();
            } catch (InterruptedException e) {
                Log.e(TAG, "Sync Exception", e);
            }
            operationComplete.release();
            //Log.v(TAG, "Deleted in " + (System.currentTimeMillis() - startTime) + " ms");
            startUpSync.release();
            return 0;
        } else if (sqlLiteSelection == null && selection.equals(selectAllGlobal)) {
            try {
                db.delete("chats", sqlLiteSelection, null);
            } catch (SQLException e) {
                Log.e(TAG, "query Error", e);
            }
            try {
                db.delete("chats1", sqlLiteSelection, null);
            } catch (SQLException e) {
                Log.e(TAG, "query Error", e);
            }
            try {
                db.delete("chats2", sqlLiteSelection, null);
            } catch (SQLException e) {
                Log.e(TAG, "query Error", e);
            }
            for (int i = 0; i < dynamoNodes.size(); i++) {
                if (dynamoNodes.get(i).equals(me) == false) {
                    SimpleDynamoMessage deleteAll = new SimpleDynamoMessage(MessageType.DELETE);
                    deleteAll.setDestinationNode(dynamoNodes.get(i));
                    deleteAll.setSelection(selection);
                    deleteAll.setKey("Source = " + me.toString());
                    try {
                        operationComplete.acquire();
                    } catch (InterruptedException e) {
                        Log.e(TAG, "Sync Exception", e);
                    }
                    sendTo(deleteAll);
                    try {
                        operationComplete.acquire();
                    } catch (InterruptedException e) {
                        Log.e(TAG, "Sync Exception", e);
                    }
                    operationComplete.release();
                }
            }
            //Log.v(TAG, "Deleted in " + (System.currentTimeMillis() - startTime) + " ms");
            startUpSync.release();
            return 0;
        } else if (sqlLiteSelection != null && isInMyRange(selection) == false) {
            try {
                operationComplete.acquire();
            } catch (InterruptedException e) {
                Log.e(TAG, "Sync Exception", e);
            }
            SimpleDynamoMessage deleteRemote = new SimpleDynamoMessage(MessageType.DELETE);
            deleteRemote.setDestinationNode(selectNode(selection));
            deleteRemote.setSelection(selection);
            deleteRemote.setKey("Source = " + me.toString());
            sendTo(deleteRemote);
            try {
                operationComplete.acquire();
            } catch (InterruptedException e) {
                Log.e(TAG, "Sync Exception", e);
            }
            if (getSuccessorNode(selectNode(selection)).equals(me) == false) {
                SimpleDynamoMessage deleteRemote_replicas_1 = new SimpleDynamoMessage(MessageType.DELETE_REPLICA_1);
                deleteRemote_replicas_1.setDestinationNode(getSuccessorNode(selectNode(selection)));
                deleteRemote_replicas_1.setSelection(selection);
                deleteRemote_replicas_1.setKey("Source = " + me.toString());
                sendTo(deleteRemote_replicas_1);
                try {
                    operationComplete.acquire();
                } catch (InterruptedException e) {
                    Log.e(TAG, "Sync Exception", e);
                }
            } else {
                //Log.v(TAG,"Deleting Replica1 Locally");
                try {
                    db.delete("chats1", sqlLiteSelection, null);
                } catch (SQLException e) {
                    Log.e(TAG, "query Error", e);
                }
            }
            if (getSuccessorNode(getSuccessorNode(selectNode(selection))).equals(me) == false) {
                SimpleDynamoMessage deleteRemote_replicas_2 = new SimpleDynamoMessage(MessageType.DELETE_REPLICA_2);
                deleteRemote_replicas_2.setDestinationNode(getSuccessorNode(getSuccessorNode(selectNode(selection))));
                deleteRemote_replicas_2.setSelection(selection);
                deleteRemote_replicas_2.setKey("Source = " + me.toString());
                sendTo(deleteRemote_replicas_2);
                try {
                    operationComplete.acquire();
                } catch (InterruptedException e) {
                    Log.e(TAG, "Sync Exception", e);
                }
            } else {
                //Log.v(TAG,"Deleting Replica2 Locally");
                try {
                    db.delete("chats2", sqlLiteSelection, null);
                } catch (SQLException e) {
                    Log.e(TAG, "query Error", e);
                }
            }
            operationComplete.release();
            //Log.v(TAG, "Deleted in " + (System.currentTimeMillis() - startTime) + " ms");
            startUpSync.release();
            return 0;
        }
        return -1;
    }

    @Override
    public String getType(Uri uri) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Uri insert(Uri uri, ContentValues values) {
        try {
            startUpSync.acquire();
        } catch (InterruptedException e) {
            Log.e(TAG, "Sync Exception");
        }
        //long startTime = System.currentTimeMillis();
        //Log.v(TAG, "insertRequest Key = " + values.getAsString("key") + " || Value " + values.getAsString("value"));

        if (isInMyRange(values.getAsString("key"))) {
            long resultOfQuery = 0;
            try {
                operationComplete.acquire();
            } catch (InterruptedException e) {
                Log.e(TAG, "Sync Exception", e);
            }
            resultOfQuery = db.insertWithOnConflict("chats", null, values, SQLiteDatabase.CONFLICT_REPLACE);
            if (resultOfQuery == -1) {
                Log.e(TAG, "Insert Failed");
            } else {
                //Log.v(TAG, "Insert Success");
                SimpleDynamoMessage insert_replica_1 = new SimpleDynamoMessage(MessageType.INSERT_REPLICA_1);
                insert_replica_1.setDestinationNode(successor);
                insert_replica_1.setKey(values.getAsString("key"));
                insert_replica_1.setValue(values.getAsString("value"));
                insert_replica_1.setSelection("Source = " + me.toString());
                sendTo(insert_replica_1);
                try {
                    operationComplete.acquire();
                } catch (InterruptedException e) {
                    Log.e(TAG, "Sync Exception", e);
                }
                SimpleDynamoMessage insert_replica_2 = new SimpleDynamoMessage(MessageType.INSERT_REPLICA_2);
                insert_replica_2.setDestinationNode(successor2);
                insert_replica_2.setKey(values.getAsString("key"));
                insert_replica_2.setValue(values.getAsString("value"));
                insert_replica_2.setSelection("Source = " + me.toString());
                sendTo(insert_replica_2);
                try {
                    operationComplete.acquire();
                } catch (InterruptedException e) {
                    Log.e(TAG, "Sync Exception", e);
                }
                operationComplete.release();
                //    Log.v(TAG, "insertedKey = " + values.getAsString("key") + " || insertedValue " + values.getAsString("value"));
            }
            //Log.v(TAG, "Inserted in " + (System.currentTimeMillis() - startTime) + " ms");
            startUpSync.release();
            return uri;
        } else {
            try {
                operationComplete.acquire();
            } catch (InterruptedException e) {
                Log.e(TAG, "Sync Exception", e);
            }
            SimpleDynamoMessage insert = new SimpleDynamoMessage(MessageType.INSERT);
            insert.setKey(values.getAsString("key"));
            insert.setValue(values.getAsString("value"));
            insert.setDestinationNode(selectNode(values.getAsString("key")));
            insert.setSelection("Source = " + me.toString());
            sendTo(insert);
            try {
                operationComplete.acquire();
            } catch (InterruptedException e) {
                Log.e(TAG, "Sync Exception", e);
            }
            if (getSuccessorNode(selectNode(values.getAsString("key"))).equals(me) == false) {
                SimpleDynamoMessage insert_replicas_1 = new SimpleDynamoMessage(MessageType.INSERT_REPLICA_1);
                insert_replicas_1.setKey(values.getAsString("key"));
                insert_replicas_1.setValue(values.getAsString("value"));
                insert_replicas_1.setSelection("Source = " + me.toString());
                insert_replicas_1.setDestinationNode(getSuccessorNode(selectNode(values.getAsString("key"))));
                sendTo(insert_replicas_1);
                try {
                    operationComplete.acquire();
                } catch (InterruptedException e) {
                    Log.e(TAG, "Sync Exception", e);
                }
            } else {
                //Log.v(TAG,"Inserting Replica1 Locally");
                db.insertWithOnConflict("chats1", null, values, SQLiteDatabase.CONFLICT_REPLACE);
            }
            if (getSuccessorNode(getSuccessorNode(selectNode(values.getAsString("key")))).equals(me) == false) {
                SimpleDynamoMessage insert_replicas_2 = new SimpleDynamoMessage(MessageType.INSERT_REPLICA_2);
                insert_replicas_2.setKey(values.getAsString("key"));
                insert_replicas_2.setValue(values.getAsString("value"));
                insert_replicas_2.setSelection("Source = " + me.toString());
                insert_replicas_2.setDestinationNode(getSuccessorNode(getSuccessorNode(selectNode(values.getAsString("key")))));
                sendTo(insert_replicas_2);
                try {
                    operationComplete.acquire();
                } catch (InterruptedException e) {
                    Log.e(TAG, "Sync Exception", e);
                }
            } else {
                //Log.v(TAG,"Inserting Replica2 Locally");
                db.insertWithOnConflict("chats2", null, values, SQLiteDatabase.CONFLICT_REPLACE);
            }
            operationComplete.release();
            //Log.v(TAG, "Inserted in " + (System.currentTimeMillis() - startTime) + " ms");
            startUpSync.release();
            return uri;
        }
    }


    @Override
    public boolean onCreate() {
        //Log.v(TAG, "OnCreate");
        mOpenHelper = new MainDatabaseHelper(getContext());
        TelephonyManager tel = (TelephonyManager) getContext().getSystemService(Context.TELEPHONY_SERVICE);
        String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
        myPortInt = Integer.parseInt(portStr);
        try {
            startUpSync.acquire();
        } catch (InterruptedException e) {
            Log.e(TAG, "Sync Exception");
        }
        Thread startUp = new Thread(new startUp());
        startUp.start();
        return true;
    }

    @Override
    public Cursor query(Uri uri, String[] projection, String selection, String[] selectionArgs, String sortOrder) {
        try {
            startUpSync.acquire();
        } catch (InterruptedException e) {
            Log.e(TAG, "Sync Exception");
        }
        //long startTime = System.currentTimeMillis();
        //Log.v(TAG, "queryRequest Key = " + selection);
        String columns[] = {"key", "value"};
        Cursor cursorToReturn = null;
        String sqlLiteSelection;
        if (selection.equals(selectAllGlobal) || selection.equals(selectAllLocal)) {
            sqlLiteSelection = null;
        } else {
            sqlLiteSelection = " key = '" + selection + "'";
        }
        //Log.v(TAG, "query=" + sqlLiteSelection);

        if (sqlLiteSelection != null && isInMyRange(selection) == true) {
            try {
                cursorToReturn = db.query("chats", columns, sqlLiteSelection, null, null, null, null);
            } catch (Exception e) {
                Log.e(TAG, "query Error");
            }
            //Log.v(TAG, "Retrieved from me in " + (System.currentTimeMillis() - startTime) + " ms");
            startUpSync.release();
            return cursorToReturn;
        } else if (sqlLiteSelection == null && selection.equals(selectAllLocal)) {
            MatrixCursor returnCursor = new MatrixCursor(new String[]{"key", "value"});
            try {
                cursorToReturn = db.query("chats", columns, sqlLiteSelection, null, null, null, null);
                if (cursorToReturn != null && cursorToReturn.moveToFirst()) {
                    do {
                        returnCursor.newRow().add("key", cursorToReturn.getString(cursorToReturn.getColumnIndex("key"))).add("value", cursorToReturn.getString(cursorToReturn.getColumnIndex("value")));
                    } while (cursorToReturn.moveToNext());
                }
                cursorToReturn.close();
            } catch (SQLException e) {
                Log.e(TAG, "query Error", e);
            }
            try {
                cursorToReturn = db.query("chats1", columns, sqlLiteSelection, null, null, null, null);
                if (cursorToReturn != null && cursorToReturn.moveToFirst()) {
                    do {
                        returnCursor.newRow().add("key", cursorToReturn.getString(cursorToReturn.getColumnIndex("key"))).add("value", cursorToReturn.getString(cursorToReturn.getColumnIndex("value")));
                    } while (cursorToReturn.moveToNext());
                }
                cursorToReturn.close();
            } catch (SQLException e) {
                Log.e(TAG, "query Error", e);
            }
            try {
                cursorToReturn = db.query("chats2", columns, sqlLiteSelection, null, null, null, null);
                if (cursorToReturn != null && cursorToReturn.moveToFirst()) {
                    do {
                        returnCursor.newRow().add("key", cursorToReturn.getString(cursorToReturn.getColumnIndex("key"))).add("value", cursorToReturn.getString(cursorToReturn.getColumnIndex("value")));
                    } while (cursorToReturn.moveToNext());
                }
                cursorToReturn.close();
            } catch (SQLException e) {
                Log.e(TAG, "query Error", e);
            }
            //Log.v(TAG, "Retrieved from me in " + (System.currentTimeMillis() - startTime) + " ms");
            startUpSync.release();
            return returnCursor;
        } else if (sqlLiteSelection == null && selection.equals(selectAllGlobal)) {
            MatrixCursor returnCursor = new MatrixCursor(new String[]{"key", "value"});
            for (int i = 0; i < nodes.length; i++) {
                if (dynamoNodes.get(i).equals(me) == false) {
                    try {
                        operationComplete.acquire();
                    } catch (InterruptedException e) {
                        Log.e(TAG, "Sync Exception", e);
                    }
                    queryResult = null;
                    SimpleDynamoMessage query = new SimpleDynamoMessage(MessageType.QUERY);
                    query.setKey("Source = " + me.toString());
                    query.setSelection(selectAllLocal);
                    query.setDestinationNode(dynamoNodes.get(i));

                    boolean success = sendTo(query);
                    try {
                        operationComplete.acquire();
                    } catch (InterruptedException e) {
                        Log.e(TAG, "Sync Exception", e);
                    }
                    if (success == true) {
                        String keys[] = new String[queryResult.size()];
                        queryResult.keySet().toArray(keys);
                        for (int j = 0; j < keys.length; j++) {
                            returnCursor.newRow().add("key", keys[j]).add("value", queryResult.get(keys[j]));
                        }
                        operationComplete.release();
                    } else if (getSuccessorNode(dynamoNodes.get(i)).equals(me) == false) {
                        SimpleDynamoMessage query_replicas_1 = new SimpleDynamoMessage(MessageType.QUERY_REPLICA_1);
                        query_replicas_1.setSelection(selectAllLocal);
                        query_replicas_1.setDestinationNode(getSuccessorNode(dynamoNodes.get(i)));
                        query_replicas_1.setKey("Source = " + me.toString());
                        success = sendTo(query_replicas_1);
                        try {
                            operationComplete.acquire();
                        } catch (InterruptedException e) {
                            Log.e(TAG, "Sync Exception", e);
                        }
                        if (success == true) {
                            String keys[] = new String[queryResult.size()];
                            queryResult.keySet().toArray(keys);
                            for (int j = 0; j < keys.length; j++) {
                                returnCursor.newRow().add("key", keys[j]).add("value", queryResult.get(keys[j]));
                            }
                            operationComplete.release();
                        } else if (getSuccessorNode(getSuccessorNode(dynamoNodes.get(i))).equals(me) == false) {
                            SimpleDynamoMessage query_replicas_2 = new SimpleDynamoMessage(MessageType.QUERY_REPLICA_2);
                            query_replicas_2.setSelection(selectAllLocal);
                            query_replicas_2.setDestinationNode(getSuccessorNode(getSuccessorNode(dynamoNodes.get(i))));
                            query_replicas_2.setKey("Source = " + me.toString());
                            success = sendTo(query_replicas_2);
                            try {
                                operationComplete.acquire();
                            } catch (InterruptedException e) {
                                Log.e(TAG, "Sync Exception", e);
                            }
                            if (success == true) {
                                String keys[] = new String[queryResult.size()];
                                queryResult.keySet().toArray(keys);
                                for (int j = 0; j < keys.length; j++) {
                                    returnCursor.newRow().add("key", keys[j]).add("value", queryResult.get(keys[j]));
                                }
                                operationComplete.release();
                            } else {
                                //Log.v(TAG, "GlobalQueryFailed");
                                operationComplete.release();
                                startUpSync.release();
                                return null;
                            }
                        } else {
                            //Log.v(TAG,"Querying All Replicas2 Locally");
                            try {
                                cursorToReturn = db.query("chats2", columns, sqlLiteSelection, null, null, null, null);

                                if (cursorToReturn != null && cursorToReturn.moveToFirst()) {
                                    do {
                                        returnCursor.newRow().add("key", cursorToReturn.getString(cursorToReturn.getColumnIndex("key"))).add("value", cursorToReturn.getString(cursorToReturn.getColumnIndex("value")));
                                    } while (cursorToReturn.moveToNext());
                                }
                            } catch (SQLException e) {
                                Log.e(TAG, "query Error", e);
                            }
                            operationComplete.release();
                        }
                    } else {
                        //Log.v(TAG,"Querying All Replicas1 Locally");
                        try {
                            cursorToReturn = db.query("chats1", columns, sqlLiteSelection, null, null, null, null);

                            if (cursorToReturn != null && cursorToReturn.moveToFirst()) {
                                do {
                                    returnCursor.newRow().add("key", cursorToReturn.getString(cursorToReturn.getColumnIndex("key"))).add("value", cursorToReturn.getString(cursorToReturn.getColumnIndex("value")));
                                } while (cursorToReturn.moveToNext());
                            }
                        } catch (SQLException e) {
                            Log.e(TAG, "query Error", e);
                        }
                        operationComplete.release();
                    }
                }
            }
            try {
                cursorToReturn = db.query("chats", columns, sqlLiteSelection, null, null, null, null);

                if (cursorToReturn != null && cursorToReturn.moveToFirst()) {
                    do {
                        returnCursor.newRow().add("key", cursorToReturn.getString(cursorToReturn.getColumnIndex("key"))).add("value", cursorToReturn.getString(cursorToReturn.getColumnIndex("value")));
                    } while (cursorToReturn.moveToNext());
                }
            } catch (SQLException e) {
                Log.e(TAG, "query Error", e);
            }

            //Log.v(TAG, "Retrieved from all nodes in " + (System.currentTimeMillis() - startTime) + " ms");
            startUpSync.release();
            return returnCursor;
        } else if (sqlLiteSelection != null && isInMyRange(selection) == false) {
            MatrixCursor returnCursor = new MatrixCursor(new String[]{"key", "value"});
            try {
                operationComplete.acquire();
            } catch (InterruptedException e) {
                Log.e(TAG, "Sync Exception", e);
            }
            queryResult = null;
            SimpleDynamoMessage query = new SimpleDynamoMessage(MessageType.QUERY);
            query.setSelection(selection);
            query.setDestinationNode(selectNode(selection));
            query.setKey("Source = " + me.toString());
            boolean success = sendTo(query);
            try {
                operationComplete.acquire();
            } catch (InterruptedException e) {
                Log.e(TAG, "Sync Exception", e);
            }
            if (success == true) {
                operationComplete.release();
                String keys[] = new String[queryResult.size()];
                queryResult.keySet().toArray(keys);
                for (int i = 0; i < keys.length; i++) {
                    returnCursor.newRow().add("key", keys[i]).add("value", queryResult.get(keys[i]));
                }
            } else if (getSuccessorNode(selectNode(selection)).equals(me) == false) {
                queryResult = null;
                SimpleDynamoMessage query_replicas_1 = new SimpleDynamoMessage(MessageType.QUERY_REPLICA_1);
                query_replicas_1.setSelection(selection);
                query_replicas_1.setDestinationNode(getSuccessorNode(selectNode(selection)));
                query_replicas_1.setKey("Source = " + me.toString());
                success = sendTo(query_replicas_1);
                try {
                    operationComplete.acquire();
                } catch (InterruptedException e) {
                    Log.e(TAG, "Sync Exception", e);
                }
                if (success == true) {
                    operationComplete.release();
                    String keys[] = new String[queryResult.size()];
                    queryResult.keySet().toArray(keys);
                    for (int i = 0; i < keys.length; i++) {
                        returnCursor.newRow().add("key", keys[i]).add("value", queryResult.get(keys[i]));
                    }
                } else if (getSuccessorNode(getSuccessorNode(selectNode(selection))).equals(me) == false) {
                    queryResult = null;
                    SimpleDynamoMessage query_replicas_2 = new SimpleDynamoMessage(MessageType.QUERY_REPLICA_2);
                    query_replicas_2.setSelection(selection);
                    query_replicas_2.setDestinationNode(getSuccessorNode(getSuccessorNode(selectNode(selection))));
                    query_replicas_2.setKey("Source = " + me.toString());
                    success = sendTo(query_replicas_2);
                    try {
                        operationComplete.acquire();
                    } catch (InterruptedException e) {
                        Log.e(TAG, "Sync Exception", e);
                    }
                    if (success == true) {
                        operationComplete.release();
                        //Log.v(TAG, queryResult.toString());
                        String keys[] = new String[queryResult.size()];
                        queryResult.keySet().toArray(keys);
                        for (int i = 0; i < keys.length; i++) {
                            returnCursor.newRow().add("key", keys[i]).add("value", queryResult.get(keys[i]));
                        }
                    } else {
                        operationComplete.release();
                        //Log.v(TAG, "QueryFailed");
                        startUpSync.release();
                        return null;
                    }
                } else {
                    //Log.v(TAG,"Querying Replicas2 Locally");
                    try {
                        cursorToReturn = db.query("chats2", columns, sqlLiteSelection, null, null, null, null);
                        if (cursorToReturn != null && cursorToReturn.moveToFirst()) {
                            do {
                                returnCursor.newRow().add("key", cursorToReturn.getString(cursorToReturn.getColumnIndex("key"))).add("value", cursorToReturn.getString(cursorToReturn.getColumnIndex("value")));
                            } while (cursorToReturn.moveToNext());
                        }
                        cursorToReturn.close();
                    } catch (SQLException e) {
                        Log.e(TAG, "query Error", e);
                    }
                    operationComplete.release();
                }
            } else {
                //Log.v(TAG,"Querying Replicas1 Locally");
                try {
                    cursorToReturn = db.query("chats1", columns, sqlLiteSelection, null, null, null, null);
                    if (cursorToReturn != null && cursorToReturn.moveToFirst()) {
                        do {
                            returnCursor.newRow().add("key", cursorToReturn.getString(cursorToReturn.getColumnIndex("key"))).add("value", cursorToReturn.getString(cursorToReturn.getColumnIndex("value")));
                        } while (cursorToReturn.moveToNext());
                    }
                    cursorToReturn.close();
                } catch (SQLException e) {
                    Log.e(TAG, "query Error", e);
                }
                operationComplete.release();
            }
            //Log.v(TAG, "Retrieved remotely in " + (System.currentTimeMillis() - startTime) + " ms");
            startUpSync.release();
            return returnCursor;
        }
        Log.e(TAG, "Should Not Reach Here");
        return null;
    }

    @Override
    public int update(Uri uri, ContentValues values, String selection, String[] selectionArgs) {
        // TODO Auto-generated method stub
        return 0;
    }

    private String genHash(String input) {
        try {
            MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
            byte[] sha1Hash = sha1.digest(input.getBytes());
            Formatter formatter = new Formatter();
            for (byte b : sha1Hash) {
                formatter.format("%02x", b);
            }
            return formatter.toString();
        } catch (NoSuchAlgorithmException e) {
            Log.e(TAG, "genHash Failed");
        }
        return null;
    }

    private boolean isInMyRange(String input) {
        String hashedInput = genHash(input);
        if (hashedInput.compareTo(predecessor.getID()) > 0 && hashedInput.compareTo(me.getID()) <= 0) {
            //Log.v(TAG, "isInMyRange = " + hashedInput + " pre = " + predecessor.getID() + " me = " + me.getID());
            return true;
        } else if (predecessor.getID().compareTo(me.getID()) > 0 && hashedInput.compareTo(predecessor.getID()) > 0 && hashedInput.compareTo(me.getID()) > 0) {
            //Log.v(TAG, "isInMyRange = " + hashedInput + " pre = " + predecessor.getID() + " me = " + me.getID());
            return true;
        } else if (predecessor.getID().compareTo(me.getID()) > 0 && hashedInput.compareTo(predecessor.getID()) < 0 && hashedInput.compareTo(me.getID()) <= 0) {
            //Log.v(TAG, "isInMyRange = " + hashedInput + " pre = " + predecessor.getID() + " me = " + me.getID());
            return true;
        } else {
            //Log.v(TAG, "isNotInMyRange = " + hashedInput + " pre = " + predecessor.getID() + " me = " + me.getID());
            return false;
        }
    }

    private DynamoNode selectNode(String input) {
        String hashedInput = genHash(input);
        DynamoNode tempNode = new DynamoNode(-1, hashedInput);
        ArrayList<DynamoNode> temp = new ArrayList<>(nodes.length + 1);
        temp.addAll(dynamoNodes);
        temp.add(tempNode);
        Collections.sort(temp, new DynamoNode.NodeComparator());
        //Log.v(TAG, "selectNode = " + temp.toString());
        int tempNodeIndex = temp.indexOf(tempNode);
        if (tempNodeIndex == temp.size() - 1) {
            return temp.get(0);
        } else {
            return temp.get(tempNodeIndex + 1);
        }
    }

    private DynamoNode getSuccessorNode(DynamoNode node) {
        int nodeIndex = dynamoNodes.indexOf(node);
        if (nodeIndex == dynamoNodes.size() - 1) {
            return dynamoNodes.get(0);
        } else {
            return dynamoNodes.get(nodeIndex + 1);
        }
    }

    private DynamoNode getPredecessorNode(DynamoNode node) {
        int nodeIndex = dynamoNodes.indexOf(node);
        if (nodeIndex == 0) {
            return dynamoNodes.get(dynamoNodes.size() - 1);
        } else {
            return dynamoNodes.get(nodeIndex - 1);
        }
    }

    protected static final class MainDatabaseHelper extends SQLiteOpenHelper {

        private static final String SQL_CREATE_MAIN = "CREATE TABLE chats (_ID INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL UNIQUE, key TEXT UNIQUE, value TEXT)";
        private static final String SQL_CREATE_MAIN_1 = "CREATE TABLE chats1 (_ID INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL UNIQUE, key TEXT UNIQUE, value TEXT)";
        private static final String SQL_CREATE_MAIN_2 = "CREATE TABLE chats2 (_ID INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL UNIQUE, key TEXT UNIQUE, value TEXT)";
        private static final String SQL_CREATE_TEST = "CREATE TABLE failureTest (_ID INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL UNIQUE, key TEXT UNIQUE)";

        MainDatabaseHelper(Context context) {
            super(context, dbName, null, 1);
        }

        @Override
        public void onCreate(SQLiteDatabase db) {
            //Log.v(TAG, "OnCreate of SQLiteOpenHelper");
            db.execSQL(SQL_CREATE_MAIN);
            db.execSQL(SQL_CREATE_MAIN_1);
            db.execSQL(SQL_CREATE_MAIN_2);
            db.execSQL(SQL_CREATE_TEST);
        }

        @Override
        public void onUpgrade(SQLiteDatabase db, int oldVersion, int newVersion) {
            Log.e(TAG, "XXX");
        }
    }

    private class startUp implements Runnable {

        @Override
        public void run() {
            Log.v(TAG, "StartUp Thread");
            db = mOpenHelper.getWritableDatabase();
            dynamoNodes = new ArrayList<>(nodes.length);
            local=new HashMap<>();
            replicas1=new HashMap<>();
            replicas2=new HashMap<>();
            for (int i = 0; i < nodes.length; i++) {
                DynamoNode temp = new DynamoNode(nodes[i] * 2, genHash(Integer.toString(nodes[i])));
                dynamoNodes.add(temp);
                if (myPortInt == nodes[i]) {
                    me = temp;
                }
            }
            Collections.sort(dynamoNodes, new DynamoNode.NodeComparator());
            int myIndex = dynamoNodes.indexOf(me);
            if (myIndex == 0) {
                predecessor = dynamoNodes.get(dynamoNodes.size() - 1);
                successor = dynamoNodes.get(myIndex + 1);
                successor2 = dynamoNodes.get(myIndex + 2);
            } else if (myIndex == dynamoNodes.size() - 1) {
                predecessor = dynamoNodes.get(myIndex - 1);
                successor = dynamoNodes.get(0);
                successor2 = dynamoNodes.get(1);
            } else if (myIndex == dynamoNodes.size() - 2) {
                predecessor = dynamoNodes.get(myIndex - 1);
                successor = dynamoNodes.get(myIndex + 1);
                successor2 = dynamoNodes.get(0);
            } else {
                predecessor = dynamoNodes.get(myIndex - 1);
                successor = dynamoNodes.get(myIndex + 1);
                successor2 = dynamoNodes.get(myIndex + 2);
            }
            //(TAG, "RING = " + dynamoNodes.toString());
            //Log.v(TAG, "Pre = " + predecessor.toString());
            //Log.v(TAG, "Mee = " + me.toString());
            //Log.v(TAG, "Suc = " + successor.toString());
            //Log.v(TAG, "Su2 = " + successor2.toString());

            ContentValues failureTestKeyVal = new ContentValues();
            failureTestKeyVal.put("key", "failureTestKey");
            long insertResult = -1;
            try {
                insertResult = db.insertOrThrow("failureTest", null, failureTestKeyVal);
            } catch (SQLException e) {
                Log.e(TAG, "Caught", e);
            }
            //Log.v(TAG, "insertResult = " + insertResult);
            if (insertResult == 1) {
                //Log.v(TAG, "Fresh Start");
                Thread requestThread = new Thread(new RequestServer());
                requestThread.start();
            } else {
                inRecovery = true;
                local.clear();
                replicas1.clear();
                replicas2.clear();
                Thread requestThread = new Thread(new RequestServer());
                requestThread.start();
                //long startTime = System.currentTimeMillis();
                //Log.v(TAG, "Starting Recovery ");
                queryResult = null;
                SimpleDynamoMessage recover_local_keys = new SimpleDynamoMessage(MessageType.QUERY_REPLICA_1);
                recover_local_keys.setDestinationNode(successor);
                recover_local_keys.setSelection(selectAllLocal);
                recover_local_keys.setKey("L-RecoverySource = " + me.toString());
                try {
                    operationComplete.acquire();
                } catch (InterruptedException e) {
                    Log.e(TAG, "Sync Exception", e);
                }
                boolean success = sendTo(recover_local_keys);
                try {
                    operationComplete.acquire();
                } catch (InterruptedException e) {
                    Log.e(TAG, "Sync Exception", e);
                }
                if (success == true) {
                    operationComplete.release();
                    if (queryResult != null) {
                        String keys[] = new String[queryResult.size()];
                        queryResult.keySet().toArray(keys);
                        for (int i = 0; i < keys.length; i++) {
                            if(local.containsKey(keys[i])==false) {
                                ContentValues values = new ContentValues();
                                values.put("key", keys[i]);
                                values.put("value", queryResult.get(keys[i]));
                                db.insertWithOnConflict("chats", null, values, SQLiteDatabase.CONFLICT_REPLACE);
                            } else {
                                //Log.v(TAG,"NotReplacing "+keys[i]);
                            }
                        }
                    }
                } else {
                    //Log.v(TAG,"Try 1 Failed");
                    SimpleDynamoMessage recover_local_keys_2 = new SimpleDynamoMessage(MessageType.QUERY_REPLICA_2);
                    recover_local_keys_2.setDestinationNode(successor2);
                    recover_local_keys_2.setSelection(selectAllLocal);
                    recover_local_keys_2.setKey("L-RecoverySource = " + me.toString());
                    success = sendTo(recover_local_keys_2);
                    try {
                        operationComplete.acquire();
                    } catch (InterruptedException e) {
                        Log.e(TAG, "Sync Exception", e);
                    }
                    if (success == true) {
                        operationComplete.release();
                        if (queryResult != null) {
                            String keys[] = new String[queryResult.size()];
                            queryResult.keySet().toArray(keys);
                            for (int i = 0; i < keys.length; i++) {
                                if(local.containsKey(keys[i])==false) {
                                    ContentValues values = new ContentValues();
                                    values.put("key", keys[i]);
                                    values.put("value", queryResult.get(keys[i]));
                                    db.insertWithOnConflict("chats", null, values, SQLiteDatabase.CONFLICT_REPLACE);
                                }else {
                                    //Log.v(TAG,"NotReplacing "+keys[i]);
                                }
                            }
                        }
                    } else {
                        operationComplete.release();
                        //Log.v(TAG, "RecoveryOfLocalKeysFailed");
                    }
                }

                queryResult = null;
                SimpleDynamoMessage recover_replicas_1 = new SimpleDynamoMessage(MessageType.QUERY);
                recover_replicas_1.setDestinationNode(predecessor);
                recover_replicas_1.setSelection(selectAllLocal);
                recover_replicas_1.setKey("R1-RecoverySource = " + me.toString());
                try {
                    operationComplete.acquire();
                } catch (InterruptedException e) {
                    Log.e(TAG, "Sync Exception", e);
                }
                success = sendTo(recover_replicas_1);
                try {
                    operationComplete.acquire();
                } catch (InterruptedException e) {
                    Log.e(TAG, "Sync Exception", e);
                }
                if (success == true) {
                    operationComplete.release();
                    if (queryResult != null) {
                        String keys[] = new String[queryResult.size()];
                        queryResult.keySet().toArray(keys);
                        for (int i = 0; i < keys.length; i++) {
                            if(replicas1.containsKey(keys[i])==false) {
                                ContentValues values = new ContentValues();
                                values.put("key", keys[i]);
                                values.put("value", queryResult.get(keys[i]));
                                db.insertWithOnConflict("chats1", null, values, SQLiteDatabase.CONFLICT_REPLACE);
                            }else {
                                //Log.v(TAG,"NotReplacing "+keys[i]);
                            }
                        }
                    }
                } else {
                    //Log.v(TAG,"Try 1 Failed");
                    SimpleDynamoMessage recover_replicas_1_2 = new SimpleDynamoMessage(MessageType.QUERY_REPLICA_2);
                    recover_replicas_1_2.setDestinationNode(successor);
                    recover_replicas_1_2.setSelection(selectAllLocal);
                    recover_replicas_1_2.setKey("R1-RecoverySource = " + me.toString());
                    try {
                        operationComplete.acquire();
                    } catch (InterruptedException e) {
                        Log.e(TAG, "Sync Exception", e);
                    }
                    success = sendTo(recover_replicas_1_2);
                    try {
                        operationComplete.acquire();
                    } catch (InterruptedException e) {
                        Log.e(TAG, "Sync Exception", e);
                    }
                    if (success == true) {
                        operationComplete.release();
                        if (queryResult != null) {
                            String keys[] = new String[queryResult.size()];
                            queryResult.keySet().toArray(keys);
                            for (int i = 0; i < keys.length; i++) {
                                if(replicas1.containsKey(keys[i])==false) {
                                    ContentValues values = new ContentValues();
                                    values.put("key", keys[i]);
                                    values.put("value", queryResult.get(keys[i]));
                                    db.insertWithOnConflict("chats1", null, values, SQLiteDatabase.CONFLICT_REPLACE);
                                }else {
                                    //Log.v(TAG,"NotReplacing "+keys[i]);
                                }
                            }
                        }
                    } else {
                        operationComplete.release();
                        //Log.v(TAG, "RecoveryOfReplicas_1KeysFailed");
                    }
                }

                queryResult = null;
                SimpleDynamoMessage recovery_replicas_2 = new SimpleDynamoMessage(MessageType.QUERY);
                recovery_replicas_2.setDestinationNode(getPredecessorNode(predecessor));
                recovery_replicas_2.setSelection(selectAllLocal);
                recovery_replicas_2.setKey("R2-RecoverySource = " + me.toString());
                //Log.v(TAG, "ToSend=" + recovery_replicas_2.toString());
                try {
                    operationComplete.acquire();
                } catch (InterruptedException e) {
                    Log.e(TAG, "Sync Exception", e);
                }
                success = sendTo(recovery_replicas_2);
                try {
                    operationComplete.acquire();
                } catch (InterruptedException e) {
                    Log.e(TAG, "Sync Exception", e);
                }
                if (success == true) {
                    operationComplete.release();
                    if (queryResult != null) {
                        String keys[] = new String[queryResult.size()];
                        queryResult.keySet().toArray(keys);
                        for (int i = 0; i < keys.length; i++) {
                            if(replicas2.containsKey(keys[i])==false) {
                                ContentValues values = new ContentValues();
                                values.put("key", keys[i]);
                                values.put("value", queryResult.get(keys[i]));
                                db.insertWithOnConflict("chats2", null, values, SQLiteDatabase.CONFLICT_REPLACE);
                            }else {
                                //Log.v(TAG,"NotReplacing "+keys[i]);
                            }
                        }
                    }
                } else {
                    //Log.v(TAG,"Try 1 Failed");
                    SimpleDynamoMessage recovery_replicas_2_2 = new SimpleDynamoMessage(MessageType.QUERY_REPLICA_1);
                    recovery_replicas_2_2.setDestinationNode(predecessor);
                    recovery_replicas_2_2.setSelection(selectAllLocal);
                    recovery_replicas_2_2.setKey("R2-RecoverySource = " + me.toString());
                    //(TAG, "ToSend=" + recovery_replicas_2_2.toString());
                    try {
                        operationComplete.acquire();
                    } catch (InterruptedException e) {
                        Log.e(TAG, "Sync Exception", e);
                    }
                    success = sendTo(recovery_replicas_2_2);
                    try {
                        operationComplete.acquire();
                    } catch (InterruptedException e) {
                        Log.e(TAG, "Sync Exception", e);
                    }
                    if (success == true) {
                        operationComplete.release();
                        if (queryResult != null) {
                            String keys[] = new String[queryResult.size()];
                            queryResult.keySet().toArray(keys);
                            for (int i = 0; i < keys.length; i++) {
                                if(replicas2.containsKey(keys[i])==false) {
                                    ContentValues values = new ContentValues();
                                    values.put("key", keys[i]);
                                    values.put("value", queryResult.get(keys[i]));
                                    db.insertWithOnConflict("chats2", null, values, SQLiteDatabase.CONFLICT_REPLACE);
                                }else {
                                    //Log.v(TAG,"NotReplacing "+keys[i]);
                                }
                            }
                        }
                    } else {
                        operationComplete.release();
                        //Log.v(TAG, "RecoveryOfReplicas_2KeysFailed");
                    }
                }
                //Log.v(TAG, "Recovery Complete in " + (System.currentTimeMillis() - startTime) + " ms");
                inRecovery = false;
            }
            startUpSync.release();

        }
    }

    private class RequestServer implements Runnable {

        @Override
        public void run() {
            //Log.v(TAG, "RequestServer Started");
            while (true) {
                ServerSocket serverSocket;
                Socket inSocket;
                ObjectOutputStream out;
                ObjectInputStream in;
                try {
                    serverSocket = new ServerSocket(SERVER_PORT);
                    serverSocket.setReuseAddress(true);
                    while (true) {
                        try {
                            inSocket = serverSocket.accept();
                            //long startTime = System.currentTimeMillis();
                            inSocket.setReuseAddress(true);
                            //inSocket.setSoTimeout(TIMEOUT);
                            in = new ObjectInputStream(inSocket.getInputStream());
                            SimpleDynamoMessage receivedMessage = null;
                            receivedMessage = (SimpleDynamoMessage) in.readObject();
                            if(receivedMessage!=null && inRecovery==false){
                                out = new ObjectOutputStream(inSocket.getOutputStream());
                                replyHandler(receivedMessage,out);
                            } else if(receivedMessage!=null && inRecovery==true){
                                out = new ObjectOutputStream(inSocket.getOutputStream());
                                if(receivedMessage.getMessageType()==MessageType.INSERT){
                                    local.put(receivedMessage.getKey(),receivedMessage.getValue());
                                    replyHandler(receivedMessage,out);
                                } else if (receivedMessage.getMessageType()==MessageType.INSERT_REPLICA_1) {
                                    replicas1.put(receivedMessage.getKey(),receivedMessage.getValue());
                                    replyHandler(receivedMessage,out);
                                } else if (receivedMessage.getMessageType()==MessageType.INSERT_REPLICA_2) {
                                    replicas2.put(receivedMessage.getKey(),receivedMessage.getValue());
                                    replyHandler(receivedMessage,out);
                                } else if(receivedMessage.getMessageType()==MessageType.DELETE){
                                    local.put(receivedMessage.getSelection(),null);
                                    replyHandler(receivedMessage,out);
                                } else if(receivedMessage.getMessageType()==MessageType.DELETE_REPLICA_1){
                                    replicas1.put(receivedMessage.getSelection(),null);
                                    replyHandler(receivedMessage,out);
                                } else if(receivedMessage.getMessageType()==MessageType.DELETE_REPLICA_2){
                                    replicas2.put(receivedMessage.getSelection(),null);
                                    replyHandler(receivedMessage,out);
                                } else {
                                    continue;
                                }
                            }
                            //Log.v(TAG,"Request processed in "+(System.currentTimeMillis()-startTime)+" ms");
                        } catch (IOException e) {
                            Log.e(TAG, "Can't accept connection", e);
                        } catch (ClassNotFoundException e) {
                            Log.e(TAG, "ClassNotFoundException", e);
                        }
                    }
                } catch (IOException e) {
                    Log.e(TAG, "ServerSocket IOException", e);
                }
            }
        }

        private void replyHandler(SimpleDynamoMessage receivedMessage, ObjectOutputStream out) throws IOException {
            //Log.v(TAG, "Handling" + receivedMessage.toString());
            if (receivedMessage.getMessageType() == MessageType.INSERT && receivedMessage.getDestinationNode().equals(me)) {
                ContentValues mContentValues = new ContentValues();
                mContentValues.put("key", receivedMessage.getKey());
                mContentValues.put("value", receivedMessage.getValue());
                Long resultOfQuery = db.insertWithOnConflict("chats", null, mContentValues, SQLiteDatabase.CONFLICT_REPLACE);
                if (resultOfQuery == -1) {
                    Log.e(TAG, "Insert Failed");
                } else {
                    //Log.v(TAG, "Insert Success");
                    //Log.v(TAG, "insertedKey = " + mContentValues.getAsString("key") + " || insertedValue " + mContentValues.getAsString("value"));
                    SimpleDynamoMessage insertReply = null;
                    insertReply = new SimpleDynamoMessage(MessageType.INSERT_DELETE_REPLY);
                    out.writeObject(insertReply);
                    Log.v(TAG, "Sent=" + insertReply.toString());
                }
                //Log.v(TAG, "END........................................................");
                return;
            } else if (receivedMessage.getMessageType() == MessageType.INSERT_REPLICA_1 && receivedMessage.getDestinationNode().equals(me)) {
                ContentValues mContentValues = new ContentValues();
                mContentValues.put("key", receivedMessage.getKey());
                mContentValues.put("value", receivedMessage.getValue());
                //Log.v(TAG, "insertReplica_1Request Key = " + mContentValues.getAsString("key") + " || Value " + mContentValues.getAsString("value"));
                long resultOfQuery = 0;
                resultOfQuery = db.insertWithOnConflict("chats1", null, mContentValues, SQLiteDatabase.CONFLICT_REPLACE);
                if (resultOfQuery == -1) {
                    Log.e(TAG, "Insert Failed");
                } else {
                    //Log.v(TAG, "Insert Success");
                    //Log.v(TAG, "insertedReplica_1Key = " + mContentValues.getAsString("key") + " || insertedValue " + mContentValues.getAsString("value"));
                    SimpleDynamoMessage insertReply = null;
                    insertReply = new SimpleDynamoMessage(MessageType.INSERT_DELETE_REPLY);
                    out.writeObject(insertReply);
                    Log.v(TAG, "Sent=" + insertReply.toString());
                }
                //Log.v(TAG, "END........................................................");
                return;
            } else if (receivedMessage.getMessageType() == MessageType.INSERT_REPLICA_2 && receivedMessage.getDestinationNode().equals(me)) {
                ContentValues mContentValues = new ContentValues();
                mContentValues.put("key", receivedMessage.getKey());
                mContentValues.put("value", receivedMessage.getValue());
                //Log.v(TAG, "insertReplica_2Request Key = " + mContentValues.getAsString("key") + " || Value " + mContentValues.getAsString("value"));
                long resultOfQuery = 0;

                resultOfQuery = db.insertWithOnConflict("chats2", null, mContentValues, SQLiteDatabase.CONFLICT_REPLACE);
                if (resultOfQuery == -1) {
                    //Log.v(TAG, "Insert Failed");
                } else {
                    //Log.v(TAG, "Insert Success");
                    //Log.v(TAG, "insertedReplica_2Key = " + mContentValues.getAsString("key") + " || insertedValue " + mContentValues.getAsString("value"));
                    SimpleDynamoMessage insertReply = null;
                    insertReply = new SimpleDynamoMessage(MessageType.INSERT_DELETE_REPLY);
                    out.writeObject(insertReply);
                    //Log.v(TAG, "Sent=" + insertReply.toString());
                }
                //Log.v(TAG, "END........................................................");
                return;
            } else if (receivedMessage.getMessageType() == MessageType.DELETE && receivedMessage.getDestinationNode().equals(me)) {
                //Log.v(TAG, "deleteRequest Key = " + receivedMessage.getSelection());
                String sqlLiteSelection;
                if (receivedMessage.getSelection().equals(selectAllGlobal) || receivedMessage.getSelection().equals(selectAllLocal)) {
                    sqlLiteSelection = null;
                } else {
                    sqlLiteSelection = " key = '" + receivedMessage.getSelection() + "'";
                }
                //Log.v(TAG, "delete query=" + sqlLiteSelection);
                if (receivedMessage.getSelection().equals(selectAllGlobal)) {
                    //Log.v(TAG, "is a global delete");
                    try {
                        db.delete("chats", sqlLiteSelection, null);
                    } catch (SQLException e) {
                        Log.e(TAG, "query Error", e);
                    }
                    try {
                        db.delete("chats1", sqlLiteSelection, null);
                    } catch (SQLException e) {
                        Log.e(TAG, "query Error", e);
                    }
                    try {
                        db.delete("chats2", sqlLiteSelection, null);
                    } catch (SQLException e) {
                        Log.e(TAG, "query Error", e);
                    }
                } else if (sqlLiteSelection != null && isInMyRange(receivedMessage.getSelection()) == true) {
                    try {
                        db.delete("chats", sqlLiteSelection, null);
                    } catch (Exception e) {
                        Log.e(TAG, "query Error");
                    }
                } else if (sqlLiteSelection == null && receivedMessage.getSelection().equals(selectAllLocal)) {
                    try {
                        db.delete("chats", sqlLiteSelection, null);
                    } catch (Exception e) {
                        Log.e(TAG, "query Error");
                    }
                }
                SimpleDynamoMessage deleteReply = null;
                deleteReply = new SimpleDynamoMessage(MessageType.INSERT_DELETE_REPLY);
                out.writeObject(deleteReply);
                //Log.v(TAG, "Sent=" + deleteReply.toString());
                //Log.v(TAG, "END........................................................");
                return;
            } else if (receivedMessage.getMessageType() == MessageType.DELETE_REPLICA_1 && receivedMessage.getDestinationNode().equals(me)) {
                //Log.v(TAG, "deleteReplica_1Request Key = " + receivedMessage.getSelection());
                String sqlLiteSelection;
                if (receivedMessage.getSelection().equals(selectAllLocal)) {
                    sqlLiteSelection = null;
                } else {
                    sqlLiteSelection = " key = '" + receivedMessage.getSelection() + "'";
                }
                //Log.v(TAG, "query=" + sqlLiteSelection);

                if (sqlLiteSelection != null) {
                    try {
                        db.delete("chats1", sqlLiteSelection, null);
                    } catch (SQLException e) {
                        Log.e(TAG, "query Error", e);
                    }
                } else if (sqlLiteSelection == null && receivedMessage.getSelection().equals(selectAllLocal)) {
                    try {
                        db.delete("chats1", sqlLiteSelection, null);
                    } catch (SQLException e) {
                        Log.e(TAG, "query Error", e);
                    }
                }
                SimpleDynamoMessage deleteReply = null;
                deleteReply = new SimpleDynamoMessage(MessageType.INSERT_DELETE_REPLY);
                out.writeObject(deleteReply);
                //Log.v(TAG, "Sent=" + deleteReply.toString());
                //Log.v(TAG, "END........................................................");
                return;
            } else if (receivedMessage.getMessageType() == MessageType.DELETE_REPLICA_2 && receivedMessage.getDestinationNode().equals(me)) {
                //Log.v(TAG, "deleteReplica_2Request Key = " + receivedMessage.getSelection());
                String sqlLiteSelection;
                if (receivedMessage.getSelection().equals(selectAllLocal)) {
                    sqlLiteSelection = null;
                } else {
                    sqlLiteSelection = " key = '" + receivedMessage.getSelection() + "'";
                }
                //Log.v(TAG, "query=" + sqlLiteSelection);

                if (sqlLiteSelection != null) {
                    try {
                        db.delete("chats2", sqlLiteSelection, null);
                    } catch (SQLException e) {
                        Log.e(TAG, "query Error", e);
                    }
                } else if (sqlLiteSelection == null && receivedMessage.getSelection().equals(selectAllLocal)) {
                    try {
                        db.delete("chats2", sqlLiteSelection, null);
                    } catch (SQLException e) {
                        Log.e(TAG, "query Error", e);
                    }
                }
                SimpleDynamoMessage deleteReply = null;
                deleteReply = new SimpleDynamoMessage(MessageType.INSERT_DELETE_REPLY);
                out.writeObject(deleteReply);
                //Log.v(TAG, "Sent=" + deleteReply.toString());
                //Log.v(TAG, "END........................................................");
                return;
            } else if (receivedMessage.getMessageType() == MessageType.QUERY && receivedMessage.getDestinationNode().equals(me)) {
                String sqlLiteSelection = null;
                if (receivedMessage.getSelection().equals(selectAllLocal)) {
                    HashMap<String, String> temp = new HashMap<>();
                    Cursor cursorToReturn = null;
                    String columns[] = {"key", "value"};
                    try {
                        cursorToReturn = db.query("chats", columns, sqlLiteSelection, null, null, null, null);

                        if (cursorToReturn != null && cursorToReturn.moveToFirst()) {
                            do {
                                temp.put(cursorToReturn.getString(cursorToReturn.getColumnIndex("key")), cursorToReturn.getString(cursorToReturn.getColumnIndex("value")));
                            } while (cursorToReturn.moveToNext());
                        }
                    } catch (Exception e) {
                        Log.e(TAG, "query Error");
                    }
                    SimpleDynamoMessage queryReply = new SimpleDynamoMessage(MessageType.QUERY_REPLY);
                    queryReply.setQueryResult(temp);
                    out.writeObject(queryReply);
                    //Log.v(TAG, "Sent=" + queryReply.toString());
                } else {
                    sqlLiteSelection = " key = '" + receivedMessage.getSelection() + "'";
                    HashMap<String, String> temp = new HashMap<>();
                    Cursor cursorToReturn = null;
                    String columns[] = {"key", "value"};
                    try {
                        cursorToReturn = db.query("chats", columns, sqlLiteSelection, null, null, null, null);
                        if (cursorToReturn != null && cursorToReturn.moveToFirst()) {
                            do {
                                temp.put(cursorToReturn.getString(cursorToReturn.getColumnIndex("key")), cursorToReturn.getString(cursorToReturn.getColumnIndex("value")));
                            } while (cursorToReturn.moveToNext());
                        }
                    } catch (Exception e) {
                        Log.e(TAG, "query Error");
                    }
                    SimpleDynamoMessage queryReply = new SimpleDynamoMessage(MessageType.QUERY_REPLY);
                    queryReply.setQueryResult(temp);
                    out.writeObject(queryReply);
                    //Log.v(TAG, "Sent=" + queryReply.toString());
                }
                //Log.v(TAG, "END........................................................");
                return;
            } else if (receivedMessage.getMessageType() == MessageType.QUERY_REPLICA_1 && receivedMessage.getDestinationNode().equals(me)) {
                String sqlLiteSelection = null;
                if (receivedMessage.getSelection().equals(selectAllLocal)) {
                    HashMap<String, String> temp = new HashMap<>();
                    Cursor cursorToReturn = null;
                    String columns[] = {"key", "value"};
                    try {
                        cursorToReturn = db.query("chats1", columns, sqlLiteSelection, null, null, null, null);

                        if (cursorToReturn != null && cursorToReturn.moveToFirst()) {
                            do {
                                temp.put(cursorToReturn.getString(cursorToReturn.getColumnIndex("key")), cursorToReturn.getString(cursorToReturn.getColumnIndex("value")));
                            } while (cursorToReturn.moveToNext());
                        }
                    } catch (SQLException e) {
                        Log.e(TAG, "query Error", e);
                    }
                    SimpleDynamoMessage queryReply = new SimpleDynamoMessage(MessageType.QUERY_REPLY);
                    queryReply.setQueryResult(temp);
                    out.writeObject(queryReply);
                    //Log.v(TAG, "Sent=" + queryReply.toString());
                } else {
                    sqlLiteSelection = " key = '" + receivedMessage.getSelection() + "'";
                    HashMap<String, String> temp = new HashMap<>();
                    Cursor cursorToReturn = null;
                    String columns[] = {"key", "value"};
                    try {
                        cursorToReturn = db.query("chats1", columns, sqlLiteSelection, null, null, null, null);

                        if (cursorToReturn != null && cursorToReturn.moveToFirst()) {
                            do {
                                temp.put(cursorToReturn.getString(cursorToReturn.getColumnIndex("key")), cursorToReturn.getString(cursorToReturn.getColumnIndex("value")));
                            } while (cursorToReturn.moveToNext());
                        }
                    } catch (Exception e) {
                        Log.e(TAG, "query Error");
                    }
                    SimpleDynamoMessage queryReply = new SimpleDynamoMessage(MessageType.QUERY_REPLY);
                    queryReply.setQueryResult(temp);
                    out.writeObject(queryReply);
                    //Log.v(TAG, "Sent=" + queryReply.toString());
                }
                //Log.v(TAG, "END........................................................");
                return;
            } else if (receivedMessage.getMessageType() == MessageType.QUERY_REPLICA_2 && receivedMessage.getDestinationNode().equals(me)) {
                String sqlLiteSelection = null;
                if (receivedMessage.getSelection().equals(selectAllLocal)) {
                    HashMap<String, String> temp = new HashMap<>();
                    Cursor cursorToReturn = null;
                    String columns[] = {"key", "value"};

                    try {
                        cursorToReturn = db.query("chats2", columns, sqlLiteSelection, null, null, null, null);

                        if (cursorToReturn != null && cursorToReturn.moveToFirst()) {
                            do {
                                temp.put(cursorToReturn.getString(cursorToReturn.getColumnIndex("key")), cursorToReturn.getString(cursorToReturn.getColumnIndex("value")));
                            } while (cursorToReturn.moveToNext());
                        }
                    } catch (SQLException e) {
                        Log.e(TAG, "query Error", e);
                    }
                    SimpleDynamoMessage queryReply = new SimpleDynamoMessage(MessageType.QUERY_REPLY);
                    queryReply.setQueryResult(temp);
                    out.writeObject(queryReply);
                    //Log.v(TAG, "Sent=" + queryReply.toString());
                } else {
                    sqlLiteSelection = " key = '" + receivedMessage.getSelection() + "'";
                    HashMap<String, String> temp = new HashMap<>();
                    Cursor cursorToReturn = null;
                    String columns[] = {"key", "value"};
                    try {
                        cursorToReturn = db.query("chats2", columns, sqlLiteSelection, null, null, null, null);
                        if (cursorToReturn != null && cursorToReturn.moveToFirst()) {
                            do {
                                temp.put(cursorToReturn.getString(cursorToReturn.getColumnIndex("key")), cursorToReturn.getString(cursorToReturn.getColumnIndex("value")));
                            } while (cursorToReturn.moveToNext());
                        }
                    } catch (Exception e) {
                        Log.e(TAG, "query Error");
                    }
                    SimpleDynamoMessage queryReply = new SimpleDynamoMessage(MessageType.QUERY_REPLY);
                    queryReply.setQueryResult(temp);
                    out.writeObject(queryReply);
                    //Log.v(TAG, "Sent=" + queryReply.toString());
                }
                //Log.v(TAG, "END........................................................");
                return;
            }
        }
    }

    private boolean sendTo(SimpleDynamoMessage messageToSend) {
        //long startTime = System.currentTimeMillis();
        //Log.v(TAG, "ToSend=" + messageToSend.toString());
        try {
            Socket senderSocket;
            senderSocket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), (messageToSend.getDestinationNode().getPort()));
            senderSocket.setReuseAddress(true);
            senderSocket.setSoTimeout(TIMEOUT);
            //Log.v(TAG, "Sending To = " + (messageToSend.getDestinationNode().getPort()) / 2);
            ObjectOutputStream out = new ObjectOutputStream(senderSocket.getOutputStream());
            out.writeObject(messageToSend);
            out.flush();
            ObjectInputStream in = new ObjectInputStream(senderSocket.getInputStream());
            SimpleDynamoMessage replyMessage = (SimpleDynamoMessage) in.readObject();
            if (replyMessage.getMessageType() == MessageType.INSERT_DELETE_REPLY || replyMessage.getMessageType() == MessageType.QUERY_REPLY) {
                //Log.v(TAG,"ReplyIn "+(System.currentTimeMillis()-startTime)+" ms");
                //Log.v(TAG,"Reply="+replyMessage.toString());
                if(replyMessage.getMessageType() == MessageType.QUERY_REPLY){
                   queryResult=(HashMap)replyMessage.getQueryResult().clone();
                }
                operationComplete.release();
                return true;
            }
        } catch (UnknownHostException f) {
            Log.e(TAG, "ClientTask UnknownHostException", f);
        } catch (SocketTimeoutException f) {
            Log.e(TAG, "ClientTask SocketTimeoutException", f);
            Log.e(TAG, "FailedNode = " + messageToSend.getDestinationNode().toString());
        } catch (IOException f) {
            Log.e(TAG, "ClientTask socket IOException", f);
            Log.e(TAG, "FailedNode = " + messageToSend.getDestinationNode().toString());
        } catch (ClassNotFoundException f) {
            Log.e(TAG, "ClientTask ClassNotFoundException", f);
        }
        operationComplete.release();
        return false;
    }
}

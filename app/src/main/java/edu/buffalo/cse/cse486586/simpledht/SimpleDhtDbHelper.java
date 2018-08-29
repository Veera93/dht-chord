package edu.buffalo.cse.cse486586.simpledht;

import android.content.Context;
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteOpenHelper;

import edu.buffalo.cse.cse486586.simpledht.SimpleDhtSchema.SimpleDhtDataEntry;

/**
 * Created by veera on 4/1/18.
 */

/*
 * Helper class that manages table creation and upgrades table if needed
 */

// From developers.android.com
public class SimpleDhtDbHelper extends SQLiteOpenHelper {

    public static final int DATABASE_VERSION = 1;
    public static final String DATABASE_NAME = "Chord.db";

    private static final String SQL_CREATE_ENTRIES =
            "CREATE TABLE " + SimpleDhtDataEntry.TABLE_NAME + " (" +
                    SimpleDhtDataEntry.COLUMN_NAME_KEY + " TEXT PRIMARY KEY," +
                    SimpleDhtDataEntry.COLUMN_NAME_VALUE + " TEXT)";

    private static final String SQL_DELETE_ENTRIES =
            "DROP TABLE IF EXISTS " + SimpleDhtDataEntry.TABLE_NAME;
    public SimpleDhtDbHelper(Context context) {
        super(context, DATABASE_NAME, null, DATABASE_VERSION);
    }

    @Override
    public void onCreate(SQLiteDatabase db) {
        db.execSQL(SQL_CREATE_ENTRIES);
    }

    @Override
    public void onUpgrade(SQLiteDatabase db, int oldVersion, int newVersion) {
        db.execSQL(SQL_DELETE_ENTRIES);
        onCreate(db);
    }
}

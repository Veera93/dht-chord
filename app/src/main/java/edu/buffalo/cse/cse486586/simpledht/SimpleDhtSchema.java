package edu.buffalo.cse.cse486586.simpledht;

import android.provider.BaseColumns;

/**
 * Created by veera on 2/16/18.
 */

/*
 * Defines the schema of the Table
 */

//From: developers.android.com
public final class SimpleDhtSchema {
    // To prevent someone from accidentally instantiating the contract class,
    // make the constructor private.
    private SimpleDhtSchema() {}

    /* Inner class that defines the table contents */
    public static class SimpleDhtDataEntry implements BaseColumns {
        public static final String TABLE_NAME = "message";
        public static final String COLUMN_NAME_KEY = "key";
        public static final String COLUMN_NAME_VALUE = "value";
    }
}

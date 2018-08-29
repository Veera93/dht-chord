package edu.buffalo.cse.cse486586.simpledht;

import android.database.Cursor;

import java.util.HashSet;
import java.util.Set;

/**
 * Created by veera on 4/3/18.
 */

public class QueryData {
    String key;
    Cursor value;
    Set<Cursor> temp = new HashSet<Cursor>();
    boolean isLocked;
}

package edu.buffalo.cse.cse486586.simpledht;

import android.content.ContentResolver;
import android.content.ContentValues;
import android.database.Cursor;
import android.net.Uri;
import android.os.AsyncTask;
import android.util.Log;
import android.view.View;
import android.view.View.OnClickListener;
import android.widget.TextView;

public class OnGDumpClickListener implements OnClickListener {

    private static final String TAG = OnGDumpClickListener.class.getName();

    private final TextView mTextView;
    private final ContentResolver mContentResolver;
    private final Uri mUri;

    public OnGDumpClickListener(TextView _tv, ContentResolver _cr) {
        mTextView = _tv;
        mContentResolver = _cr;
        mUri = buildUri("content", "edu.buffalo.cse.cse486586.simpledht.provider");
    }

    private Uri buildUri(String scheme, String authority) {
        Uri.Builder uriBuilder = new Uri.Builder();
        uriBuilder.authority(authority);
        uriBuilder.scheme(scheme);
        return uriBuilder.build();
    }

    @Override
    public void onClick(View v) {
        new Task().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR);
    }

    private class Task extends AsyncTask<Void, String, Void> {

        @Override
        protected Void doInBackground(Void... params) {
            if (testQuery()) {
                publishProgress("Query success\n");
            } else {
                publishProgress("Query fail\n");
            }
            return null;
        }

        protected void onProgressUpdate(String...strings) {
            mTextView.append(strings[0]);

            return;
        }

        private boolean testQuery() {
            try {
                Cursor resultCursor = mContentResolver.query(mUri, null, "*", null, null);
                    if (resultCursor == null) {
                        Log.e(TAG, "Result null");
                        throw new Exception();
                    }
                while (resultCursor.moveToNext()) {
                    int valueIndex = resultCursor.getColumnIndex(SimpleDhtSchema.SimpleDhtDataEntry.COLUMN_NAME_VALUE);
                    int keyIndex = resultCursor.getColumnIndex(SimpleDhtSchema.SimpleDhtDataEntry.COLUMN_NAME_KEY);
                    publishProgress("KEY: "+resultCursor.getString(keyIndex)+" VALUE: "+resultCursor.getString(valueIndex)+"\n");
                }

                    resultCursor.close();
            } catch (Exception e) {
                return false;
            }
            return true;
        }

        private boolean testDelete() {
            try {
                int deletecount = mContentResolver.delete(mUri, "key0" ,null);
                if(deletecount == 0) {
                    return false;
                } else {
                    return true;
                }
            } catch (Exception e) {
                return false;
            }
        }
    }
}

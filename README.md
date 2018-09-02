# Simple Distributed Hash Table

## Aim

The goal of this project is to develop a simplified chord. The system is responsible for chord ring formation, handle insert, delete and query operations based on chord protocol (without implementing finger tables and handle nodes leaving DHT or failing). Each emulator can be considered as a seperate node in chord sense.

In this project the following has been implemented,

1. ID space partitioning and re-partitioning using SHA-1 hash function
2. Ring based routing
3. Node joins to a DHT containing data


## Operations supported

1. Insert key-value pair
2. Query key
3. Query all keys in the current node
4. Query all keys in the chord
5. Delete key
6. Delete all keys in the current node
7. Delete all keys in the chord

## Testing

Points  : Testing Criteria 
1       : Local insert/query/delete operations work on a DHT containing a single AVD.
2       : The insert operation works correctly with static membership of 5 AVDs.
2       : The query operation works correctly with static membership of 5 AVDs.
2       : The insert operation works correctly with between 1 and 5 AVDs (and possibly changing membership).
2       : The query operation works correctly with between 1 and 5 AVDs (and possibly changing membership).
1       : The delete operation works correctly with between 1 and 5 AVDs (and possibly changing membership).

How to run the grading script: (Use python-2.7)

The python files and the grading scripts can be found under the scripts directory

1. Create AVD: python create_avd.py
2. Start AVD (5 AVDs): python run_avd.py 5
3. Start the emulator network using port 10000: python set_redir.py 10000
4. Test the APK by running the grading script and pass the APK file to the program. 
[grading_script] [apk_name].apk

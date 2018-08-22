# Chord-Based-DHT
Implemented the operations of insert, delete and query of keys in a Chord based Distributed Hash Table. Used SHA-1 hash function to generate IDs for the nodes and the keys. Handled node join in the appropriate position in the chord based on ID.

## Steps To Run The Program:
1. Use python-2.7 to run the commands
2. Create AVD:
```
python create_avd.py
python updateavd.py
```
3. Start the AVD:
```
python run_avd.py 5
```
4. Starting the emulator network:
```
python set_redir.py 10000
```
5. Test the program by running the grading script along with the build APK file of the program. (The grading is done in phase as mentioned below)
```
.\simpledht-grading.exe app-debug_dht.apk
```

## Testing Phase:
1. 1%: Local insert/query/delete operations work on a DHT containing a single AVD.
2. 2%: The insert operation works correctly with static membership of 5 AVDs.
3. 2%: The query operation works correctly with static membership of 5 AVDs.
4. 2%: The insert operation works correctly with between 1 and 5 AVDs (and possibly changing membership).
5. 2%: The query operation works correctly with between 1 and 5 AVDs (and possibly changing membership).
6. 1%: The delete operation works correctly with between 1 and 5 AVDs (and possibly changing membership).

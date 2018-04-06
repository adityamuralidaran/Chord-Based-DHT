package edu.buffalo.cse.cse486586.simpledht;

/**
 * Created by Aditya Subramanian Muralidaran on 4/5/2018.
 * NodeObject class to hold the information of the current AVD in Chord DHT
 */

public class NodeObject {
    public String myPort;
    public String myPort_hash;
    // Variables for predecessor
    public String prePort;
    public String prePort_hash;
    // Variables for successor
    public String sucPort;
    public String sucPort_hash;

    public NodeObject(String myPort, String myPort_hash, String prePort, String prePort_hash, String sucPort, String sucPort_hash) {
        this.myPort = myPort;
        this.myPort_hash = myPort_hash;
        this.prePort = prePort;
        this.prePort_hash = prePort_hash;
        this.sucPort = sucPort;
        this.sucPort_hash = sucPort_hash;
    }

    public NodeObject() { }

    public String getMyPort() {
        return myPort;
    }

    public void setMyPort(String myPort) {
        this.myPort = myPort;
    }

    public String getMyPort_hash() {
        return myPort_hash;
    }

    public void setMyPort_hash(String myPort_hash) {
        this.myPort_hash = myPort_hash;
    }

    public String getPrePort() {
        return prePort;
    }

    public void setPrePort(String prePort) {
        this.prePort = prePort;
    }

    public String getPrePort_hash() {
        return prePort_hash;
    }

    public void setPrePort_hash(String prePort_hash) {
        this.prePort_hash = prePort_hash;
    }

    public String getSucPort() {
        return sucPort;
    }

    public void setSucPort(String sucPort) {
        this.sucPort = sucPort;
    }

    public String getSucPort_hash() {
        return sucPort_hash;
    }

    public void setSucPort_hash(String sucPort_hash) {
        this.sucPort_hash = sucPort_hash;
    }
}

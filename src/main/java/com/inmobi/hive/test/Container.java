package com.inmobi.hive.test;

import java.io.PrintWriter;

/**
 * Created by panos on 1/7/2016.
 */

/* Class used to print a Container
   of an Exareme Plan
 */

public class Container {

    String name;
    String IP;
    int port;
    int data_transfer_port;

    public Container(String n, String ip, int p, int d){
        name = n;
        IP = ip;
        port = p;
        data_transfer_port = d;
    }

    public int getData_transfer_port() {
        return data_transfer_port;
    }

    public String getIP() {
        return IP;
    }

    public int getPort() {
        return port;
    }

    public String getName() {
        return name;
    }

    public void setIP(String IP) {
        this.IP = IP;
    }

    public void setName(String name) {
        this.name = name;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public void setData_transfer_port(int data_transfer_port) {
        this.data_transfer_port = data_transfer_port;
    }

    /*public void printContainer(PrintWriter outputFile){
        outputFile.println("\t{\n");
        outputFile.println("\t\tname")
    }*/

}

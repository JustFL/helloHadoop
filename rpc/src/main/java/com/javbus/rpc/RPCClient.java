package com.javbus.rpc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;

import java.io.IOException;
import java.net.InetSocketAddress;

public class RPCClient {
    public static void main(String[] args) throws IOException {

        RPCProtocol clinet = RPC.getProxy(RPCProtocol.class,
                RPCProtocol.versionID,
                new InetSocketAddress("localhost", 8888),
                new Configuration());

        clinet.mkdir("/hello");

    }
}

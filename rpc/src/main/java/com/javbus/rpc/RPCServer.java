package com.javbus.rpc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;

import java.io.IOException;

public class RPCServer implements RPCProtocol{

    @Override
    public void mkdir(String path) {
        System.out.println("服务器接收到请求" + ":" + path);
    }

    public static void main(String[] args) throws IOException {
        RPC.Server server = new RPC.Builder(new Configuration())
                .setBindAddress("localhost")
                .setPort(8888)
                .setInstance(new RPCServer())
                .setProtocol(RPCProtocol.class).build();

        server.start();
        System.out.println("服务器已经启动");
    }
}

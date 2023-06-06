package com.demo;

import com.googlecode.jsonrpc4j.JsonRpcServer;

import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;

/**
 * @author wang.yubin
 * @since 2023/6/6
 */
public class RpcServlet extends HttpServlet {

    private final JsonRpcServer rpcServer;

    public RpcServlet() {
        super();
        // JsonRpcServer会按照json-rpc请求，调用UserServiceImpl中的方法
        rpcServer = new JsonRpcServer(new UserServiceImpl(), UserService.class);
    }

    @Override
    protected void service(HttpServletRequest request, HttpServletResponse response) throws IOException {
        rpcServer.handle(request, response);
    }

}

package com.demo;

import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.eclipse.jetty.webapp.WebAppContext;

/**
 * @author wang.yubin
 * @since 2023/6/6
 */
public class Server {

    public static void main(String[] args) throws Throwable {

        // 服务器的监听端口
        org.eclipse.jetty.server.Server server = new org.eclipse.jetty.server.Server(9999);

        // 连接器和服务器互相关联
        server.addConnector(new ServerConnector(server));

        // 新建Servlet处理器
        ServletHandler servletHandler = new ServletHandler();
        ServletHolder servletHolder = servletHandler.addServletWithMapping(RpcServlet.class, "/*");
        servletHolder.setInitOrder(1);

        // 服务器-应用上下文-servlet互相关联
        ServletContextHandler context = new ServletContextHandler(server, "/", ServletContextHandler.SESSIONS);
        context.setServletHandler(servletHandler);
        server.setHandler(context);

        // 启动服务器
        server.start();
        server.join();
    }

    /**
     * 因web资源加载问题而弃用
     */
    private void xmlServer() throws Exception {
        // 服务器的监听端口
        org.eclipse.jetty.server.Server server = new org.eclipse.jetty.server.Server(9999);
        // 关联一个已经存在的上下文
        WebAppContext context = new WebAppContext();
        // 设置描述符位置
        context.setDescriptor("/dubbo-demo/json-rpc-demo/src/main/webapp/WEB-INF/web.xml");
        // 设置Web内容上下文路径
        context.setResourceBase("/dubbo-demo/json-rpc-demo/src/main/webapp");
        // 设置上下文路径
        context.setContextPath("/");
        context.setParentLoaderPriority(true);
        server.setHandler(context);
        server.start();
        server.join();
    }

}

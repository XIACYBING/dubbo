/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.dubbo.rpc.filter;

import org.apache.dubbo.common.extension.Activate;
import org.apache.dubbo.common.logger.Logger;
import org.apache.dubbo.common.logger.LoggerFactory;
import org.apache.dubbo.common.utils.ConfigUtils;
import org.apache.dubbo.common.utils.NamedThreadFactory;
import org.apache.dubbo.rpc.Filter;
import org.apache.dubbo.rpc.Invocation;
import org.apache.dubbo.rpc.Invoker;
import org.apache.dubbo.rpc.Result;
import org.apache.dubbo.rpc.RpcException;
import org.apache.dubbo.rpc.support.AccessLogData;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Iterator;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static org.apache.dubbo.common.constants.CommonConstants.PROVIDER;
import static org.apache.dubbo.rpc.Constants.ACCESS_LOG_KEY;

/**
 * 访问日志过滤器
 * <p>
 * Record access log for the service.
 * <p>
 * Logger key is <code><b>dubbo.accesslog</b></code>.
 * In order to configure access log appear in the specified appender only, additivity need to be configured in log4j's
 * config file, for example:
 * <code>
 * <pre>
 * &lt;logger name="<b>dubbo.accesslog</b>" <font color="red">additivity="false"</font>&gt;
 *    &lt;level value="info" /&gt;
 *    &lt;appender-ref ref="foo" /&gt;
 * &lt;/logger&gt;
 * </pre></code>
 */
@Activate(group = PROVIDER, value = ACCESS_LOG_KEY)
public class AccessLogFilter implements Filter {

    private static final Logger logger = LoggerFactory.getLogger(AccessLogFilter.class);

    /**
     * 日志记录器的key
     */
    private static final String LOG_KEY = "dubbo.accesslog";

    private static final String LINE_SEPARATOR = "line.separator";

    /**
     * 日志数据最大缓存条数，超过该条数时需要主动写入数据到日志文件中
     */
    private static final int LOG_MAX_BUFFER = 5000;

    /**
     * 日志写入任务的执行间隔，单位为毫秒
     */
    private static final long LOG_OUTPUT_INTERVAL = 5000;

    /**
     * 日志文件后缀中日期的格式
     */
    private static final String FILE_DATE_FORMAT = "yyyyMMdd";

    // It's safe to declare it as singleton since it runs on single thread only
    private static final DateFormat FILE_NAME_FORMATTER = new SimpleDateFormat(FILE_DATE_FORMAT);

    /**
     * 日志数据在内存中的缓存集合，当超过{@link #LOG_MAX_BUFFER}后，会主动被写入到日志文件中；或每{@link #LOG_OUTPUT_INTERVAL}毫秒后，会被{@link #LOG_SCHEDULED}调度器的任务写入到日志文件中
     */
    private static final Map<String, Queue<AccessLogData>> LOG_ENTRIES = new ConcurrentHashMap<>();

    /**
     * 日志调度池，用于定时将日志数据写入到文件中
     */
    private static final ScheduledExecutorService LOG_SCHEDULED =
        Executors.newSingleThreadScheduledExecutor(new NamedThreadFactory("Dubbo-Access-Log", true));

    /**
     * Default constructor initialize demon thread for writing into access log file with names with access log key
     * defined in url <b>accesslog</b>
     */
    public AccessLogFilter() {

        // 提交一个调度任务，每5000毫秒就将日志缓存数据刷新到日志文件中
        LOG_SCHEDULED.scheduleWithFixedDelay(this::writeLogToFile, LOG_OUTPUT_INTERVAL, LOG_OUTPUT_INTERVAL,
            TimeUnit.MILLISECONDS);
    }

    /**
     * This method logs the access log for service method invocation call.
     *
     * @param invoker service
     * @param inv     Invocation service method.
     * @return Result from service method.
     * @throws RpcException
     */
    @Override
    public Result invoke(Invoker<?> invoker, Invocation inv) throws RpcException {
        try {

            // 获取日志文件key，默认是true/default，也可以自定义key，自定义的key将会被作为日志数据缓存的key和日志文件的名称
            String accessLogKey = invoker.getUrl().getParameter(ACCESS_LOG_KEY);

            // 如果没有accessLogKey，则不记录日志
            if (ConfigUtils.isNotEmpty(accessLogKey)) {

                // 生成空日志，填充访问日志
                AccessLogData logData = AccessLogData.newLogData();
                logData.buildAccessLogData(invoker, inv);

                // 记录日志
                log(accessLogKey, logData);
            }
        } catch (Throwable t) {
            logger.warn("Exception in AccessLogFilter of service(" + invoker + " -> " + inv + ")", t);
        }
        return invoker.invoke(inv);
    }

    private void log(String accessLog, AccessLogData accessLogData) {

        // 获取日志缓存数据集合
        Queue<AccessLogData> logQueue = LOG_ENTRIES.computeIfAbsent(accessLog, k -> new ConcurrentLinkedQueue<>());

        // 日志缓存数据还没有超出限制条数，则直接将当前日志数据放入缓存中
        if (logQueue.size() < LOG_MAX_BUFFER) {
            logQueue.add(accessLogData);
        }

        // 否则需要先写入数据到日志文件，再保存当前日志数据
        else {
            logger.warn("AccessLog buffer is full. Do a force writing to file to clear buffer.");

            // 将当前accessLog对应的队列数据写入到文件中
            //just write current logQueue to file.
            writeLogQueueToFile(accessLog, logQueue);

            // 写入完成后，将当前访问日志添加到队列中
            //after force writing, add accessLogData to current logQueue
            logQueue.add(accessLogData);
        }
    }

    private void writeLogQueueToFile(String accessLog, Queue<AccessLogData> logQueue) {
        try {

            // 如果是默认key（true/default），则调用logger接口输出日志
            if (ConfigUtils.isDefault(accessLog)) {
                processWithServiceLogger(logQueue);
            }

            // 否则将日志数据写入到自定义的日志文件中
            else {
                File file = new File(accessLog);
                createIfLogDirAbsent(file);
                if (logger.isDebugEnabled()) {
                    logger.debug("Append log to " + accessLog);
                }
                renameFile(file);

                // 写入数据到自定义的日志文件中
                processWithAccessKeyLogger(logQueue, file);
            }
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
    }

    /**
     * 写入所有日志内存数据到日志文件中，用于定时调度器{@link #LOG_SCHEDULED}调度任务的执行
     */
    private void writeLogToFile() {

        // 日志内存数据集合不为空才处理
        if (!LOG_ENTRIES.isEmpty()) {

            // 循环日志内存数据，并调用方法写入数据
            for (Map.Entry<String, Queue<AccessLogData>> entry : LOG_ENTRIES.entrySet()) {
                String accessLog = entry.getKey();
                Queue<AccessLogData> logQueue = entry.getValue();
                writeLogQueueToFile(accessLog, logQueue);
            }
        }
    }

    private void processWithAccessKeyLogger(Queue<AccessLogData> logQueue, File file) throws IOException {

        // 包装日志文件对应的File为FileWriter
        try (FileWriter writer = new FileWriter(file, true)) {

            // 循环日志文件内存数据，每写入一条数据就移除一条数据，每条数据之间用LINE_SEPARATOR对应的分隔符去分割
            for (Iterator<AccessLogData> iterator = logQueue.iterator(); iterator.hasNext(); iterator.remove()) {
                writer.write(iterator.next().getLogMessage());
                writer.write(System.getProperty(LINE_SEPARATOR));
            }

            // 将数据刷到磁盘上
            writer.flush();
        }
    }

    /**
     * 根据当前正在使用的日志工厂，获取对应{@link #LOG_KEY}和{@link AccessLogData#getServiceName()}组装成的日志记录器，并调用接口输出访问日志
     *
     * @param logQueue 日志数据队列
     */
    private void processWithServiceLogger(Queue<AccessLogData> logQueue) {
        for (Iterator<AccessLogData> iterator = logQueue.iterator(); iterator.hasNext(); iterator.remove()) {
            AccessLogData logData = iterator.next();

            // 获取对应的日志记录器，并输出日志
            LoggerFactory.getLogger(LOG_KEY + "." + logData.getServiceName()).info(logData.getLogMessage());
        }
    }

    private void createIfLogDirAbsent(File file) {
        File dir = file.getParentFile();
        if (null != dir && !dir.exists()) {
            dir.mkdirs();
        }
    }

    private void renameFile(File file) {
        if (file.exists()) {
            String now = FILE_NAME_FORMATTER.format(new Date());
            String last = FILE_NAME_FORMATTER.format(new Date(file.lastModified()));
            if (!now.equals(last)) {
                File archive = new File(file.getAbsolutePath() + "." + last);
                file.renameTo(archive);
            }
        }
    }
}

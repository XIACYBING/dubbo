package org.apache.dubbo.config;

import lombok.extern.slf4j.Slf4j;
import org.apache.dubbo.common.extension.ExtensionLoader;
import org.apache.dubbo.common.threadpool.manager.ExecutorRepository;
import org.apache.dubbo.common.utils.ExecutorUtil;
import org.apache.dubbo.config.bootstrap.DubboBootstrap;
import org.apache.dubbo.config.bootstrap.DubboBootstrapStartStopListener;
import org.apache.dubbo.config.utils.ReferenceConfigCache;

import java.util.concurrent.Executor;
import java.util.concurrent.atomic.LongAdder;

/**
 * 输出异步代理的相关信息监听器
 *
 * @author wang.yubin
 * @since 2023/11/6
 */
@Slf4j
public class PrintAsyncProxyInfoListener implements DubboBootstrapStartStopListener {

    public volatile static boolean printed = false;
    public static final LongAdder NANO_ADDER = new LongAdder();
    public static final LongAdder INIT_NANO_ADDER = new LongAdder();

    public static final Executor EXECUTOR =
        ExtensionLoader.getExtensionLoader(ExecutorRepository.class).getDefaultExtension().getSharedExecutor();

    @Override
    public void onStart(DubboBootstrap bootstrap) {

        // 单纯从refer数量判断当前应用是否有消费者角色
        if (ReferenceConfigCache.getCache().getReferredReferences().isEmpty()) {
            return;
        }

        if (printed) {
            return;
        }

        synchronized (PrintAsyncProxyInfoListener.class) {

            if (printed) {
                return;
            }

            log.info("新代理加载总耗时[{}]毫秒", NANO_ADDER.sum() / 1000000);
            log.info("原代理加载总耗时[{}]毫秒", INIT_NANO_ADDER.sum() / 1000000);

            printed = true;
        }
    }

    @Override
    public void onStop(DubboBootstrap bootstrap) {
        printed = false;
        NANO_ADDER.reset();
        INIT_NANO_ADDER.reset();
        ExecutorUtil.gracefulShutdown(EXECUTOR, 300);
        log.info("Dubbo stop");
    }

}

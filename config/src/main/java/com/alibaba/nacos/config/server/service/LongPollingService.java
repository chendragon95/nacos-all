/*
 * Copyright 1999-2018 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.nacos.config.server.service;

import com.alibaba.nacos.common.notify.Event;
import com.alibaba.nacos.common.notify.NotifyCenter;
import com.alibaba.nacos.common.notify.listener.Subscriber;
import com.alibaba.nacos.common.utils.CollectionUtils;
import com.alibaba.nacos.common.utils.ExceptionUtil;
import com.alibaba.nacos.common.utils.StringUtils;
import com.alibaba.nacos.config.server.model.SampleResult;
import com.alibaba.nacos.config.server.model.event.LocalDataChangeEvent;
import com.alibaba.nacos.config.server.monitor.MetricsMonitor;
import com.alibaba.nacos.config.server.utils.ConfigExecutor;
import com.alibaba.nacos.config.server.utils.GroupKey;
import com.alibaba.nacos.config.server.utils.LogUtil;
import com.alibaba.nacos.config.server.utils.MD5Util;
import com.alibaba.nacos.config.server.utils.RequestUtil;
import com.alibaba.nacos.plugin.control.ControlManagerCenter;
import com.alibaba.nacos.plugin.control.connection.request.ConnectionCheckRequest;
import com.alibaba.nacos.plugin.control.connection.response.ConnectionCheckResponse;
import org.springframework.stereotype.Service;

import javax.servlet.AsyncContext;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static com.alibaba.nacos.config.server.utils.LogUtil.MEMORY_LOG;
import static com.alibaba.nacos.config.server.utils.LogUtil.PULL_LOG;

/**
 * LongPollingService.
 *
 * @author Nacos
 */
@Service
public class LongPollingService {
    
    private static final int FIXED_POLLING_INTERVAL_MS = 10000;
    
    private static final int SAMPLE_PERIOD = 100;
    
    private static final int SAMPLE_TIMES = 3;
    
    private static final String TRUE_STR = "true";
    
    private Map<String, Long> retainIps = new ConcurrentHashMap<>();
    
    private static boolean isFixedPolling() {
        return SwitchService.getSwitchBoolean(SwitchService.FIXED_POLLING, false);
    }
    
    private static int getFixedPollingInterval() {
        return SwitchService.getSwitchInteger(SwitchService.FIXED_POLLING_INTERVAL, FIXED_POLLING_INTERVAL_MS);
    }
    
    public boolean isClientLongPolling(String clientIp) {
        return getClientPollingRecord(clientIp) != null;
    }
    
    public Map<String, String> getClientSubConfigInfo(String clientIp) {
        ClientLongPolling record = getClientPollingRecord(clientIp);
        
        if (record == null) {
            return Collections.<String, String>emptyMap();
        }
        
        return record.clientMd5Map;
    }
    
    public SampleResult getSubscribleInfo(String dataId, String group, String tenant) {
        String groupKey = GroupKey.getKeyTenant(dataId, group, tenant);
        SampleResult sampleResult = new SampleResult();
        Map<String, String> lisentersGroupkeyStatus = new HashMap<>(50);
        
        for (ClientLongPolling clientLongPolling : allSubs) {
            if (clientLongPolling.clientMd5Map.containsKey(groupKey)) {
                lisentersGroupkeyStatus.put(clientLongPolling.ip, clientLongPolling.clientMd5Map.get(groupKey));
            }
        }
        sampleResult.setLisentersGroupkeyStatus(lisentersGroupkeyStatus);
        return sampleResult;
    }
    
    public SampleResult getSubscribleInfoByIp(String clientIp) {
        SampleResult sampleResult = new SampleResult();
        Map<String, String> lisentersGroupkeyStatus = new HashMap<>(50);
        
        for (ClientLongPolling clientLongPolling : allSubs) {
            if (clientLongPolling.ip.equals(clientIp)) {
                // One ip can have multiple listener.
                if (!lisentersGroupkeyStatus.equals(clientLongPolling.clientMd5Map)) {
                    lisentersGroupkeyStatus.putAll(clientLongPolling.clientMd5Map);
                }
            }
        }
        sampleResult.setLisentersGroupkeyStatus(lisentersGroupkeyStatus);
        return sampleResult;
    }
    
    /**
     * Aggregate the sampling IP and monitoring configuration information in the sampling results. There is no problem
     * for the merging strategy to cover the previous one with the latter.
     *
     * @param sampleResults sample Results.
     * @return Results.
     */
    public SampleResult mergeSampleResult(List<SampleResult> sampleResults) {
        SampleResult mergeResult = new SampleResult();
        Map<String, String> lisentersGroupkeyStatus = new HashMap<>(50);
        for (SampleResult sampleResult : sampleResults) {
            Map<String, String> lisentersGroupkeyStatusTmp = sampleResult.getLisentersGroupkeyStatus();
            for (Map.Entry<String, String> entry : lisentersGroupkeyStatusTmp.entrySet()) {
                lisentersGroupkeyStatus.put(entry.getKey(), entry.getValue());
            }
        }
        mergeResult.setLisentersGroupkeyStatus(lisentersGroupkeyStatus);
        return mergeResult;
    }
    
    /**
     * Collect application subscribe configinfos.
     *
     * @return configinfos results.
     */
    public Map<String, Set<String>> collectApplicationSubscribeConfigInfos() {
        if (allSubs == null || allSubs.isEmpty()) {
            return null;
        }
        HashMap<String, Set<String>> app2Groupkeys = new HashMap<>(50);
        for (ClientLongPolling clientLongPolling : allSubs) {
            if (StringUtils.isEmpty(clientLongPolling.appName) || "unknown"
                    .equalsIgnoreCase(clientLongPolling.appName)) {
                continue;
            }
            Set<String> appSubscribeConfigs = app2Groupkeys.get(clientLongPolling.appName);
            Set<String> clientSubscribeConfigs = clientLongPolling.clientMd5Map.keySet();
            if (appSubscribeConfigs == null) {
                appSubscribeConfigs = new HashSet<>(clientSubscribeConfigs.size());
            }
            appSubscribeConfigs.addAll(clientSubscribeConfigs);
            app2Groupkeys.put(clientLongPolling.appName, appSubscribeConfigs);
        }
        
        return app2Groupkeys;
    }
    
    public SampleResult getCollectSubscribleInfo(String dataId, String group, String tenant) {
        List<SampleResult> sampleResultLst = new ArrayList<>(50);
        for (int i = 0; i < SAMPLE_TIMES; i++) {
            SampleResult sampleTmp = getSubscribleInfo(dataId, group, tenant);
            if (sampleTmp != null) {
                sampleResultLst.add(sampleTmp);
            }
            if (i < SAMPLE_TIMES - 1) {
                try {
                    Thread.sleep(SAMPLE_PERIOD);
                } catch (InterruptedException e) {
                    LogUtil.CLIENT_LOG.error("sleep wrong", e);
                }
            }
        }
        
        return mergeSampleResult(sampleResultLst);
    }
    
    public SampleResult getCollectSubscribleInfoByIp(String ip) {
        SampleResult sampleResult = new SampleResult();
        sampleResult.setLisentersGroupkeyStatus(new HashMap<String, String>(50));
        for (int i = 0; i < SAMPLE_TIMES; i++) {
            SampleResult sampleTmp = getSubscribleInfoByIp(ip);
            if (sampleTmp != null) {
                if (sampleTmp.getLisentersGroupkeyStatus() != null && !sampleResult.getLisentersGroupkeyStatus()
                        .equals(sampleTmp.getLisentersGroupkeyStatus())) {
                    sampleResult.getLisentersGroupkeyStatus().putAll(sampleTmp.getLisentersGroupkeyStatus());
                }
            }
            if (i < SAMPLE_TIMES - 1) {
                try {
                    Thread.sleep(SAMPLE_PERIOD);
                } catch (InterruptedException e) {
                    LogUtil.CLIENT_LOG.error("sleep wrong", e);
                }
            }
        }
        return sampleResult;
    }
    
    private ClientLongPolling getClientPollingRecord(String clientIp) {
        if (allSubs == null) {
            return null;
        }
        
        for (ClientLongPolling clientLongPolling : allSubs) {
            HttpServletRequest request = (HttpServletRequest) clientLongPolling.asyncContext.getRequest();
            
            if (clientIp.equals(RequestUtil.getRemoteIp(request))) {
                return clientLongPolling;
            }
        }
        
        return null;
    }
    
    /**
     * Add LongPollingClient.
     *
     * @param req              HttpServletRequest.
     * @param rsp              HttpServletResponse.
     * @param clientMd5Map     clientMd5Map.
     * @param probeRequestSize probeRequestSize.
     */
    public void addLongPollingClient(HttpServletRequest req, HttpServletResponse rsp, Map<String, String> clientMd5Map,
            int probeRequestSize) {

        // 长轮询超时时间
        String str = req.getHeader(LongPollingService.LONG_POLLING_HEADER);
        // 长轮询超时不需要延迟(不需要长轮询)标志 (客户端header中设置的Long-Pulling-Timeout-No-Hangup值)
        String noHangUpFlag = req.getHeader(LongPollingService.LONG_POLLING_NO_HANG_UP_HEADER);
        // 获取服务端固定的延迟时间, 默认值0.5s
        int delayTime = SwitchService.getSwitchInteger(SwitchService.FIXED_DELAY_TIME, 500);
        
        // 为LoadBalance添加延迟时间，将提前500 ms返回一个响应，以避免客户端超时
        long timeout = -1L;
        if (isFixedPolling()) {
            // 定点长轮询时设置超时时间, 默认值10s.  (定点长轮询时到超时时间才响应客户端的, 非定点轮询则在超时时间范围内有变化就立即响应客户端)
            timeout = Math.max(10000, getFixedPollingInterval());
        } else {
            // 超时时间, 客户端设置时间-0.5s, 最大值为10s
            timeout = Math.max(10000, Long.parseLong(str) - delayTime);
            long start = System.currentTimeMillis();

            // 根据请求参数的md5值和服务端md5值判断, 获取md5已变化的 配置groupKey列表
            List<String> changedGroups = MD5Util.compareMd5(req, rsp, clientMd5Map);
            if (changedGroups.size() > 0) {
                // 有配置已改变, 响应数据, 结束
                generateResponse(req, rsp, changedGroups);
                LogUtil.CLIENT_LOG.info("{}|{}|{}|{}|{}|{}|{}", System.currentTimeMillis() - start, "instant", RequestUtil.getRemoteIp(req), "polling", clientMd5Map.size(), probeRequestSize, changedGroups.size());
                return;
            } else if (noHangUpFlag != null && noHangUpFlag.equalsIgnoreCase(TRUE_STR)) {
                // 本次请求不需要延迟(不需要长轮询), 结束
                // 若不存在变更, 且客户端header中设置的Long-Pulling-Timeout-No-Hangup值为true, 则结束本次请求 (这种情况为客户端查询的配置文件中存在初始化的CacheData)
                LogUtil.CLIENT_LOG.info("{}|{}|{}|{}|{}|{}|{}", System.currentTimeMillis() - start, "nohangup", RequestUtil.getRemoteIp(req), "polling", clientMd5Map.size(), probeRequestSize, changedGroups.size());
                return;
            }
        }

        // 连接校验, 不展开
        String ip = RequestUtil.getRemoteIp(req);
        ConnectionCheckResponse connectionCheckResponse = checkLimit(req);
        if (!connectionCheckResponse.isSuccess()) {
            generate503Response(req, rsp, connectionCheckResponse.getMessage());
            return;
        }
        
        // 获取异步上下文对象. request和response对象存储在AsyncContext中, 方便后续异步线程中使用
        final AsyncContext asyncContext = req.startAsync();
        // AsyncContext.setTimeout() is incorrect, Control by oneself
        asyncContext.setTimeout(0L);
        
        String appName = req.getHeader(RequestUtil.CLIENT_APPNAME_HEADER);
        String tag = req.getHeader("Vipserver-Tag");
        // 使用线程池执行 ClientLongPolling这个Runnable
        ConfigExecutor.executeLongPolling(
                new ClientLongPolling(asyncContext, clientMd5Map, ip, probeRequestSize, timeout, appName, tag));
    }
    
    private ConnectionCheckResponse checkLimit(HttpServletRequest httpServletRequest) {
        String ip = RequestUtil.getRemoteIp(httpServletRequest);
        String appName = httpServletRequest.getHeader(RequestUtil.CLIENT_APPNAME_HEADER);
        ConnectionCheckRequest connectionCheckRequest = new ConnectionCheckRequest(ip, appName, "LongPolling");
        ConnectionCheckResponse checkResponse = ControlManagerCenter.getInstance().getConnectionControlManager()
                .check(connectionCheckRequest);
        return checkResponse;
    }
    
    public static boolean isSupportLongPolling(HttpServletRequest req) {
        return null != req.getHeader(LONG_POLLING_HEADER);
    }
    
    @SuppressWarnings("PMD.ThreadPoolCreationRule")
    public LongPollingService() {
        // 初始化所有订阅者(当前所有保持长轮询的客户端)对象
        allSubs = new ConcurrentLinkedQueue<>();

        // 定时任务每10秒统计 所有订阅者allSubs个数, 刷新到MetricsMonitor.longPolling中 (源码未见其作用)
        ConfigExecutor.scheduleLongPolling(new StatTask(), 0L, 10L, TimeUnit.SECONDS);
        
        // 注册LocalDataChangeEvent事件 到 通知中心NotifyCenter. 即注册生产者, 不展开
        NotifyCenter.registerToPublisher(LocalDataChangeEvent.class, NotifyCenter.ringBufferSize);
        
        // 注册一个订阅者 来订阅通知中心的的事件 (仅处理LocalDataChangeEvent事件). 即注册消费者, 不展开
        // 发布事件示例NotifyCenter.publishEvent(new LocalDataChangeEvent(groupKey));
        NotifyCenter.registerSubscriber(new Subscriber() {
            
            @Override
            public void onEvent(Event event) {
                // 服务端定点长轮询, 则不处理 (应该是由客户端超时时间的定时任务处理, 即ClientLongPolling处理)
                if (isFixedPolling()) {
                    // Ignore.
                } else {
                    // 仅处理LocalDataChangeEvent事件
                    if (event instanceof LocalDataChangeEvent) {
                        LocalDataChangeEvent evt = (LocalDataChangeEvent) event;
                        // 启动新线程, 执行处理
                        ConfigExecutor.executeLongPolling(new DataChangeTask(evt.groupKey, evt.isBeta, evt.betaIps));
                    }
                }
            }
            
            @Override
            public Class<? extends Event> subscribeType() {
                return LocalDataChangeEvent.class;
            }
        });
    }
    
    public static final String LONG_POLLING_HEADER = "Long-Pulling-Timeout";
    
    public static final String LONG_POLLING_NO_HANG_UP_HEADER = "Long-Pulling-Timeout-No-Hangup";
    
    /**
     * ClientLongPolling subscibers.
     */
    final Queue<ClientLongPolling> allSubs;
    
    class DataChangeTask implements Runnable {
        
        @Override
        public void run() {
            try {
                // 多余的一行代码
                ConfigCacheService.getContentBetaMd5(groupKey);

                // 遍历所有订阅者(当前所有保持长轮询的客户端)对象
                for (Iterator<ClientLongPolling> iter = allSubs.iterator(); iter.hasNext(); ) {
                    ClientLongPolling clientSub = iter.next();
                    // 判断当前客户端订阅者是否关注该groupKey配置文件变更
                    if (clientSub.clientMd5Map.containsKey(groupKey)) {
                        // 如果事件是beta(nacos的测试版本), 但客户端ip不在beta的ip列表, 则跳过
                        if (isBeta && !CollectionUtils.contains(betaIps, clientSub.ip)) {
                            continue;
                        }
                        
                        // 如果事件有tag, 但客户端的与其不相等, 则跳过
                        if (StringUtils.isNotBlank(tag) && !tag.equals(clientSub.tag)) {
                            continue;
                        }

                        // 保留当前客户端ip和时间戳 (目前好像没有使用到)
                        getRetainIps().put(clientSub.ip, System.currentTimeMillis());
                        // 移除订阅者(当前保持长轮询的客户端)对象
                        iter.remove();
                        LogUtil.CLIENT_LOG.info("{}|{}|{}|{}|{}|{}|{}", (System.currentTimeMillis() - changeTime), "in-advance", RequestUtil.getRemoteIp((HttpServletRequest) clientSub.asyncContext.getRequest()), "polling", clientSub.clientMd5Map.size(), clientSub.probeRequestSize, groupKey);
                        // 响应客户端(会结束掉超时的定时任务)
                        clientSub.sendResponse(Arrays.asList(groupKey));
                    }
                }
                
            } catch (Throwable t) {
                LogUtil.DEFAULT_LOG.error("data change error: {}", ExceptionUtil.getStackTrace(t));
            }
        }
        
        DataChangeTask(String groupKey, boolean isBeta, List<String> betaIps) {
            this(groupKey, isBeta, betaIps, null);
        }
        
        DataChangeTask(String groupKey, boolean isBeta, List<String> betaIps, String tag) {
            this.groupKey = groupKey;
            this.isBeta = isBeta;
            this.betaIps = betaIps;
            this.tag = tag;
        }
        
        final String groupKey;
        
        final long changeTime = System.currentTimeMillis();
        
        final boolean isBeta;
        
        final List<String> betaIps;
        
        final String tag;
    }
    
    class StatTask implements Runnable {
        
        @Override
        public void run() {
            MEMORY_LOG.info("[long-pulling] client count " + allSubs.size());
            MetricsMonitor.getLongPollingMonitor().set(allSubs.size());
        }
    }
    
    class ClientLongPolling implements Runnable {
        @Override
        public void run() {
            // 执行定时任务, 定时任务时间间隔为timeoutTime
            asyncTimeoutFuture = ConfigExecutor.scheduleLongPolling(() -> {
                try {
                    // 保留当前客户端ip和时间戳 (目前好像没有使用到)
                    getRetainIps().put(ClientLongPolling.this.ip, System.currentTimeMillis());

                    // 移除订阅者. 处理已经到timeout时间的客户端请求
                    boolean removeFlag = allSubs.remove(ClientLongPolling.this);

                    // 移除成功, 说明超时时间已到
                    if (removeFlag) {
                        // 服务端定点轮询的, 则到了超时时间才查询是否变更(到达超时时间之前及时配置文件变更也不处理), 进行响应客户端
                        if (isFixedPolling()) {
                            LogUtil.CLIENT_LOG.info("{}|{}|{}|{}|{}|{}", (System.currentTimeMillis() - createTime), "fix", RequestUtil.getRemoteIp((HttpServletRequest) asyncContext.getRequest()), "polling", clientMd5Map.size(), probeRequestSize);
                            // 获取变化的配置文件列表
                            List<String> changedGroups = MD5Util
                                    .compareMd5((HttpServletRequest) asyncContext.getRequest(),
                                            (HttpServletResponse) asyncContext.getResponse(), clientMd5Map);
                            if (changedGroups.size() > 0) {
                                // 响应发生变化的配置文件列表
                                sendResponse(changedGroups);
                            } else {
                                // 响应空
                                sendResponse(null);
                            }
                        } else {
                            LogUtil.CLIENT_LOG.info("{}|{}|{}|{}|{}|{}", (System.currentTimeMillis() - createTime), "timeout", RequestUtil.getRemoteIp((HttpServletRequest) asyncContext.getRequest()), "polling", clientMd5Map.size(), probeRequestSize);
                            sendResponse(null);
                        }
                    } else {
                        // 移除失败. 说明定时任务和事件处理并发执行了, 事件处理先以本定时任务的移除代码响应了客户端, 将本次请求长轮询任务移出了allSubs队列
                        LogUtil.DEFAULT_LOG.warn("client subsciber's relations delete fail.");
                    }
                } catch (Throwable t) {
                    LogUtil.DEFAULT_LOG.error("long polling error:" + t.getMessage(), t.getCause());
                }

            }, timeoutTime, TimeUnit.MILLISECONDS);
            // 加入队列, 待超时时间到后再处理
            allSubs.add(this);
        }

        void sendResponse(List<String> changedGroups) {
            // 取消超时的定时任务
            if (null != asyncTimeoutFuture) {
                asyncTimeoutFuture.cancel(false);
            }
            // 响应客户端
            generateResponse(changedGroups);
        }

        void generateResponse(List<String> changedGroups) {
    
            if (null == changedGroups) {
                // 告诉web容器发送http响应
                asyncContext.complete();
                return;
            }
            HttpServletResponse response = (HttpServletResponse) asyncContext.getResponse();
            try {
                final String respString = MD5Util.compareMd5ResultString(changedGroups);
                
                // Disable cache.
                response.setHeader("Pragma", "no-cache");
                response.setDateHeader("Expires", 0);
                response.setHeader("Cache-Control", "no-cache,no-store");
                response.setStatus(HttpServletResponse.SC_OK);
                // 写入变更的配置文件列表
                response.getWriter().println(respString);
                asyncContext.complete();
            } catch (Exception ex) {
                PULL_LOG.error(ex.toString(), ex);
                asyncContext.complete();
            }
        }
        
        ClientLongPolling(AsyncContext ac, Map<String, String> clientMd5Map, String ip, int probeRequestSize,
                long timeoutTime, String appName, String tag) {
            this.asyncContext = ac;
            this.clientMd5Map = clientMd5Map;
            this.probeRequestSize = probeRequestSize;
            this.createTime = System.currentTimeMillis();
            this.ip = ip;
            this.timeoutTime = timeoutTime;
            this.appName = appName;
            this.tag = tag;
        }
        
        final AsyncContext asyncContext;
        
        final Map<String, String> clientMd5Map;
        
        final long createTime;
        
        final String ip;
        
        final String appName;
        
        final String tag;
        
        final int probeRequestSize;
        
        final long timeoutTime;
        
        Future<?> asyncTimeoutFuture;
        
        @Override
        public String toString() {
            return "ClientLongPolling{" + "clientMd5Map=" + clientMd5Map + ", createTime=" + createTime + ", ip='" + ip
                    + '\'' + ", appName='" + appName + '\'' + ", tag='" + tag + '\'' + ", probeRequestSize="
                    + probeRequestSize + ", timeoutTime=" + timeoutTime + '}';
        }
    }
    
    void generateResponse(HttpServletRequest request, HttpServletResponse response, List<String> changedGroups) {
        if (null == changedGroups) {
            return;
        }
        
        try {
            // 将发生变化的配置groupKey列表转换为字符串返回给客户端
            final String respString = MD5Util.compareMd5ResultString(changedGroups);
            // Disable cache.
            response.setHeader("Pragma", "no-cache");
            response.setDateHeader("Expires", 0);
            response.setHeader("Cache-Control", "no-cache,no-store");
            response.setStatus(HttpServletResponse.SC_OK);
            response.getWriter().println(respString);
        } catch (Exception ex) {
            PULL_LOG.error(ex.toString(), ex);
        }
    }
    
    void generate503Response(HttpServletRequest request, HttpServletResponse response, String message) {
        
        try {
            
            // Disable cache.
            response.setHeader("Pragma", "no-cache");
            response.setDateHeader("Expires", 0);
            response.setHeader("Cache-Control", "no-cache,no-store");
            response.setStatus(HttpServletResponse.SC_SERVICE_UNAVAILABLE);
            response.getWriter().println(message);
        } catch (Exception ex) {
            PULL_LOG.error(ex.toString(), ex);
        }
    }
    
    public Map<String, Long> getRetainIps() {
        return retainIps;
    }
    
    public void setRetainIps(Map<String, Long> retainIps) {
        this.retainIps = retainIps;
    }
    
    public int getSubscriberCount() {
        return allSubs.size();
    }
}

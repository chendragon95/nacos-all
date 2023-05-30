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

package com.alibaba.nacos.config.server.controller;

import com.alibaba.nacos.api.model.v2.ErrorCode;
import com.alibaba.nacos.api.model.v2.Result;
import com.alibaba.nacos.common.constant.HttpHeaderConsts;
import com.alibaba.nacos.common.http.param.MediaType;
import com.alibaba.nacos.common.utils.IoUtils;
import com.alibaba.nacos.common.utils.JacksonUtils;
import com.alibaba.nacos.common.utils.Pair;
import com.alibaba.nacos.common.utils.StringUtils;
import com.alibaba.nacos.config.server.constant.Constants;
import com.alibaba.nacos.config.server.enums.FileTypeEnum;
import com.alibaba.nacos.config.server.model.CacheItem;
import com.alibaba.nacos.config.server.model.ConfigInfoBase;
import com.alibaba.nacos.config.server.service.ConfigCacheService;
import com.alibaba.nacos.config.server.service.LongPollingService;
import com.alibaba.nacos.config.server.service.repository.ConfigInfoBetaPersistService;
import com.alibaba.nacos.config.server.service.repository.ConfigInfoPersistService;
import com.alibaba.nacos.config.server.service.repository.ConfigInfoTagPersistService;
import com.alibaba.nacos.config.server.service.trace.ConfigTraceService;
import com.alibaba.nacos.config.server.utils.*;
import com.alibaba.nacos.plugin.encryption.handler.EncryptionHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

import static com.alibaba.nacos.config.server.utils.LogUtil.PULL_LOG;

/**
 * ConfigServlet inner for aop.
 *
 * @author Nacos
 */
@Service
public class ConfigServletInner {

    private static final int TRY_GET_LOCK_TIMES = 9;
    
    private static final int START_LONG_POLLING_VERSION_NUM = 204;
    
    private static final Logger LOGGER = LoggerFactory.getLogger(ConfigServletInner.class);
    
    private final LongPollingService longPollingService;
    
    private  ConfigInfoPersistService configInfoPersistService;
    
    private  ConfigInfoBetaPersistService configInfoBetaPersistService;
    
    private  ConfigInfoTagPersistService configInfoTagPersistService;
    
    public ConfigServletInner(LongPollingService longPollingService, ConfigInfoPersistService configInfoPersistService,
            ConfigInfoBetaPersistService configInfoBetaPersistService,
            ConfigInfoTagPersistService configInfoTagPersistService) {
        this.longPollingService = longPollingService;
        this.configInfoPersistService = configInfoPersistService;
        this.configInfoBetaPersistService = configInfoBetaPersistService;
        this.configInfoTagPersistService = configInfoTagPersistService;
    }
    
    /**
     * long polling the config.
     */
    public String doPollingConfig(HttpServletRequest request, HttpServletResponse response,
            Map<String, String> clientMd5Map, int probeRequestSize) throws IOException {
        
        // 长轮询 (这里判断request的header中Long-Pulling-Timeout是否为空)
        if (LongPollingService.isSupportLongPolling(request)) {
            longPollingService.addLongPollingClient(request, response, clientMd5Map, probeRequestSize);
            return HttpServletResponse.SC_OK + "";
        }




        // 非长轮询, 不展开
        List<String> changedGroups = MD5Util.compareMd5(request, response, clientMd5Map);
        
        // Compatible with short polling result.
        String oldResult = MD5Util.compareMd5OldResult(changedGroups);
        String newResult = MD5Util.compareMd5ResultString(changedGroups);
        
        String version = request.getHeader(Constants.CLIENT_VERSION_HEADER);
        if (version == null) {
            version = "2.0.0";
        }
        int versionNum = Protocol.getVersionNumber(version);
        
        // Before 2.0.4 version, return value is put into header.
        if (versionNum < START_LONG_POLLING_VERSION_NUM) {
            response.addHeader(Constants.PROBE_MODIFY_RESPONSE, oldResult);
            response.addHeader(Constants.PROBE_MODIFY_RESPONSE_NEW, newResult);
        } else {
            request.setAttribute("content", newResult);
        }
        
        // Disable cache.
        response.setHeader("Pragma", "no-cache");
        response.setDateHeader("Expires", 0);
        response.setHeader("Cache-Control", "no-cache,no-store");
        response.setStatus(HttpServletResponse.SC_OK);
        return HttpServletResponse.SC_OK + "";
    }
    
    /**
     * Execute to get config [API V1].
     */
    public String doGetConfig(HttpServletRequest request, HttpServletResponse response, String dataId, String group,
            String tenant, String tag, String isNotify, String clientIp) throws IOException, ServletException {
        return doGetConfig(request, response, dataId, group, tenant, tag, isNotify, clientIp, false);
    }
    
    /**
     * Execute to get config [API V1] or [API V2].
     */
    public String doGetConfig(HttpServletRequest request, HttpServletResponse response, String dataId, String group,
            String tenant, String tag, String isNotify, String clientIp, boolean isV2)
            throws IOException, ServletException {
        // 设置notify值
        boolean notify = false;
        if (StringUtils.isNotBlank(isNotify)) {
            notify = Boolean.parseBoolean(isNotify);
        }
        // 接口路径为v2则设置CONTENT_TYPE为json
        if (isV2) {
            response.setHeader(HttpHeaderConsts.CONTENT_TYPE, MediaType.APPLICATION_JSON);
        }
        
        final String groupKey = GroupKey2.getKey(dataId, group, tenant);
        // 获取autoTag值
        String autoTag = request.getHeader("Vipserver-Tag");
        // 获取客户端appName
        String requestIpApp = RequestUtil.getAppName(request);

        // 获取配置的读锁 0-没有数据或失败. 正数-获取锁成功. 负数-获取锁失败
        int lockResult = tryConfigReadLock(groupKey);
        // 获取请求ip地址, 其实就是入参的clientIp
        final String requestIp = RequestUtil.getRemoteIp(request);
        boolean isBeta = false;
        boolean isSli = false;
        if (lockResult > 0) {
            // LockResult > 0 表示cacheItem不为空，其他线程不能删除该cacheItem
            FileInputStream fis = null;
            try {
                String md5 = Constants.NULL;
                long lastModified = 0L;
                CacheItem cacheItem = ConfigCacheService.getContentCache(groupKey);
                if (cacheItem.isBeta() && cacheItem.getIps4Beta().contains(clientIp)) {
                    isBeta = true;
                }

                // 设置response的CONTENT_TYPE, 没有值则默认为text (v2的固定为json)
                final String configType = (null != cacheItem.getType()) ? cacheItem.getType() : FileTypeEnum.TEXT.getFileType();
                response.setHeader("Config-Type", configType);
                FileTypeEnum fileTypeEnum = FileTypeEnum.getFileTypeEnumByFileExtensionOrFileType(configType);
                String contentTypeHeader = fileTypeEnum.getContentType();
                response.setHeader(HttpHeaderConsts.CONTENT_TYPE, contentTypeHeader);
                if (isV2) {
                    response.setHeader(HttpHeaderConsts.CONTENT_TYPE, MediaType.APPLICATION_JSON);
                }
                
                File file = null;
                ConfigInfoBase configInfoBase = null;
                PrintWriter out;


                if (isBeta) {
                    md5 = cacheItem.getMd54Beta();
                    lastModified = cacheItem.getLastModifiedTs4Beta();
                    if (PropertyUtil.isDirectRead()) {
                        // 读取数据库
                        configInfoBase = configInfoBetaPersistService.findConfigInfo4Beta(dataId, group, tenant);
                    } else {
                        // 读取配置文件
                        file = DiskUtil.targetBetaFile(dataId, group, tenant);
                    }
                    response.setHeader("isBeta", "true");
                } else {
                    if (StringUtils.isBlank(tag)) {
                        if (isUseTag(cacheItem, autoTag)) {
                            if (cacheItem.tagMd5 != null) {
                                md5 = cacheItem.tagMd5.get(autoTag);
                            }
                            if (cacheItem.tagLastModifiedTs != null) {
                                lastModified = cacheItem.tagLastModifiedTs.get(autoTag);
                            }
                            if (PropertyUtil.isDirectRead()) {
                                configInfoBase = configInfoTagPersistService.findConfigInfo4Tag(dataId, group, tenant, autoTag);
                            } else {
                                file = DiskUtil.targetTagFile(dataId, group, tenant, autoTag);
                            }
                            
                            response.setHeader(com.alibaba.nacos.api.common.Constants.VIPSERVER_TAG,
                                    URLEncoder.encode(autoTag, StandardCharsets.UTF_8.displayName()));
                        } else {
                            md5 = cacheItem.getMd5();
                            lastModified = cacheItem.getLastModifiedTs();
                            if (PropertyUtil.isDirectRead()) {
                                configInfoBase = configInfoPersistService.findConfigInfo(dataId, group, tenant);
                            } else {
                                file = DiskUtil.targetFile(dataId, group, tenant);
                            }
                            if (configInfoBase == null && fileNotExist(file)) {
                                // FIXME CacheItem
                                // No longer exists. It is impossible to simply calculate the push delayed. Here, simply record it as - 1.
                                ConfigTraceService.logPullEvent(dataId, group, tenant, requestIpApp, -1,
                                        ConfigTraceService.PULL_EVENT_NOTFOUND, -1, requestIp, notify);
                                
                                // pullLog.info("[client-get] clientIp={}, {},
                                // no data",
                                // new Object[]{clientIp, groupKey});
                                
                                return get404Result(response, isV2);
                            }
                            isSli = true;
                        }
                    } else {
                        if (cacheItem.tagMd5 != null) {
                            md5 = cacheItem.tagMd5.get(tag);
                        }
                        if (cacheItem.tagLastModifiedTs != null) {
                            Long lm = cacheItem.tagLastModifiedTs.get(tag);
                            if (lm != null) {
                                lastModified = lm;
                            }
                        }
                        if (PropertyUtil.isDirectRead()) {
                            configInfoBase = configInfoTagPersistService.findConfigInfo4Tag(dataId, group, tenant, tag);
                        } else {
                            file = DiskUtil.targetTagFile(dataId, group, tenant, tag);
                        }
                        if (configInfoBase == null && fileNotExist(file)) {
                            // FIXME CacheItem
                            // No longer exists. It is impossible to simply calculate the push delayed. Here, simply record it as - 1.
                            ConfigTraceService.logPullEvent(dataId, group, tenant, requestIpApp, -1,
                                    ConfigTraceService.PULL_EVENT_NOTFOUND, -1, requestIp, notify && isSli);
                            
                            // pullLog.info("[client-get] clientIp={}, {},
                            // no data",
                            // new Object[]{clientIp, groupKey});
                            
                            return get404Result(response, isV2);
                        }
                    }
                }
                
                response.setHeader(Constants.CONTENT_MD5, md5);
                
                // 设置response的值.
                response.setHeader("Pragma", "no-cache");
                response.setDateHeader("Expires", 0);
                response.setHeader("Cache-Control", "no-cache,no-store");
                if (PropertyUtil.isDirectRead()) {
                    response.setDateHeader("Last-Modified", lastModified);
                } else {
                    fis = new FileInputStream(file);
                    response.setDateHeader("Last-Modified", file.lastModified());
                }

                // 读取数据库的配置返回
                if (PropertyUtil.isDirectRead()) {
                    // 解密出内容
                    Pair<String, String> pair = EncryptionHandler.decryptHandler(dataId, configInfoBase.getEncryptedDataKey(), configInfoBase.getContent());
                    out = response.getWriter();
                    if (isV2) {
                        out.print(JacksonUtils.toJson(Result.success(pair.getSecond())));
                    } else {
                        out.print(pair.getSecond());
                    }
                    out.flush();
                    out.close();
                }
                // 读取配置文件返回
                else {
                    String fileContent = IoUtils.toString(fis, StandardCharsets.UTF_8.name());
                    String encryptedDataKey = cacheItem.getEncryptedDataKey();
                    // 解密出内容
                    Pair<String, String> pair = EncryptionHandler.decryptHandler(dataId, encryptedDataKey, fileContent);
                    String decryptContent = pair.getSecond();
                    out = response.getWriter();
                    if (isV2) {
                        out.print(JacksonUtils.toJson(Result.success(decryptContent)));
                    } else {
                        out.print(decryptContent);
                    }
                    out.flush();
                    out.close();
                }

                // 打印日志
                LogUtil.PULL_CHECK_LOG.warn("{}|{}|{}|{}", groupKey, requestIp, md5, TimeUtils.getCurrentTimeStr());
                final long delayed = System.currentTimeMillis() - lastModified;
                ConfigTraceService.logPullEvent(dataId, group, tenant, requestIpApp, lastModified, ConfigTraceService.PULL_EVENT_OK, delayed, requestIp, notify && isSli);
            } finally {
                // 释放读锁, 关闭文件流
                releaseConfigReadLock(groupKey);
                IoUtils.closeQuietly(fis);
            }
        } else if (lockResult == 0) {
            // 没有数据, 打印拉取日志, 返回404状态码
            // FIXME CacheItem No longer exists. It is impossible to simply calculate the push delayed. Here, simply record it as - 1.
            ConfigTraceService.logPullEvent(dataId, group, tenant, requestIpApp, -1, ConfigTraceService.PULL_EVENT_NOTFOUND, -1, requestIp, notify && isSli);
            return get404Result(response, isV2);
        } else {
            // 该文件正在读, 打印拉取日志, 返回409状态码
            PULL_LOG.info("[client-get] clientIp={}, {}, get data during dump", clientIp, groupKey);
            return get409Result(response, isV2);
        }
        
        return HttpServletResponse.SC_OK + "";
    }
    
    private static void releaseConfigReadLock(String groupKey) {
        ConfigCacheService.releaseReadLock(groupKey);
    }
    
    private String get404Result(HttpServletResponse response, boolean isV2) throws IOException {
        response.setStatus(HttpServletResponse.SC_NOT_FOUND);
        PrintWriter writer = response.getWriter();
        if (isV2) {
            writer.println(JacksonUtils.toJson(Result.failure(ErrorCode.RESOURCE_NOT_FOUND, "config data not exist")));
        } else {
            writer.println("config data not exist");
        }
        return HttpServletResponse.SC_NOT_FOUND + "";
    }
    
    private String get409Result(HttpServletResponse response, boolean isV2) throws IOException {
        response.setStatus(HttpServletResponse.SC_CONFLICT);
        PrintWriter writer = response.getWriter();
        if (isV2) {
            writer.println(JacksonUtils.toJson(Result
                    .failure(ErrorCode.RESOURCE_CONFLICT, "requested file is being modified, please try later.")));
        } else {
            writer.println("requested file is being modified, please try later.");
        }
        return HttpServletResponse.SC_CONFLICT + "";
    }
    
    /**
     * Try to add read lock.
     *
     * @param groupKey groupKey string value.
     * @return 0-没有数据或失败. 正数-获取锁成功. 负数-获取锁失败
     * -1-获取读锁失败(正在写) 0-配置文件不存在(CACHE中不存在对于的CacheItem) 1-获取读锁成功
     */
    private static int tryConfigReadLock(String groupKey) {
        // 默认获取读锁失败
        int lockResult = -1;
        
        // 尝试获取读锁, 最多尝试10次
        for (int i = TRY_GET_LOCK_TIMES; i >= 0; --i) {
            // 尝试获取读锁
            lockResult = ConfigCacheService.tryReadLock(groupKey);
            
            // 数据不存在(CACHE中不存在对于的CacheItem), 直接返回
            if (0 == lockResult) {
                break;
            }
            
            // 获取读锁成功, 直接返回
            if (lockResult > 0) {
                break;
            }
            
            // 先挂起1ms再重试
            if (i > 0) {
                try {
                    Thread.sleep(1);
                } catch (Exception e) {
                    LogUtil.PULL_CHECK_LOG.error("An Exception occurred while thread sleep", e);
                }
            }
        }
        
        return lockResult;
    }
    
    private static boolean isUseTag(CacheItem cacheItem, String tag) {
        if (cacheItem != null && cacheItem.tagMd5 != null && cacheItem.tagMd5.size() > 0) {
            return StringUtils.isNotBlank(tag) && cacheItem.tagMd5.containsKey(tag);
        }
        return false;
    }
    
    private static boolean fileNotExist(File file) {
        return file == null || !file.exists();
    }
    
}

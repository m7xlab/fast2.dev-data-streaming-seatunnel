/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.engine.server.rest.service;

import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.ArrayNode;
import org.apache.seatunnel.shade.org.apache.commons.lang3.StringUtils;

import org.apache.seatunnel.common.utils.FileUtils;
import org.apache.seatunnel.common.utils.JsonUtils;
import org.apache.seatunnel.engine.common.config.server.HttpConfig;
import org.apache.seatunnel.engine.server.SeaTunnelServer;
import org.apache.seatunnel.engine.server.operation.GetNodeHttpPortOperation;
import org.apache.seatunnel.engine.server.utils.NodeEngineUtil;

import com.hazelcast.cluster.Member;
import com.hazelcast.internal.json.JsonArray;
import com.hazelcast.internal.json.JsonObject;
import com.hazelcast.spi.impl.NodeEngineImpl;
import lombok.extern.slf4j.Slf4j;
import scala.Tuple3;

import java.io.File;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.seatunnel.engine.server.rest.RestConstant.REST_URL_GET_ALL_LOG_NAME;
import static org.apache.seatunnel.engine.server.rest.RestConstant.REST_URL_LOGS;

@Slf4j
public class LogService extends BaseLogService {

    /** Query parameter of a log link naming the member that owns the log file. */
    public static final String NODE_PARAM = "node";

    public LogService(NodeEngineImpl nodeEngine) {
        super(nodeEngine);
    }

    /**
     * Returns the cap in bytes on the content of a single log response, translated from {@code
     * log-response-max-size-mb}. A value <= 0 means unlimited.
     */
    public long maxLogResponseBytes() {
        return getSeaTunnelServer(false)
                .getSeaTunnelConfig()
                .getEngineConfig()
                .getHttpConfig()
                .getLogResponseMaxSizeBytes();
    }

    public List<String> allLogName() {
        String logPath = getLogPath();
        List<File> logFileList = FileUtils.listFile(logPath);
        if (logFileList == null) {
            return new ArrayList<>();
        }
        return logFileList.stream().map(File::getName).collect(Collectors.toList());
    }

    public List<Tuple3<String, String, String>> allLogNameList(String jobId) {

        SeaTunnelServer seaTunnelServer = getSeaTunnelServer(false);
        HttpConfig httpConfig =
                seaTunnelServer.getSeaTunnelConfig().getEngineConfig().getHttpConfig();
        String contextPath = httpConfig.getContextPath();

        List<Tuple3<String, String, String>> allLogNameList = new ArrayList<>();

        for (Member member : nodeEngine.getClusterService().getMembers()) {
            String host = member.getAddress().getHost();
            Integer nodeHttpPort = memberHttpPort(member);
            if (nodeHttpPort == null) {
                continue;
            }

            String url = "http://" + host + ":" + nodeHttpPort + contextPath;
            String logUrl = url + REST_URL_GET_ALL_LOG_NAME;

            String allName =
                    httpConfig.isEnableBasicAuth()
                            ? sendGet(
                                    logUrl,
                                    httpConfig.getBasicAuthUsername(),
                                    httpConfig.getBasicAuthPassword())
                            : sendGet(logUrl);

            if (StringUtils.isBlank(allName)) {
                log.warn("GET {} returned empty body (null/empty). Skip this node.", logUrl);
                continue;
            }

            if (log.isDebugEnabled()) {
                log.debug("Request: {} , Result: {}", url, allName);
            }
            ArrayNode jsonNodes = JsonUtils.parseArray(allName);

            final String nodeId = host + ":" + nodeHttpPort;
            jsonNodes.forEach(
                    jsonNode -> {
                        String fileName = jsonNode.asText();
                        if (StringUtils.isNotBlank(jobId) && !fileName.contains(jobId)) {
                            return;
                        }
                        allLogNameList.add(
                                new Tuple3<>(
                                        nodeId,
                                        contextPath
                                                + REST_URL_LOGS
                                                + "/"
                                                + fileName
                                                + "?"
                                                + NODE_PARAM
                                                + "="
                                                + nodeId,
                                        fileName));
                    });
        }

        return allLogNameList;
    }

    /**
     * Finds the cluster member that a log link's {@code node} parameter refers to.
     *
     * @param node member id in the {@code host:httpPort} form reported by the log list
     * @return the matching member, or {@code null} when no cluster member matches
     */
    public Member findMember(String node) {
        for (Member member : nodeEngine.getClusterService().getMembers()) {
            Integer nodeHttpPort = memberHttpPort(member);
            if (nodeHttpPort != null
                    && (member.getAddress().getHost() + ":" + nodeHttpPort).equals(node)) {
                return member;
            }
        }
        return null;
    }

    /**
     * Reads a log file from another cluster member through that member's REST service, so that a
     * client only needs to reach the node it is already talking to.
     *
     * @param member member that owns the log file
     * @param logName log file name
     * @return the log content, or {@code null} when the member did not return it
     */
    public String memberLog(Member member, String logName) {
        Integer nodeHttpPort = memberHttpPort(member);
        if (nodeHttpPort == null) {
            return null;
        }
        HttpConfig httpConfig =
                getSeaTunnelServer(false).getSeaTunnelConfig().getEngineConfig().getHttpConfig();
        String logUrl;
        try {
            logUrl =
                    "http://"
                            + member.getAddress().getHost()
                            + ":"
                            + nodeHttpPort
                            + httpConfig.getContextPath()
                            + REST_URL_LOGS
                            + "/"
                            + URLEncoder.encode(logName, StandardCharsets.UTF_8.name())
                                    .replace("+", "%20");
        } catch (UnsupportedEncodingException e) {
            throw new IllegalStateException(e);
        }
        return httpConfig.isEnableBasicAuth()
                ? sendGet(
                        logUrl,
                        httpConfig.getBasicAuthUsername(),
                        httpConfig.getBasicAuthPassword())
                : sendGet(logUrl);
    }

    private Integer memberHttpPort(Member member) {
        try {
            return (Integer)
                    NodeEngineUtil.sendOperationToMemberNode(
                                    nodeEngine, new GetNodeHttpPortOperation(), member.getAddress())
                            .get();
        } catch (Exception e) {
            log.warn("Failed to get HTTP port from member {}, skip.", member.getAddress(), e);
            return null;
        }
    }

    public JsonArray allNodeLogFormatJson(String jobId) {

        return allLogNameList(jobId).stream()
                .map(
                        tuple -> {
                            JsonObject jsonObject = new JsonObject();
                            jsonObject.add("node", tuple._1());
                            jsonObject.add("logLink", tuple._2());
                            jsonObject.add("logName", tuple._3());
                            return jsonObject;
                        })
                .collect(JsonArray::new, JsonArray::add, JsonArray::add);
    }

    public String allNodeLogFormatHtml(String jobId) {
        StringBuilder logLink = new StringBuilder();

        allLogNameList(jobId)
                .forEach(tuple -> logLink.append(buildLogLink(tuple._2(), tuple._3())));
        return buildWebSiteContent(logLink);
    }

    public String currentNodeLog() {
        List<File> logFileList = FileUtils.listFile(getLogPath());
        StringBuilder logLink = new StringBuilder();
        if (logFileList != null) {
            for (File file : logFileList) {
                logLink.append(buildLogLink("log/" + file.getName(), file.getName()));
            }
        }

        return buildWebSiteContent(logLink);
    }
}

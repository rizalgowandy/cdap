/*
 * Copyright © 2014-2021 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package io.cdap.cdap.common.http;

import io.cdap.cdap.api.auditlogging.AuditLogWriter;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.proto.security.Credential;
import io.cdap.cdap.security.spi.authentication.SecurityRequestContext;
import io.cdap.cdap.security.spi.authentication.UnauthenticatedException;
import io.cdap.cdap.security.spi.authorization.AuditLogContext;
import io.cdap.cdap.security.spi.authorization.AuditLogRequest;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpUtil;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.util.AttributeKey;
import java.io.IOException;
import java.util.Queue;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An UpstreamHandler that verifies the userId in a request header and updates the {@code
 * SecurityRequestContext}.
 */
public class AuthenticationChannelHandler extends ChannelDuplexHandler {

  private static final Logger LOG = LoggerFactory.getLogger(AuthenticationChannelHandler.class);

  private static final String EMPTY_USER_ID = "CDAP-empty-user-id";
  private static final Credential EMPTY_USER_CREDENTIAL = new Credential(
      "CDAP-empty-user-credential",
      Credential.CredentialType.INTERNAL);
  private static final String EMPTY_USER_IP = "CDAP-empty-user-ip";
  static final String AUDIT_LOG_REQ_BUILDER_ATTR = "AUDIT_LOG_REQ_BUILDER";
  static final String AUDIT_LOG_USER_IP_ATTR = "AUDIT_LOG_USER_IP";
  static final String CDAP_USER_ID_ATTR = "CDAP_USER_ID";
  static final String CDAP_USER_CREDENTIAL_ATTR = "CDAP_USER_CREDENTIAL";
  static final String AUDIT_LOG_CONTEXT_QUEUE_ATTR = "AUDIT_LOG_CONTEXT_QUEUE";

  private final boolean internalAuthEnabled;
  private final boolean auditLoggingEnabled;
  private final AuditLogWriter auditLogWriter;

  public AuthenticationChannelHandler(boolean internalAuthEnabled, boolean auditLoggingEnabled,
                                      AuditLogWriter auditLogWriter) {
    this.internalAuthEnabled = internalAuthEnabled;
    this.auditLoggingEnabled = auditLoggingEnabled;
    this.auditLogWriter = auditLogWriter;
  }

  /**
   * Decode the AccessTokenIdentifier passed as a header and set it in a ThreadLocal. Returns a 401
   * if the identifier is malformed.
   */
  @Override
  public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
    SecurityRequestContext.reset();

    // TODO: CDAP-21121 ensure request is authorized before sending response
    if (msg instanceof HttpRequest) {

      String currentUserId = null;
      Credential currentUserCredential = null;
      String currentUserIp = null;

      if (internalAuthEnabled) {
        // When internal auth is enabled, all requests should typically have user id and credential
        // associated with them, for instance, end user credential for user originated ones and
        // internal system credential for system originated requests. If there is none, set
        // default empty user id and credential.
        currentUserId = EMPTY_USER_ID;
        currentUserCredential = EMPTY_USER_CREDENTIAL;
        currentUserIp = EMPTY_USER_IP;
      }
      // TODO: authenticate the user using user id - CDAP-688
      HttpRequest request = (HttpRequest) msg;
      String userId = request.headers().get(Constants.Security.Headers.USER_ID);
      if (userId != null) {
        currentUserId = userId;
      }
      String userIp = request.headers().get(Constants.Security.Headers.USER_IP);
      if (userIp != null) {
        currentUserIp = userIp;
      }
      String authHeader = request.headers().get(Constants.Security.Headers.RUNTIME_TOKEN);
      if (authHeader != null) {
        int idx = authHeader.trim().indexOf(' ');
        if (idx < 0) {
          LOG.error("Invalid Authorization header format for {}@{}", currentUserId, currentUserIp);
          if (internalAuthEnabled) {
            throw new UnauthenticatedException("Invalid Authorization header format");
          }
        } else {
          String credentialTypeStr = authHeader.substring(0, idx);
          try {
            Credential.CredentialType credentialType = Credential.CredentialType.fromQualifiedName(
                credentialTypeStr);
            String credentialValue = authHeader.substring(idx + 1).trim();
            currentUserCredential = new Credential(credentialValue, credentialType);
            SecurityRequestContext.setUserCredential(currentUserCredential);
          } catch (IllegalArgumentException e) {
            LOG.error("Invalid credential type in Authorization header: {}", credentialTypeStr);
            throw new UnauthenticatedException(e);
          }
        }
      }
      LOG.trace("Got user ID '{}' user IP '{}' from IP '{}' and authorization header length '{}'",
          userId, userIp, ctx.channel().remoteAddress(),
          authHeader == null ? "NULL" : authHeader.length());
      SecurityRequestContext.setUserId(currentUserId);
      SecurityRequestContext.setUserCredential(currentUserCredential);
      SecurityRequestContext.setUserIp(currentUserIp);
      //Also set userIp in ATTR , to be used in audit logging incase it was replaced at a later stage
      ctx.channel().attr(AttributeKey.valueOf(AUDIT_LOG_USER_IP_ATTR)).set(currentUserIp);
      ctx.channel().attr(AttributeKey.valueOf(CDAP_USER_ID_ATTR)).set(currentUserId);
      ctx.channel().attr(AttributeKey.valueOf(CDAP_USER_CREDENTIAL_ATTR)).set(currentUserCredential);
    } else {
      Object userIpObj = ctx.channel().attr(AttributeKey.valueOf(AUDIT_LOG_USER_IP_ATTR)).get();
      Object userIdObj = ctx.channel().attr(AttributeKey.valueOf(CDAP_USER_ID_ATTR)).get();
      Object userCredentialObj =
          ctx.channel().attr(AttributeKey.valueOf(CDAP_USER_CREDENTIAL_ATTR)).get();
      if (userIpObj != null) {
        SecurityRequestContext.setUserIp((String) userIpObj);
      }
      if (userIdObj != null && userCredentialObj != null) {
        SecurityRequestContext.setUserId((String) userIdObj);
        SecurityRequestContext.setUserCredential((Credential) userCredentialObj);
      }
    }

    try {
      ctx.fireChannelRead(msg);
    } finally {
      setAuditLogMetaDataInChannel(ctx);
      SecurityRequestContext.reset();
    }
  }

  /**
   * If Audit logging is enabled then it sends the collection of audit events stored in {@link SecurityRequestContext}
   * Or Attribute of channel to get stored in a messaging system.
   */
  @Override
  public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
    publishAuditLogRequest(ctx);
    super.write(ctx, msg, promise);
  }

  /**
   * Need to handle for the case when "write" is not called in the netty channel.
   */
  @Override
  public void close(ChannelHandlerContext ctx, ChannelPromise promise) throws Exception {
    publishAuditLogRequest(ctx);
    super.close(ctx, promise);
  }

  @Override
  public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
    LOG.error("Got exception: {}", cause.getMessage(), cause);
    // TODO: add WWW-Authenticate header for 401 response -  REACTOR-900
    HttpResponse response = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1,
        HttpResponseStatus.UNAUTHORIZED);
    HttpUtil.setContentLength(response, 0);
    HttpUtil.setKeepAlive(response, false);
    ctx.channel().writeAndFlush(response).addListener(ChannelFutureListener.CLOSE);
  }

  /**
   * Check the audit log attribute attached to a channel.
   * It's not null, publish it and set it to Null.
   */
  private void publishAuditLogRequest(ChannelHandlerContext ctx) throws IOException {
    if (!auditLoggingEnabled) {
      return;
    }
    AuditLogRequest auditLogRequest = getAuditLogRequest(ctx);
    if (auditLogRequest != null) {
      auditLogWriter.publish(auditLogRequest);
    }
  }


  @Nullable
  private AuditLogRequest getAuditLogRequest(ChannelHandlerContext ctx) {

    Object auditLogContextsQueueAttr =
      ctx.channel().attr(AttributeKey.valueOf(AUDIT_LOG_CONTEXT_QUEUE_ATTR)).get();

    // If NO audit logs, then return NULL.
    if (auditLogContextsQueueAttr == null) {
      return null;
    }

    Queue<AuditLogContext> auditLogContextsQueue =  (Queue<AuditLogContext>) auditLogContextsQueueAttr;

    Object userIpObj = ctx.channel().attr(AttributeKey.valueOf(AUDIT_LOG_USER_IP_ATTR)).get();
    String userIp = userIpObj == null ? SecurityRequestContext.getUserIp() : (String) userIpObj;

    // Check Attr for AuditLogRequest Builder.
    Object builderObj = ctx.channel().attr(AttributeKey.valueOf(AUDIT_LOG_REQ_BUILDER_ATTR)).get();
    AuditLogRequest.Builder builder = builderObj == null ? SecurityRequestContext.getAuditLogRequestBuilder() :
      (AuditLogRequest.Builder) builderObj;

    //If the AuditLogContextsQueue is NOT empty, then the AuditLogRequest.Builder should be NEVER be null.
    //It should either come from ATTR of pipeline or SecurityRequestContext.
    //Ideally we will never encounter this.
    if (builder == null) {
      LOG.error("The meta data required to publish audit logs are missing or null. Following Audit logs will be "
                  + "skipped: {}", auditLogContextsQueue.stream().map(String::valueOf)
                  .collect(Collectors.joining(", ")));
      //Returning null as Operation is already completed.
      return null;
    }

    //Clear Attributes
    clearAttributes(ctx);

    return builder
      .userIp(userIp)
      .auditLogContextQueue(auditLogContextsQueue)
      .build();
  }

  /**
   * Stores metadata from SecurityRequestContext inside channelRead's Finally method.
   */
  private void setAuditLogMetaDataInChannel(ChannelHandlerContext ctx) {

    if (!auditLoggingEnabled) {
      return;
    }

    Queue<AuditLogContext> auditLogContextQueue = SecurityRequestContext.getAuditLogQueue();

    // In either case, if Queue from SecurityRequestContext is empty, then no Audit Logs.
    if (!auditLogContextQueue.isEmpty()) {
      ctx.channel().attr(AttributeKey.valueOf(AUDIT_LOG_CONTEXT_QUEUE_ATTR))
        .set(auditLogContextQueue);
    }

    // Store all audit metadata info stored in AuditLogRequest.Builder in ATTR from  AuditLogSetterHook#postCall
    // This is just to ensure we don't lose metadata information if already populated because of some RESET call on
    // SecurityRequestContext. This Also ensures that if Thread changes in Close / Write , then this info is preserved.
    AuditLogRequest.Builder builder = SecurityRequestContext.getAuditLogRequestBuilder();
    if (builder != null) {
      ctx.channel().attr(AttributeKey.valueOf(AUDIT_LOG_REQ_BUILDER_ATTR)).set(builder);
    }
  }

  private void clearAttributes(ChannelHandlerContext ctx) {
    ctx.channel().attr(AttributeKey.valueOf(AUDIT_LOG_REQ_BUILDER_ATTR)).set(null);
    ctx.channel().attr(AttributeKey.valueOf(AUDIT_LOG_USER_IP_ATTR)).set(null);
    ctx.channel().attr(AttributeKey.valueOf(AUDIT_LOG_CONTEXT_QUEUE_ATTR)).set(null);
  }
}

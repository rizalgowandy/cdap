/*
 * Copyright © 2015-2019 Cask Data, Inc.
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

package io.cdap.cdap.gateway.handlers;

import com.google.common.io.Closeables;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.google.inject.Inject;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.security.AuditDetail;
import io.cdap.cdap.common.security.AuditPolicy;
import io.cdap.cdap.data2.transaction.TransactionSystemClientAdapter;
import io.cdap.cdap.gateway.handlers.util.AbstractAppFabricHttpHandler;
import io.cdap.http.BodyProducer;
import io.cdap.http.HandlerContext;
import io.cdap.http.HttpResponder;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.http.EmptyHttpHeaders;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Type;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import javax.annotation.Nullable;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.QueryParam;
import org.apache.hadoop.conf.Configuration;
import org.apache.tephra.InvalidTruncateTimeException;
import org.apache.tephra.Transaction;
import org.apache.tephra.TransactionCouldNotTakeSnapshotException;
import org.apache.tephra.TransactionSystemClient;
import org.apache.tephra.TxConstants;
import org.apache.tephra.txprune.RegionPruneInfo;
import org.apache.tephra.txprune.hbase.InvalidListPruningDebug;
import org.apache.tephra.txprune.hbase.RegionsAtTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Handler to for managing transaction states.
 */
@Path(Constants.Gateway.API_VERSION_3)
public class TransactionHttpHandler extends AbstractAppFabricHttpHandler {

  private static final Logger LOG = LoggerFactory.getLogger(TransactionHttpHandler.class);
  private static final Gson GSON = new Gson();
  private static final Type STRING_LONG_MAP_TYPE = new TypeToken<Map<String, Long>>() {
  }.getType();
  private static final Type STRING_LONG_SET_MAP_TYPE = new TypeToken<Map<String, Set<Long>>>() {
  }.getType();
  private static final String PRUNING_TOOL_CLASS_NAME = "org.apache.tephra.hbase.txprune.InvalidListPruningDebugTool";

  private final Configuration hConf;
  private final CConfiguration cConf;
  private final TransactionSystemClient txClient;
  private final boolean pruneEnable;
  private volatile InvalidListPruningDebug pruningDebug;

  @Inject
  public TransactionHttpHandler(Configuration hConf, CConfiguration cConf,
      TransactionSystemClient txClient) {
    this.hConf = hConf;
    this.cConf = cConf;
    this.txClient = new TransactionSystemClientAdapter(txClient);
    this.pruneEnable = cConf.getBoolean(TxConstants.TransactionPruning.PRUNE_ENABLE,
        TxConstants.TransactionPruning.DEFAULT_PRUNE_ENABLE);
  }

  /**
   * Retrieve the state of the transaction manager.
   */
  @Path("/transactions/state")
  @GET
  public void getTxManagerSnapshot(HttpRequest request, HttpResponder responder)
      throws TransactionCouldNotTakeSnapshotException, IOException {
    LOG.trace("Taking transaction manager snapshot at time {}", System.currentTimeMillis());
    LOG.trace("Took and retrieved transaction manager snapshot successfully.");

    final InputStream in = txClient.getSnapshotInputStream();
    try {
      responder.sendContent(HttpResponseStatus.OK, new BodyProducer() {

        @Override
        public ByteBuf nextChunk() throws Exception {
          ByteBuf buffer = Unpooled.buffer(4096);
          buffer.writeBytes(in, 4096);
          return buffer;
        }

        @Override
        public void finished() throws Exception {
          Closeables.closeQuietly(in);
        }

        @Override
        public void handleError(@Nullable Throwable cause) {
          Closeables.closeQuietly(in);
        }
      }, EmptyHttpHeaders.INSTANCE);
    } catch (Exception e) {
      Closeables.closeQuietly(in);
      throw e;
    }
  }

  /**
   * Invalidate a transaction.
   *
   * @param txId transaction ID.
   */
  @Path("/transactions/{tx-id}/invalidate")
  @POST
  public void invalidateTx(HttpRequest request, HttpResponder responder,
      @PathParam("tx-id") String txId) {
    try {
      long txIdLong = Long.parseLong(txId);
      boolean success = txClient.invalidate(txIdLong);
      if (success) {
        LOG.info("Transaction {} successfully invalidated", txId);
        responder.sendStatus(HttpResponseStatus.OK);
      } else {
        LOG.info("Transaction {} could not be invalidated: not in progress.", txId);
        responder.sendStatus(HttpResponseStatus.CONFLICT);
      }
    } catch (NumberFormatException e) {
      LOG.info("Could not invalidate transaction: {} is not a valid tx id", txId);
      responder.sendStatus(HttpResponseStatus.BAD_REQUEST);
    }
  }

  @Path("/transactions/invalid/remove/until")
  @POST
  @AuditPolicy(AuditDetail.REQUEST_BODY)
  public void truncateInvalidTxBefore(FullHttpRequest request,
      HttpResponder responder) throws InvalidTruncateTimeException {
    Map<String, Long> body;
    try {
      body = parseBody(request, STRING_LONG_MAP_TYPE);
    } catch (IllegalArgumentException e) {
      responder.sendString(HttpResponseStatus.BAD_REQUEST, "Invalid time value in request");
      return;
    }

    if (body == null || !body.containsKey("time")) {
      responder.sendString(HttpResponseStatus.BAD_REQUEST, "Time not specified");
      return;
    }

    long time = body.get("time");
    txClient.truncateInvalidTxBefore(time);
    responder.sendStatus(HttpResponseStatus.OK);
  }

  @Path("/transactions/invalid/remove/ids")
  @POST
  @AuditPolicy(AuditDetail.REQUEST_BODY)
  public void truncateInvalidTx(FullHttpRequest request, HttpResponder responder) {
    Map<String, Set<Long>> body;
    try {
      body = parseBody(request, STRING_LONG_SET_MAP_TYPE);
    } catch (IllegalArgumentException e) {
      responder.sendString(HttpResponseStatus.BAD_REQUEST, "Invalid ids specified in request");
      return;
    }

    if (body == null || !body.containsKey("ids")) {
      responder.sendString(HttpResponseStatus.BAD_REQUEST, "Transaction ids not specified");
      return;
    }

    Set<Long> txIds = body.get("ids");
    txClient.truncateInvalidTx(txIds);
    responder.sendStatus(HttpResponseStatus.OK);
  }

  @Path("/transactions/invalid/size")
  @GET
  public void invalidTxSize(HttpRequest request, HttpResponder responder) {
    int invalidSize = txClient.getInvalidSize();
    responder.sendJson(HttpResponseStatus.OK,
        GSON.toJson(Collections.singletonMap("size", invalidSize)));
  }

  @Path("/transactions/invalid")
  @GET
  public void invalidList(HttpRequest request, HttpResponder responder,
      @QueryParam("limit") @DefaultValue("-1") int limit) {
    Transaction tx = txClient.startShort();
    txClient.abort(tx);
    long[] invalids = tx.getInvalids();
    if (limit == -1) {
      responder.sendJson(HttpResponseStatus.OK, GSON.toJson(invalids));
      return;
    }
    responder.sendJson(HttpResponseStatus.OK,
        GSON.toJson(Arrays.copyOf(invalids, Math.min(limit, invalids.length))));
  }

  /**
   * Reset the state of the transaction manager.
   */
  @Path("/transactions/state")
  @POST
  public void resetTxManagerState(HttpRequest request, HttpResponder responder) {
    txClient.resetState();
    responder.sendStatus(HttpResponseStatus.OK);
  }

  /**
   * Trigger transaction pruning.
   */
  @Path("/transactions/prune/now")
  @POST
  public void pruneNow(HttpRequest request, HttpResponder responder) {
    txClient.pruneNow();
    responder.sendStatus(HttpResponseStatus.OK);
  }


  @Path("/transactions/prune/regions/{region-name}")
  @GET
  public void getPruneInfo(HttpRequest request, HttpResponder responder,
      @PathParam("region-name") String regionName) {
    try {
      if (!initializePruningDebug(responder)) {
        return;
      }

      RegionPruneInfo pruneInfo = pruningDebug.getRegionPruneInfo(regionName);
      if (pruneInfo == null) {
        responder.sendString(HttpResponseStatus.NOT_FOUND,
            "No prune upper bound has been registered for this region yet.");
        return;
      }
      responder.sendJson(HttpResponseStatus.OK, GSON.toJson(pruneInfo));
    } catch (Exception e) {
      responder.sendString(HttpResponseStatus.BAD_REQUEST, e.getMessage());
      LOG.debug("Exception while trying to fetch the RegionPruneInfo.", e);
    }
  }

  @Path("/transactions/prune/regions")
  @GET
  public void getTimeRegions(HttpRequest request, HttpResponder responder,
      @QueryParam("time") @DefaultValue("now") String time) {
    try {
      if (!initializePruningDebug(responder)) {
        return;
      }

      RegionsAtTime timeRegionInfo = pruningDebug.getRegionsOnOrBeforeTime(time);
      responder.sendJson(HttpResponseStatus.OK, GSON.toJson(timeRegionInfo));
    } catch (Exception e) {
      responder.sendString(HttpResponseStatus.BAD_REQUEST, e.getMessage());
      LOG.debug("Exception while trying to fetch the time region.", e);
    }
  }

  @Path("/transactions/prune/regions/idle")
  @GET
  public void getIdleRegions(HttpRequest request, HttpResponder responder,
      @QueryParam("limit") @DefaultValue("-1") int numRegions,
      @QueryParam("time") @DefaultValue("now") String time) {
    try {
      if (!initializePruningDebug(responder)) {
        return;
      }

      SortedSet<? extends RegionPruneInfo> pruneInfos = pruningDebug.getIdleRegions(numRegions,
          time);
      responder.sendJson(HttpResponseStatus.OK, GSON.toJson(pruneInfos));
    } catch (Exception e) {
      responder.sendString(HttpResponseStatus.BAD_REQUEST, e.getMessage());
      LOG.debug("Exception while trying to fetch the idle regions.", e);
    }
  }

  @Path("/transactions/prune/regions/block")
  @GET
  public void getRegionsToBeCompacted(HttpRequest request, HttpResponder responder,
      @QueryParam("limit") @DefaultValue("-1") int numRegions,
      @QueryParam("time") @DefaultValue("now") String time) {
    try {
      if (!initializePruningDebug(responder)) {
        return;
      }

      Set<String> regionNames = pruningDebug.getRegionsToBeCompacted(numRegions, time);
      responder.sendJson(HttpResponseStatus.OK, GSON.toJson(regionNames));
    } catch (Exception e) {
      responder.sendString(HttpResponseStatus.BAD_REQUEST, e.getMessage());
      LOG.debug("Exception while trying to get the regions that needs to be compacted.", e);
    }
  }

  private boolean initializePruningDebug(HttpResponder responder) {
    if (!pruneEnable) {
      responder.sendString(HttpResponseStatus.BAD_REQUEST, "Invalid List Pruning is not enabled.");
      return false;
    }

    synchronized (this) {
      if (pruningDebug != null) {
        return true;
      }

      // Copy over both cConf and hConf into the pruning configuration
      Configuration configuration = new Configuration();
      configuration.clear();
      // First copy hConf and then cConf so that we retain the values from cConf for any parameters defined in both
      copyConf(configuration, hConf);
      copyConf(configuration, cConf);

      try {
        @SuppressWarnings("unchecked")
        Class<? extends InvalidListPruningDebug> clazz =
            (Class<? extends InvalidListPruningDebug>) getClass().getClassLoader()
                .loadClass(PRUNING_TOOL_CLASS_NAME);
        this.pruningDebug = clazz.newInstance();
        pruningDebug.initialize(configuration);
      } catch (Exception e) {
        LOG.error("Not able to instantiate pruning debug class", e);
        responder.sendString(HttpResponseStatus.INTERNAL_SERVER_ERROR,
            "Cannot instantiate the pruning debug tool: " + e.getMessage());
        pruningDebug = null;
        return false;
      }
      return true;
    }
  }

  @Override
  public void destroy(HandlerContext context) {
    super.destroy(context);
    synchronized (this) {
      if (pruningDebug != null) {
        try {
          pruningDebug.destroy();
        } catch (IOException e) {
          LOG.error("Error destroying pruning debug instance", e);
        }
      }
    }
  }

  private void copyConf(Configuration to, Iterable<Map.Entry<String, String>> from) {
    for (Map.Entry<String, String> entry : from) {
      to.set(entry.getKey(), entry.getValue());
    }
  }
}

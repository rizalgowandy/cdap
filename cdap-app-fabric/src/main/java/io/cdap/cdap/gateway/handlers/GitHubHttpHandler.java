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

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.inject.Inject;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.gateway.handlers.util.AbstractAppFabricHttpHandler;
import io.cdap.cdap.internal.app.store.GitHubStore;
import io.cdap.cdap.internal.github.GitHubRepo;
import io.cdap.http.HttpResponder;
import io.netty.handler.codec.http.FullHttpMessage;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;

import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import javax.validation.constraints.NotNull;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;

@Path(Constants.Gateway.API_VERSION_3)
public class GitHubHttpHandler extends AbstractAppFabricHttpHandler {
  private static final Gson GSON = new Gson();
  private final GitHubStore gitStore;


  @Inject
  GitHubHttpHandler(GitHubStore gitStore) {
    this.gitStore = gitStore;
  }

  @Path("repos/github")
  @GET
  public void getRepos(HttpRequest request, HttpResponder responder) throws Exception {
    responder.sendJson(HttpResponseStatus.OK, GSON.toJson(gitStore.getRepos()));
  }

  /**
   * Returns user repository information
   */
  @Path("repos/github/{repo}")
  @GET
  public void getRepoInfo(HttpRequest request, HttpResponder responder,
                          @NotNull @PathParam("repo") String repo) throws Exception {
    if (gitStore.getRepo(repo) != null) {
      responder.sendJson(HttpResponseStatus.OK, GSON.toJson(gitStore.getRepo(repo)));
    } else {
      responder.sendStatus(HttpResponseStatus.NOT_FOUND);
    }
  }

  @Path("repos/github/{repo}")
  @PUT
  public void addRepoInfo(FullHttpRequest request, HttpResponder responder,
                          @NotNull @PathParam("repo") String repo) throws Exception {

    try {
      GitHubRepo githubRequest = GSON.fromJson(request.content().toString(StandardCharsets.UTF_8)
          , GitHubRepo.class);

      if (githubRequest.validateAllFields()) {
        //test the repo's connection to ensure it exists.
        if (testRepoConnection(githubRequest) == HttpURLConnection.HTTP_OK) {
          String authString = "token " + githubRequest.getAuthString();
          gitStore.addOrUpdateRepo(repo, githubRequest.getUrl(), githubRequest.getDefaultBranch(),
              authString);
          responder.sendString(HttpResponseStatus.OK, "Repository Information Saved.");
        } else if (testRepoConnection(githubRequest) == HttpURLConnection.HTTP_MOVED_PERM) {
          responder.sendString(HttpResponseStatus.MOVED_PERMANENTLY, "Repository has been moved.");
        } else if (testRepoConnection(githubRequest) == HttpURLConnection.HTTP_FORBIDDEN) {
          responder.sendString(HttpResponseStatus.FORBIDDEN, "You do not have access to this repository.");
        } else {
          responder.sendString(HttpResponseStatus.NOT_FOUND, "Repository was not found.");
        }
        //If the user leaves fields blank, throw back which fields are missing
      } else {
        String errorString = "Please enter a ";
        if (!githubRequest.validNickname()) {
          errorString += "nickname, ";
        }
        if (!githubRequest.validUrl()) {
          errorString += "url, ";
        }
        if (!githubRequest.validDefaultBranch()) {
          errorString += "default branch, ";
        }
        if (!githubRequest.validAuthString()) {
          errorString += "authorization token, ";
        }
        responder.sendString(HttpResponseStatus.BAD_REQUEST,
            errorString.substring(0, errorString.length() - 2));
      }
    } catch (Exception ex) {
      responder.sendString(HttpResponseStatus.INTERNAL_SERVER_ERROR, "Please enter fields formatted correctly.");
    }


  }

  /**
   * Deletes current repository info
   */
  @Path("repos/github/{repo}")
  @DELETE
  public void deleteRepoInfo(HttpRequest request, HttpResponder responder,
                              @NotNull @PathParam("repo") String repo) throws Exception {
    if (gitStore.getRepo(repo) != null) {
      gitStore.deleteRepo(repo);
      responder.sendStatus(HttpResponseStatus.OK);
    } else {
      responder.sendStatus(HttpResponseStatus.NOT_FOUND);
    }
  }

  @Path("repos/github/{repo}/testconnection")
  @POST
  public void testRepoConnection(HttpRequest request, HttpResponder responder,
      @NotNull @PathParam("repo") String repo) throws Exception {
    GitHubRepo test = gitStore.getRepo(repo);
    URL url = new URL(parseUrl(test.getUrl()));
    HttpURLConnection con = (HttpURLConnection) url.openConnection();
    con.setRequestMethod("GET");
    con.setRequestProperty("Authorization", test.getAuthString());
    if (con.getResponseCode() == HttpURLConnection.HTTP_OK) {
      responder.sendString(HttpResponseStatus.OK, "Connection Successful.");
    } else if (con.getResponseCode() == HttpURLConnection.HTTP_MOVED_PERM) {
      responder.sendString(HttpResponseStatus.MOVED_PERMANENTLY, "Repository has been moved.");
    } else if (con.getResponseCode() == HttpURLConnection.HTTP_FORBIDDEN) {
      responder.sendString(HttpResponseStatus.FORBIDDEN, "You do not have access to this repository.");
    } else {
      responder.sendString(HttpResponseStatus.NOT_FOUND, "Repository was not found.");
    }
  }

  @POST
  @Path("repos/github/checkin/{repo}")
  public void checkInRepo(FullHttpRequest request, HttpResponder responder,
      @NotNull @PathParam("repo") String repo) throws Exception {

    try {
      JsonObject jObj = GSON
          .fromJson(request.content().toString(StandardCharsets.UTF_8), JsonObject.class);

      String branch = gitStore.getRepo(repo).getDefaultBranch();
      if (jObj.has("branch")) {
        branch = jObj.getAsJsonPrimitive("branch").toString().
            replace("\"", "");
      }

      String path = jObj.getAsJsonPrimitive("path").toString().
          replace("\"", "");

      GitHubRepo gitHubRepo = gitStore.getRepo(repo);
      URL url = new URL(parseUrl(gitHubRepo.getUrl()) + "/contents/" + path + "?ref=" + branch);
      HttpURLConnection con = (HttpURLConnection) url.openConnection();
      con.setRequestMethod("GET");
      con.setRequestProperty("Authorization", gitHubRepo.getAuthString());

      BufferedReader reader = new BufferedReader(new InputStreamReader(con.getInputStream()));

      if (con.getResponseCode() == HttpURLConnection.HTTP_OK) {
        responder.sendString(HttpResponseStatus.OK, retrieveContent(reader));
      } else if (con.getResponseCode() == HttpURLConnection.HTTP_FORBIDDEN) {
        responder.sendString(HttpResponseStatus.FORBIDDEN, "You do not have access to this file");
      } else {
        responder
            .sendString(HttpResponseStatus.NOT_FOUND, "File not found, check filepath + branch.");
      }
      reader.close();
    } catch (Exception ex) {
      responder.sendString(HttpResponseStatus.NOT_FOUND, "Verify that file exists.");
    }
  }

  @POST
  @Path("repos/github/checkout/{repo}")
  public void checkOutRepo(FullHttpRequest request, HttpResponder responder,
      @NotNull @PathParam("repo") String repo) throws Exception {

    JsonObject pipelineInput = GSON.fromJson(request.content().toString(StandardCharsets.UTF_8), JsonObject.class);

    String branch = gitStore.getRepo(repo).getDefaultBranch();
    if (pipelineInput.has("branch")) {
      branch = pipelineInput.getAsJsonPrimitive("branch").toString().
          replace("\"", "");
    }

    String path = pipelineInput.getAsJsonPrimitive("path").toString().
        replace("\"", "");

    GitHubRepo gitHubRepo = gitStore.getRepo(repo);
    URL url = new URL(parseUrl(gitHubRepo.getUrl()) + "/contents/" + path + "?ref=" + branch);
    HttpURLConnection con = (HttpURLConnection) url.openConnection();

    con.setRequestMethod("PUT");
    con.setDoOutput(true);
    con.setRequestProperty("Authorization", gitHubRepo.getAuthString());

    JsonObject pipelineOutput  = new JsonObject();


    String message = pipelineInput.getAsJsonPrimitive("message").toString()
        .replace("\"", "");
    pipelineOutput.addProperty("message", message);

    String rawContent = pipelineInput.getAsJsonPrimitive("content").toString()
        .replace("\"", "");
    pipelineOutput.addProperty("content", Base64.getEncoder().encodeToString(rawContent.getBytes(
        StandardCharsets.UTF_8)));

    if (pipelineInput.has("sha")) {
      String sha = pipelineInput.getAsJsonPrimitive("sha").toString()
          .replace("\"", "");
      pipelineOutput.addProperty("sha", sha);
    }

    DataOutputStream outputStream = new DataOutputStream(con.getOutputStream());
    outputStream.write(pipelineOutput.toString().getBytes(StandardCharsets.UTF_8));
    outputStream.flush();
    outputStream.close();

    if (con.getResponseCode() == HttpURLConnection.HTTP_CREATED) {
      responder.sendString(HttpResponseStatus.OK, "Checkout successful.");
    } else if (con.getResponseCode() == HttpURLConnection.HTTP_CONFLICT) {
      responder.sendString(HttpResponseStatus.CONFLICT, "This file already exists, "
          + "provide a sha in order to update it");
    } else if (con.getResponseCode() == 422) {
      responder.sendString(HttpResponseStatus.UNAUTHORIZED,
          "Please check your authorization key and file path");
    } else {
      responder.sendString(HttpResponseStatus.NOT_FOUND, con.getResponseCode() + " file destination not found");
    }

  }

  public String parseUrl(String url) throws Exception {
    //Parsing SSH git url which has the format -> https://github.com/{owner or org}/{repository name}
    URI parser = new URI(url);
    String path = parser.getPath();
    int parse = path.indexOf("/");
    //parse the owner and repo name
    String owner = path.substring(0, parse);
    String name = path.substring(parse + 1);
    //Put it all together
    return "https://api.github.com/repos" + owner + "/"
        + name;
  }

  public String retrieveContent(BufferedReader reader) throws Exception {
    String input;
    StringBuilder response = new StringBuilder();
    while ((input = reader.readLine()) != null) {
      response.append(input);
    }
    //get the actual content or JSON pipeline
    String encodedContent = GSON.fromJson(response.toString(), JsonObject.class).
        getAsJsonPrimitive("content").toString().
        replace("\"", "");

    String[] contentLines = encodedContent.split("\\\\n");
    StringBuilder content = new StringBuilder();

    for (String s : contentLines) {
      content.append(new String(Base64.getDecoder().decode(s.getBytes(StandardCharsets.UTF_8)), "UTF-8"));
    }

    return content.toString();
  }

  public int testRepoConnection(GitHubRepo repo) throws Exception {
    URL url = new URL(parseUrl(repo.getUrl()));
    HttpURLConnection con = (HttpURLConnection) url.openConnection();
    con.setRequestMethod("GET");

    con.setRequestProperty("Authorization", repo.getAuthString());

    return con.getResponseCode();
  }
}

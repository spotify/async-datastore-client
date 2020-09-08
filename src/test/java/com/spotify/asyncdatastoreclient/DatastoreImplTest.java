package com.spotify.asyncdatastoreclient;

import com.google.api.client.auth.oauth2.Credential;
import com.google.api.client.http.HttpRequest;
import com.google.api.client.http.protobuf.ProtoHttpContent;
import com.google.protobuf.Int32Value;
import org.asynchttpclient.Request;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class DatastoreImplTest {

  @Test
  public void testEnsureCachedTokenUpdatesOnExternalRefresh() throws Exception {

    Credential mockCredential = new Credential.Builder(new Credential.AccessMethod() {
      @Override
      public void intercept(HttpRequest httpRequest, String s) {
        // noop
      }

      @Override
      public String getAccessTokenFromRequest(HttpRequest httpRequest) {
        return "";
      }
    }).build();

    DatastoreConfig config = DatastoreConfig.builder().credential(mockCredential).build();

    DatastoreImpl client = new DatastoreImpl(config);

    // Thread 1 refreshes credential
    mockCredential.setAccessToken("access-token-1");
    client.refreshAccessToken();

    // Thread 2 refreshes credential
    mockCredential.setAccessToken("access-token-2");
    client.refreshAccessToken();

    Request request = client.prepareRequest("", new ProtoHttpContent(
      Int32Value.newBuilder().setValue(123).build())).build();

    // Make sure request is made with latest access token
    assertEquals("Bearer access-token-2", request.getHeaders().get("Authorization"));
  }
}

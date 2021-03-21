package net.wuxianjie.elasticsearch.config;

import java.io.IOException;
import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;

public class Initialization {

  private final RestHighLevelClient client;

  private Initialization() {
    client = new RestHighLevelClient(
      RestClient.builder(
        new HttpHost("localhost", 9200, "http")));
  }

  private static class SingletonHelper {

    private static final Initialization INSTANCE = new Initialization();
  }

  public static Initialization getInstance() {
    return SingletonHelper.INSTANCE;
  }

  public RestHighLevelClient getClient() {
    return this.client;
  }

  public void close() throws IOException {
    this.client.close();
  }
}

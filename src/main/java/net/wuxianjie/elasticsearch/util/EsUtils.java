package net.wuxianjie.elasticsearch.util;

import java.io.IOException;
import net.wuxianjie.elasticsearch.config.Initialization;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;

public class EsUtils {

  public static void deleteIndex(String indexName) throws IOException {

    DeleteIndexRequest request = new DeleteIndexRequest(indexName);

    RestHighLevelClient client = Initialization.getInstance().getClient();
    client.indices().delete(request, RequestOptions.DEFAULT);
  }

  public static void bulkIndex(BulkRequest request) throws IOException {

    try (RestHighLevelClient client = Initialization.getInstance().getClient()) {

      BulkResponse bulkResponse = client.bulk(request, RequestOptions.DEFAULT);

      if (bulkResponse.hasFailures()) {
        for (BulkItemResponse bulkItemResponse : bulkResponse) {
          if (bulkItemResponse.isFailed()) {
            BulkItemResponse.Failure failure = bulkItemResponse.getFailure();
            throw new RuntimeException(failure.getMessage(), failure.getCause());
          }
        }
      }
    }
  }
}

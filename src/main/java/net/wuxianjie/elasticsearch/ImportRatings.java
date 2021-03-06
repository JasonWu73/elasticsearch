package net.wuxianjie.elasticsearch;

import com.opencsv.CSVReaderHeaderAware;
import com.opencsv.exceptions.CsvValidationException;
import java.io.FileReader;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Map;
import net.wuxianjie.elasticsearch.util.FileUtils;
import net.wuxianjie.elasticsearch.util.EsUtils;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.common.xcontent.XContentType;

public class ImportRatings {

  public static final String PATH_TO_CSV = "ml-latest-small/ratings.csv";
  public static final String INDEX_NAME = "ratings";

  public static void main(String[] args) throws CsvValidationException, IOException, URISyntaxException {
    EsUtils.bulkIndex(readCsv());
  }

  private static BulkRequest readCsv() throws IOException, CsvValidationException, URISyntaxException {

    Map<String, String> movieMap = ImportMovies.getMovieMap();

    BulkRequest request = new BulkRequest();

    try (CSVReaderHeaderAware reader = new CSVReaderHeaderAware(new FileReader(FileUtils.getFilePath(PATH_TO_CSV)))) {

      Map<String, String> row;
      while ((row = reader.readMap()) != null) {

        String movieId = row.get("movieId");

        request.add(new IndexRequest(INDEX_NAME)
          .source(XContentType.JSON,
            "user_id", row.get("userId"),
            "movie_id", movieId,
            "movie_title", movieMap.get(movieId),
            "rating", row.get("rating"),
            "timestamp", row.get("timestamp")));
      }
    }

    return request;
  }
}

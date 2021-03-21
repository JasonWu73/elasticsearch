package net.wuxianjie.elasticsearch;

import com.opencsv.CSVReaderHeaderAware;
import com.opencsv.exceptions.CsvValidationException;
import java.io.FileReader;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;
import net.wuxianjie.elasticsearch.util.CsvUtils;
import net.wuxianjie.elasticsearch.util.EsUtils;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.common.xcontent.XContentType;

public class ImportMovies {

  public static final String PATH_TO_CSV = "ml-latest-small/movies.csv";
  public static final String INDEX_NAME = "movies";

  public static void main(String[] args) throws IOException, URISyntaxException, CsvValidationException {

    EsUtils.bulkIndex(readCsv(PATH_TO_CSV));
  }

  public static BulkRequest readCsv(String pathToCsv) throws IOException, CsvValidationException, URISyntaxException {

    BulkRequest request = new BulkRequest();

    CSVReaderHeaderAware reader = new CSVReaderHeaderAware(new FileReader(CsvUtils.getFilePath(pathToCsv)));

    Map<String, String> row;
    while ((row = reader.readMap()) != null) {

      String titleAndYear = row.get("title");
      Map<String, String> titleMap = parseTitle(titleAndYear);

      request.add(new IndexRequest(INDEX_NAME)
        .id(row.get("movieId"))
        .source(XContentType.JSON,
          "id", row.get("movieId"),
          "title", titleMap.get("title"),
          "year", titleMap.get("year"),
          "genre", row.get("genres")));
    }

    return request;
  }

  public static Map<String, String> parseTitle(String str) {

    int beginIndexOfYear = str.lastIndexOf(" (");

    if (beginIndexOfYear == -1) {
      return new HashMap<>() {{
        put("title", str);
        put("year", null);
      }};
    }

    return new HashMap<>() {{
      put("title", str.substring(0, beginIndexOfYear));
      put("year", str.substring(beginIndexOfYear + 2, str.length() - 1));
    }};
  }
}
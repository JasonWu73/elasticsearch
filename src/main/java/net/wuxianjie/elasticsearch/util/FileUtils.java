package net.wuxianjie.elasticsearch.util;

import java.io.File;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Paths;
import net.wuxianjie.elasticsearch.ImportMovies;

public class FileUtils {

  public static String getFilePath(String path) throws URISyntaxException {

    if (path == null || path.isBlank()) {
      throw new IllegalArgumentException("文件路径不能为空");
    }

    URL res = ImportMovies.class.getClassLoader().getResource(path);
    if (res == null) {
      throw new IllegalArgumentException("文件路径错误");
    }

    File file = Paths.get(res.toURI()).toFile();
    return file.getAbsolutePath();
  }
}

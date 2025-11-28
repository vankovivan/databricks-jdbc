package com.databricks.jdbc.common.util;

import com.databricks.jdbc.common.CompressionCodec;
import com.databricks.jdbc.exception.DatabricksParsingException;
import com.databricks.jdbc.exception.DatabricksSQLException;
import com.databricks.jdbc.log.JdbcLogger;
import com.databricks.jdbc.log.JdbcLoggerFactory;
import com.databricks.jdbc.model.telemetry.enums.DatabricksDriverErrorCode;
import java.io.IOException;
import java.io.InputStream;
import net.jpountz.lz4.LZ4FrameInputStream;

public class DecompressionUtil {

  private static final JdbcLogger LOGGER = JdbcLoggerFactory.getLogger(DecompressionUtil.class);

  private static InputStream decompressLZ4Frame(InputStream compressedStream, String context)
      throws DatabricksSQLException {
    LOGGER.debug("Decompressing using LZ4 Frame algorithm. Context: {}", context);
    try {
      return new LZ4FrameInputStream(compressedStream);
    } catch (IOException e) {
      String errorMessage =
          String.format("Unable to de-compress LZ4 Frame compressed result %s", context);
      LOGGER.error(e, errorMessage);
      throw new DatabricksParsingException(
          errorMessage, e, DatabricksDriverErrorCode.DECOMPRESSION_ERROR);
    }
  }

  public static InputStream decompress(
      InputStream compressedStream, CompressionCodec compressionCodec, String context)
      throws DatabricksSQLException {
    if (compressionCodec == null || compressedStream == null) {
      LOGGER.debug("Compression is NONE /InputStream is `NULL`. Skipping compression.");
      return compressedStream;
    }
    switch (compressionCodec) {
      case NONE:
        LOGGER.debug("Compression type is `NONE`. Skipping compression.");
        return compressedStream;
      case LZ4_FRAME:
        return decompressLZ4Frame(compressedStream, context);
      default:
        String errorMessage =
            String.format("Unknown compression type: %s. Context : %s", compressionCodec, context);
        LOGGER.error(errorMessage);
        throw new DatabricksSQLException(
            errorMessage, DatabricksDriverErrorCode.DECOMPRESSION_ERROR);
    }
  }
}

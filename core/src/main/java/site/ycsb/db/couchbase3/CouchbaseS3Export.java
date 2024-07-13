package site.ycsb.db.couchbase3;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.cli.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;

import java.io.IOException;
import java.nio.file.Files;
import org.apache.hadoop.fs.Path;
import java.nio.file.Paths;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;

/**
 * Clean Cluster after Testing.
 */
public class CouchbaseS3Export {
  public static final String CLUSTER_HOST = "couchbase.hostname";
  public static final String CLUSTER_USER = "couchbase.username";
  public static final String CLUSTER_PASSWORD = "couchbase.password";
  public static final String CLUSTER_SSL = "couchbase.sslMode";
  public static final String CLUSTER_BUCKET = "couchbase.bucket";
  public static final String CLUSTER_SCOPE = "couchbase.scope";
  public static final String CLUSTER_COLLECTION = "couchbase.collection";
  public static final String CLUSTER_S3_BUCKET = "couchbase.s3Bucket";

  public static void main(String[] args) {
    Options options = new Options();
    CommandLine cmd = null;
    Properties properties = new Properties();

    Option source = new Option("p", "properties", true, "source properties");
    source.setRequired(true);
    options.addOption(source);

    CommandLineParser parser = new DefaultParser();
    HelpFormatter formatter = new HelpFormatter();

    try {
      cmd = parser.parse(options, args);
    } catch (ParseException e) {
      System.out.println(e.getMessage());
      formatter.printHelp("ClusterInit", options);
      System.exit(1);
    }

    String propFile = cmd.getOptionValue("properties");

    try {
      properties.load(Files.newInputStream(Paths.get(propFile)));
    } catch (IOException e) {
      System.out.println("can not open properties file: " + e.getMessage());
      e.printStackTrace(System.err);
      System.exit(1);
    }

    try {
      doImport(properties);
    } catch (Exception e) {
      System.err.println("Error: " + e);
      e.printStackTrace(System.err);
      System.exit(1);
    }
  }

  public static Schema createSchema(String tableName, JsonNode sample) {
    ObjectMapper mapper = new ObjectMapper();
    ObjectNode config = mapper.createObjectNode();
    config.put("type", "record");
    config.put("name", tableName);
    config.put("namespace", "com.example");
    ArrayNode fields = mapper.createArrayNode();
    for (Iterator<Map.Entry<String, JsonNode>> it = sample.fields(); it.hasNext(); ) {
      Map.Entry<String, JsonNode> column = it.next();
      if (column.getValue().isBoolean()) {
        fields.add(mapper.createObjectNode().put("name", column.getKey()).put("type", "boolean"));
      } else if (column.getValue().isInt()) {
        fields.add(mapper.createObjectNode().put("name", column.getKey()).put("type", "int"));
      } else if (column.getValue().isTextual()) {
        fields.add(mapper.createObjectNode().put("name", column.getKey()).put("type", "string"));
      } else if (column.getValue().isDouble()) {
        fields.add(mapper.createObjectNode().put("name", column.getKey()).put("type", "double"));
      } else if (column.getValue().isBinary()) {
        fields.add(mapper.createObjectNode().put("name", column.getKey()).put("type", "bytes"));
      } else if (column.getValue().isFloat()) {
        fields.add(mapper.createObjectNode().put("name", column.getKey()).put("type", "float"));
      } else if (column.getValue().isLong()) {
        fields.add(mapper.createObjectNode().put("name", column.getKey()).put("type", "long"));
      } else if (column.getValue().isNull()) {
        fields.add(mapper.createObjectNode().put("name", column.getKey()).put("type", "null"));
      }
    }
    config.set("fields", fields);
    return new Schema.Parser().parse(config.toString());
  }

  public static GenericRecord getRecord(Schema schema, JsonNode document) {
    GenericRecord record = new GenericData.Record(schema);
    for (Iterator<Map.Entry<String, JsonNode>> it = document.fields(); it.hasNext(); ) {
      Map.Entry<String, JsonNode> column = it.next();
      if (column.getValue().isBoolean()) {
        record.put(column.getKey(), column.getValue());
      } else if (column.getValue().isInt()) {
        record.put(column.getKey(), Integer.valueOf(column.getValue().asInt()));
      } else if (column.getValue().isTextual()) {
        record.put(column.getKey(), column.getValue());
      } else if (column.getValue().isDouble()) {
        record.put(column.getKey(), Double.valueOf(column.getValue().asDouble()));
      } else if (column.getValue().isBinary()) {
        record.put(column.getKey(), column.getValue());
      } else if (column.getValue().isFloat()) {
        record.put(column.getKey(), Double.valueOf(column.getValue().asDouble()));
      } else if (column.getValue().isLong()) {
        record.put(column.getKey(), Long.valueOf(column.getValue().asLong()));
      } else if (column.getValue().isNull()) {
        record.put(column.getKey(), column.getValue());
      }
    }
    return record;
  }

  public static JsonNode getSample(String host, String user, String password, boolean ssl, String bucket, String scope, String collection) {
    CouchbaseConnect.CouchbaseBuilder dbBuilder = new CouchbaseConnect.CouchbaseBuilder();
    CouchbaseConnect db;
    dbBuilder.connect(host, user, password)
        .ssl(ssl)
        .bucket(bucket)
        .scope(scope)
        .collection(collection);
    db = dbBuilder.build();
    return db.runQuery(String.format("SELECT * from %s LIMIT 1", collection)).get(0).get(collection);
  }

  public static void doImport(Properties properties) {
    String clusterHost = properties.getProperty(CLUSTER_HOST, CouchbaseConnect.DEFAULT_HOSTNAME);
    String clusterUser = properties.getProperty(CLUSTER_USER, CouchbaseConnect.DEFAULT_USER);
    String clusterPassword = properties.getProperty(CLUSTER_PASSWORD, CouchbaseConnect.DEFAULT_PASSWORD);
    boolean clusterSsl = properties.getProperty(CLUSTER_SSL, CouchbaseConnect.DEFAULT_SSL_SETTING).equals("true");
    String clusterBucket = properties.getProperty(CLUSTER_BUCKET, "bench");
    String clusterScope = properties.getProperty(CLUSTER_SCOPE, "bench");
    String clusterCollection = properties.getProperty(CLUSTER_COLLECTION, "bench");
    String s3Bucket = properties.getProperty(CLUSTER_S3_BUCKET, "bucket");

    System.err.println("Exporting data");

    exportCollection(clusterHost, clusterUser, clusterPassword, clusterSsl, clusterBucket, clusterScope, clusterCollection);
  }

  private static void exportCollection(String host, String user, String password, boolean ssl, String bucket, String scope, String collection) {
    CouchbaseStream stream = new CouchbaseStream(host, user, password, bucket, ssl, scope, collection);
    Path path = new Path("output", collection + ".parquet");

    JsonNode sample = getSample(host, user, password, ssl, bucket, scope, collection);
    Schema schema = createSchema(collection, sample);

    try {
      ParquetWriter<GenericRecord> writer = AvroParquetWriter.<GenericRecord>builder(path)
          .withRowGroupSize(ParquetWriter.DEFAULT_BLOCK_SIZE)
          .withPageSize(ParquetWriter.DEFAULT_PAGE_SIZE)
          .withSchema(schema)
          .withConf(new Configuration())
          .withCompressionCodec(CompressionCodecName.SNAPPY)
          .withValidation(false)
          .withDictionaryEncoding(false)
          .withWriteMode(ParquetFileWriter.Mode.OVERWRITE)
          .build();
      stream.streamDocuments();
      stream.startToNow();
      stream.getDrain().forEach(o -> {
        DocumentBuffer buffer = new DocumentBuffer(o);
        ObjectMapper mapper = new ObjectMapper();
        try {
          JsonNode node = mapper.readTree(buffer.getContent());
          writer.write(getRecord(schema, node));
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      });
      writer.close();
      stream.stop();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private CouchbaseS3Export() {
    super();
  }
}

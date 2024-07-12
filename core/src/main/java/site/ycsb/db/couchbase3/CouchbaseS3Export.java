package site.ycsb.db.couchbase3;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.avro.Schema;
import org.apache.commons.cli.*;
import site.ycsb.TableKeyType;
import site.ycsb.TableKeys;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
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

  public Schema createSchema(String tableName, JsonNode sample) {
    Schema schema = new Schema.Parser().parse(
        "{\n" +
        "  \"type\": \"record\",\n" +
        "  \"name\": \"people\",\n" +
        "  \"namespace\": \"com.example\",\n" +
        "  \"fields\": [\n" +
        "    {\"name\": \"firstName\", \"type\": \"string\"},\n" +
        "    {\"name\": \"lastName\", \"type\": \"string\"},\n" +
        "    {\"name\": \"gender\", \"type\": \"string\"},\n" +
        "    {\"name\": \"age\", \"type\": \"int\"},\n" +
        "    {\"name\": \"number\", \"type\": \"string\"}\n" +
        "  ]\n" +
        "}");
    return schema;
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

//    ParquetWriter<JsonNode> writer =
//        JsonParquetWriter.Builder(path)
//            .withSchema(schema)
//            .withConf(conf)
//            .withCompressionCodec(CompressionCodecName.SNAPPY)
//            .withDictionaryEncoding(true)
//            .withPageSize(1024 * 1024)
//            .build();

    try {
      stream.streamDocuments();
      stream.startToNow();
      stream.getDrain().forEach(o -> {
        DocumentBuffer buffer = new DocumentBuffer(o);
        ObjectMapper mapper = new ObjectMapper();
        try {
          JsonNode node = mapper.readTree(buffer.getContent());

        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      });
      stream.stop();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private CouchbaseS3Export() {
    super();
  }
}

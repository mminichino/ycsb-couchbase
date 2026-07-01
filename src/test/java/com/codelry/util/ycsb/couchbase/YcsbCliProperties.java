package com.codelry.util.ycsb.couchbase;

import com.codelry.util.ycsb.Benchmark;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * Builds benchmark {@link Properties} from YCSB Client-style CLI arguments.
 */
final class YcsbCliProperties {

  private YcsbCliProperties() {
  }

  static Properties forBenchmark(String[] args) {
    ParsedCli parsed = parse(args);
    Properties props = new Properties();

    for (Properties fileProps : parsed.propertyFiles()) {
      for (String name : fileProps.stringPropertyNames()) {
        props.setProperty(name, fileProps.getProperty(name));
      }
    }

    mergeResource(props, "ycsb.properties");
    mergeResource(props, "db.properties");

    for (String name : parsed.cliOverrides().stringPropertyNames()) {
      props.setProperty(name, parsed.cliOverrides().getProperty(name));
    }

    return props;
  }

  private static ParsedCli parse(String[] args) {
    Properties cliOverrides = new Properties();
    List<Properties> propertyFiles = new ArrayList<>();
    int argIndex = 0;

    if (args.length == 0) {
      throw new IllegalArgumentException("At least one CLI argument is required.");
    }

    while (argIndex < args.length && args[argIndex].startsWith("-")) {
      switch (args[argIndex]) {
        case "-threads" -> {
          argIndex = requireValue(args, argIndex, "-threads");
          cliOverrides.setProperty(Benchmark.THREAD_COUNT_PROPERTY, args[argIndex]);
          argIndex++;
        }
        case "-target" -> {
          argIndex = requireValue(args, argIndex, "-target");
          cliOverrides.setProperty(Benchmark.TARGET_PROPERTY, args[argIndex]);
          argIndex++;
        }
        case "-load" -> {
          cliOverrides.setProperty(Benchmark.DO_TRANSACTIONS_PROPERTY, String.valueOf(false));
          argIndex++;
        }
        case "-t" -> {
          cliOverrides.setProperty(Benchmark.DO_TRANSACTIONS_PROPERTY, String.valueOf(true));
          argIndex++;
        }
        case "-s" -> {
          cliOverrides.setProperty(Benchmark.STATUS_PROPERTY, String.valueOf(true));
          argIndex++;
        }
        case "-db" -> {
          argIndex = requireValue(args, argIndex, "-db");
          cliOverrides.setProperty(Benchmark.DB_PROPERTY, args[argIndex]);
          argIndex++;
        }
        case "-l" -> {
          argIndex = requireValue(args, argIndex, "-l");
          cliOverrides.setProperty(Benchmark.LABEL_PROPERTY, args[argIndex]);
          argIndex++;
        }
        case "-P" -> {
          argIndex = requireValue(args, argIndex, "-P");
          propertyFiles.add(loadFile(args[argIndex]));
          argIndex++;
        }
        case "-p" -> {
          argIndex = requireValue(args, argIndex, "-p");
          putKeyValue(cliOverrides, args[argIndex]);
          argIndex++;
        }
        case "-manual" -> {
          cliOverrides.setProperty(Benchmark.MANUAL_MODE, "true");
          argIndex++;
        }
        default -> throw new IllegalArgumentException("Unknown option: " + args[argIndex]);
      }
    }

    if (argIndex != args.length) {
      throw new IllegalArgumentException("Unexpected argument: " + args[argIndex]);
    }

    return new ParsedCli(propertyFiles, cliOverrides);
  }

  private static int requireValue(String[] args, int optionIndex, String optionName) {
    if (optionIndex + 1 >= args.length) {
      throw new IllegalArgumentException("Missing argument value for " + optionName);
    }
    return optionIndex + 1;
  }

  private static void putKeyValue(Properties props, String keyValue) {
    int separator = keyValue.indexOf('=');
    if (separator < 0) {
      throw new IllegalArgumentException("Argument '-p' must be in key=value format: " + keyValue);
    }
    props.setProperty(keyValue.substring(0, separator), keyValue.substring(separator + 1));
  }

  private static Properties loadFile(String propertyFile) {
    Properties fileProps = new Properties();
    try (FileInputStream in = new FileInputStream(propertyFile)) {
      fileProps.load(in);
      return fileProps;
    } catch (IOException e) {
      throw new IllegalStateException("Cannot load properties file: " + propertyFile, e);
    }
  }

  private static void mergeResource(Properties props, String propertyFile) {
    ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
    if (classLoader == null) {
      classLoader = YcsbCliProperties.class.getClassLoader();
    }
    try (InputStream in = classLoader.getResourceAsStream(propertyFile)) {
      if (in == null) {
        throw new IllegalStateException("Properties resource not found on classpath: " + propertyFile);
      }
      Properties resourceProps = new Properties();
      resourceProps.load(in);
      for (String name : resourceProps.stringPropertyNames()) {
        props.setProperty(name, resourceProps.getProperty(name));
      }
    } catch (IOException e) {
      throw new IllegalStateException("Cannot load properties resource: " + propertyFile, e);
    }
  }

  private record ParsedCli(List<Properties> propertyFiles, Properties cliOverrides) {
  }
}

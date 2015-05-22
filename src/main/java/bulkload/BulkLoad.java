/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package bulkload;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.math.BigDecimal;
import java.net.HttpURLConnection;
import java.net.URL;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.List;

import org.supercsv.io.CsvListReader;
import org.supercsv.prefs.CsvPreference;

import org.apache.cassandra.config.Config;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.io.sstable.CQLSSTableWriter;

/**
 * Usage: java bulkload.BulkLoad
 */
public class BulkLoad
{
  public static final String CSV_PATH = "pdp.csv";

  /** Default output directory */
  public static final String DEFAULT_OUTPUT_DIR = "./data";

  /** Keyspace name */
  public static final String KEYSPACE = "whyso";
  /** Table name */
  public static final String TABLE = "visit";

    /**
     * Schema for bulk loading table.
     * It is important not to forget adding keyspace name before table name,
     * otherwise CQLSSTableWriter throws exception.
     */
    public static final String SCHEMA = String.format("CREATE TABLE %s.%s (" +
      "property_number text, " +
      "referer_domain text, " +
      "file text, " +
      "host text, " +
      "host_header text, " +
      "property_type text, " +
      "referer text, " +
      "req_time text, " +
      "response_time text, " +
      "uri text, " +
      "uri_path text, " +
      "useragent text, " +
      "x_forwarded_for text, " +
      "x_forwarded_for_origin text, " +
      "PRIMARY KEY (property_number) " +
      ")", KEYSPACE, TABLE);

    /**
     * INSERT statement to bulk load.
     * It is like prepared statement. You fill in place holder for each data.
     */
     public static final String INSERT_STMT = String.format("INSERT INTO %s.%s (" +
            "property_number, referer_domain, file, host, host_header, property_type, " +
            "referer, req_time, response_time, uri, uri_path, useragent, x_forwarded_for, x_forwarded_for_origin" +
            ") VALUES (" +
            "?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?" +
            ")", KEYSPACE, TABLE);
    public static String getOrElse(String a) {
        if(a==null){
            return "";
        }else {
         return a;
        }
    }

    public static void main(String[] args)
    {

        // magic!
      Config.setClientMode(true);

        // Create output directory that has keyspace and table name in the path
      File outputDir = new File(DEFAULT_OUTPUT_DIR + File.separator + KEYSPACE + File.separator + TABLE);
      if (!outputDir.exists() && !outputDir.mkdirs())
      {
        throw new RuntimeException("Cannot create output directory: " + outputDir);
      }

        // Prepare SSTable writer
      CQLSSTableWriter.Builder builder = CQLSSTableWriter.builder();
        // set output directory
      builder.inDirectory(outputDir)
               // set target schema
      .forTable(SCHEMA)
               // set CQL statement to put data
      .using(INSERT_STMT)
               // set partitioner if needed
               // default is Murmur3Partitioner so set if you use different one.
      .withPartitioner(new Murmur3Partitioner());
      CQLSSTableWriter writer = builder.build();


      try 
      {
        BufferedReader reader = new BufferedReader(new FileReader(CSV_PATH));
        CsvListReader csvReader = new CsvListReader(reader, CsvPreference.STANDARD_PREFERENCE);
        csvReader.getHeader(true);

                // Write to SSTable while reading data
        List<String> line;
        while ((line = csvReader.read()) != null)
        {
          System.out.println(line);
                    // We use Java types here based on
                    // http://www.datastax.com/drivers/java/2.0/com/datastax/driver/core/DataType.Name.html#asJavaClass%28%29
          writer.addRow(
            new String(line.get(0)),
            new String(getOrElse(line.get(1))),
            new String(getOrElse(line.get(2))),
            new String(getOrElse(line.get(3))),
            new String(getOrElse(line.get(4))),
            new String(getOrElse(line.get(5))),
            new String(getOrElse(line.get(6))),
            new String(getOrElse(line.get(7))),
            new String(getOrElse(line.get(8))),
            new String(getOrElse(line.get(9))),
            new String(getOrElse(line.get(10))),
            new String(getOrElse(line.get(11))),
            new String(getOrElse(line.get(12))),
            new String(getOrElse(line.get(13))));
        }
      }
      catch (InvalidRequestException | IOException e)
      {
        e.printStackTrace();
      }


      try
      {
        writer.close();
      }
      catch (IOException ignore) {}
    }
  }

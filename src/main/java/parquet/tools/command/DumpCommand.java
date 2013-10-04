/**
 * Copyright 2013 ARRIS, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package parquet.tools.command;

import java.io.PrintWriter;

import org.apache.commons.cli.CommandLine;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import com.google.common.base.Joiner;

import parquet.column.ColumnDescriptor;
import parquet.hadoop.ParquetFileReader;
import parquet.hadoop.metadata.ParquetMetadata;
import parquet.io.ColumnIO;
import parquet.io.ColumnIOFactory;
import parquet.io.GroupColumnIO;
import parquet.io.MessageColumnIO;
import parquet.io.PrimitiveColumnIO;
import parquet.schema.MessageType;
import parquet.tools.Main;
import parquet.tools.util.MetadataUtils;
import parquet.tools.util.PrettyPrintWriter;
import parquet.tools.util.PrettyPrintWriter.WhiteSpaceHandler;

public class DumpCommand extends ArgsOnlyCommand {
  public static final String TABS = "    ";
  public static final int BLOCK_BUFFER_SIZE = 64 * 1024;
  public static final String[] USAGE = new String[] {
    "<input>",
    "where <input> is the parquet file to print to stdout"
  };

  public DumpCommand() {
    super(1, 1);
  }

  @Override
  public String[] getUsageDescription() {
    return USAGE;
  }

  @Override
  public void execute(CommandLine options) throws Exception {
    super.execute(options);

    String[] args = options.getArgs();
    String input = args[0];
    
    Configuration conf = new Configuration();
    ParquetMetadata metaData = ParquetFileReader.readFooter(conf, new Path(input));
    MessageType schema = metaData.getFileMetaData().getSchema();

    ColumnIOFactory columnIOFactory = new ColumnIOFactory();
    MessageColumnIO columnIO = columnIOFactory.getColumnIO(schema, schema);
    show((GroupColumnIO)columnIO, "");

    for (PrimitiveColumnIO primColIO : columnIO.getLeaves()) {
      Main.out.print("path: ");
      boolean first = true;
      for (ColumnIO path : primColIO.getPath()) {
        if (!first) Main.out.print(".");
        Main.out.print(path.getName());
        first = false;
      }
      Main.out.println();

      ColumnDescriptor desc = primColIO.getColumnDescriptor();
      Main.out.println("path: " + columnIO.getName() + '.' + Joiner.on('.').skipNulls().join(desc.getPath()));
      Main.out.println("type: " + desc.getType());
      Main.out.println("maximum definition level: " + desc.getMaxDefinitionLevel());
      Main.out.println("maximum repetition level: " + desc.getMaxRepetitionLevel());
    }
  }

  private void show(GroupColumnIO columnIO, String tabs) {
    Main.out.println(tabs + "name: " + columnIO.getName());
    Main.out.println(tabs + "indx: " + columnIO.getIndex());
    for (int i = 0, e = columnIO.getChildrenCount(); i < e; ++i) {
      ColumnIO child = columnIO.getChild(i);
      if (child.getType().isPrimitive()) {
        show(child, tabs + TABS);
      } else {
        show((GroupColumnIO)child, tabs + TABS);
      }
    }
  }

  private void show(ColumnIO columnIO, String tabs) {
    Main.out.println(tabs + "name: " + columnIO.getName());
    Main.out.println(tabs + "indx: " + columnIO.getIndex());
    Main.out.println(tabs + "type: " + columnIO.getType());
  }
}

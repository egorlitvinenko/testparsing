package org.egorlitvinenko.testdisruptor.byteStreamParsing.handler.parsePacket;

import com.lmax.disruptor.EventHandler;
import com.univocity.parsers.csv.CsvParser;
import com.univocity.parsers.csv.CsvParserSettings;
import org.egorlitvinenko.testdisruptor.byteStreamParsing.event.ParsePacketEvent;
import org.egorlitvinenko.testdisruptor.byteStreamParsing.util.ColumnType;

import java.io.*;
import java.util.function.Consumer;

/**
 * @author Egor Litvinenko
 */
public class ReadLineHandler implements EventHandler<ParsePacketEvent> {

    private final ColumnType[] types;

    private final LineReader lineReader;
    private final Consumer<Boolean> endConsumer;

    public ReadLineHandler(String file,
                           char delimiter,
                           char quote,
                           ColumnType[] types,
                           Consumer<Boolean> endConsumer) {
        this.types = types;
        this.endConsumer = endConsumer;
        this.lineReader = new BufferedReader(file, delimiter, quote, types.length);
    }

    @Override
    public void onEvent(ParsePacketEvent event, long sequence, boolean endOfBatch) throws Exception {
        String[] line = lineReader.readLine();
        if (null == line) {
            event.setEnd(true);
            endConsumer.accept(Boolean.TRUE);
        } else {
            for (int i = 0; i < line.length; ++i) {
                switch (types[i]) {
                    case INT_32:
                        event.setInt32String(line[i], i);
                        break;
                    case DOUBLE:
                        event.setDoubleString(line[i], i);
                        break;
                    case LOCAL_DATE:
                        event.setLocalDateString(line[i], i);
                        break;
                    case STRING:
                        event.setString(line[i], i);
                        break;
                    case SQL_DATE:
                        event.setSqlDateString(line[i], i);
                        break;
                }
            }
        }
    }

    private interface LineReader {
        String[] readLine();
    }

    private class UnivosityReader implements LineReader {
        private final CsvParser csvParser;

        private UnivosityReader(String file,
                               char delimiter,
                               char quote) {
            CsvParserSettings csvParserSettings = new CsvParserSettings();
            csvParserSettings.getFormat().setDelimiter(delimiter);
            csvParserSettings.getFormat().setQuote(quote);
            csvParserSettings.setHeaderExtractionEnabled(true);

            csvParser = new CsvParser(csvParserSettings);
            try {
                csvParser.beginParsing(new FileReader(new File(file)));
            } catch (FileNotFoundException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public String[] readLine() {
            return csvParser.parseNext();
        }
    }

    private class BufferedReader implements LineReader {

        private final StringBuilder currentLine;
        private final String[] lineBuffer;
        private final Reader reader;
        private final char delimiter, quote;
        private int counter = 0;

        private BufferedReader(String file,
                               char delimiter,
                               char quote,
                               int size) {
            this.delimiter = delimiter;
            this.quote = quote;
            lineBuffer = new String[size];
            currentLine = new StringBuilder();
            try {
                this.reader = new java.io.BufferedReader(new FileReader(new File(file)));
            } catch (FileNotFoundException e) {
                throw new RuntimeException(e);
            }
            // skipFirst
            readLine();
        }

        @Override
        public String[] readLine() {
            try {
                currentLine.setLength(0);
                counter = 0;
                int read;
                while (0 < (read = reader.read())) {
                    char ch = (char) read;
                    if (ch == '\n') {
                        lineBuffer[counter] = currentLine.toString();
                        return lineBuffer;
                    } else if (ch == delimiter) {
                        lineBuffer[counter++] = currentLine.toString();
                        currentLine.setLength(0);
                    } else if (ch != quote) {
                        currentLine.append(ch);
                    }
                }
                if (read == -1) {
                    return null;
                }
                return lineBuffer;
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

    }

}

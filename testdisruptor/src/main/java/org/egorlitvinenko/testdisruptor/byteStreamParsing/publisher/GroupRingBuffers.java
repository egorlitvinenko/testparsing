package org.egorlitvinenko.testdisruptor.byteStreamParsing.publisher;

/**
 * @author Egor Litvinenko
 */
public class GroupRingBuffers {

    public final DoubleParsePublisher doubleParsePublisher;
    public final Int32ParsePublisher int32ParsePublisher;
    public final LocalDateParsePublisher localDateParsePublisher;

    public GroupRingBuffers(DoubleParsePublisher doubleParsePublisher,
                            Int32ParsePublisher int32ParsePublisher,
                            LocalDateParsePublisher localDateParsePublisher) {
        this.doubleParsePublisher = doubleParsePublisher;
        this.int32ParsePublisher = int32ParsePublisher;
        this.localDateParsePublisher = localDateParsePublisher;
    }
}

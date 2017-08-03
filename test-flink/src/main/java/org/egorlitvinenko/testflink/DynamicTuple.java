package org.egorlitvinenko.testflink;

import org.apache.flink.api.java.tuple.Tuple;

import java.util.Collection;

/**
 * @author Egor Litvinenko
 */
public class DynamicTuple extends Tuple {

    private final Class[] types;

    public DynamicTuple(Class... types) {
        this.types = types;
    }

    public DynamicTuple(Collection<Class> types) {
        this.types = types.toArray(new Class[types.size()]);
    }

    @Override
    public <T> T getField(int pos) {
        return (T) types[pos];
    }

    @Override
    public <T> void setField(T value, int pos) {
        if (pos >= this.types.length || pos < 0) {
            throw new IllegalArgumentException("Pos" + pos);
        }
        this.types[pos] = (Class) value;
    }

    @Override
    public int getArity() {
        return types.length;
    }

    @Override
    public <T extends Tuple> T copy() {
        return (T) new DynamicTuple(this.types);
    }

}

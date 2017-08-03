package org.egorlitvinenko.testdisruptor.byteStreamParsing.factory;

import org.egorlitvinenko.testdisruptor.byteStreamParsing.model.TableRow;

import java.util.Collections;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReferenceArray;

/**
 * @author Egor Litvinenko
 */
public class ArrayTableRowPool {

    private final TableRowProvider tableRowProvider;

    private final TableRow[] pool;
    private final AtomicReferenceArray<Boolean> reserved;
    private AtomicInteger poolIndex, reusing;

    public static ArrayTableRowPool allocate(int poolSize, TableRowProvider tableRowProvider) {
        return new ArrayTableRowPool(poolSize, tableRowProvider);
    }

    private ArrayTableRowPool(int poolSize,
                              TableRowProvider tableRowProvider) {
        this.tableRowProvider = tableRowProvider;

        this.pool      = new TableRow[poolSize];
        this.reserved  = new AtomicReferenceArray<>(Collections.nCopies(poolSize, Boolean.FALSE).toArray(new Boolean[poolSize]));
        this.poolIndex = new AtomicInteger(0);
        this.reusing   = new AtomicInteger(0);
        init();
    }

    public int getReusing() {
        return reusing.get();
    }

    public TableRow get() {
        TableRow answer;
        int pi = poolIndex.get();
        if (!this.reserved.get(pi)) {
            answer = this.pool[pi];
            this.reserved.set(pi, Boolean.TRUE);
            poolIndex.compareAndSet(pi, pi + 1);
            if (pi + 1 >= this.pool.length) {
                poolIndex.compareAndSet(pi + 1, 0);
                reusing.incrementAndGet();
            }
        } else {
            throw new IllegalStateException("Pool is out of bounds - " + poolIndex);
        }
        return answer;
    }

    void free(int index) {
        this.reserved.set(index, Boolean.FALSE);
    }

    private void init() {
        for (int i = 0; i < this.pool.length; ++i) {
            this.pool[i] = new ArrayPooledTableRow(this, i, tableRowProvider.create());
        }
    }

    public interface TableRowProvider {
        TableRow create();
    }

}

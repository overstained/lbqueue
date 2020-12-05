package queue.index;

import queue.log.IndexEntry;
import queue.log.SearchResult;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.util.OptionalInt;
import java.util.concurrent.locks.ReentrantLock;

import static java.nio.channels.FileChannel.MapMode.READ_WRITE;

public class OffsetIndex {
    private static final int ENTRY_SIZE = 8;

    private final int maxIndexSize;
    private final long baseOffset;
    private long lastOffset;
    private int entries;

    private MappedByteBuffer mmap;
    private final ReentrantLock lock = new ReentrantLock();

    public OffsetIndex(File path,
                       long baseOffset,
                       int maxIndexSize) {
        this.baseOffset = baseOffset;
        this.maxIndexSize = maxIndexSize;
        this.mmap = loadIndex(path);
        IndexEntry lastEntry = entries == 0 ? new IndexEntry(baseOffset, 0) : parseEntry(mmap, entries - 1);
        this.lastOffset = lastEntry.getKey();
    }

    private MappedByteBuffer loadIndex(File file) {
        boolean newlyCreated = false;
        try {
            newlyCreated = file.createNewFile();
        } catch (IOException e) {
            e.printStackTrace();
        }

        try(RandomAccessFile randomAccessFile = new RandomAccessFile(file, "rw")) {
            if(newlyCreated) {
                randomAccessFile.setLength(roundDownToExactMultiple(maxIndexSize));
            }
            var fileChannel = randomAccessFile.getChannel();
            var length = randomAccessFile.length();
            mmap = fileChannel.map(READ_WRITE, 0, length);
            if(newlyCreated) {
                mmap.position(0);
            } else {
                mmap.position(roundDownToExactMultiple(mmap.limit()));
            }
            entries = mmap.position() / ENTRY_SIZE;
            return mmap;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private int roundDownToExactMultiple(int number) { return ENTRY_SIZE * (number / ENTRY_SIZE); }

    public IndexEntry lookup(long offset) {
        var index = mmap.duplicate();
        SearchResult searchResult =
                binarySearch(index, offset, 0, entries - 1);
        return parseEntry(index, searchResult.lowerBound);
    }

    public void append(long offset, int position) {
        lock.lock();
        if (entries == 0 || offset > lastOffset) {
            mmap.putInt(getReverseRelativeOffset(offset));
            mmap.putInt(position);
            entries += 1;
            lastOffset = offset;
        } else {
            throw new RuntimeException();
        }
        lock.unlock();
    }

    private SearchResult binarySearch(ByteBuffer index, long key, int begin, int end) {
        var low = begin;
        var high = end;
        while(low < high) {
            var mid = (low + high + 1) >>> 1;
            var found = parseEntry(index, mid);
            var compareResult = Long.compare(found.getKey(), key);
            if(compareResult > 0)
                high = mid - 1;
            else if(compareResult < 0)
                low = mid;
            else
                return new SearchResult(mid, mid);
        }
        return new SearchResult(low, (low == entries - 1)? -1 : low + 1);
    }

    private IndexEntry parseEntry(ByteBuffer index, int n) {
        return new IndexEntry(
                getRelativeOffset(index, n),
                getPhysicalOffset(index, n));
    }

    private long getRelativeOffset(ByteBuffer index, int n) {
        return baseOffset + index.getInt(n * ENTRY_SIZE);
    }

    private int getPhysicalOffset(ByteBuffer index, int n) {
        return index.getInt(n * ENTRY_SIZE + 4);
    }

    private int getReverseRelativeOffset(long offset) {
        return toReverseRelative(offset)
                .orElseThrow(RuntimeException::new);
    }

    private OptionalInt toReverseRelative(long offset) {
        var relativeOffset = offset - baseOffset;
        if (relativeOffset < 0 || relativeOffset > Integer.MAX_VALUE)
            return OptionalInt.empty();
        return OptionalInt.of((int) relativeOffset);
    }

    public boolean canAppendOffset(long offset) {
        return toReverseRelative(offset).isPresent();
    }

    public long getBaseOffset() {
        return baseOffset;
    }

    public long getLastOffset() {
        return lastOffset;
    }

    public boolean isFull() {
        return entries >= maxIndexSize;
    }

    public void flush() {
        try {
            lock.lock();
            mmap.force();
        } finally {
           lock.unlock();
        }
    }
}

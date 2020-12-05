package queue.index;

import org.junit.jupiter.api.Test;
import queue.log.IndexEntry;

import java.io.IOException;

import static java.util.Map.entry;
import static java.util.Map.ofEntries;
import static org.assertj.core.api.Assertions.assertThat;
import static queue.utils.TestFileUtils.createTemporaryOffsetIndex;

class OffsetIndexTest {

    @Test
    void retrievesLowerBoundEntryFromSparseIndexForInexistentOffset() throws IOException {
        int baseOffset = 1000000;

        createTemporaryOffsetIndex(baseOffset, ofEntries(
                entry(3, 0),
                entry(5, 48),
                entry(6, 70),
                entry(8, 123)
        ), index -> {
            IndexEntry indexEntry = index.lookup(toAbsoluteOffset(baseOffset, 4));

            assertThat(indexEntry)
                    .extracting(IndexEntry::getKey, IndexEntry::getValue)
                    .containsExactly(toAbsoluteOffset(baseOffset,3), 0);
        });
    }

    @Test
    void retrievesLowerBoundEntryFromSparseIndexForExistentOffset() throws IOException {
        int baseOffset = 1000000;

        createTemporaryOffsetIndex(baseOffset, ofEntries(
                entry(3, 0),
                entry(5, 48),
                entry(6, 70),
                entry(8, 123)
        ), index -> {
            IndexEntry indexEntry = index.lookup(toAbsoluteOffset(baseOffset, 5));

            assertThat(indexEntry)
                    .extracting(IndexEntry::getKey, IndexEntry::getValue)
                    .containsExactly(toAbsoluteOffset(baseOffset,5), 48);
        });
    }

    @Test
    void retrievesLowerBoundEntryFromSparseIndex_WhenOffsetOutsideLeftFrontier() throws IOException {
        int baseOffset = 1000000;

        createTemporaryOffsetIndex(baseOffset, ofEntries(
                entry(3, 0),
                entry(5, 48)
        ), index -> {
            IndexEntry indexEntry = index.lookup(toAbsoluteOffset(baseOffset, 0));

            assertThat(indexEntry)
                    .extracting(IndexEntry::getKey, IndexEntry::getValue)
                    .containsExactly(toAbsoluteOffset(baseOffset,3), 0);
        });
    }

    @Test
    void retrievesLowerBoundEntryFromSparseIndex_WhenOffsetOutsideRightFrontier() throws IOException {
        int baseOffset = 1000000;

        createTemporaryOffsetIndex(baseOffset, ofEntries(
                entry(3, 0),
                entry(5, 48)
        ), index -> {
            IndexEntry indexEntry = index.lookup(toAbsoluteOffset(baseOffset, 6));

            assertThat(indexEntry)
                    .extracting(IndexEntry::getKey, IndexEntry::getValue)
                    .containsExactly(toAbsoluteOffset(baseOffset,5), 48);
        });
    }

    @Test
    void computesLastOffsetCorrectly() throws IOException {
        int baseOffset = 1000000;

        createTemporaryOffsetIndex(baseOffset, ofEntries(
                entry(3, 0),
                entry(5, 48),
                entry(8, 78),
                entry(14, 125)
        ), index -> {
            assertThat(index.getLastOffset()).isEqualTo(toAbsoluteOffset(baseOffset, 14));
        });
    }

    private long toAbsoluteOffset(int baseOffset, int relativeOffset) {
        return baseOffset + relativeOffset;
    }

}
package queue.log;

import org.junit.jupiter.api.Test;

import java.io.EOFException;
import java.nio.ByteBuffer;

import static java.util.List.of;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static queue.utils.RecordUtils.record;
import static queue.utils.TestFileUtils.openLogWithEntries;

class UtilsTest {

    @Test
    void readsBytesAtPositionIntoBuffer() throws Exception {
        openLogWithEntries(of(
                record(4, 1000, 0, "test")
        ), fileChannel -> {
            ByteBuffer logMessage = ByteBuffer.allocate(4);
            Utils.readFullyOrFail(fileChannel, logMessage, 12, "log message");

            assertThat(new String(logMessage.array())).isEqualTo("test");
        });
    }

    @Test
    void throwsIllegalArgumentException_WhenPositionIsNegative() throws Exception {
        openLogWithEntries(of(
                record(4, 1000, 0, "test")
        ), fileChannel -> {
            ByteBuffer logMessage = ByteBuffer.allocate(0);
            assertThatThrownBy(() ->
                    Utils.readFullyOrFail(fileChannel, logMessage, -1, "log message")
            )
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessage("The file channel position cannot be negative, but it is -1");
        });
    }

    @Test
    void throwsEOFException_WhenEndOfFileIsReachedBeforeFillingBuffer() throws Exception {
        openLogWithEntries(of(
                record(4, 1000, 0, "test")
        ), fileChannel -> {
            ByteBuffer logMessage = ByteBuffer.allocate(10);
            assertThatThrownBy(() ->
                    Utils.readFullyOrFail(fileChannel, logMessage, 10, "log message")
            )
            .isInstanceOf(EOFException.class);
        });
    }

}
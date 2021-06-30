package cs555.overlay.wireformats;
import cs555.overlay.wireformats.Protocol;
import java.io.IOException;

public interface Event {
	byte getType() throws IOException;
	byte[] getBytes() throws IOException;
}
package cs555.overlay.wireformats;
import cs555.overlay.wireformats.Protocol;

import java.io.IOException;

/**
 * All messages implement this interface. All events can then
 * be handled by a generic function (onEvent) which can, without
 * knowing the exact message type, call the correct method to deal
 * with the event.
 *
 * @author hayne
 */
public interface Event {

	/**
	 * Returns the byte-sized type of the message.
	 * 
	 * @return message type
	 */
	byte getType();

	/**
	 * Marshalls the data stored in class variables
	 * into a message to be sent over the newtork.
	 * 
	 * @return marshalled bytes
	 * @throws IOException
	 */
	byte[] getBytes() throws IOException;
}
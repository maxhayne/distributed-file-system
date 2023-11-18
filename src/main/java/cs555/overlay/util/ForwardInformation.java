package cs555.overlay.util;

import cs555.overlay.wireformats.Event;

/**
 * A record used only to return multiple pieces of information about where and
 * what to send out as a repair message.
 *
 * @author hayne
 */
public record ForwardInformation( String firstHop, Event repairMessage ) {}

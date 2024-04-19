package org.jgroups.protocols;

import org.jgroups.Message;
import org.jgroups.util.Buffer;

/**
 * New multicast protocols based on fixed-size xmit windows and message ACKs<rb/>
 * Details: https://issues.redhat.com/browse/JGRP-2780
 * @author Bela Ban
 * @since  5.4
 */
public class NAKACK4 extends ReliableMulticast {

    @Override
    protected Buffer<Message> createXmitWindow(long initial_seqno) {
        throw new UnsupportedOperationException();
    }


}

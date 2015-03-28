package com.ociweb.pronghorn.exampleStages;

import com.ociweb.pronghorn.ring.RingBuffer;
import com.ociweb.pronghorn.ring.proxy.EventConsumer;
import com.ociweb.pronghorn.stage.PronghornStage;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;

public class InputStageEventConsumerExample extends PronghornStage {

	private final EventConsumer consumer;
		
	public static final String testSymbol = "IBM";
	public static final String testCompanyName = "International Business Machines";
	public static final double testOpen = 23.43;
	public static final double testClose = 72.3;
	public static final double testHigh = 80;
	public static final double testLow = 20.001;
    public static final long   testVolume = 10000000;	
	
	
	protected InputStageEventConsumerExample(GraphManager graphManager, RingBuffer output) {
		super(graphManager, NONE, output);		
		consumer = new EventConsumer(output);
	}
	
	@Override
	public void run() {
				
		DailyQuoteConsumer dq = EventConsumer.create(consumer, DailyQuoteConsumer.class);
		if (null != dq) {//null when there is no room on the output ring
			populateFields(dq);
			EventConsumer.publish(consumer, dq);
		}
		
	}

	private void populateFields(DailyQuoteConsumer dq) {
		dq.writeEmptyField(null);
		
		dq.writeClosedPrice(testClose);
		dq.writeOpenPrice(testOpen);
		dq.writeHighPrice(testHigh);
		dq.writeLowPrice(testLow);
		dq.writeVolume(testVolume);
		
		dq.writeSymbol(testSymbol);
		dq.writeCompanyName(testCompanyName);
	}

}

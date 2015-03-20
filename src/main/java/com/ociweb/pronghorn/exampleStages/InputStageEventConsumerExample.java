package com.ociweb.pronghorn.exampleStages;

import com.ociweb.pronghorn.ring.RingBuffer;
import com.ociweb.pronghorn.ring.proxy.EventConsumer;
import com.ociweb.pronghorn.stage.PronghornStage;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;

public class InputStageEventConsumerExample extends PronghornStage {

	private final EventConsumer consumer;
		
	public final String testSymbol = "IBM";
	public final String testCompanyName = "International Business Machines";
	
	protected InputStageEventConsumerExample(GraphManager graphManager, RingBuffer output) {
		super(graphManager, NONE, output);		
		consumer = new EventConsumer(output);
	}
			
	@Override
	public void run() {
				
		DailyQuote dq = EventConsumer.create(consumer, DailyQuote.class);
		if (null != dq) {
			
			dq.writeEmptyField(null);
			
			dq.writeOpenPrice(23.43);
			dq.writeClosedPrice(72.3);
			dq.writeHighPrice(80);
			dq.writeLowPrice(20.001);
			dq.writeVolume(10000000);
			
			dq.writeSymbol(testSymbol);
			dq.writeCompanyName(testCompanyName);
			
			EventConsumer.publish(consumer, dq);
		}
	}

}

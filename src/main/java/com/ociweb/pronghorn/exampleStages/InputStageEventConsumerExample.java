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
			
	//TODO: add new compiled version of this.  At compile time the annotations are processed and 
	//      this source will be modified into a highLevel API usage using code generation.
	//TODO: Must have normal Java BlockingQueue example as a benchmark
	//TODO: after graph is built and before scheduling, we may also run the code generation.
	//TODO: AAA, For reader in ALL APIs must also supply clear of previous read. 
	
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

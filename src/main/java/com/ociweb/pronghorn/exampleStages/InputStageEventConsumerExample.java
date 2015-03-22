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
			
	//TODO: add new compiled version of this.  At compile time the annotations are processed and 
	//      this source will be modified into a highLevel API usage using code generation.
	//TODO: Must have normal Java BlockingQueue example as a benchmark
	//TODO: after graph is built and before scheduling, we may also run the code generation.
	//TODO: AAA, For reader in ALL APIs must also supply clear of previous read. 
	
	@Override
	public void run() {
				
		DailyQuote dq = EventConsumer.create(consumer, DailyQuote.class);
		if (null != dq) {
			populateFields(dq);
			EventConsumer.publish(consumer, dq);
		}
		
	}

	private void populateFields(DailyQuote dq) {
		dq.writeEmptyField(null);
		
		dq.writeOpenPrice(testOpen);
		dq.writeClosedPrice(testClose);
		dq.writeHighPrice(testHigh);
		dq.writeLowPrice(testLow);
		dq.writeVolume(testVolume);
		
		dq.writeSymbol(testSymbol);
		dq.writeCompanyName(testCompanyName);
	}

}

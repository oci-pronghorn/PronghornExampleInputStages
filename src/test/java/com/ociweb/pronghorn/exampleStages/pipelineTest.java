package com.ociweb.pronghorn.exampleStages;

import static org.junit.Assert.*;

import java.util.concurrent.TimeUnit;

import org.junit.Test;

import com.ociweb.pronghorn.ring.FieldReferenceOffsetManager;
import com.ociweb.pronghorn.ring.RingBuffer;
import com.ociweb.pronghorn.ring.RingBufferConfig;
import com.ociweb.pronghorn.ring.RingReader;
import com.ociweb.pronghorn.ring.loader.TemplateHandler;
import com.ociweb.pronghorn.stage.ConsoleStage;
import com.ociweb.pronghorn.stage.PronghornStage;
import com.ociweb.pronghorn.stage.route.SplitterStage;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;
import com.ociweb.pronghorn.stage.scheduling.StageScheduler;
import com.ociweb.pronghorn.stage.scheduling.ThreadPerStageScheduler;

public class pipelineTest {

	private static final long TIMEOUT_SECONDS = 3;

	@Test
	public void runThisTest() {
		
        ///////
		//When doing devlopment and testing be sure that assertions are on by adding -ea to the JVM arguments
		//This will enable a lot of helpful to catch configuration and setup errors earlier
		/////
		
		FieldReferenceOffsetManager from;
		try {
			from = TemplateHandler.loadFrom("/exampleTemplate.xml");
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
				
		int messagesOnRing = 10;
		int maxLengthVarField = 80;
		
		RingBufferConfig ringBufferConfig = new RingBufferConfig(from, messagesOnRing, maxLengthVarField);
	
		RingBuffer ringBuffer = new RingBuffer(ringBufferConfig);
		RingBuffer ringBuffer21 = new RingBuffer(ringBufferConfig.grow2x());
		RingBuffer ringBuffer22 = new RingBuffer(ringBufferConfig.grow2x());
		
		
   	    GraphManager gm = new GraphManager();
   	    
		
		InputStageLowLevelExample  iso = new InputStageLowLevelExample(gm, ringBuffer);
		
		//simple stage that reports to the console
	   
		SplitterStage stage = new SplitterStage(gm,ringBuffer, ringBuffer21, ringBuffer22);		
		
		//ConsoleStage console = new ConsoleStage(gm, ringBuffer21); //this stage can be used to watch the test
		DumpStageHighLevel dumpStage1 = new DumpStageHighLevel(gm, ringBuffer21);	
		
		
		DumpStageHighLevel dumpStage2 = new DumpStageHighLevel(gm, ringBuffer22);
		
		
	    StageScheduler scheduler = new ThreadPerStageScheduler(GraphManager.cloneAll(gm));
		 
		scheduler.startup();

		//run test for 2 seconds then shutdown
		try {
			Thread.sleep(2000);
		} catch (InterruptedException e) {
		}
		
		//NOTE: if the tested input stage is the sort of stage that calls shutdown on its own then
		//      you do not need the above sleep
		//      you do not need the below shutdown
		//
		scheduler.shutdown();
		
        boolean cleanExit = scheduler.awaitTermination(TIMEOUT_SECONDS, TimeUnit.SECONDS);
		if (!cleanExit) {
			
			System.err.println("RingBuffer: "+ringBuffer);//print the details of the ring buffer
			fail("Did not shtudown cleanly");
			 
		}		
				
	}
		
	
	private final class DumpStageHighLevel extends PronghornStage {
		private final RingBuffer inputRing;


		private DumpStageHighLevel(GraphManager gm,RingBuffer inputRing) {
			super(gm, inputRing, NONE);
			this.inputRing = inputRing;
		}

		@Override
		public void run() {
				
					//try also releases previously read fragments
					while (RingReader.tryReadFragment(inputRing)) {												
						
						assert(RingReader.isNewMessage(inputRing)) : "This test should only have one simple message made up of one fragment";
						int msgId = RingReader.getMsgIdx(inputRing);
						
						RingReader.releaseReadLock(inputRing);
					} 
					return;
	
		}
	}
	
	
}

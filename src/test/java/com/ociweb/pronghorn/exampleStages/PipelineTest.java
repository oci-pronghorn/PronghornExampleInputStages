package com.ociweb.pronghorn.exampleStages;

import java.util.concurrent.TimeUnit;

import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.Assert.*;

import com.ociweb.pronghorn.ring.FieldReferenceOffsetManager;
import com.ociweb.pronghorn.ring.RingBuffer;
import com.ociweb.pronghorn.ring.RingBufferConfig;
import com.ociweb.pronghorn.ring.loader.TemplateHandler;
import com.ociweb.pronghorn.stage.PronghornStage;
import com.ociweb.pronghorn.stage.monitor.MonitorConsoleStage;
import com.ociweb.pronghorn.stage.monitor.MonitorFROM;
import com.ociweb.pronghorn.stage.route.RoundRobinRouteStage;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;
import com.ociweb.pronghorn.stage.scheduling.StageScheduler;
import com.ociweb.pronghorn.stage.scheduling.ThreadPerStageScheduler;

public class PipelineTest {

	static final long TIMEOUT_SECONDS = 3;
	static final long TEST_LENGTH_IN_SECONDS = 11;
	
	private static FieldReferenceOffsetManager from;
	public static final int messagesOnRing = 1<<14;
	public static final int monitorMessagesOnRing = 7;
	
	private static final int maxLengthVarField = 40;
	
	private final Integer monitorRate = Integer.valueOf(50000000);
	
	private static RingBufferConfig ringBufferConfig;
	private static RingBufferConfig ringBufferMonitorConfig;

	@BeforeClass
	public static void loadSchema() {
		///////
		//When doing development and testing be sure that assertions are on by adding -ea to the JVM arguments
		//This will enable a lot of helpful to catch configuration and setup errors earlier
		/////
		
		try {
			from = TemplateHandler.loadFrom("/exampleTemplate.xml");
			ringBufferConfig = new RingBufferConfig(from, messagesOnRing, maxLengthVarField);
			ringBufferMonitorConfig = new RingBufferConfig(MonitorFROM.buildFROM(), monitorMessagesOnRing, maxLengthVarField);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
		
		System.gc();
	}
	
	
	@Test
	public void lowLevelInputStageTest() {
								
		final byte[] expected = new String("tcp://localhost:1883thingFortytworoot/colors/blue        ").getBytes();	
		
		int j = 8;	   		
		while (--j>=0) {
			expected[expected.length-(8-j)] = (byte)j;
		}
		//build the expected data that should be found on the byte ring.
		final int[] expectedInts = new int[]{0, 0, 20, 20, 13, 42, 33, 16, 49, 8, 0, 57};		
		final int expectedMsg = 0;
		
	   	CheckStageArguments checkArgs = new CheckStageArguments() {

			@Override
			public int expectedMessageIdx() {
				return expectedMsg;
			}
	
			@Override
			public byte[] expectedBytes() {
				return expected;
			}
	
			@Override
			public int[] expectedInts() {
				return expectedInts;
			}
	   		 
	   	 };
				
	   	GraphManager gm = new GraphManager();
		
		PronghornStage stage = buildSplitterTree(checkArgs, gm, ringBufferConfig, false);
		
		InputStageLowLevelExample producer = new InputStageLowLevelExample(gm, GraphManager.getInputPipe(gm, stage, 1));
				

		//If we can ask for stages by some id then we can look them up at the end as needed.
		
		
		addMonitorAndTest(gm, expected.length, " low level ");
	}


	private void addMonitorAndTest(GraphManager gm, int expectedLength,
			String label) {
		//Add monitoring
		MonitorConsoleStage.attach(gm, monitorRate, ringBufferMonitorConfig);
		
		//Enable batching
		GraphManager.enableBatching(gm);

		
		RingBuffer ringForDataCount = GraphManager.getInputPipe(gm, GraphManager.findStageByPath(gm, 1, 1, 2), 1);
		timeAndRunTest(ringForDataCount, gm, label, TEST_LENGTH_IN_SECONDS, (CheckVarLengthValuesStage)GraphManager.findStageByPath(gm, 1, 1, 1), 
				                            (CheckVarLengthValuesStage)GraphManager.findStageByPath(gm, 1, 1, 2), 
				                            (CheckVarLengthValuesStage)GraphManager.findStageByPath(gm, 1, 1, 3),
				                            (CheckVarLengthValuesStage)GraphManager.findStageByPath(gm, 1, 1, 4)				                            
				);
	}


	private static PronghornStage buildSplitterTree(CheckStageArguments checkArgs, GraphManager gm, RingBufferConfig config, boolean deepTest) {
		
		CheckVarLengthValuesStage dumpStage11 = new CheckVarLengthValuesStage(gm, new RingBuffer(config), checkArgs, deepTest);
		CheckVarLengthValuesStage dumpStage12 = new CheckVarLengthValuesStage(gm, new RingBuffer(config), checkArgs, deepTest);		
		CheckVarLengthValuesStage dumpStage21 = new CheckVarLengthValuesStage(gm, new RingBuffer(config), checkArgs, deepTest);
		CheckVarLengthValuesStage dumpStage22 = new CheckVarLengthValuesStage(gm, new RingBuffer(config), checkArgs, deepTest);	
		
		RoundRobinRouteStage stage = new RoundRobinRouteStage(gm, 
				                                                 new RingBuffer(config.grow2x()), 
				                                                 GraphManager.getInputPipe(gm, dumpStage11), 
				                                                 GraphManager.getInputPipe(gm, dumpStage12),
				                                                 GraphManager.getInputPipe(gm, dumpStage21),
																 GraphManager.getInputPipe(gm, dumpStage22)				
																); 	

		return stage;
	}
	
	
	@Test
	public void lowLevelInputStageSimple40Test() {
							
		RingBufferConfig config = new RingBufferConfig(FieldReferenceOffsetManager.RAW_BYTES, messagesOnRing, InputStageLowLevel40ByteBaselineExample.payload.length);
		RingBuffer ringBuffer1 = new RingBuffer(config);

		final byte[] expectedBytes = InputStageLowLevel40ByteBaselineExample.payload;
		
		
		CheckStageArguments checkArgs = new CheckStageArguments() {
			
			@Override
			public int expectedMessageIdx() {
				return 0;
			}
			
			@Override
			public byte[] expectedBytes() {
				return expectedBytes;
			}
			
			@Override
			public int[] expectedInts() {
				return new int[]{0,0,InputStageLowLevel40ByteBaselineExample.payload.length};
			}
			
		};
				
		GraphManager gm = new GraphManager();
		InputStageLowLevel40ByteBaselineExample  iso = new InputStageLowLevel40ByteBaselineExample(gm, ringBuffer1);
		CheckVarLengthValuesStage dumpStage22 = new CheckVarLengthValuesStage(gm, ringBuffer1, checkArgs, false); //NO DEEP CHECK
		
		GraphManager.enableBatching(gm);
		
		timeAndRunTest(ringBuffer1, gm, " Simple40LowLevel", TEST_LENGTH_IN_SECONDS, dumpStage22);
		
	}
	
	@Test
	public void lowLevel40InputStageTest() {
								
		RingBufferConfig config = new RingBufferConfig(FieldReferenceOffsetManager.RAW_BYTES, messagesOnRing, InputStageLowLevel40ByteBaselineExample.payload.length);
				

		
		final byte[] expectedBytes = InputStageLowLevel40ByteBaselineExample.payload;
		
		
		CheckStageArguments checkArgs = new CheckStageArguments() {
			
			@Override
			public int expectedMessageIdx() {
				return 0;
			}
			
			@Override
			public byte[] expectedBytes() {
				return expectedBytes;
			}
			
			@Override
			public int[] expectedInts() {
				return new int[]{0,0,InputStageLowLevel40ByteBaselineExample.payload.length};
			}
			
		};
        
		
		GraphManager gm = new GraphManager();
		
		
		PronghornStage stage = buildSplitterTree(checkArgs, gm, config, true);
       InputStageLowLevel40ByteBaselineExample producer = new InputStageLowLevel40ByteBaselineExample(gm, GraphManager.getInputPipe(gm, stage, 1));
				
		
       addMonitorAndTest(gm, 0, " low level 40 ");

	}
	
	
	@Test
	public void highLevelInputStageTest() {								

		
   	    //	build the expected data that should be found on the byte ring.
   	    final byte[] expected = (InputStageHighLevelExample.testSymbol+InputStageHighLevelExample.testCompanyName).getBytes();
   	    final int expectedMsg = from.messageStarts[1];
   	    final int[] expectedInts =  new int[] {8, 0, 3, 3, 31, 0, 0, 2, 0, 10250, 2, 0, 10250, 2, 0, 10250, 2, 0, 10250, 0, 10000000, 34};
		
	   	CheckStageArguments checkArgs = new CheckStageArguments() {

			@Override
			public int expectedMessageIdx() {
				return expectedMsg;
			}
	
			@Override
			public byte[] expectedBytes() {
				return expected;
			}
	
			@Override
			public int[] expectedInts() {
				return expectedInts;
			}
	   		 
	   	 };
	   	 
	   	 GraphManager gm = new GraphManager();
	   	 
	   	PronghornStage stage = buildSplitterTree(checkArgs, gm, ringBufferConfig, true);
			
			InputStageHighLevelExample producer = new InputStageHighLevelExample(gm, GraphManager.getInputPipe(gm, stage, 1));
					
			addMonitorAndTest(gm, 0, " high level ");
		

		
	}


		
	@Test
	public void eventConsumerInputStageTest() {
								
	
		
		CheckStageArguments checkArgs = new CheckStageArguments() {
			//	build the expected data that should be found on the byte ring.
			final byte[] expected = (InputStageEventConsumerExample.testSymbol+InputStageEventConsumerExample.testCompanyName).getBytes();
			int expectedMsg = from.messageStarts[1];   	    
			final int[] expectedInts = new int[] {8, 0, 3, 3, 31, 0, 0, 2, 0, 2343, 2, 0, 8000, 2, 0, 2000, 2, 0, 7230, 0, 10000000, 34};
			//simple stage that reports to the console

			@Override
			public int expectedMessageIdx() {
				return expectedMsg;
			}

			@Override
			public byte[] expectedBytes() {
				return expected;
			}

			@Override
			public int[] expectedInts() {
				return expectedInts;
			}
			
		};
		int expectedBytes = InputStageEventConsumerExample.testSymbol.length() + InputStageEventConsumerExample.testCompanyName.length();

		
		GraphManager gm = new GraphManager();
		
		PronghornStage stage = buildSplitterTree(checkArgs, gm, ringBufferConfig, true);
		
		InputStageEventConsumerExample producer = new InputStageEventConsumerExample(gm, GraphManager.getInputPipe(gm, stage, 1));
				
		
		addMonitorAndTest(gm, 0, " event consumer ");

		
		
	}
	

	private long timeAndRunTest(RingBuffer ringBuffer, GraphManager gm,
			String label, long testInSeconds, CheckVarLengthValuesStage ... countStages) {
		StageScheduler scheduler = new ThreadPerStageScheduler(GraphManager.cloneAll(gm));
		 
	    long startTime = System.currentTimeMillis();
		scheduler.startup();

		try {
			Thread.sleep(testInSeconds*1000);
		} catch (InterruptedException e) {
		}
		
		//NOTE: if the tested input stage is the sort of stage that calls shutdown on its own then
		//      you do not need the above sleep
		//      you do not need the below shutdown
		//
		scheduler.shutdown();
		
        boolean cleanExit = scheduler.awaitTermination(TIMEOUT_SECONDS, TimeUnit.SECONDS);
  
 
        long duration = System.currentTimeMillis()-startTime;

		int j = countStages.length;
		long messages=0;
		long bytes=0;
		while (--j>=0) {
			messages+=countStages[j].messageCount();
			bytes+=countStages[j].totalBytes();
		}
		
		if ((duration>0) && (messages>0)) {
			long bytesMoved = (4l*RingBuffer.headPosition(ringBuffer))+bytes;
			
			
			float mbMoved = (8f*bytesMoved)/(float)(1<<20);
			
			int bytesPerMessage = (int)(bytesMoved/messages);
			
			float fmsg = messages;
			float fdur = duration;
			System.out.println("TotalMessages:"+messages + 
					           " Msg/Ms:"+(fmsg/fdur) +
					           " Mb/Ms:"+(mbMoved/fdur) +
					           label +" B/Msg:"+bytesPerMessage +" cleanExit:"+cleanExit
							  );
		} else {
			System.out.println(label+" warning duration:"+duration+" messages:"+messages);
		}
		
		//assertTrue("RingBuffer: "+ringBuffer, cleanExit);
	
		return messages;
	}
	
	
	interface CheckStageArguments {		
		
		public int expectedMessageIdx(); 
		public byte[] expectedBytes();
		public int[] expectedInts();
        		
	}
	
	
}

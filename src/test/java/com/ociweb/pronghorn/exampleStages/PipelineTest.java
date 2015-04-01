package com.ociweb.pronghorn.exampleStages;

import static com.ociweb.pronghorn.ring.RingBuffer.releaseReadLock;
import static org.junit.Assert.*;

import java.util.Arrays;
import java.util.concurrent.TimeUnit;

import org.junit.BeforeClass;
import org.junit.Test;

import com.ociweb.pronghorn.ring.FieldReferenceOffsetManager;
import com.ociweb.pronghorn.ring.RingBuffer;
import com.ociweb.pronghorn.ring.RingBuffer.PaddedInt;
import com.ociweb.pronghorn.ring.RingBuffer.PaddedLong;
import com.ociweb.pronghorn.ring.RingBufferConfig;
import com.ociweb.pronghorn.ring.loader.TemplateHandler;
import com.ociweb.pronghorn.stage.PronghornStage;
import com.ociweb.pronghorn.stage.monitor.MonitorConsoleStage;
import com.ociweb.pronghorn.stage.monitor.MonitorFROM;
import com.ociweb.pronghorn.stage.monitor.RingBufferMonitorStage;
import com.ociweb.pronghorn.stage.route.RoundRobinRouteStage;
import com.ociweb.pronghorn.stage.route.SplitterStage;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;
import com.ociweb.pronghorn.stage.scheduling.StageScheduler;
import com.ociweb.pronghorn.stage.scheduling.ThreadPerStageScheduler;

public class PipelineTest {

	static final long TIMEOUT_SECONDS = 3;
	static final long TEST_LENGTH_IN_SECONDS = 7;
	
	private static FieldReferenceOffsetManager from;
	public static final int messagesOnRing = 1<<10;
	public static final int monitorMessagesOnRing = 7;
	
	private static final int maxLengthVarField = 40;
	
	private final Integer monitorRate = Integer.valueOf(50000000);
	
	private static RingBufferConfig ringBufferConfig;
	private static RingBufferConfig ringBufferMonitorConfig;

	@BeforeClass
	public static void loadSchema() {
		System.gc();
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
		
		SplitterStage stage = buildSplitterTree(checkArgs, gm);
		
		InputStageLowLevelExample producer = new InputStageLowLevelExample(gm, GraphManager.getInputRing(gm, stage, 1));
				

		//If we can ask for stages by some id then we can look them up at the end as needed.
		
		
		//Add monitoring
		MonitorConsoleStage.attach(gm, monitorRate, ringBufferMonitorConfig);
		
		//Enable batching
		GraphManager.enableBatching(gm);

		
		RingBuffer ringForDataCount = GraphManager.getInputRing(gm, GraphManager.findStageByPath(gm, 1, 1, 2), 1);
		long totalMessages = timeAndRunTest(ringForDataCount, gm, expected.length, " low level", 
				                            (CheckVarLengthValuesStage)GraphManager.findStageByPath(gm, 1, 1, 2, 1), 
				                            (CheckVarLengthValuesStage)GraphManager.findStageByPath(gm, 1, 1, 2, 2));
		assertEquals(((CheckVarLengthValuesStage)GraphManager.findStageByPath(gm, 1, 1, 1, 1)).messageCount() + 
				     ((CheckVarLengthValuesStage)GraphManager.findStageByPath(gm, 1, 1, 1, 2)).messageCount(), 
				     totalMessages);
		
	}


	private SplitterStage buildSplitterTree(CheckStageArguments checkArgs,	GraphManager gm) {
		
		CheckVarLengthValuesStage dumpStage11 = new CheckVarLengthValuesStage(gm, new RingBuffer(ringBufferConfig), checkArgs);
		CheckVarLengthValuesStage dumpStage12 = new CheckVarLengthValuesStage(gm, new RingBuffer(ringBufferConfig), checkArgs);		
		CheckVarLengthValuesStage dumpStage21 = new CheckVarLengthValuesStage(gm, new RingBuffer(ringBufferConfig), checkArgs);
		CheckVarLengthValuesStage dumpStage22 = new CheckVarLengthValuesStage(gm, new RingBuffer(ringBufferConfig), checkArgs);	
		
		RoundRobinRouteStage router21 = new RoundRobinRouteStage(gm, 
				                                                 new RingBuffer(ringBufferConfig.grow2x()), 
				                                                 GraphManager.getInputRing(gm, dumpStage11), 
				                                                 GraphManager.getInputRing(gm, dumpStage12)); 	
		
		RoundRobinRouteStage router22 = new RoundRobinRouteStage(gm, 
				                                                 new RingBuffer(ringBufferConfig.grow2x()),																	
																 GraphManager.getInputRing(gm, dumpStage21),
																 GraphManager.getInputRing(gm, dumpStage22)); 
        
		SplitterStage stage = new SplitterStage(gm, 
				                                new RingBuffer(ringBufferConfig), 
												GraphManager.getInputRing(gm, router21),
												GraphManager.getInputRing(gm, router22));
		return stage;
	}
	
	
	@Test
	public void lowLevelInputStageSimple40Test() {
							
		RingBufferConfig config = new RingBufferConfig(FieldReferenceOffsetManager.RAW_BYTES, 1<<14, 50);
		RingBuffer ringBuffer = new RingBuffer(config);
		
		
   	    GraphManager gm = new GraphManager();
   	    	
		
		InputStageLowLevel40ByteBaselineExample  iso = new InputStageLowLevel40ByteBaselineExample(gm, ringBuffer); //full queue

		final byte[] expectedBytes = iso.payload;
		
		
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
				return new int[]{0,0,32};
			}
			
		};
		
		CheckVarLengthValuesStage dumpStage22 = new CheckVarLengthValuesStage(gm, ringBuffer, checkArgs);
		
		GraphManager.enableBatching(gm);
		
		timeAndRunTest(ringBuffer, gm, 0, " Simple40LowLevel", dumpStage22);   
		
	}
	
	@Test
	public void lowLevel40InputStageTest() {
								
		RingBufferConfig config = new RingBufferConfig(FieldReferenceOffsetManager.RAW_BYTES, 1<<14, 50);
				
		
   	    GraphManager gm = new GraphManager();
   	    
   	    RingBuffer ringBuffer = new RingBuffer(config);
   	    RingBuffer ringBuffer21 = new RingBuffer(config.grow2x().grow2x()); //TODO: this is showing round robin to be a problem.
   	    RingBuffer ringBuffer211 = new RingBuffer(config);
   	    RingBuffer ringBuffer212 = new RingBuffer(config);
   	    RingBuffer ringBuffer22 = new RingBuffer(config.grow2x().grow2x());
   	    RingBuffer ringBuffer221 = new RingBuffer(config);
   	    RingBuffer ringBuffer222 = new RingBuffer(config);

        InputStageLowLevel40ByteBaselineExample  iso = new InputStageLowLevel40ByteBaselineExample(gm, ringBuffer); //full queue
		
		final byte[] expectedBytes = iso.payload;
		
		
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
				return new int[]{0,0,32};
			}
			
		};
        
        
		//simple stage that reports to the console
	   
		SplitterStage stage = new SplitterStage(gm, ringBuffer, ringBuffer21, ringBuffer22);	//mostly full queue
		
		RoundRobinRouteStage router21 = new RoundRobinRouteStage(gm, ringBuffer21, ringBuffer211, ringBuffer212); //mostly empty
		CheckVarLengthValuesStage dumpStage11 = new CheckVarLengthValuesStage(gm, ringBuffer211, checkArgs);
		CheckVarLengthValuesStage dumpStage12 = new CheckVarLengthValuesStage(gm, ringBuffer212, checkArgs);
					
		RoundRobinRouteStage router22 = new RoundRobinRouteStage(gm, ringBuffer22, ringBuffer221, ringBuffer222); //mostly empty
		CheckVarLengthValuesStage dumpStage21 = new CheckVarLengthValuesStage(gm, ringBuffer221, checkArgs);
		CheckVarLengthValuesStage dumpStage22 = new CheckVarLengthValuesStage(gm, ringBuffer222, checkArgs);
		
		//Add monitoring
		MonitorConsoleStage.attach(gm, monitorRate, ringBufferMonitorConfig);
		
		//Enable batching
		GraphManager.enableBatching(gm);
				
		
		long totalMessages = timeAndRunTest(ringBuffer22, gm, 0, " low level 40 ", dumpStage21, dumpStage22);
		long expectedMessages = dumpStage11.messageCount() + dumpStage12.messageCount();
		assertEquals(expectedMessages,totalMessages);
		
	}
	
	
	@Test
	public void highLevelInputStageTest() {								
	
		RingBuffer ringBuffer = new RingBuffer(ringBufferConfig);
		RingBuffer ringBuffer21 = new RingBuffer(ringBufferConfig.grow2x());
		RingBuffer ringBuffer211 = new RingBuffer(ringBufferConfig);
		RingBuffer ringBuffer212 = new RingBuffer(ringBufferConfig);
		RingBuffer ringBuffer22 = new RingBuffer(ringBufferConfig.grow2x());
		RingBuffer ringBuffer221 = new RingBuffer(ringBufferConfig);
		RingBuffer ringBuffer222 = new RingBuffer(ringBufferConfig);
		
		
   	    GraphManager gm = new GraphManager();
		
   	    InputStageHighLevelExample  iso = new InputStageHighLevelExample(gm, ringBuffer);
		
   	    //	build the expected data that should be found on the byte ring.
   	    final byte[] expected = (iso.testSymbol+iso.testCompanyName).getBytes();
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
   	    
   	    //simple stage that reports to the console
	   
		SplitterStage stage = new SplitterStage(gm,ringBuffer, ringBuffer21, ringBuffer22);	

		RoundRobinRouteStage router21 = new RoundRobinRouteStage(gm, ringBuffer21, ringBuffer211, ringBuffer212);
		CheckVarLengthValuesStage dumpStage11 = new CheckVarLengthValuesStage(gm, ringBuffer211, checkArgs);
		CheckVarLengthValuesStage dumpStage12 = new CheckVarLengthValuesStage(gm, ringBuffer212, checkArgs);
					
		RoundRobinRouteStage router22 = new RoundRobinRouteStage(gm, ringBuffer22, ringBuffer221, ringBuffer222);
		CheckVarLengthValuesStage dumpStage21 = new CheckVarLengthValuesStage(gm, ringBuffer221, checkArgs);
		CheckVarLengthValuesStage dumpStage22 = new CheckVarLengthValuesStage(gm, ringBuffer222, checkArgs);

		//Turn on monitoring
		MonitorConsoleStage.attach(gm, monitorRate, ringBufferMonitorConfig);
		
		//Enable batching
		GraphManager.enableBatching(gm);
		
		
	    long totalMessages = timeAndRunTest(ringBuffer22, gm, expected.length, " HighLevel", dumpStage21, dumpStage22);   
	    long expectedMessages = dumpStage11.messageCount() + dumpStage12.messageCount();
	    assertEquals(expectedMessages,totalMessages);
		
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
		
		SplitterStage stage = buildSplitterTree(checkArgs, gm);
		
		InputStageEventConsumerExample producer = new InputStageEventConsumerExample(gm, GraphManager.getInputRing(gm, stage, 1));
				
		
		//Add monitoring
		MonitorConsoleStage.attach(gm, monitorRate, ringBufferMonitorConfig);
		
		//Enable batching
		GraphManager.enableBatching(gm);

		
		RingBuffer ringForDataCount = GraphManager.getInputRing(gm, GraphManager.findStageByPath(gm, 1, 1, 2), 1);
		long totalMessages = timeAndRunTest(ringForDataCount, gm, expectedBytes, " event consumer", 
				                            (CheckVarLengthValuesStage)GraphManager.findStageByPath(gm, 1, 1, 2, 1), 
				                            (CheckVarLengthValuesStage)GraphManager.findStageByPath(gm, 1, 1, 2, 2));
		assertEquals(((CheckVarLengthValuesStage)GraphManager.findStageByPath(gm, 1, 1, 1, 1)).messageCount() + 
				     ((CheckVarLengthValuesStage)GraphManager.findStageByPath(gm, 1, 1, 1, 2)).messageCount(), 
				     totalMessages);
		
		
	}
	

	private long timeAndRunTest(RingBuffer ringBuffer, GraphManager gm,
			final int expectedBytes, String label, CheckVarLengthValuesStage ... countStages) {
		StageScheduler scheduler = new ThreadPerStageScheduler(GraphManager.cloneAll(gm));
		 
	    long startTime = System.currentTimeMillis();
		scheduler.startup();

		try {
			Thread.sleep(TEST_LENGTH_IN_SECONDS*1000);
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
				
		if (0!=duration && 0!=messages) {
			if (expectedBytes>0) {
				assertEquals(bytes,(messages*expectedBytes));
			}
			long bytesMoved = (4*RingBuffer.headPosition(ringBuffer))+bytes;
			float mbMoved = (8f*bytesMoved)/(float)(1<<20);
			
			int bytesPerMessage = (int)(bytesMoved/messages);
			
			System.out.println("TotalMessages:"+messages + 
					           " Msg/Ms:"+(messages/(float)duration) +
					           " Mb/Ms:"+((mbMoved/(float)duration)) +
					           label +" B/Msg:"+bytesPerMessage
							  );
		}
		
		//assertTrue("RingBuffer: "+ringBuffer, cleanExit);
	
		return messages;
	}
	
	
	private interface CheckStageArguments {		
		
		public int expectedMessageIdx(); 
		public byte[] expectedBytes();
		public int[] expectedInts();
        		
	}

	private final class CheckVarLengthValuesStage extends PronghornStage {
		

		private final RingBuffer inputRing;
		private final int expectedMessageIdx;
		private final byte[] expectedBytes;
		private final int[] expectedInts;
		private final FieldReferenceOffsetManager from;
		private final int fragSize;

		private volatile long count;
		private volatile long bytes;
		
		private final boolean testData = true;
		

		private CheckVarLengthValuesStage(GraphManager gm, RingBuffer inputRing, CheckStageArguments args) {
			super(gm, inputRing, NONE);
			this.inputRing = inputRing;
			this.expectedMessageIdx = args.expectedMessageIdx();
			this.expectedBytes = testData ? args.expectedBytes() : null;
			this.expectedInts = testData ? args.expectedInts() :  null;
			this.from = RingBuffer.from(inputRing);
			this.fragSize = from.fragDataSize[expectedMessageIdx];
			
		}

		public long messageCount() {
			return count;
		}
		
		public long totalBytes() {
			return bytes;
		}

		//TODO: build a standard java blocking Queue example as a point of comparison.
		//      Must know if event consumer is faster or slower.
		
		@Override
		public void run() {
					runTest(fragSize, inputRing, inputRing.mask, inputRing.buffer, inputRing.workingTailPos, inputRing.byteWorkingTailPos);
	
		}

		private void runTest(int fragSize, RingBuffer inputRing, int mask, int[] buffer, PaddedLong workingTailPos, PaddedInt byteWorkingTailPos) {
			int c = 0;		
			int b = 0;
			
			while (RingBuffer.contentToLowLevelRead(inputRing,fragSize)) {
				long nextTargetHead = RingBuffer.confirmLowLevelRead(inputRing, fragSize);

				//Instead of pulling each variable field this pulls them all at once
				int len = buffer[ (((int)nextTargetHead) - 1) & mask];
			    
				deepValueTesting(len);
				
				workingTailPos.value = nextTargetHead;
				byteWorkingTailPos.value += len;
				b += len;
				c++;
				
				releaseReadLock(inputRing);
			
			};	
			count += c;
			bytes += b;
		}

		private void deepValueTesting(int len) {
			//checking the primary ints
			if (null!=expectedInts) {
				testExpectedInts();
			}					
						            
			        			            
			if (null!= expectedBytes) {
				testExpectedBytes(len);
			}
		}

		private void testExpectedBytes(int len) {
			int base = RingBuffer.bytesReadBase(inputRing);
			int msgId = RingBuffer.takeMsgIdx(inputRing);
			assertEquals(expectedMessageIdx, msgId);
			assertEquals(expectedBytes.length, len);
			int i = len;
			while (--i>=0) {
				if (expectedBytes[i] != inputRing.byteBuffer[((base+i)&inputRing.byteMask)]) {	
					fail("String does not match at index "+i+" of "+len+"   tailPos:"+RingBuffer.tailPosition(inputRing)+" byteFailurePos:"+(base+i)+" masked "+((base+i)&inputRing.byteMask));
				}
			}
		}

		private void testExpectedInts() {
			long primaryPos = inputRing.workingTailPos.value;
			int j = fragSize;
			//int[] expectedInts = new int[fragSize];
			while (--j>=0) {
				if (expectedInts[j] != inputRing.buffer[(int)(primaryPos+j)&inputRing.mask]) {
				    System.err.println("failure after message "+count);
					System.err.println(Arrays.toString(expectedInts));
					
					try {
			          System.err.println( Arrays.toString(Arrays.copyOfRange(inputRing.buffer, (int)primaryPos&inputRing.mask, (int)(primaryPos+fragSize)&inputRing.mask)) );
					} catch (Throwable t) {
						 // ignore
					}
					//fail("Ints do not match at index "+j+" of "+fragSize+"   tailPos:"+RingBuffer.tailPosition(inputRing)+" byteFailurePos:"+(primaryPos+j)+" masked "+((primaryPos+j)&inputRing.mask));
				}
			}
		}

	}
	
	
}

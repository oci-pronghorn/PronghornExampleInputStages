package com.ociweb.pronghorn.exampleStages;

import static com.ociweb.pronghorn.ring.RingBuffer.releaseReadLock;
import static org.junit.Assert.*;

import java.util.Arrays;
import java.util.concurrent.TimeUnit;

import org.junit.BeforeClass;
import org.junit.Test;

import com.ociweb.pronghorn.ring.FieldReferenceOffsetManager;
import com.ociweb.pronghorn.ring.RingBuffer;
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
	static final long TEST_LENGTH_IN_SECONDS = 5;
	
	private static FieldReferenceOffsetManager from;
	public static final int messagesOnRing = 200;
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
								
	
		RingBuffer ringBuffer = new RingBuffer(ringBufferConfig);
		RingBuffer ringBuffer21 = new RingBuffer(ringBufferConfig.grow2x());
		RingBuffer ringBuffer211 = new RingBuffer(ringBufferConfig);
		RingBuffer ringBuffer212 = new RingBuffer(ringBufferConfig);
		RingBuffer ringBuffer22 = new RingBuffer(ringBufferConfig.grow2x());
		RingBuffer ringBuffer221 = new RingBuffer(ringBufferConfig);
		RingBuffer ringBuffer222 = new RingBuffer(ringBufferConfig);
		
		//Just to confirm that we are starting at zero before running the test.
		assertEquals(0, RingBuffer.headPosition(ringBuffer));
		assertEquals(0, RingBuffer.bytesHeadPosition(ringBuffer));
		assertEquals(0, RingBuffer.headPosition(ringBuffer21));
		assertEquals(0, RingBuffer.bytesHeadPosition(ringBuffer21));
		
		//build the expected data that should be found on the byte ring.
		final byte[] expected = new String("tcp://localhost:1883thingFortytworoot/colors/blue        ").getBytes();
		int j = 8;
		while (--j>=0) {
			expected[expected.length-(8-j)] = (byte)j;
		}
		final int[] expectedInts = new int[]{0, 0, 20, 20, 13, 42, 33, 16, 49, 8, 0, 57};		
		int expectedMsg = 0;
		
   	    GraphManager gm = new GraphManager();
   	    
   	    //TODO: each stage can get a NAME.
		
   	    
		
		InputStageLowLevelExample  iso = new InputStageLowLevelExample(gm, ringBuffer); //full queue
		
		//simple stage that reports to the console
	   
		SplitterStage stage = new SplitterStage(gm, ringBuffer, ringBuffer21, ringBuffer22);	//mostly full queue
		
		RoundRobinRouteStage router21 = new RoundRobinRouteStage(gm, ringBuffer21, ringBuffer211, ringBuffer212); //mostly empty
		CheckVarLengthValuesStage dumpStage11 = new CheckVarLengthValuesStage(gm, ringBuffer211, expectedMsg, expected, expectedInts);
		CheckVarLengthValuesStage dumpStage12 = new CheckVarLengthValuesStage(gm, ringBuffer212, expectedMsg, expected, expectedInts);
					
		RoundRobinRouteStage router22 = new RoundRobinRouteStage(gm, ringBuffer22, ringBuffer221, ringBuffer222); //mostly empty
		CheckVarLengthValuesStage dumpStage21 = new CheckVarLengthValuesStage(gm, ringBuffer221, expectedMsg, expected, expectedInts);
		CheckVarLengthValuesStage dumpStage22 = new CheckVarLengthValuesStage(gm, ringBuffer222, expectedMsg, expected, expectedInts);
		
		GraphManager.addAnnotation(gm, GraphManager.SCHEDULE_RATE, monitorRate, new MonitorConsoleStage(gm, GraphManager.attachMonitorsToGraph(gm, monitorRate, ringBufferMonitorConfig)));
		
		//Enable batching
		GraphManager.enableBatching(gm);
				
		
		long totalMessages = timeAndRunTest(ringBuffer22, gm, expected, " low level", dumpStage21, dumpStage22);
		long expectedMessages = dumpStage11.messageCount() + dumpStage12.messageCount();
		assertEquals(expectedMessages,totalMessages);
		
	}
	
	
	@Test
	public void lowLevelInputStageSimpleTest() {
								
	
		RingBuffer ringBuffer = new RingBuffer(ringBufferConfig);
		
		
		//build the expected data that should be found on the byte ring.
		final byte[] expected = new String("tcp://localhost:1883thingFortytworoot/colors/blue        ").getBytes();
		int j = 8;
		while (--j>=0) {
			expected[expected.length-(8-j)] = (byte)j;
		}
		final int[] expectedInts = new int[]{0, 0, 20, 20, 13, 42, 33, 16, 49, 8, 0, 57};		
		int expectedMsg = 0;
		
   	    GraphManager gm = new GraphManager();
   	    
   		//Enable batching
		GraphManager.enableBatching(gm);
			
		
		InputStageLowLevelExample  iso = new InputStageLowLevelExample(gm, ringBuffer); //full queue
		
		CheckVarLengthValuesStage dumpStage22 = new CheckVarLengthValuesStage(gm, ringBuffer, expectedMsg, null, null /*expected, expectedInts*/);
		
		timeAndRunTest(ringBuffer, gm, expected, " SimpleLowLevel", dumpStage22);   
		
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
		
		//Just to confirm that we are starting at zero before running the test.
		assertEquals(0, RingBuffer.headPosition(ringBuffer));
		assertEquals(0, RingBuffer.bytesHeadPosition(ringBuffer));
		assertEquals(0, RingBuffer.headPosition(ringBuffer21));
		assertEquals(0, RingBuffer.bytesHeadPosition(ringBuffer21));
		
		
   	    GraphManager gm = new GraphManager();
		
   	    InputStageHighLevelExample  iso = new InputStageHighLevelExample(gm, ringBuffer);
		
   	    //	build the expected data that should be found on the byte ring.
   	    final byte[] expected = (iso.testSymbol+iso.testCompanyName).getBytes();
   	    int expectedMsg = from.messageStarts[1];
   	    final int[] expectedInts =  new int[] {8, 0, 3, 3, 31, 0, 0, 2, 0, 10250, 2, 0, 10250, 2, 0, 10250, 2, 0, 10250, 0, 10000000, 34};
		
   	    //simple stage that reports to the console
	   
		SplitterStage stage = new SplitterStage(gm,ringBuffer, ringBuffer21, ringBuffer22);	

		RoundRobinRouteStage router21 = new RoundRobinRouteStage(gm, ringBuffer21, ringBuffer211, ringBuffer212);
		CheckVarLengthValuesStage dumpStage11 = new CheckVarLengthValuesStage(gm, ringBuffer211, expectedMsg, expected, expectedInts);
		CheckVarLengthValuesStage dumpStage12 = new CheckVarLengthValuesStage(gm, ringBuffer212, expectedMsg, expected, expectedInts);
					
		RoundRobinRouteStage router22 = new RoundRobinRouteStage(gm, ringBuffer22, ringBuffer221, ringBuffer222);
		CheckVarLengthValuesStage dumpStage21 = new CheckVarLengthValuesStage(gm, ringBuffer221, expectedMsg, expected, expectedInts);
		CheckVarLengthValuesStage dumpStage22 = new CheckVarLengthValuesStage(gm, ringBuffer222, expectedMsg, expected, expectedInts);

		//Turn on monitoring
		GraphManager.addAnnotation(gm, GraphManager.SCHEDULE_RATE, monitorRate, new MonitorConsoleStage(gm, GraphManager.attachMonitorsToGraph(gm, monitorRate, ringBufferMonitorConfig)));	
		
		//Enable batching
		GraphManager.enableBatching(gm);
		
		
	    long totalMessages = timeAndRunTest(ringBuffer22, gm, expected, " HighLevel", dumpStage21, dumpStage22);   
	    long expectedMessages = dumpStage11.messageCount() + dumpStage12.messageCount();
	    assertEquals(expectedMessages,totalMessages);
		
	}


		
	@Test
	public void eventConsumerInputStageTest() {
								
	
		RingBuffer ringBuffer = new RingBuffer(ringBufferConfig);
		RingBuffer ringBuffer21 = new RingBuffer(ringBufferConfig.grow2x());
		RingBuffer ringBuffer211 = new RingBuffer(ringBufferConfig);
		RingBuffer ringBuffer212 = new RingBuffer(ringBufferConfig);
		RingBuffer ringBuffer22 = new RingBuffer(ringBufferConfig.grow2x());
		RingBuffer ringBuffer221 = new RingBuffer(ringBufferConfig);
		RingBuffer ringBuffer222 = new RingBuffer(ringBufferConfig);
		
		//Just to confirm that we are starting at zero before running the test.
		assertEquals(0, RingBuffer.headPosition(ringBuffer));
		assertEquals(0, RingBuffer.bytesHeadPosition(ringBuffer));
		assertEquals(0, RingBuffer.headPosition(ringBuffer21));
		assertEquals(0, RingBuffer.bytesHeadPosition(ringBuffer21));
		
		
   	    GraphManager gm = new GraphManager();
   	    
				
        InputStageEventConsumerExample  iso = new InputStageEventConsumerExample(gm, ringBuffer);
		
   	    //	build the expected data that should be found on the byte ring.
   	    final byte[] expected = (iso.testSymbol+iso.testCompanyName).getBytes();
   	    int expectedMsg = from.messageStarts[1];   	    
		final int[] expectedInts = new int[] {8, 0, 3, 3, 31, 0, 0, 2, 0, 2343, 2, 0, 8000, 2, 0, 2000, 2, 0, 7230, 0, 10000000, 34};
   	    //simple stage that reports to the console
	   
		
		SplitterStage stage = new SplitterStage(gm,ringBuffer, ringBuffer21, ringBuffer22);
				
		RoundRobinRouteStage router21 = new RoundRobinRouteStage(gm, ringBuffer21, ringBuffer211, ringBuffer212);
		CheckVarLengthValuesStage dumpStage11 = new CheckVarLengthValuesStage(gm, ringBuffer211, expectedMsg, expected, expectedInts);
		CheckVarLengthValuesStage dumpStage12 = new CheckVarLengthValuesStage(gm, ringBuffer212, expectedMsg, expected, expectedInts);
					
		RoundRobinRouteStage router22 = new RoundRobinRouteStage(gm, ringBuffer22, ringBuffer221, ringBuffer222);
		CheckVarLengthValuesStage dumpStage21 = new CheckVarLengthValuesStage(gm, ringBuffer221, expectedMsg, expected, expectedInts);
		CheckVarLengthValuesStage dumpStage22 = new CheckVarLengthValuesStage(gm, ringBuffer222, expectedMsg, expected, expectedInts);
		
		GraphManager.addAnnotation(gm, GraphManager.SCHEDULE_RATE, monitorRate, new MonitorConsoleStage(gm, GraphManager.attachMonitorsToGraph(gm, monitorRate, ringBufferMonitorConfig)));
		
		//Enable batching
		GraphManager.enableBatching(gm);
		
		long totalMessages = timeAndRunTest(ringBuffer22, gm, expected, " EventConsumer", dumpStage21, dumpStage22);   
	    long expectedMessages = dumpStage11.messageCount() + dumpStage12.messageCount();
	    assertEquals(expectedMessages,totalMessages);
	}
	

	private long timeAndRunTest(RingBuffer ringBuffer, GraphManager gm,
			final byte[] expected, String label, CheckVarLengthValuesStage ... countStages) {
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
 

		int j = countStages.length;
		long messages=0;
		long bytes=0;
		while (--j>=0) {
			messages+=countStages[j].messageCount();
			bytes+=countStages[j].totalBytes();
		}
				
		long duration = System.currentTimeMillis()-startTime;
		if (0!=duration) {
			assertEquals(bytes,(messages*expected.length));
			
			long bytesMoved = (4*RingBuffer.headPosition(ringBuffer))+bytes;
			float mbMoved = (8f*bytesMoved)/(float)(1<<20);
			
			System.out.println("TotalMessages:"+messages + 
					           " Msg/Ms:"+(messages/(float)duration) +
					           " Mb/Ms:"+((mbMoved/(float)duration)) +
					           label
							  );
		}
		
		//assertTrue("RingBuffer: "+ringBuffer, cleanExit);
	
		return messages;
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

		private CheckVarLengthValuesStage(GraphManager gm,RingBuffer inputRing, 
				                          int expectedMessageIdx, 
				                          byte[] expectedBytes,
				                          int[] expectedInts
				                          ) {
			super(gm, inputRing, NONE);
			this.inputRing = inputRing;
			this.expectedMessageIdx = expectedMessageIdx;
			this.expectedBytes = expectedBytes;
			this.expectedInts = expectedInts;
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
							
					while (RingBuffer.contentToLowLevelRead(inputRing,fragSize)) {
						long nextTargetHead = RingBuffer.confirmLowLevelRead(inputRing, fragSize);
				        
						//checking the primary ints
						if (null!=expectedInts) {
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
						
			            int msgId = RingBuffer.takeMsgIdx(inputRing);
			            			            
			            //Instead of pulling each variable field this pulls them all at once
			            int base = RingBuffer.bytesReadBase(inputRing);
			            int len = inputRing.buffer[(int)((nextTargetHead - 1)&inputRing.mask)];
					            	
			            
			            if (null!= expectedBytes) {
			            	assertEquals(expectedMessageIdx, msgId);
			            	assertEquals(expectedBytes.length, len);
							int i = len;
							while (--i>=0) {
								if (expectedBytes[i] != inputRing.byteBuffer[((base+i)&inputRing.byteMask)]) {	
									fail("String does not match at index "+i+" of "+len+"   tailPos:"+RingBuffer.tailPosition(inputRing)+" byteFailurePos:"+(base+i)+" masked "+((base+i)&inputRing.byteMask));
								}
							}
			            }
						
						inputRing.workingTailPos.value = nextTargetHead;
						inputRing.byteWorkingTailPos.value += len;
						
						releaseReadLock(inputRing);
			            	
			        	count++;
			        	
			        	bytes += len;
		        	
					};	
	
		}

	}
	
	
}

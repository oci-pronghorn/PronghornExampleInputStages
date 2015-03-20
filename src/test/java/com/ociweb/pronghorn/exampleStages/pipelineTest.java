package com.ociweb.pronghorn.exampleStages;

import static com.ociweb.pronghorn.ring.RingBuffer.releaseReadLock;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.util.Arrays;
import java.util.concurrent.TimeUnit;

import org.junit.BeforeClass;
import org.junit.Test;

import com.ociweb.pronghorn.ring.FieldReferenceOffsetManager;
import com.ociweb.pronghorn.ring.RingBuffer;
import com.ociweb.pronghorn.ring.RingBufferConfig;
import com.ociweb.pronghorn.ring.loader.TemplateHandler;
import com.ociweb.pronghorn.stage.PronghornStage;
import com.ociweb.pronghorn.stage.route.SplitterStage;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;
import com.ociweb.pronghorn.stage.scheduling.StageScheduler;
import com.ociweb.pronghorn.stage.scheduling.ThreadPerStageScheduler;

public class pipelineTest {

	private static final long TIMEOUT_SECONDS = 4;
	private static final long TEST_LENGTH_IN_SECONDS = 7;
	
	private static FieldReferenceOffsetManager from;
	private static final int messagesOnRing = 3000;
	private static final int maxLengthVarField = 40;
	
	private static RingBufferConfig ringBufferConfig;

	@BeforeClass
	public static void loadSchema() {
		///////
		//When doing development and testing be sure that assertions are on by adding -ea to the JVM arguments
		//This will enable a lot of helpful to catch configuration and setup errors earlier
		/////
		
		try {
			from = TemplateHandler.loadFrom("/exampleTemplate.xml");
			ringBufferConfig = new RingBufferConfig(from, messagesOnRing, maxLengthVarField);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
		
	}
	
	
	@Test
	public void lowLevelInputStageTest() {
								
	
		RingBuffer ringBuffer = new RingBuffer(ringBufferConfig);
		RingBuffer ringBuffer21 = new RingBuffer(ringBufferConfig.grow2x());
		RingBuffer ringBuffer22 = new RingBuffer(ringBufferConfig.grow2x());
		
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
		
		
   	    GraphManager gm = new GraphManager();
   	    
		
		InputStageLowLevelExample  iso = new InputStageLowLevelExample(gm, ringBuffer);
		
		//simple stage that reports to the console
	   
//		SplitterStage stage = new SplitterStage(gm,ringBuffer, ringBuffer21, ringBuffer22);	
//		//ConsoleStage console = new ConsoleStage(gm, ringBuffer21); //this stage can be used to watch the test
//		CheckVarLengthValuesStage dumpStage1 = new CheckVarLengthValuesStage(gm, ringBuffer21, 0, expected, expectedInts);					
//		CheckVarLengthValuesStage dumpStage2 = new CheckVarLengthValuesStage(gm, ringBuffer22, 0, expected, expectedInts);
		
		//Use this line if you wan to skip the split but  you must comment out the above lines starting with the splitterStage
		CheckVarLengthValuesStage dumpStage1 = new CheckVarLengthValuesStage(gm, ringBuffer,  0, expected, expectedInts);	
		
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
//        assertEquals(dumpStage1.messageCount(),dumpStage2.messageCount());
		if (!cleanExit) {
			
			System.err.println("RingBuffer: "+ringBuffer);//print the details of the ring buffer
			fail("Did not shtudown cleanly");
			 
		}		
				
		long duration = System.currentTimeMillis()-startTime;
		if (0!=duration) {
			long messages = dumpStage1.messageCount();
			long bytes = dumpStage1.totalBytes();	
			assertEquals(bytes,(messages*expected.length));
			
			long bytesMoved = (4*RingBuffer.headPosition(ringBuffer))+bytes;
			float mbMoved = (8f*bytesMoved)/(float)(1<<20);
			
			System.out.println("TotalMessages:"+messages + 
					           " Msg/Ms:"+(messages/(float)duration) +
					           " Mb/Ms:"+((mbMoved/(float)duration))					           
							  );
		}
		
	}
	
	
	@Test
	public void highLevelInputStageTest() {								
	
		RingBuffer ringBuffer = new RingBuffer(ringBufferConfig);
		RingBuffer ringBuffer21 = new RingBuffer(ringBufferConfig.grow2x());
		RingBuffer ringBuffer22 = new RingBuffer(ringBufferConfig.grow2x());
		
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
   	    final int[] expectedInts =  new int[] {8, 0, 3, 3, 31, 0, -1, 2, 0, 10250, 2, 0, 10250, 2, 0, 10250, 2, 0, 10250, 0, 10000000, 34};
		
   	    //simple stage that reports to the console
	   
//		SplitterStage stage = new SplitterStage(gm,ringBuffer, ringBuffer21, ringBuffer22);	
//		//ConsoleStage console = new ConsoleStage(gm, ringBuffer21); //this stage can be used to watch the test
//		CheckVarLengthValuesStage dumpStage1 = new CheckVarLengthValuesStage(gm, ringBuffer21, expectedMsg, expected, expectedInts);					
//		CheckVarLengthValuesStage dumpStage2 = new CheckVarLengthValuesStage(gm, ringBuffer22, expectedMsg, expected, expectedInts);
		
		//Use this line if you wan to skip the split but  you must comment out the above lines starting with the splitterStage
		CheckVarLengthValuesStage dumpStage1 = new CheckVarLengthValuesStage(gm, ringBuffer, expectedMsg, expected, expectedInts);	
		
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
     //   assertEquals(dumpStage1.messageCount(),dumpStage2.messageCount());
		if (!cleanExit) {
			
			System.err.println("RingBuffer: "+ringBuffer);//print the details of the ring buffer
			fail("Did not shtudown cleanly");
			 
		}		
				
		long duration = System.currentTimeMillis()-startTime;
		if (0!=duration) {
			long messages = dumpStage1.messageCount();
			long bytes = dumpStage1.totalBytes();	
			assertEquals(bytes,(messages*expected.length));
			
			long bytesMoved = (4*RingBuffer.headPosition(ringBuffer))+bytes;
			float mbMoved = (8f*bytesMoved)/(float)(1<<20);
			
			System.out.println("TotalMessages:"+messages + 
					           " Msg/Ms:"+(messages/(float)duration) +
					           " Mb/Ms:"+((mbMoved/(float)duration))					           
							  );
		}
		
	}
		
	@Test
	public void eventConsumerInputStageTest() {
								
	
		RingBuffer ringBuffer = new RingBuffer(ringBufferConfig);
		RingBuffer ringBuffer21 = new RingBuffer(ringBufferConfig.grow2x());
		RingBuffer ringBuffer22 = new RingBuffer(ringBufferConfig.grow2x());
		
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
		final int[] expectedInts = new int[] {8, 0, 3, 3, 31, 0, 0, 2, 0, 2000, 0, 0, 10000000, 34, 8, 0, 10000000, 34, 8, 0, 10000000, 34};
   	    //simple stage that reports to the console
	   
//		SplitterStage stage = new SplitterStage(gm,ringBuffer, ringBuffer21, ringBuffer22);	
//		//ConsoleStage console = new ConsoleStage(gm, ringBuffer21); //this stage can be used to watch the test
//		CheckVarLengthValuesStage dumpStage1 = new CheckVarLengthValuesStage(gm, ringBuffer21, expectedMsg, expected, expectedInts);					
//		CheckVarLengthValuesStage dumpStage2 = new CheckVarLengthValuesStage(gm, ringBuffer22, expectedMsg, expected, expectedInts);
//		
		//Use this line if you wan to skip the split but  you must comment out the above lines starting with the splitterStage
		CheckVarLengthValuesStage dumpStage1 = new CheckVarLengthValuesStage(gm, ringBuffer, expectedMsg, expected, null);	
		
	    StageScheduler scheduler = new ThreadPerStageScheduler(GraphManager.cloneAll(gm));
		 
	    //TODO: add deeper testing so we can confirm the expected stock prices here.
	    
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
  //      assertEquals(dumpStage1.messageCount(),dumpStage2.messageCount());
		if (!cleanExit) {
			
			System.err.println("RingBuffer: "+ringBuffer);//print the details of the ring buffer
			fail("Did not shtudown cleanly");
			 
		}		
				
		long duration = System.currentTimeMillis()-startTime;
		if (0!=duration) {
			long messages = dumpStage1.messageCount();
			long bytes = dumpStage1.totalBytes();	
			assertEquals(bytes,(messages*expected.length));
			
			long bytesMoved = (4*RingBuffer.headPosition(ringBuffer))+bytes;
			float mbMoved = (8f*bytesMoved)/(float)(1<<20);
			
			System.out.println("TotalMessages:"+messages + 
					           " Msg/Ms:"+(messages/(float)duration) +
					           " Mb/Ms:"+((mbMoved/(float)duration))					           
							  );
		}
		
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
		

		@Override
		public void startup() {
			RingBuffer.initLowLevelReader(inputRing);
			super.startup();
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
			            assertEquals(expectedMessageIdx, msgId);
			            			            
			            //Instead of pulling each variable field this pulls them all at once
			            int base = RingBuffer.bytesReadBase(inputRing);
			            int len = inputRing.buffer[(int)((nextTargetHead - 1)&inputRing.mask)];
					            	
			            assertEquals(expectedBytes.length, len);
			            
						int i = len;
						while (--i>=0) {
							if (expectedBytes[i] != inputRing.byteBuffer[((base+i)&inputRing.byteMask)]) {	
								fail("String does not match at index "+i+" of "+len+"   tailPos:"+RingBuffer.tailPosition(inputRing)+" byteFailurePos:"+(base+i)+" masked "+((base+i)&inputRing.byteMask));
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

package com.ociweb.pronghorn.exampleStages;

import static org.junit.Assert.fail;

import java.util.Arrays;

import com.ociweb.pronghorn.exampleStages.PipelineTest.CheckStageArguments;
import com.ociweb.pronghorn.ring.FieldReferenceOffsetManager;
import com.ociweb.pronghorn.ring.RingBuffer;
import com.ociweb.pronghorn.ring.RingBuffer.PaddedLong;
import com.ociweb.pronghorn.stage.PronghornStage;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;

public final class CheckVarLengthValuesStage extends PronghornStage {
	

	private final RingBuffer inputRing;
	private final int expectedMessageIdx;
	private final byte[] expectedBytes;
	private final int[] expectedInts;
	private final FieldReferenceOffsetManager from;
	private final int fragSize;

	private volatile long count;
	private volatile long bytes;
	
	private final boolean testData;
	

	public CheckVarLengthValuesStage(GraphManager gm, RingBuffer inputRing, CheckStageArguments args, boolean testData) {
		super(gm, inputRing, NONE);
		this.testData = testData;
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

	
	@Override
	public void run() {
				if (RingBuffer.contentToLowLevelRead(inputRing,fragSize)) {
					consumeMessages(inputRing);
				}

	}

	private void consumeMessages(RingBuffer inputRing) {
		runTest2(fragSize, inputRing, inputRing.mask, RingBuffer.primaryBuffer(inputRing), RingBuffer.getWorkingTailPositionObject(inputRing));
	}

	private void runTest2(int fragSize, RingBuffer inputRing, int mask,	int[] buffer, PaddedLong workingTailPos) {
		long c = 0;		
		long b = 0;
		do {
			b += consumeMessage(fragSize, inputRing, mask, buffer, workingTailPos);
			c++;					
		} while (RingBuffer.contentToLowLevelRead(inputRing,fragSize));	

		count += c;
		bytes += b;
	}

	private long consumeMessage(int fragSize, RingBuffer inputRing,	int mask, int[] buffer, PaddedLong workingTailPos) {
		
		RingBuffer.confirmLowLevelRead(inputRing, fragSize);

		deepValueTesting();

		//Do after testing so not to move this needed position
		RingBuffer.addAndGetWorkingTail(inputRing, fragSize-1);//One less do releaseReads can grab the byte length
		return RingBuffer.releaseReads(inputRing);
	}

	private void deepValueTesting() {
		//checking the primary ints
		if (null!=expectedInts) {
			testExpectedInts();
		}		    			            
		if (null!= expectedBytes) {
			testExpectedBytes();
		}
	}

	private void testExpectedBytes() {
		int base = RingBuffer.bytesReadBase(inputRing);
		int msgId = RingBuffer.peekInt(inputRing);//must use non-distructive read
		if (expectedMessageIdx!=msgId) {
			messageCountMismatch(msgId);
		}
	    deepByteCheck(expectedBytes.length, base, expectedBytes.length, inputRing.byteMask, RingBuffer.byteBuffer(inputRing));
	}

	private void deepByteCheck(int len, int base, int i, int byteMask, byte[] byteBuffer) {
		int errIdx = -1;
	    while (--i>=0) {
			if (expectedBytes[i] != byteBuffer[((base+i)&byteMask)]) {	
			    errIdx = i;
			}
		}		
	    if (errIdx>=0) {
	        showByteError(len, base, errIdx);
	    }
	    
	}

	private void messageCountMismatch(int msgId) {
		fail("did not expect message id of "+msgId);
	}

	private void byteLengthMismatch(int len) {
		fail("did not expect byte length of "+len);
	}

	private void showByteError(int len, int base, int i) {
		fail("String does not match at index "+i+" of "+len+"   tailPos:"+RingBuffer.tailPosition(inputRing)+" byteFailurePos:"+(base+i)+" masked "+((base+i)&inputRing.byteMask));
	}

	private void testExpectedInts() {
		long primaryPos = RingBuffer.getWorkingTailPosition(inputRing);
		int j = fragSize;
		while (--j>=0) {
			if (expectedInts[j] != RingBuffer.primaryBuffer(inputRing)[(int)(primaryPos+j)&inputRing.mask]) {
			    showIntError(primaryPos);
			}
		}
	}

	private void showIntError(long primaryPos) {
		System.err.println("failure after message "+count);
		System.err.println("EXP:"+Arrays.toString(expectedInts));		
		System.err.println("ACT:"+Arrays.toString(Arrays.copyOfRange(RingBuffer.primaryBuffer(inputRing), (int)primaryPos&inputRing.mask, (int)(primaryPos+fragSize)&inputRing.mask)) );

		fail("Ints do not match ");
	}

}
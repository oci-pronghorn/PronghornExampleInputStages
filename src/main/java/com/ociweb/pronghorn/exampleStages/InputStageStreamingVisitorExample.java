package com.ociweb.pronghorn.exampleStages;

import java.nio.ByteBuffer;

import com.ociweb.pronghorn.ring.FieldReferenceOffsetManager;
import com.ociweb.pronghorn.ring.RingBuffer;
import com.ociweb.pronghorn.ring.stream.StreamingVisitorWriter;
import com.ociweb.pronghorn.ring.stream.StreamingWriteVisitor;
import com.ociweb.pronghorn.stage.PronghornStage;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;

public class InputStageStreamingVisitorExample extends PronghornStage {

    
    //TODO: incomplete and needs unit test to run this.
	

    
	private final class ExampleWriterVisitor implements StreamingWriteVisitor {
	    
	    final int msgIdx;
	    
        public ExampleWriterVisitor(FieldReferenceOffsetManager from) {
            
            msgIdx = from.messageStarts[1];
        }

        @Override
        public boolean paused() {
            return false;
        }

        @Override
        public int pullMessageIdx() { 
            return msgIdx;
        }

        @Override
        public boolean isAbsent(String name, long id) {
            //no fields are optional
            return false;
        }

        @Override
        public long pullSignedLong(String name, long id) {
            throw new UnsupportedOperationException("There is no signed long field");
        }

        @Override
        public long pullUnsignedLong(String name, long id) {
            //could confirm that id is for the volume field
            return 10000000l;
        }

        @Override
        public int pullSignedInt(String name, long id) {
            throw new UnsupportedOperationException("There is no signed int field");
        }

        @Override
        public int pullUnsignedInt(String name, long id) {
            // TODO Auto-generated method stub
            return 0;
        }

        @Override
        public int pullDecimalExponent(String name, long id) {
            //switch on id for the right field
            return 2;//all the fields are to use 2 decimal places
        }
        
        @Override
        public long pullDecimalMantissa(String name, long id) {
            //switch on id for the right field
            //high, low, open, close
            return 10250;//all the fields happen to have the same value
        }

        @Override
        public CharSequence pullASCII(String name, long id) {
            switch((int)id) {
                case 4:
                        return InputStageHighLevelExample.testSymbol;
                case 84:
                        return InputStageHighLevelExample.testCompanyName;
            }
            throw new UnsupportedOperationException("Unknown message id:"+id);
        }

        @Override
        public CharSequence pullUTF8(String name, long id) {
            //could confirm that this id is for the empty field
            return ""; //null can be returned but to make the unit test work we must fill every spot.
        }

        @Override
        public ByteBuffer pullByteBuffer(String name, long id) {
            throw new UnsupportedOperationException();
        }

        @Override
        public int pullSequenceLength(String name, long id) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void startup() {
            
        }

        @Override
        public void shutdown() {
            
        }

        @Override
        public void templateClose(String name, long id) {
            
        }

        @Override
        public void sequenceClose(String name, long id) {
            
        }

        @Override
        public void fragmentClose(String name, long id) {
            
        }

        @Override
        public void fragmentOpen(String string, long l) {
            
        }
    }

	private final StreamingVisitorWriter writer;
	
	
	protected InputStageStreamingVisitorExample(GraphManager graphManager, RingBuffer output) {
        super(graphManager, NONE, output);


        StreamingWriteVisitor visitor = new ExampleWriterVisitor(RingBuffer.from(output));
        
        writer = new StreamingVisitorWriter(output, visitor );
    }


    
    @Override
    public void startup() {
        writer.startup();
    }
    
    @Override
    public void run() {
        writer.run();
    }
    
    @Override
    public void shutdown() {
        writer.shutdown();
    }



}

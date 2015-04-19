package com.ociweb.pronghorn.exampleStages;

import static com.ociweb.pronghorn.ring.RingWriter.writeASCII;
import static com.ociweb.pronghorn.ring.RingWriter.writeDecimal;
import static com.ociweb.pronghorn.ring.RingWriter.writeLong;
import static com.ociweb.pronghorn.ring.RingWriter.writeUTF8;

import java.nio.ByteBuffer;

import com.ociweb.pronghorn.ring.FieldReferenceOffsetManager;
import com.ociweb.pronghorn.ring.RingBuffer;
import com.ociweb.pronghorn.ring.stream.StreamingReadVisitor;
import com.ociweb.pronghorn.ring.stream.StreamingVisitorWriter;
import com.ociweb.pronghorn.ring.stream.StreamingWriteVisitor;
import com.ociweb.pronghorn.stage.PronghornStage;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;

public class InputStageStreamingVisitorExample extends PronghornStage {

	
//    writeDecimal(output, FIELD_HIGH_PRICE,   2, 10250);
//    writeDecimal(output, FIELD_LOW_PRICE,    2, 10250);
//    writeDecimal(output, FIELD_OPEN_PRICE,   2, 10250);
//    writeDecimal(output, FIELD_CLOSED_PRICE, 2, 10250);
//    
//    //All fields do not need to be written or written in order however
//    // in order to build unit tests we must write every field here
//    //If fields are not written however keep in mind that they will be filled with garbage.
//    
//    writeUTF8(output, FIELD_EMPTY, (char[])null, 0, -1);
//    
//    writeLong(output, FIELD_VOLUME, 10000000l);         
//    
//    writeASCII(output, FIELD_SYMBOL, testSymbol);
//    writeASCII(output, FIELD_COMPANY_NAME, testCompanyName);
    
	private final class ExampleWriterVisitor implements StreamingWriteVisitor {
        @Override
        public boolean paused() {
            // TODO Auto-generated method stub
            return false;
        }

        @Override
        public int pullMessageIdx() {
            // TODO Auto-generated method stub
            return 0;
        }

        @Override
        public boolean isAbsent(String name, long id) {
            // TODO Auto-generated method stub
            return false;
        }

        @Override
        public long pullSignedLong(String name, long id) {
            // TODO Auto-generated method stub
            return 0;
        }

        @Override
        public long pullUnsignedLong(String name, long id) {
            // TODO Auto-generated method stub
            return 0;
        }

        @Override
        public int pullSignedInt(String name, long id) {
            // TODO Auto-generated method stub
            return 0;
        }

        @Override
        public int pullUnsignedInt(String name, long id) {
            // TODO Auto-generated method stub
            return 0;
        }

        @Override
        public long pullDecimalMantissa(String name, long id) {
            // TODO Auto-generated method stub
            return 0;
        }

        @Override
        public int pullDecimalExponent(String name, long id) {
            // TODO Auto-generated method stub
            return 0;
        }

        @Override
        public CharSequence pullASCII(String name, long id) {
            // TODO Auto-generated method stub
            return null;
        }

        @Override
        public CharSequence pullUTF8(String name, long id) {
            // TODO Auto-generated method stub
            return null;
        }

        @Override
        public ByteBuffer pullByteBuffer(String name, long id) {
            // TODO Auto-generated method stub
            return null;
        }

        @Override
        public int pullSequenceLength(String name, long id) {
            // TODO Auto-generated method stub
            return 0;
        }

        @Override
        public void startup() {
            // TODO Auto-generated method stub
            
        }

        @Override
        public void shutdown() {
            // TODO Auto-generated method stub
            
        }

        @Override
        public void templateClose(String name, long id) {
            // TODO Auto-generated method stub
            
        }

        @Override
        public void sequenceClose(String name, long id) {
            // TODO Auto-generated method stub
            
        }

        @Override
        public void fragmentClose(String name, long id) {
            // TODO Auto-generated method stub
            
        }

        @Override
        public void fragmentOpen(String string, long l) {
            // TODO Auto-generated method stub
            
        }
    }

    private StreamingReadVisitor visitor;
	private final StreamingVisitorWriter writer;
	private FieldReferenceOffsetManager from;
	
	
	protected InputStageStreamingVisitorExample(GraphManager graphManager, RingBuffer output) {
        super(graphManager, NONE, output);


        StreamingWriteVisitor visitor = new ExampleWriterVisitor();
        
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

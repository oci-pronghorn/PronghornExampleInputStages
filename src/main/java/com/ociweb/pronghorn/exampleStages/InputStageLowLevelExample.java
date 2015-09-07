package com.ociweb.pronghorn.exampleStages;

import static com.ociweb.pronghorn.pipe.Pipe.*;

import com.ociweb.pronghorn.pipe.FieldReferenceOffsetManager;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.stage.PronghornStage;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;

public class InputStageLowLevelExample extends PronghornStage {

	//all members should be private final unless the reason is documented with a clear comment 
	private final Pipe output;
		
	private final int msgIdx;
	private final int sizeOfFragment;
	private final FieldReferenceOffsetManager FROM; //Acronym so this is in all caps (this holds the schema)
	
	
	private int fragToWrite;
	private final String serverURI = "tcp://localhost:1883";
	private final String clientId = "thingFortytwo";
	private final int    clientIdIndex = 42;	
	private final String topic = "root/colors/blue";
	private final byte[] payload = new byte[]{0,1,2,3,4,5,6,7};
	private final int serverURILength = serverURI.length();
	private final int clientIdLength = clientId.length();
	private final int topicLength = topic.length();
	private final int payloadLength = payload.length;
	
	
	/**
	 * This is an example of a input stage that uses the low level API.  This is the most difficult to use API for 
	 * the ring buffer but it is generally the fastest and in some corner case becomes the simplest.
	 * 
	 * When should I use the low level API?
	 *           * When you are only using simple messages (no nested structure, can be done but takes work)
	 *           * When you only have 1 message ( or very few message types)
	 *           * When the template XML is never expected to change (without a recompile of the code)
	 *           * When building schema agnostic stages for general use
	 *        
	 * 
	 * What are the significant features of the low level API?
	 *           * Fields are all written in hard coded order with hard coded types
	 *           * No fields are skipped when using the API, every field must be written or read.
	 *           * If the consumer is far ahead the check for room does not need to update from tail position
	 *           * Writes can be batched to release larger blocks to the consumer for fewer head position updates
	 *           * ASCII, UTF8 and Bytes can be accumulated on the buffer followed by a single total on the primary ring.
	 *               (see the implementation of addASCII, addUTF8 for this)
	 * 
	 * 
	 * @param graphManager
	 * @param output
	 */
	protected InputStageLowLevelExample(GraphManager graphManager, Pipe output) {
		super(graphManager, NONE, output);
		
		////////
		//STORE OTHER FIELDS THAT WILL BE REQUIRED IN STARTUP
		////////
	
		this.output = output;
		
		FROM = Pipe.from(output);
		
		//all the script positions for every message is found in this array
		//the length of this array should match the count of templates
		this.msgIdx = FROM.messageStarts[0]; //for this demo we are just using the first message template
		
		this.fragToWrite  = msgIdx;
		
		validateSchemaSupported(FROM);
		
		//low level API can write multiple message and messages with multiple fragments but it 
		//becomes more difficult. (That is what the high level API is more commonly used for)
		//In this example we are writing 1 message that is made up of 1 fragment
		sizeOfFragment = FROM.fragDataSize[msgIdx];
				
	}


	private void validateSchemaSupported(FieldReferenceOffsetManager from) {
		
		///////////
		//confirm that the schema in the output is the same one that we want to support in this stage.
		//if not we should throw now to stop the construction early
		///////////
		
		if (!"MQTTMsg".equals(from.fieldNameScript[msgIdx])) {
			throw new UnsupportedOperationException("Expected to find message template MQTTMsg");
		}
		if (100!=from.fieldIdScript[msgIdx]) {
			throw new UnsupportedOperationException("Expected to find message template MQTTMsg with id 100");
		}
	}
		
	
	@Override
	public void startup() {
		try{
			//setup the output ring for low level writing			
			 //TODO: AA, working to remove this.
		
		    ///////
			//PUT YOUR LOGIC HERE FOR CONNECTING TO THE DATABASE OR OTHER SOURCE OF INFORMATION
			//////
			
			
					
		} catch (Throwable t) {
			throw new RuntimeException(t);
		}
	}
	
	
	@Override
	public void run() {
		///////
		//The run method should not block and should exhaustively write to the target queue until its empty
		//Run will continue to be called by the scheduler as long as shutdown() has not been called on this stage
		//Schedulers may choose not to call run as often if it is determined that other stages need to be done first		
		//////
		
		
		///////
		//Instead of hard coding these to at this point you would check with the input to determine what message should be sent
		//unless this source only produces one kind of message 
		///////
	    int count = 100;//only do this many at a time to allow for shutdown
		while (--count>=0 && roomToLowLevelWrite(output, sizeOfFragment) ) {

			//////
			//gather all the data to be written
			//nothing to do for this example because we are using constants
			//declaring the constant values here would cause GC and slow down the data feeding into the test
			/////

			
			//when using the low level API 
			//     **  Messages must start with addMsgIdx (fragments do not need to start with anything)
			//     **  Every field must be written
			//     **  Every field must be written in order
			//     **  Every fragment must end with publishWrites
			
			//when starting a new message this method must be used to record the message template id, and do internal housekeeping
			writeMessage(output);
	

			
			//publish this fragment
			publishWrites(output);
			

			/////
			//in this case consumedSize is exactly the requiredSize however this need not always be true.
			//for example if we had 2 different template messages that we wrote we could set the required size to the 
			//max of the two messages then after write set the proper value.  This way we can avoid any expensive
			//call to find the type when we are prevented from writing by the full queue.
			////
			
			//only increment upon success
			confirmLowLevelWrite(output, sizeOfFragment);			
		}
		
		
		///////
		//when the end of the input source is reached and no other messages are to be processed
		//stages can terminate on their own by calling shutdown();
		//this is common for stages that do not take input streams but produce output streams
		//when the scheduler detects terminated upstream stages with empty queues it will call shutdown on the downstream stages.
		//in/out Stages must always publish to their output before releasing their input to ensure clean shutdown.
		///////
				
	}


    private void writeMessage(Pipe output) {
        //NOTE: if writing a decimal field it is done with two statements
        //      addIntValue(EXPONENT, output) //this is a value between -64 and +64 for moving the decimal place of the mantissa
        //      addLongValue(MANTISSA,  output) //this is a normal long value holding all the digits of the decimal
        
        //NOTE: if writing a ieee double or float use on of the following
        //      addIntValue(Float.floatToIntBits(value),output);
        //      addLongValue(Double.floatToIntBits(value),output);
         
        
        addMsgIdx(output, fragToWrite);					
        			
        addASCII(serverURI, 0, serverURILength, output); //serverURI
        addUTF8(clientId, clientIdLength, output); //clientId			
        addIntValue(clientIdIndex, output);  //clientId index								
        addASCII(topic, 0, topicLength, output); //topic		
        addByteArray(payload, 0, payloadLength, output);// payload 
        
        addIntValue(0, output); //QoS field
    }


	@Override
	public void shutdown() {
		//if batching was used this will publish any waiting fragments
		//RingBuffer.publishAllWrites(output);
		
		try{
			
		    ///////
			//PUT YOUR LOGIC HERE TO CLOSE CONNECTIONS FROM THE DATABASE OR OTHER SOURCE OF INFORMATION
			//////
			
		} catch (Throwable t) {
			throw new RuntimeException(t);
		} 
	}

}

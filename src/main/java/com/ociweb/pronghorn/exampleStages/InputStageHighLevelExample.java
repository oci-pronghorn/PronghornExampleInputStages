package com.ociweb.pronghorn.exampleStages;

import static com.ociweb.pronghorn.ring.FieldReferenceOffsetManager.lookupFieldLocator;
import static com.ociweb.pronghorn.ring.FieldReferenceOffsetManager.lookupTemplateLocator;
import static com.ociweb.pronghorn.ring.RingWriter.publishWrites;
import static com.ociweb.pronghorn.ring.RingWriter.tryWriteFragment;
import static com.ociweb.pronghorn.ring.RingWriter.writeASCII;
import static com.ociweb.pronghorn.ring.RingWriter.writeDecimal;
import static com.ociweb.pronghorn.ring.RingWriter.writeLong;
import static com.ociweb.pronghorn.ring.RingWriter.writeUTF8;

import com.ociweb.pronghorn.ring.FieldReferenceOffsetManager;
import com.ociweb.pronghorn.ring.RingBuffer;
import com.ociweb.pronghorn.stage.PronghornStage;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;

public class InputStageHighLevelExample extends PronghornStage {

	private final RingBuffer output;
	private final FieldReferenceOffsetManager FROM; //Acronym so this is in all caps (this holds the schema)
	
	private final int MSG_DAILY_QUOTE;
	private final int FIELD_SYMBOL;
	private final int FIELD_COMPANY_NAME;
	
	private final int FIELD_EMPTY;
	
	private final int FIELD_HIGH_PRICE;
	private final int FIELD_LOW_PRICE;
	private final int FIELD_OPEN_PRICE;
	private final int FIELD_CLOSED_PRICE;
	private final int FIELD_VOLUME;
	
	private final int dailyMessageIdx;
		
	public static final String testSymbol = "IBM";
	public static final String testCompanyName = "International Business Machines";
	
	protected InputStageHighLevelExample(GraphManager graphManager, RingBuffer output) {
		super(graphManager, NONE, output);
		
		////////
		//STORE OTHER FIELDS THAT WILL BE REQUIRED IN STARTUP
		////////
	
		this.output = output;
		FROM = RingBuffer.from(output);
		
		this.dailyMessageIdx = FROM.messageStarts[1];
		
		//NOTE: instead of String names, the template Ids can also be used to look up the locators.
		
		MSG_DAILY_QUOTE = lookupTemplateLocator("DailyQuoteMsg",FROM);  
		
		FIELD_SYMBOL = lookupFieldLocator("Symbol", MSG_DAILY_QUOTE, FROM);
		FIELD_COMPANY_NAME = lookupFieldLocator("Company Name", MSG_DAILY_QUOTE, FROM);
		
		FIELD_EMPTY = lookupFieldLocator("Optional Additional I18N Note", MSG_DAILY_QUOTE, FROM);
		
		FIELD_HIGH_PRICE = lookupFieldLocator("High Price", MSG_DAILY_QUOTE, FROM);
		FIELD_LOW_PRICE = lookupFieldLocator("Low Price", MSG_DAILY_QUOTE, FROM);
		FIELD_OPEN_PRICE = lookupFieldLocator("Open Price", MSG_DAILY_QUOTE, FROM);
		FIELD_CLOSED_PRICE = lookupFieldLocator("Closed Price", MSG_DAILY_QUOTE, FROM);
		
		FIELD_VOLUME = lookupFieldLocator("Volume", MSG_DAILY_QUOTE, FROM);
		
		
	}
	
	
	@Override
	public void startup() {
		
		try{
						
		    ///////
			//PUT YOUR LOGIC HERE FOR CONNTECTING TO THE DATABASE OR OTHER SOURCE OF INFORMATION
			//////
			
			
					
		} catch (Throwable t) {
			throw new RuntimeException(t);
		}
		//call the super.starup() last to keep schedulers from getting too eager and starting early
		super.startup();
	}
	
	
	
	@Override
	public void run() {
		
	    int count=100;
		while (--count>=0 && tryWriteFragment(output, dailyMessageIdx)) {
			
			
			//When using this API the type of the field must match the type of method used
			//NOTE: There are plans for auto-converting type methods (if you need it please let me know)
									
			writeDecimal(output, FIELD_HIGH_PRICE,   2, 10250);
			writeDecimal(output, FIELD_LOW_PRICE,    2, 10250);
			writeDecimal(output, FIELD_OPEN_PRICE,   2, 10250);
			writeDecimal(output, FIELD_CLOSED_PRICE, 2, 10250);
			
			//All fields do not need to be written or written in order however
			// in order to build unit tests we must write every field here
			//If fields are not written however keep in mind that they will be filled with garbage.
			
			writeUTF8(output, FIELD_EMPTY, (char[])null, 0, -1);
			
			writeLong(output, FIELD_VOLUME, 10000000l);			
			
			writeASCII(output, FIELD_SYMBOL, testSymbol);
			writeASCII(output, FIELD_COMPANY_NAME, testCompanyName);
			
			
			publishWrites(output);
		}
		
		
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

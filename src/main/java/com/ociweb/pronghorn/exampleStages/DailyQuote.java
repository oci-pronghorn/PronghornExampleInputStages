package com.ociweb.pronghorn.exampleStages;

import com.ociweb.pronghorn.ring.proxy.ProngTemplateField;
import com.ociweb.pronghorn.ring.proxy.ProngTemplateMessage;

@ProngTemplateMessage(templateId=10000)
public interface DailyQuote {

	@ProngTemplateField(fieldId=4)	
	void writeSymbol(String symbol);

	@ProngTemplateField(fieldId=84)	
	void writeCompanyName(String name);
	
	@ProngTemplateField(fieldId=103)	
	void writeEmptyField(String empty);	
	
	@ProngTemplateField(fieldId=118, decimalPlaces=2)
	void writeOpenPrice(double price);
	
	@ProngTemplateField(fieldId=118, decimalPlaces=2)
	void writeHighPrice(double price);
	
	@ProngTemplateField(fieldId=118, decimalPlaces=2)
	void writeLowPrice(double price);
	
	@ProngTemplateField(fieldId=118, decimalPlaces=2)
	void writeClosedPrice(double price);
	
	@ProngTemplateField(fieldId=178)
	void writeVolume(long volume);
	

}

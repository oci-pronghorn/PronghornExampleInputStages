package com.ociweb.pronghorn.exampleStages;

import com.ociweb.pronghorn.pipe.FieldReferenceOffsetManager;
import com.ociweb.pronghorn.pipe.MessageSchema;
import com.ociweb.pronghorn.pipe.RawDataSchema;
public class ExampleSchema extends MessageSchema {

    public final static FieldReferenceOffsetManager FROM = new FieldReferenceOffsetManager(
            new int[]{0xc0400007,0xa0000000,0xa0000001,0x88000000,0xa0000002,0xb8000003,0x80000001,0xc0200007,0xc040000d,0xa0000004,0xa0000005,0xac000006,0xb0000002,0x98000000,0xb0000003,0x98000001,0xb0000004,0x98000002,0xb0000005,0x98000003,0x90000004,0xc020000d},
            (short)0,
            new String[]{"MQTTMsg","serverURI","clientid","index","topic","payload","qos",null,"DailyQuoteMsg","Symbol","Company Name","Optional Additional I18N Note","Open Price","Open Price","High Price","High Price","Low Price","Low Price","Closed Price","Closed Price","Volume",null},
            new long[]{100, 110, 111, 112, 120, 121, 122, 0, 10000, 4, 84, 103, 118, 118, 134, 134, 150, 150, 166, 166, 178, 0},
            new String[]{"global",null,null,null,null,null,null,null,"global",null,null,null,null,null,null,null,null,null,null,null,null,null},
            "exampleTemplate.xml");
    
    public static final ExampleSchema instance = new ExampleSchema();
    
    public static final int MSG_MQTTMSG_100 = 0x0;
    public static final int MSG_MQTTMSG_100_FIELD_SERVERURI_110 = 0x4000001;
    public static final int MSG_MQTTMSG_100_FIELD_CLIENTID_111 = 0x4000003;
    public static final int MSG_MQTTMSG_100_FIELD_INDEX_112 = 0x1000005;
    public static final int MSG_MQTTMSG_100_FIELD_TOPIC_120 = 0x4000006;
    public static final int MSG_MQTTMSG_100_FIELD_PAYLOAD_121 = 0x7000008;
    public static final int MSG_MQTTMSG_100_FIELD_QOS_122 = 0xa;
    public static final int MSG_DAILYQUOTEMSG_10000 = 0x8;
    public static final int MSG_DAILYQUOTEMSG_10000_FIELD_SYMBOL_4 = 0x4000001;
    public static final int MSG_DAILYQUOTEMSG_10000_FIELD_COMPANY_NAME_84 = 0x4000003;
    public static final int MSG_DAILYQUOTEMSG_10000_FIELD_OPTIONAL_ADDITIONAL_I18N_NOTE_103 = 0x5800005;
    public static final int MSG_DAILYQUOTEMSG_10000_FIELD_OPEN_PRICE_118 = 0x6000007;
    public static final int MSG_DAILYQUOTEMSG_10000_FIELD_HIGH_PRICE_134 = 0x600000a;
    public static final int MSG_DAILYQUOTEMSG_10000_FIELD_LOW_PRICE_150 = 0x600000d;
    public static final int MSG_DAILYQUOTEMSG_10000_FIELD_CLOSED_PRICE_166 = 0x6000010;
    public static final int MSG_DAILYQUOTEMSG_10000_FIELD_VOLUME_178 = 0x2000013;
    
    protected ExampleSchema() {
        super(FROM);
    }
        
}

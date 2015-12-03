package com.ociweb.pronghorn.exampleStages;

import static org.junit.Assert.assertTrue;

import org.junit.Test;

import com.ociweb.pronghorn.pipe.util.build.FROMValidation;

public class SchemaValidation {

    
    @Test
    public void groveResponseFROMTest() {
        assertTrue(FROMValidation.testForMatchingFROMs("/exampleTemplate.xml", "FROM", ExampleSchema.FROM));
        assertTrue(FROMValidation.testForMatchingLocators(ExampleSchema.instance));
    }
     
    
}

package com.ociweb.pronghorn.exampleStages;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.Test;

public class baselineTest {

	private final class DailyQuoteNode implements DailyQuote {
		
		private String symbol;
		private String companyName;
		private double openPrice;
		private double closePrice;
		private double highPrice;
		private double lowPrice;
		private long volume;
		
		@Override
		public void writeSymbol(String symbol) {
			this.symbol = symbol;
		}

		@Override
		public void writeCompanyName(String name) {
			this.companyName = name;
		}

		@Override
		public void writeEmptyField(String empty) {
			//Do nothing.
		}

		@Override
		public void writeOpenPrice(double price) {
			this.openPrice = price;
		}

		@Override
		public void writeHighPrice(double price) {
			this.highPrice = price;	
		}

		@Override
		public void writeLowPrice(double price) {
			this.lowPrice = price;
		}

		@Override
		public void writeClosedPrice(double price) {
			this.closePrice = price;
		}

		@Override
		public void writeVolume(long volume) {
			this.volume = volume;
		}

		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = prime * result + getOuterType().hashCode();
			long temp;
			temp = Double.doubleToLongBits(closePrice);
			result = prime * result + (int) (temp ^ (temp >>> 32));
			result = prime * result
					+ ((companyName == null) ? 0 : companyName.hashCode());
			temp = Double.doubleToLongBits(highPrice);
			result = prime * result + (int) (temp ^ (temp >>> 32));
			temp = Double.doubleToLongBits(lowPrice);
			result = prime * result + (int) (temp ^ (temp >>> 32));
			temp = Double.doubleToLongBits(openPrice);
			result = prime * result + (int) (temp ^ (temp >>> 32));
			result = prime * result
					+ ((symbol == null) ? 0 : symbol.hashCode());
			result = prime * result + (int) (volume ^ (volume >>> 32));
			return result;
		}

		@Override
		public boolean equals(Object obj) {
			if (this == obj) {
				return true;
			}
			if (obj == null) {
				return false;
			}
			if (getClass() != obj.getClass()) {
				return false;
			}
			DailyQuoteNode other = (DailyQuoteNode) obj;
			if (!getOuterType().equals(other.getOuterType())) {
				return false;
			}
			if (Double.doubleToLongBits(closePrice) != Double
					.doubleToLongBits(other.closePrice)) {
				return false;
			}
			if (companyName == null) {
				if (other.companyName != null) {
					return false;
				}
			} else if (!companyName.equals(other.companyName)) {
				return false;
			}
			if (Double.doubleToLongBits(highPrice) != Double
					.doubleToLongBits(other.highPrice)) {
				return false;
			}
			if (Double.doubleToLongBits(lowPrice) != Double
					.doubleToLongBits(other.lowPrice)) {
				return false;
			}
			if (Double.doubleToLongBits(openPrice) != Double
					.doubleToLongBits(other.openPrice)) {
				return false;
			}
			if (symbol == null) {
				if (other.symbol != null) {
					return false;
				}
			} else if (!symbol.equals(other.symbol)) {
				return false;
			}
			if (volume != other.volume) {
				return false;
			}
			return true;
		}

		private baselineTest getOuterType() {
			return baselineTest.this;
		}
	}

	@Test
	public void baselineTest() {
		//LinkedBlockingQueue
		//final BlockingQueue<DailyQuote> queue = new ArrayBlockingQueue<DailyQuote>(PipelineTest.messagesOnRing);		
		final BlockingQueue<DailyQuote> queue = new LinkedBlockingQueue<DailyQuote>(PipelineTest.messagesOnRing);
		final BlockingQueue<DailyQuote> queue11 = new LinkedBlockingQueue<DailyQuote>(PipelineTest.messagesOnRing);
		final BlockingQueue<DailyQuote> queue12 = new LinkedBlockingQueue<DailyQuote>(PipelineTest.messagesOnRing);
		final BlockingQueue<DailyQuote> queue121 = new LinkedBlockingQueue<DailyQuote>(PipelineTest.messagesOnRing);
		final BlockingQueue<DailyQuote> queue122 = new LinkedBlockingQueue<DailyQuote>(PipelineTest.messagesOnRing);
		
		final AtomicBoolean isLiving = new AtomicBoolean(true);
		final AtomicLong messages1 = new AtomicLong();
		final AtomicLong messages2 = new AtomicLong();
		
		final DailyQuote expected = new DailyQuoteNode();
		expected.writeSymbol(InputStageEventConsumerExample.testSymbol);
		expected.writeCompanyName(InputStageEventConsumerExample.testCompanyName);
		expected.writeHighPrice(InputStageEventConsumerExample.testHigh);
		expected.writeLowPrice(InputStageEventConsumerExample.testLow);
		expected.writeOpenPrice(InputStageEventConsumerExample.testOpen);
		expected.writeClosedPrice(InputStageEventConsumerExample.testClose);
		expected.writeVolume(InputStageEventConsumerExample.testVolume);
		
		
		Runnable generator = new Runnable() {
			
			
			@Override
			public void run() {
				while (isLiving.get()) {
					
					DailyQuote newInstance = new DailyQuoteNode();
					
					newInstance.writeSymbol(InputStageEventConsumerExample.testSymbol);
					newInstance.writeCompanyName(InputStageEventConsumerExample.testCompanyName);
					newInstance.writeHighPrice(InputStageEventConsumerExample.testHigh);
					newInstance.writeLowPrice(InputStageEventConsumerExample.testLow);
					newInstance.writeOpenPrice(InputStageEventConsumerExample.testOpen);
					newInstance.writeClosedPrice(InputStageEventConsumerExample.testClose);
					newInstance.writeVolume(InputStageEventConsumerExample.testVolume);								

					while (!queue.offer(newInstance) && isLiving.get()){
						Thread.yield();
					};
	
				}	
	//			System.out.println("exit generator");
			}			
		};
		
		Runnable splitter = new Runnable() {

			@Override
			public void run() {
				while (isLiving.get()) {
					
					while (!queue.isEmpty() && isLiving.get()) {
						DailyQuote item = queue.remove();
						
							while (!queue11.offer(item) && isLiving.get()){
								Thread.yield();
							};
							while (!queue12.offer(item) && isLiving.get()) {
								Thread.yield();
							};	
						
					}					
				}
	//			System.out.println("exit splitter");
			}			
		};
		
		Runnable dumper1 = new Runnable() {
			int count;
			
			@Override
			public void run() {
				while (isLiving.get()) {
					
					while (!queue11.isEmpty() && isLiving.get()) {
						DailyQuote item = queue11.remove();
						if (!item.equals(expected)) {
							fail("Objects no not match");
						}
						count++;
					}					
				}				
				messages1.set(count);
//				System.out.println("exit dumper1");
			}			
		};
		
		
		Runnable router = new Runnable() {

			@Override
			public void run() {
				int count = 0;
				while (isLiving.get()) {
					
					while (!queue12.isEmpty() && isLiving.get()) {
						DailyQuote item = queue12.remove();
						
						   //half one way and half the other
						   if (0==(1&count++)) {						   
							   while (!queue121.offer(item) && isLiving.get()){
								   Thread.yield();
							   }
						   } else {
							   while (!queue122.offer(item) && isLiving.get()) {
								   Thread.yield();
							   }							   
						   }	
					}					
				}
	//			System.out.println("exit router");
			}			
		};
		
		Runnable dumper21 = new Runnable() {
			int count;
			
			@Override
			public void run() {
				while (isLiving.get()) {
					
					while (!queue121.isEmpty() && isLiving.get()) {
						DailyQuote item = queue121.remove();
						if (!item.equals(expected)) {
							fail("Objects no not match");
						}
						count++;
					}					
				}				
				messages2.set(count);
	//			System.out.println("exit dumper21");
			}			
		};
		
		Runnable dumper22 = new Runnable() {
			int count;
			
			@Override
			public void run() {
				while (isLiving.get()) {
					
					while (!queue122.isEmpty() && isLiving.get()) {
						DailyQuote item = queue122.remove();
						if (!item.equals(expected)) {
							fail("Objects no not match");
						}
						count++;
					}					
				}				
				messages2.set(count);
	//			System.out.println("exit dumper22");
			}			
		};
		
		ExecutorService executor = Executors.newFixedThreadPool(6);
		
		
	    long startTime = System.currentTimeMillis();
	    executor.execute(generator);
	    executor.execute(splitter);
	    executor.execute(dumper1);	    
	    executor.execute(router);
	    executor.execute(dumper21);
	    executor.execute(dumper22);
	    
		try {
			Thread.sleep(PipelineTest.TEST_LENGTH_IN_SECONDS*1000);
		} catch (InterruptedException e) {
		}
		isLiving.set(false);
		executor.shutdown();
		try {
			//System.out.println("await shutdown");
			boolean ok = executor.awaitTermination(PipelineTest.TIMEOUT_SECONDS, TimeUnit.SECONDS);
			assertTrue(ok);
		} catch (InterruptedException e) {
			//ignore;
		}
		
		long duration = System.currentTimeMillis()-startTime;
		if (0!=duration) {
			
			System.out.println("TotalMessages:"+messages1 + 
					           " Msg/Ms:"+(messages1.get()/(float)duration) 	+ "         Baseline with BlockingQueue "+Math.abs(messages1.get()-messages2.get())				           
							  );
			System.gc();
		}
		
	}
	
	
}

package com.ociweb.pronghorn.exampleStages;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.LinkedTransferQueue;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TransferQueue;
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

	//TOOD: need to build Java8 baseline exmple and Java7 handoff
	
	@Test
	public void baselineBlockingQueueTest() {
		
		//TransferQueue<DailyQuote> xx = new LinkedTransferQueue<DailyQuote>();
		
		//LinkedBlockingQueue
		//final BlockingQueue<DailyQuote> queue = new ArrayBlockingQueue<DailyQuote>(PipelineTest.messagesOnRing);		
		final BlockingQueue<DailyQuote> queue = new ArrayBlockingQueue<DailyQuote>(PipelineTest.messagesOnRing);
		
		final BlockingQueue<DailyQuote> queue1 = new ArrayBlockingQueue<DailyQuote>(PipelineTest.messagesOnRing);
		final BlockingQueue<DailyQuote> queue11 = new ArrayBlockingQueue<DailyQuote>(PipelineTest.messagesOnRing);
		final BlockingQueue<DailyQuote> queue12 = new ArrayBlockingQueue<DailyQuote>(PipelineTest.messagesOnRing);
		
		final BlockingQueue<DailyQuote> queue2 = new ArrayBlockingQueue<DailyQuote>(PipelineTest.messagesOnRing);
		final BlockingQueue<DailyQuote> queue21 = new ArrayBlockingQueue<DailyQuote>(PipelineTest.messagesOnRing);
		final BlockingQueue<DailyQuote> queue22 = new ArrayBlockingQueue<DailyQuote>(PipelineTest.messagesOnRing);
		
		final AtomicBoolean isLiving = new AtomicBoolean(true);
		final AtomicLong messages11 = new AtomicLong();
		final AtomicLong messages12 = new AtomicLong();
		final AtomicLong messages21 = new AtomicLong();
		final AtomicLong messages22 = new AtomicLong();
		
		
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

				//To make the test same as the other tests object creation is done outside the loop.
				DailyQuote newInstance = new DailyQuoteNode();
				
				newInstance.writeSymbol(InputStageEventConsumerExample.testSymbol);
				newInstance.writeCompanyName(InputStageEventConsumerExample.testCompanyName);
				newInstance.writeHighPrice(InputStageEventConsumerExample.testHigh);
				newInstance.writeLowPrice(InputStageEventConsumerExample.testLow);
				newInstance.writeOpenPrice(InputStageEventConsumerExample.testOpen);
				newInstance.writeClosedPrice(InputStageEventConsumerExample.testClose);
				newInstance.writeVolume(InputStageEventConsumerExample.testVolume);								

				while (isLiving.get()) {				

					while (!queue.offer(newInstance) && isLiving.get()){
						Thread.yield();
					};
	
				}	
			}			
		};
		
		Runnable splitter = new Runnable() {

			@Override
			public void run() {
				while (isLiving.get()) {
					
					while (!queue.isEmpty() && isLiving.get()) {
						DailyQuote item = queue.remove();
						
							while (!queue1.offer(item) && isLiving.get()){
								Thread.yield();
							};
							while (!queue2.offer(item) && isLiving.get()) {
								Thread.yield();
							};	
						
					}					
				}
			}			
		};

		Runnable router11 = new Runnable() {

			@Override
			public void run() {
				int count = 0;
				while (isLiving.get()) {
					
					while (!queue1.isEmpty() && isLiving.get()) {
						DailyQuote item = queue1.remove();
						
						   //half one way and half the other
						   if (0==(1&count++)) {						   
							   while (!queue11.offer(item) && isLiving.get()){
								   Thread.yield();
							   }
						   } else {
							   while (!queue12.offer(item) && isLiving.get()) {
								   Thread.yield();
							   }							   
						   }	
					}					
				}
			}			
		};
		
		Runnable dumper11 = new Runnable() {
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
				messages11.set(count);
			}			
		};
		
		Runnable dumper12 = new Runnable() {
			int count;
			
			@Override
			public void run() {
				while (isLiving.get()) {
					
					while (!queue12.isEmpty() && isLiving.get()) {
						DailyQuote item = queue12.remove();
						if (!item.equals(expected)) {
							fail("Objects no not match");
						}
						count++;
					}					
				}				
				messages12.set(count);
			}			
		};
		
		
		Runnable router12 = new Runnable() {

			@Override
			public void run() {
				int count = 0;
				while (isLiving.get()) {
					
					while (!queue2.isEmpty() && isLiving.get()) {
						DailyQuote item = queue2.remove();
						
						   //half one way and half the other
						   if (0==(1&count++)) {						   
							   while (!queue21.offer(item) && isLiving.get()){
								   Thread.yield();
							   }
						   } else {
							   while (!queue22.offer(item) && isLiving.get()) {
								   Thread.yield();
							   }							   
						   }	
					}					
				}
			}			
		};
		
		Runnable dumper21 = new Runnable() {
			int count;
			
			@Override
			public void run() {
				while (isLiving.get()) {
					
					while (!queue21.isEmpty() && isLiving.get()) {
						DailyQuote item = queue21.remove();
						if (!item.equals(expected)) {
							fail("Objects no not match");
						}
						count++;
					}					
				}				
				messages21.set(count);
			}			
		};
		
		Runnable dumper22 = new Runnable() {
			int count;
			
			@Override
			public void run() {
				while (isLiving.get()) {
					
					while (!queue22.isEmpty() && isLiving.get()) {
						DailyQuote item = queue22.remove();
						if (!item.equals(expected)) {
							fail("Objects no not match");
						}
						count++;
					}					
				}				
				messages22.set(count);
			}			
		};
		
		ExecutorService executor = Executors.newFixedThreadPool(8);
		
		
	    long startTime = System.currentTimeMillis();
	    executor.execute(generator);
	    executor.execute(splitter);
	    
	    executor.execute(router11);
	    executor.execute(dumper11);
	    executor.execute(dumper12);	    
	    
	    executor.execute(router12);
	    executor.execute(dumper21);
	    executor.execute(dumper22);
	    
		try {
			Thread.sleep(PipelineTest.TEST_LENGTH_IN_SECONDS*1000);
		} catch (InterruptedException e) {
		}
		isLiving.set(false);
		executor.shutdown();
		try {
			boolean ok = executor.awaitTermination(PipelineTest.TIMEOUT_SECONDS, TimeUnit.SECONDS);
			assertTrue(ok);
		} catch (InterruptedException e) {
			//ignore;
		}
		
		long duration = System.currentTimeMillis()-startTime;
		if (0!=duration) {
			
			long totalMessages1 = messages11.get()+messages12.get();
			long totalMessages2 = messages21.get()+messages22.get();			
			
			
			System.out.println("TotalMessages:"+totalMessages1 + 
					           " Msg/Ms:"+(totalMessages1/(float)duration) 	+ "         Baseline with BlockingQueue "+totalMessages1+" vs "+totalMessages2				           
							  );
			System.gc();
		}
		
	}
	
	
	@Test
	public void baselineTransferQueueTest() {
		
		//WARNING: this test makes use of unbounded queues	
		final TransferQueue<DailyQuote> queue = new LinkedTransferQueue<DailyQuote>();
		
		final TransferQueue<DailyQuote> queue1 = new LinkedTransferQueue<DailyQuote>();
		final TransferQueue<DailyQuote> queue11 = new LinkedTransferQueue<DailyQuote>();
		final TransferQueue<DailyQuote> queue12 = new LinkedTransferQueue<DailyQuote>();
		
		final TransferQueue<DailyQuote> queue2 = new LinkedTransferQueue<DailyQuote>();
		final TransferQueue<DailyQuote> queue21 = new LinkedTransferQueue<DailyQuote>();
		final TransferQueue<DailyQuote> queue22 = new LinkedTransferQueue<DailyQuote>();
		
		final AtomicBoolean isLiving = new AtomicBoolean(true);
		final AtomicLong messages11 = new AtomicLong();
		final AtomicLong messages12 = new AtomicLong();
		final AtomicLong messages21 = new AtomicLong();
		final AtomicLong messages22 = new AtomicLong();
		
		
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

				//To make the test same as the other tests object creation is done outside the loop.
				DailyQuote newInstance = new DailyQuoteNode();
				
				newInstance.writeSymbol(InputStageEventConsumerExample.testSymbol);
				newInstance.writeCompanyName(InputStageEventConsumerExample.testCompanyName);
				newInstance.writeHighPrice(InputStageEventConsumerExample.testHigh);
				newInstance.writeLowPrice(InputStageEventConsumerExample.testLow);
				newInstance.writeOpenPrice(InputStageEventConsumerExample.testOpen);
				newInstance.writeClosedPrice(InputStageEventConsumerExample.testClose);
				newInstance.writeVolume(InputStageEventConsumerExample.testVolume);								

				try {
					while (isLiving.get()) {				
						queue.transfer(newInstance);	
					}	
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}			
		};
		
		Runnable splitter = new Runnable() {

			@Override
			public void run() {
				try {
					while (isLiving.get()) {
						DailyQuote item = queue.take();
						queue1.transfer(item);
						queue2.transfer(item);				
					}
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}			
		};

		Runnable router11 = new Runnable() {

			@Override
			public void run() {
				try {
					int count = 0;
					while (isLiving.get()) {
						
							DailyQuote item = queue1.take();
							
							   //half one way and half the other
							   if (0==(1&count++)) {	
								   queue11.transfer(item);							
							   } else {
								   queue12.transfer(item);						   
							   }	
				
					}
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}			
		};
		
		Runnable dumper11 = new Runnable() {
			int count;
			
			@Override
			public void run() {
				try {
					while (isLiving.get()) {
						
							DailyQuote item = queue11.take();
							if (!item.equals(expected)) {
								fail("Objects no not match");
							}
							count++;
									
					}				
					messages11.set(count);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}			
		};
		
		Runnable dumper12 = new Runnable() {
			int count;
			
			@Override
			public void run() {
				try{
					while (isLiving.get()) {
						
							DailyQuote item = queue12.take();
							if (!item.equals(expected)) {
								fail("Objects no not match");
							}
							count++;
				
					}				
					messages12.set(count);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}			
		};
		
		
		Runnable router12 = new Runnable() {

			@Override
			public void run() {
				try{
					int count = 0;
					while (isLiving.get()) {
						
							DailyQuote item = queue2.take();
							
							   //half one way and half the other
							   if (0==(1&count++)) {	
								   queue21.transfer(item);
							   } else {
								   queue22.transfer(item);		   
							   }	
									
					}
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}			
		};
		
		Runnable dumper21 = new Runnable() {
			int count;
			
			@Override
			public void run() {
				try{
					while (isLiving.get()) {
						
							DailyQuote item = queue21.take();
							if (!item.equals(expected)) {
								fail("Objects no not match");
							}
							count++;
						}					
								
					messages21.set(count);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}			
		};
		
		Runnable dumper22 = new Runnable() {

			@Override
			public void run() {
				try {
					while (isLiving.get()) {
						
							DailyQuote item = queue22.take();
							if (!item.equals(expected)) {
								fail("Objects no not match");
							}
							messages22.incrementAndGet();	
					}				
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}			
		};
		
		ExecutorService executor = Executors.newFixedThreadPool(8);
		
		
	    long startTime = System.currentTimeMillis();
	    executor.execute(generator);
	    executor.execute(splitter);
	    
	    executor.execute(router11);
	    executor.execute(dumper11);
	    executor.execute(dumper12);	    
	    
	    executor.execute(router12);
	    executor.execute(dumper21);
	    executor.execute(dumper22);
	    
		try {
			Thread.sleep(PipelineTest.TEST_LENGTH_IN_SECONDS*1000);
		} catch (InterruptedException e) {
		}
		isLiving.set(false);
		executor.shutdown();
		try {
			boolean ok = executor.awaitTermination(PipelineTest.TIMEOUT_SECONDS, TimeUnit.SECONDS);
			//unable to do a clean shutdown for this test so we do not bother checking
		} catch (InterruptedException e) {
			//ignore;
		}
		
		long duration = System.currentTimeMillis()-startTime;
		if (0!=duration) {
			
			long totalMessages1 = messages11.get()+messages12.get();
			long totalMessages2 = messages21.get()+messages22.get();			
			
			
			System.out.println("TotalMessages:"+totalMessages1 + 
					           " Msg/Ms:"+(totalMessages1/(float)duration) 	+ "         Baseline with TransferQueue "+totalMessages1+" vs "+totalMessages2				           
							  );
			System.gc();
		}
		
	}
	
}

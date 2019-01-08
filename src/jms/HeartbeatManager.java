package jms;

import static java.lang.System.err;
import static java.lang.System.out;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class HeartbeatManager {
	
//	private static final long MILLIS_TO_WAIT = 10 * 1000L;
//	final ExecutorService executor;
//	final Future<String> future;

	public HeartbeatManager() {

	}
	
//	public static String heartbeatReceived() {
//		return "OK";
//	}
//	
//	private void timeout() {
//		executor = Executors.newSingleThreadExecutor(); 
//		
//		future = executor.submit( HeartbeatManager::heartbeatReceived);
//
//        try
//            {
//            // where we wait for task to complete
//            final String result = future.get( MILLIS_TO_WAIT, TimeUnit.MILLISECONDS );
//            out.println( "result: " + result );
//            }
//
//        catch ( TimeoutException e )
//            {
//            err.println( "task timed out" );
//            future.cancel( true /* mayInterruptIfRunning */ );
//            }
//
//        catch ( InterruptedException e )
//            {
//            err.println( "task interrupted" );
//            }
//
//        catch ( ExecutionException e )
//            {
//            err.println( "task aborted" );
//            }
//
//        executor.shutdownNow();
//	}

}

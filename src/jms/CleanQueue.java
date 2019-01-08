package jms;

import java.util.Hashtable;

import javax.jms.Connection;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.QueueConnection;
import javax.jms.QueueConnectionFactory;
import javax.jms.QueueReceiver;
import javax.jms.QueueSession;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;

import com.sun.messaging.ConnectionConfiguration;
import com.sun.messaging.ConnectionFactory;
 
public class CleanQueue implements MessageListener {
	
	private String ID = "0";
	private String queueName;
	
	private ConnectionFactory myConnFactory;
    private Connection myConn;
    private com.sun.messaging.Queue myQueue;
    private MessageConsumer receiver;
    private MessageProducer sender;
    private Session mySess; 
    
    // callback when the message exist in the queue
    public void onMessage(Message msg) {
        try {
            String msgText;
            if (msg instanceof TextMessage) {
                msgText = ((TextMessage) msg).getText();
            } else {
                msgText = msg.toString();
            }
            System.out.println("Message Received: " + msgText);
        } catch (JMSException jmse) {
            System.err.println("An exception occurred: " + jmse.getMessage());
        }
    }
    
    public CleanQueue(String queueName, String id) {
    	this.queueName = queueName;
    	this.ID = id;
    	try {
			init(true);
		} catch (NamingException | JMSException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    }
    
    public CleanQueue(String queueName) {
    	this.queueName = queueName;
    	try {
			init(false);
		} catch (NamingException | JMSException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    }
    
    public CleanQueue() {
    	try {
			init(false);
		} catch (NamingException | JMSException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    }
    
    
    // create a connection to the WLS using a JNDI context
    public void init(Boolean id)
            throws NamingException, JMSException {

    	myConnFactory = new ConnectionFactory();
        myConnFactory.setProperty(ConnectionConfiguration.imqAddressList, "localhost");
        myConn = myConnFactory.createConnection();
        mySess = myConn.createSession(false, Session.AUTO_ACKNOWLEDGE);
        myQueue = new com.sun.messaging.Queue(queueName);
        
        if(id) receiver = mySess.createConsumer(myQueue, "ID='" + this.ID + "'");
        else receiver = mySess.createConsumer(myQueue); 
        receiver.setMessageListener(this);
               
        myConn.start();
    }
    
    // close sender, connection and the session
    public void close() throws JMSException {
        receiver.close();
        mySess.close();
        myConn.close();
    }
    
    // start receiving messages from the queue
    public void receive() throws Exception {
        
        
        //System.out.println("Connected to " + queue.toString() + ", receiving messages...");
        try {
            synchronized (this) {
                while (true) {
                    this.wait();
                }
            }
        } finally {
            close();
            System.out.println("Finished.");
        }
    }
    
    public static void main(String[] args) throws Exception {
    	CleanQueue cleaner;
    	if(args.length == 2)
    		cleaner = new CleanQueue(args[0], args[1]);
    	else if(args.length == 1)
    		cleaner = new CleanQueue(args[0]);
    	else
    		return;
    	cleaner.receive();
    	
    }
}
package jms;

import java.util.Timer;
import java.util.TimerTask;

import javax.jms.Connection;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.naming.NamingException;

import com.sun.messaging.ConnectionConfiguration;
import com.sun.messaging.ConnectionFactory;

public class Node implements NodeInterface, MessageListener {
	
	private String queueName = "dsv";
	
	// a message that will be sent to the queue
    private TextMessage textMsg;
    
    private String ID;
    
    private String nextNode;
    
    private String previousID;
    
    
    private ConnectionFactory myConnFactory;
    private Connection myConn;
    private com.sun.messaging.Queue myQueue;
    private MessageConsumer receiver;
    private MessageProducer sender;
    private Session mySess;
	
	public Node(String id, String previousId) {
		this.ID = id;
		this.nextNode = id;
		this.previousID = previousId;
		
		try {
			init();
		} catch (NamingException | JMSException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
	
	
	@Override
	public void onMessage(Message msg) {
		try {
            String msgText;
            if (msg instanceof TextMessage) {
                msgText = ((TextMessage) msg).getText();
                
                // update nextNode
                if( msg.propertyExists("LOGIN") && msg.getBooleanProperty("LOGIN")) {
                	System.out.println("LOGIN INFO RECEIVED: " + msgText);
                	
                	String nextNodeOld = this.nextNode;
                	
                	this.nextNode = msgText;
                	
                	// Update NEXTNODE info in the new node
                	textMsg.clearProperties();
                	textMsg.setObjectProperty("ID", this.nextNode);
                	textMsg.setBooleanProperty("NEXTNODE", true);
                	this.textMsg.setText(nextNodeOld);
                	sender.send(this.textMsg);
                	
                	// Updating PREVIOUSNODE info in the previous NEXTNODE
                	if(this.ID == nextNodeOld) this.previousID = this.nextNode;
                	else {
	                	textMsg.clearProperties();
	                	textMsg.setObjectProperty("ID", nextNodeOld);
	                	textMsg.setBooleanProperty("PREVIOUSNODE", true);
	                	textMsg.setText(this.nextNode);
	                	sender.send(textMsg);
                	}
                	
                }
                else if(msg.propertyExists("NEXTNODE") && msg.getBooleanProperty("NEXTNODE")) {
                	this.nextNode = msgText;
                	System.out.println("NEXTNODE NEXTNODE UPDATED: " + this.nextNode);
                }
                else if(msg.propertyExists("PREVIOUSNODE") && msg.getBooleanProperty("PREVIOUSNODE")) {
                	this.previousID = msgText;
                }
                
            }
		} catch(Exception e) {
			e.printStackTrace();
			System.exit(1);
		}
                	
	}

	@Override
	public void login(String id) {
		System.out.println("SENDING LOGIN INFO TO NODE " + id);
		try {
			textMsg.clearProperties();
			textMsg.setObjectProperty("ID", id);
			textMsg.setBooleanProperty("LOGIN", true);
			textMsg.setText(this.ID);
			sender.send(textMsg);
			System.out.println("Login Message sent!");
		} catch (JMSException e) {
			e.printStackTrace();
			System.exit(1);
		}
	}

	@Override
	public void logout() {
		if(this.ID != this.nextNode && this.ID != this.previousID) {
			try {
				System.out.println("SENDING LOGOUT TO " + this.previousID + " (previousID) AND " + this.nextNode + " (nextNode)");
				textMsg.clearProperties();
				textMsg.setObjectProperty("ID", this.previousID);
				textMsg.setBooleanProperty("NEXTNODE", true);
				textMsg.setText(this.nextNode);
				sender.send(textMsg);
				textMsg.clearProperties();
				textMsg.setObjectProperty("ID", this.nextNode);
				textMsg.setBooleanProperty("PREVIOUSNODE", true);
				textMsg.setText(this.previousID);
				sender.send(textMsg);
			}
			catch(Exception e) {
				e.printStackTrace();
			}
		}

	}
	
	
	
    // create a connection to the WLS using a JNDI context
    public void init()
            throws NamingException, JMSException {        
       
        myConnFactory = new ConnectionFactory();
        myConnFactory.setProperty(ConnectionConfiguration.imqAddressList, "192.168.56.3:7676");
        myConn = myConnFactory.createConnection();
        mySess = myConn.createSession(false, Session.AUTO_ACKNOWLEDGE);
        myQueue = new com.sun.messaging.Queue(queueName);
        
        receiver = mySess.createConsumer(myQueue, "ID='" + this.ID + "'");
        receiver.setMessageListener(this);
        
        sender = mySess.createProducer(myQueue);
        textMsg = mySess.createTextMessage();
        
        myConn.start();
                
    }
    
    // close sender, connection and the session
    public void close() throws JMSException {
        receiver.close();
        sender.close();
        mySess.close();
        myConn.close();
    }
	
    // start receiving messages from the queue
    public void receive() throws Exception {
                
        //System.out.println("Connected to " + myQueue.toString() + ", receiving messages...");
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
		Node node;
		
		if(args.length > 1) {
        	node = new Node(args[0], args[1]);
        	node.login(args[1]);
        }
        else {
        	node = new Node(args[0], args[0]);
        }
		
		// Logout when the process ends
		Runtime.getRuntime().addShutdownHook(new Thread() {
        	public void run() {
        		System.out.println("Initiating shutdown protocol!");
        		node.logout();
        		System.out.println("System exited!");
        	}
        });
		
		Timer t = new Timer();
        
        t.schedule(new TimerTask() {
        	@Override
			public void run() {
        		System.out.println();
        		System.out.println("ID: " + node.ID);
        		System.out.println("NextNode: " + node.nextNode );
        		System.out.println("PreviousNode: " + node.previousID);
        		System.out.println("=============");
        	}
        }, 0, 5000);
		
		node.receive();
        
	}
	
	

}

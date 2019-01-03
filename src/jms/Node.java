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
 
public class Node implements MessageListener, NodeInterface {
        
    // a message that will be sent to the queue
    private TextMessage textMsg;
    
    private String ID;
    
    private String nextNode;
    
    private String previousID;
    
    
    private ConnectionFactory myConnFactory;
    private Connection myConn;
    private com.sun.messaging.Queue myQueue;
    private MessageConsumer receiver;
    private static MessageProducer sender;
    private Session mySess;

    
        
    public Node(String id, String previousID, String queueName) {
		this.ID = id;
		this.nextNode = id;
		this.previousID = previousID;
		
		System.out.println("ID = " + this.ID);
		System.out.println("Previous ID = " + this.previousID);
		
		try {
			init(queueName);
		} catch (NamingException | JMSException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}

	// callback when the message exist in the queue
    @Override
	public void onMessage(Message msg) {
        try {
            String msgText;
            if (msg instanceof TextMessage) {
                msgText = ((TextMessage) msg).getText();
                
                // update nextNode
                if( msg.propertyExists("LOGIN") && ((String) msg.getObjectProperty("LOGIN")).equals("true") ) {
                	String aux = this.nextNode;
                	// clear old properties and send new message
                	System.out.println("LOGIN MESSAGE RECEIVED FROM "+ msgText);
                	
                	this.nextNode = msgText;
                	System.out.println("LOGIN NEXTNODE UPDATED: " + this.nextNode);
                	System.out.println("LOGIN AUX: " + aux);
                	
                	textMsg.clearProperties();
                	textMsg.setObjectProperty("ID", this.nextNode);
                	textMsg.setObjectProperty("NextNode", "true");
                	this.textMsg.setText(aux);
                	// send message
                	Node.sender.send(this.textMsg);
                	
                }
                
                //update NextNode after login message sent
                else if(msg.propertyExists("NextNode") && ((String) msg.getObjectProperty("NextNode")).equals("true") ) {
                	this.nextNode = msgText;
                	System.out.println("NEXTNODE NEXTNODE UPDATED: " + this.nextNode);
                	System.out.println("SENDING NEW INFO TO NODE " + msgText);
                	
                	textMsg.clearProperties();
                	textMsg.setObjectProperty("ID", msgText);
                	textMsg.setObjectProperty("PreviousNode", "true");
                	this.textMsg.setText(this.ID);
                	// send message
                	Node.sender.send(this.textMsg);
                }
                
                else if(msg.propertyExists("LOGOUT") && ((String) msg.getObjectProperty("LOGOUT")).equals("true")) {
                	this.nextNode = msgText;
                	System.out.println("LOGOUT\nNextNode Connected to: " + this.nextNode);
                }
                
                else if(msg.propertyExists("PreviousNode") && ((String) msg.getObjectProperty("PreviousNode")).equals("true")) {
                	this.previousID = msgText;
                }
                
            } else {
                msgText = msg.toString();
            }
//            System.out.println("Message Received: " + msgText);
        } catch (JMSException jmse) {
            System.err.println("An exception occurred: " + jmse.getMessage());
        }
        
        
    }
    
    // create a connection to the WLS using a JNDI context
    public void init(String queueName)
            throws NamingException, JMSException {        
       
        myConnFactory = new ConnectionFactory();
        myConnFactory.setProperty(ConnectionConfiguration.imqAddressList, "192.168.56.3:7676");
        myConn = myConnFactory.createConnection();
        mySess = myConn.createSession(false, Session.AUTO_ACKNOWLEDGE);
        myQueue = new com.sun.messaging.Queue(queueName);
        
        receiver = mySess.createConsumer(myQueue);
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
    public void receive(String queueName) throws Exception {
        
        textMsg.setObjectProperty("ID", "123");
        
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
    
    
    // Send this ID to previous Node so it can connect to this node
	public void login(String id) {
		System.out.println("SENDING LOGIN INFO TO NODE " + id);
		try {
			textMsg.clearProperties();
			textMsg.setObjectProperty("ID", id);
			textMsg.setObjectProperty("LOGIN", "true");
			textMsg.setText(this.ID);
			sender.send(textMsg);
			System.out.println("Login Message sent!");
		} catch (JMSException e) {
			e.printStackTrace();
			System.exit(1);
		}
	}
	
	public void logout() {
		
		if(this.ID != this.nextNode && this.ID != this.previousID) {
			try {
				System.out.println("SENDING LOGOUT TO " + this.previousID + " (previousID) AND " + this.nextNode + " (nextNode)");
				textMsg.clearProperties();
				textMsg.setObjectProperty("ID", this.previousID);
				textMsg.setObjectProperty("LOGOUT", "true");
				textMsg.setText(this.nextNode);
				sender.send(textMsg);
				textMsg.clearProperties();
				textMsg.setObjectProperty("ID", this.nextNode);
				textMsg.setObjectProperty("PreviousNode", "true");
				textMsg.setText(this.previousID);
				sender.send(textMsg);
				System.out.println("Message sent!");
			}
			catch(Exception e) {
				e.printStackTrace();
			}
		}
		
	}
	
	public String getNextNode() {
		return nextNode;
	}
	
	
	public static void main(String[] args) throws Exception {
        // input arguments
        String queueName = "dsv";  
        // create the producer object and receive the message      
        Node node;
        
        if(args.length > 1) {
        	node = new Node(args[0], args[1], queueName);
        	node.login(args[1]);
        }
        else {
        	node = new Node(args[0], "-1", queueName);
        }
        
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
        		System.out.println("NextNode: " + node.getNextNode() );
        		System.out.println("PreviousNode: " + node.previousID);
        		System.out.println("=============");
        	}
        }, 0, 5000);
        
//        t.schedule(new TimerTask() {
//        	public void run() {
//        		try {
//        			textMsg.clearProperties();
//        			textMsg.setObjectProperty("ID", node.getNextNode());
//        			textMsg.setText("MESSAGE SENT BY NODE " + node.ID);
//        			sender.send(textMsg);
//        		}
//        		catch(Exception e) {
//        			System.exit(1);
//        		}
//        	}
//        }, 0, 3000);
        
        node.receive(queueName);
    }
	
	protected Object getId() {
		// TODO Auto-generated method stub
		return null;
	}

}
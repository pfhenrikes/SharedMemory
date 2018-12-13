package jms;

import java.util.Hashtable;
import java.util.Timer;
import java.util.TimerTask;

import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageListener;
import javax.jms.Queue;
import javax.jms.QueueConnection;
import javax.jms.QueueConnectionFactory;
import javax.jms.QueueReceiver;
import javax.jms.QueueSender;
import javax.jms.QueueSession;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;
 
public class Node implements MessageListener, NodeInterface {

    // connection factory
    private QueueConnectionFactory qconFactory;
    
    // connection to a queue
    private QueueConnection qcon;
    
    // session within a connection
    private QueueSession qsession;
    
    // queue receiver that receives a message to the queue
    private QueueReceiver qreceiver;
    
    // queue sender that sends a message to the queue
    private QueueSender qsender;
    
    // queue where the message will be sent to
    private Queue queue;
    
    // a message that will be sent to the queue
    private TextMessage textMsg;
    
    private String ID;
    
    private String nextNode;
    
    private String previousID;
        
    public Node(String id, String previousID, String queueName) {
		this.ID = id;
		this.nextNode = id;
		this.previousID = previousID;
		
		Hashtable<String, String> env = new Hashtable<String, String>();
        env.put(Context.INITIAL_CONTEXT_FACTORY, Config.JNDI_FACTORY);
        env.put(Context.PROVIDER_URL, Config.PROVIDER_URL);

        InitialContext ic;
		try {
			ic = new InitialContext(env);
			init(ic, queueName);
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
                	// clear old properties and send new message
                	textMsg.clearProperties();
                	textMsg.setObjectProperty("ID", msgText);
                	textMsg.setObjectProperty("NextNode", "true");
                	this.textMsg.setText(this.nextNode);
                	
                	// send message
                	this.qsender.send(this.textMsg);
                	System.out.println("MY ID WAS SENT!");
                	this.nextNode = msgText;
                	System.out.println("Connected to: " + this.nextNode);
                }
                
                //update NextNode after login message sent
                else if(msg.propertyExists("NextNode") && ((String) msg.getObjectProperty("NextNode")).equals("true") ) {
                	this.nextNode = msgText;
                	System.out.println("NextCode Connected to: " + this.nextNode);
                }
                
                else if(msg.propertyExists("LOGOUT") && ((String) msg.getObjectProperty("LOGOUT")).equals("true")) {
                	this.nextNode = msgText;
                	System.out.println("NextCode Connected to: " + this.nextNode);
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
    public void init(Context ctx, String queueName)
            throws NamingException, JMSException {

        qconFactory = (QueueConnectionFactory) ctx.lookup(Config.JMS_FACTORY);
        qcon = qconFactory.createQueueConnection();
        qsession = qcon.createQueueSession(false, Session.AUTO_ACKNOWLEDGE);
        queue = (Queue) ctx.lookup(queueName);

        qreceiver = qsession.createReceiver(queue, "ID = '" + this.ID + "'");
        qreceiver.setMessageListener(this); 
       
        qsender = qsession.createSender(queue);
        textMsg = qsession.createTextMessage();
                
        qcon.start();
    }
    
    // close sender, connection and the session
    public void close() throws JMSException {
        qreceiver.close();
        qsession.close();
        qcon.close();
    }
    
    // start receiving messages from the queue
    public void receive(String queueName) throws Exception {
        
        textMsg.setObjectProperty("ID", "123");
        
        System.out.println("Connected to " + queue.toString() + ", receiving messages...");
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
    
    

	public void login(String id) {
		System.out.println("Logging into " + id);
		try {
			textMsg.clearProperties();
			textMsg.setObjectProperty("ID", id);
			textMsg.setObjectProperty("LOGIN", "true");
			textMsg.setText(this.ID);
			qsender.send(textMsg);
			System.out.println("Message sent!");
		} catch (JMSException e) {
			e.printStackTrace();
			System.exit(1);
		}
	}
	
	public void logout() {
		
		if(this.ID != this.nextNode && this.ID != this.previousID) {
			try {
				textMsg.clearProperties();
				textMsg.setObjectProperty("ID", this.previousID);
				textMsg.setObjectProperty("LOGOUT", "true");
				textMsg.setText(this.nextNode);
				qsender.send(textMsg);
				textMsg.clearProperties();
				textMsg.setObjectProperty("ID", this.nextNode);
				textMsg.setObjectProperty("PreviousNode", "true");
				textMsg.setText(this.previousID);
				qsender.send(textMsg);
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
        String queueName = "jms/dsv-election" ;
            
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
        		System.out.println("NextNode: " + node.getNextNode() );
        	}
        }, 0, 5000);
        
        node.receive(queueName);
    }
}
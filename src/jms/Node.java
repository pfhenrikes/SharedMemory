package jms;

import java.util.Timer;
import java.util.TimerTask;

import javax.jms.Connection;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.MessageProducer;
import javax.jms.ObjectMessage;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.naming.NamingException;

import com.sun.messaging.ConnectionConfiguration;
import com.sun.messaging.ConnectionFactory;

public class Node implements NodeInterface, MessageListener {
	
	private String queueName = "dsv";
	
	// a message that will be sent to the queue
    private TextMessage textMsg;
    private ObjectMessage objectMsg;
    
    
    private int ID;
    
    private int nextNode;
    
    private int previousID;
    
    
    // Election and Leader
    private Boolean isParticipant = false;
    private int leaderId;
    
    
    private ConnectionFactory myConnFactory;
    private Connection myConn;
    private com.sun.messaging.Queue myQueue;
    private MessageConsumer receiver;
    private MessageProducer sender;
    private Session mySess;
	
	public Node(int id, int previousId, int leaderId) {
		this.ID = id;
		this.nextNode = id;
		this.previousID = previousId;
		this.leaderId = leaderId;
		
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
                	
                	int nextNodeOld = this.nextNode;
                	
                	this.nextNode = Integer.parseInt(msgText);
                	
                	LoginDataResponse data = new LoginDataResponse(nextNodeOld, this.leaderId);
                	
                	// Update NEXTNODE info in the new node
                	objectMsg.clearProperties();
                	objectMsg.setObjectProperty("ID", Integer.toString(this.nextNode));
                	objectMsg.setBooleanProperty("NEXTNODE", true);
                	objectMsg.setObject(data);
                	sender.send(objectMsg);
                	
                	
                	// ALTERAR ALTERAR !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
                	// CIRCULO UNIDIRECIONAL
                	
                	// Updating PREVIOUSNODE info in the previous NEXTNODE
                	if(this.ID == nextNodeOld) this.previousID = this.nextNode;
                	else {
	                	textMsg.clearProperties();
	                	textMsg.setObjectProperty("ID", Integer.toString(nextNodeOld));
	                	textMsg.setBooleanProperty("PREVIOUSNODE", true);
	                	textMsg.setText(Integer.toString(this.nextNode));
	                	sender.send(textMsg);
                	}
                	
                }
                else if(msg.propertyExists("NEXTNODE") && msg.getBooleanProperty("NEXTNODE")) {
                	this.nextNode = Integer.parseInt(msgText);
                	System.out.println("NEXTNODE NEXTNODE UPDATED: " + this.nextNode);
                }
                else if(msg.propertyExists("PREVIOUSNODE") && msg.getBooleanProperty("PREVIOUSNODE")) {
                	this.previousID = Integer.parseInt(msgText);
                }
                
                else if(msg.propertyExists("LOGOUT") && msg.getBooleanProperty("LOGOUT")) {
                	this.previousID = Integer.parseInt(msgText);
                	if(Integer.parseInt(msg.getStringProperty("ORIGINID")) == this.leaderId) {
                		this.leaderId = -1;
                		if(this.ID != this.nextNode) {
                			sendElection(this.ID);
                			System.out.println("ELECTION IS IN PROGRESS");
                		}
                		else {
                			this.leaderId = this.ID;
                			System.out.println("ELECTION NOT NEEDED");
                		}
                	}
                }
                
                // Election process
                else if(msg.propertyExists("ELECTION") && msg.getBooleanProperty("ELECTION")) {
                	System.out.println("ELECTION MESSAGE RECEIVED");
                	int idMsg = Integer.parseInt(msgText);
                	System.out.println("PROPOSED LEADER: " + idMsg);
                	if(idMsg > this.ID) {
                		System.out.println("SENDING MESSAGE WITHOUT CHANGING ID");
                		sendElection(idMsg);
                		this.isParticipant = true;
                	}
                	else if(idMsg < this.ID && !this.isParticipant) {
                		System.out.println("SENDING MY ID AS LEADER");
                		sendElection(this.ID);
                		this.isParticipant = true;
                	}
                	else if(idMsg < this.ID && this.isParticipant) {
                		System.out.println("ELECTION MESSAGE DISCARDED!");
                	}
                	else {
                		System.out.println("IM THE LEADER");
                		this.leaderId = this.ID;
                		sendElectionFinished();
                		System.out.println("ENDING ELECTION");
                	}
                }
                
                else if(msg.propertyExists("LEADER") && msg.getBooleanProperty("LEADER")) {
                	System.out.println("LEADER MESSAGE RECEIVED");
                	int leader = Integer.parseInt(msgText);
                	if(this.leaderId == leader) System.out.println("ELECTION IS OVER");
                	else {
	                	this.leaderId = Integer.parseInt(msgText);
	                	sendElectionFinished();
                	}
                }
                
                
            }
            
            else if(msg instanceof ObjectMessage) {
            	LoginDataResponse data = (LoginDataResponse) ((ObjectMessage) msg).getObject();
            	
            	this.nextNode = data.getNextNode();
            	this.leaderId = data.getLeaderId();
            	
            }
            
            
		} catch(Exception e) {
			e.printStackTrace();
			System.exit(1);
		}
                	
	}
	
	public void sendElectionFinished() {
		try {
			this.isParticipant = false;
			textMsg.clearProperties();
			textMsg.setObjectProperty("ID", Integer.toString(this.nextNode));
			textMsg.setBooleanProperty("LEADER", true);
			textMsg.setText(Integer.toString(this.leaderId));
			sender.send(textMsg);
		} catch(JMSException e) {
			e.printStackTrace();
			System.exit(1);
		}
	}
	
	public void sendElection(int idLeader) {
		try {
			this.isParticipant = true;
			textMsg.clearProperties();
			textMsg.setObjectProperty("ID", Integer.toString(this.nextNode));
			textMsg.setBooleanProperty("ELECTION", true);
			textMsg.setText(Integer.toString(idLeader));
			sender.send(textMsg);
		} catch(JMSException e) {
			e.printStackTrace();
			System.exit(1);
		}
	}
	

	@Override
	public void login(int arg1) {
		System.out.println("SENDING LOGIN INFO TO NODE " + arg1);
		try {
			textMsg.clearProperties();
			textMsg.setObjectProperty("ID", Integer.toString(arg1));
			textMsg.setBooleanProperty("LOGIN", true);
			textMsg.setText(Integer.toString(this.ID));
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
				textMsg.setObjectProperty("ID", Integer.toString(this.previousID));
				textMsg.setBooleanProperty("NEXTNODE", true);
				textMsg.setText(Integer.toString(this.nextNode));
				sender.send(textMsg);
				
				
				textMsg.clearProperties();
				textMsg.setObjectProperty("ID", Integer.toString(this.nextNode));
				textMsg.setBooleanProperty("LOGOUT", true);
				textMsg.setStringProperty("ORIGINID", Integer.toString(this.ID));
				textMsg.setText(Integer.toString(this.nextNode));
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
        objectMsg = mySess.createObjectMessage();
        
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
        	int arg0 = Integer.parseInt(args[0]);
        	int arg1 = Integer.parseInt(args[1]);
			
			node = new Node(arg0, arg1, -1);
        	node.login(arg1);
        }
        else {
        	int arg0 = Integer.parseInt(args[0]);
        	node = new Node(arg0, arg0, arg0);
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
        		System.out.println("LeaderId: " + node.leaderId);
        		System.out.println("=============");
        	}
        }, 0, 5000);
		       
		node.receive();
        
	}
	
	

}

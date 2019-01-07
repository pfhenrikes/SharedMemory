package jms;

import java.util.LinkedList;
import java.util.Random;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.CountDownLatch;

import javax.jms.Connection;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.MessageProducer;
import javax.jms.ObjectMessage;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.naming.NamingException;

import com.sun.messaging.ConnectionConfiguration;
import com.sun.messaging.ConnectionFactory;

public class Node implements NodeInterface, MessageListener {
	
	private String queueName = "dsv";
	private String leaderQueueName = "dsvLeader";
	
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
    private Queue myQueue;
    private Queue myQueueLeader;
    private MessageConsumer receiver;
    private MessageProducer sender;
    private Session mySess;
	
    private MessageProducer senderLeader;
    private MessageConsumer receiverLeader;
    
    
    private java.util.Queue<Integer> criticalAccessFIFO = new LinkedList<Integer>();
    private Boolean criticalAccessInUse = false;
    
    
    private CountDownLatch continueSignal = new CountDownLatch(1);
    
    
    // ################
    private SharedVariable sharedVariable = new SharedVariable(0,0);
    
    
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
            // If msg is a TextMessage
            if (msg instanceof TextMessage) {
                msgText = ((TextMessage) msg).getText();
                
                // update nextNode
                if( msg.propertyExists("LOGIN") && msg.getBooleanProperty("LOGIN")) {
                	System.out.println("LOGIN INFO RECEIVED: " + msgText);
                	
                	int nextNodeOld = this.nextNode;
                	
                	this.nextNode = Integer.parseInt(msgText);
                	
                	LoginDataResponse data = new LoginDataResponse(nextNodeOld, this.leaderId);
                	
                	// Update NEXTNODE info in the new node
                	synchronized(objectMsg) {
	                	objectMsg.clearProperties();
	                	objectMsg.setObjectProperty("ID", Integer.toString(this.nextNode));
	                	objectMsg.setBooleanProperty("NEXTNODE", true);
	                	objectMsg.setObject(data);
	                	sender.send(objectMsg);
                	}
                	
                	
                	// ALTERAR ALTERAR !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
                	// CIRCULO UNIDIRECIONAL
                	
                	// Updating PREVIOUSNODE info in the previous NEXTNODE
                	if(this.ID == nextNodeOld) this.previousID = this.nextNode;
                	else {
                		synchronized(textMsg) {
	                		textMsg.clearProperties();
		                	textMsg.setObjectProperty("ID", Integer.toString(nextNodeOld));
		                	textMsg.setBooleanProperty("PREVIOUSNODE", true);
		                	textMsg.setText(Integer.toString(this.nextNode));
		                	sender.send(textMsg);
                		}
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
                
                else if(msg.propertyExists("REQUEST") && msg.getBooleanProperty("REQUEST")) {
                	
    				System.out.println("RECEIVED: " + msgText);
    				
    				Destination replyQueue = msg.getJMSReplyTo();
    				
    				MessageProducer tempProducer = mySess.createProducer(replyQueue);
    				
    				synchronized(objectMsg) {
	    				objectMsg.clearProperties();
	    				objectMsg.setJMSDestination(replyQueue);
	    				objectMsg.setJMSCorrelationID(msg.getJMSCorrelationID());
	    				objectMsg.setObject(sharedVariable);
    				
	    				//Thread.sleep(3000);
	    				
	    				tempProducer.send(objectMsg);
    				}
    				
    				tempProducer.close();
                }
                
                else if(msg.propertyExists("PERMISSION") && msg.getBooleanProperty("PERMISSION")) {
                	if(criticalAccessInUse) {
                		criticalAccessFIFO.add(Integer.parseInt(msgText));
                	}
                	else {
                		this.criticalAccessInUse = true;
                		sendPermissionGranted(Integer.parseInt(msgText));
                		
                	}
                }
                
                else if(msg.propertyExists("RELEASED") && msg.getBooleanProperty("RELEASED")) {
                	if(!criticalAccessFIFO.isEmpty()) {
                		int id = criticalAccessFIFO.remove();
                		sendPermissionGranted(id);
                	}
                	else {
                		criticalAccessInUse = false;
                	}
                }
                
                
                
            }
            
            // If a message is ObjectMessage
            else if(msg instanceof ObjectMessage) {
            	
            	if(msg.propertyExists("NEXTNODE") && msg.getBooleanProperty("NEXTNODE")) {
	            	
	            	LoginDataResponse data = (LoginDataResponse) ((ObjectMessage) msg).getObject();
	            	
	            	this.nextNode = data.getNextNode();
	            	this.leaderId = data.getLeaderId();
            	}
            	
            	else if(msg.propertyExists("WRITE") && msg.getBooleanProperty("WRITE")) {
            		SharedVariable variable = (SharedVariable) ((ObjectMessage) msg).getObject();
            		
            		System.out.println("WRITE REQUEST - NUMBER: " + variable.getNumber() + " ID: "+variable.getId());
            		
            		if(sharedVariable.getId() < variable.getId()) {
            			updateSharedVariable(variable.getNumber(), variable.getId());
            		}
            		
            		System.out.println("PROPAGATING CHANGES");
            		propagateWrite();
            		
            	}
            	
            	else if(msg.propertyExists("UPDATEMEMORY") && msg.getBooleanProperty("UPDATEMEMORY")) {
            		if(this.ID != this.leaderId) {
            			System.out.println("UPDATING SHARED VARIABLE");
            			SharedVariable variable = (SharedVariable) ((ObjectMessage)msg).getObject();
            			updateSharedVariable(variable.getNumber(), variable.getId());
            			propagateWrite();
            			System.out.println("NUMBER: " + sharedVariable.getNumber() + " ID: " + sharedVariable.getId());
            		}
            		else {
            			System.out.println("PROPAGATE FINISHED");
            		}
            	}
            	
            	else if(msg.propertyExists("GRANTED") && msg.getBooleanProperty("GRANTED")) {
            		SharedVariable variable = (SharedVariable) ((ObjectMessage)msg).getObject();
            		if(sharedVariable.getId() < variable.getId()) {
            			updateSharedVariable(variable.getNumber(), variable.getId());
            		}
            		
            		// Notify sleeping working thread that the write request was accepted
            		continueSignal.countDown();
            		
            	}
            	
            }
            
            
		} catch(Exception e) {
			e.printStackTrace();
			System.exit(1);
		}
                	
	}
	
	private void sendPermissionGranted(int id) {
		try {
			synchronized(objectMsg) {
				objectMsg.clearProperties();
				objectMsg.setBooleanProperty("GRANTED", true);
				objectMsg.setStringProperty("ID", Integer.toString(id));
				objectMsg.setObject(sharedVariable);
				sender.send(objectMsg);
			}
		} catch (JMSException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}


	// send modified variable to every node
	private void propagateWrite() {
		try {
			synchronized(objectMsg) {
				objectMsg.clearProperties();
				objectMsg.setBooleanProperty("UPDATEMEMORY", true);
				objectMsg.setStringProperty("ID", Integer.toString(this.nextNode));
				objectMsg.setObject(sharedVariable);
				
				sender.send(objectMsg);
			}
			
		} catch (JMSException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
	}


	public void sendElectionFinished() {
		try {
			this.isParticipant = false;
			synchronized(textMsg) {
				textMsg.clearProperties();
				textMsg.setObjectProperty("ID", Integer.toString(this.nextNode));
				textMsg.setBooleanProperty("LEADER", true);
				textMsg.setText(Integer.toString(this.leaderId));
				sender.send(textMsg);
			}
			
			//starts listening to the leaderQueue
			receiverLeader = mySess.createConsumer(myQueueLeader);
			receiverLeader.setMessageListener(this);
			
		} catch(JMSException e) {
			e.printStackTrace();
			System.exit(1);
		}
	}
	
	public void sendElection(int idLeader) {
		try {
			this.isParticipant = true;
			synchronized(textMsg) {
				textMsg.clearProperties();
				textMsg.setObjectProperty("ID", Integer.toString(this.nextNode));
				textMsg.setBooleanProperty("ELECTION", true);
				textMsg.setText(Integer.toString(idLeader));
				sender.send(textMsg);
			}
			
		} catch(JMSException e) {
			e.printStackTrace();
			System.exit(1);
		}
	}

	@Override
	public void login(int arg1) {
		System.out.println("SENDING LOGIN INFO TO NODE " + arg1);
		try {
			synchronized(textMsg) {
				textMsg.clearProperties();
				textMsg.setObjectProperty("ID", Integer.toString(arg1));
				textMsg.setBooleanProperty("LOGIN", true);
				textMsg.setText(Integer.toString(this.ID));
				sender.send(textMsg);
			}
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
				synchronized(textMsg) {
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
				
				close();
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
        myQueueLeader = new com.sun.messaging.Queue(leaderQueueName);
        
        receiver = mySess.createConsumer(myQueue, "ID='" + this.ID + "'");
        receiver.setMessageListener(this);
        
        sender = mySess.createProducer(myQueue);
        senderLeader = mySess.createProducer(myQueueLeader);
        sender.setTimeToLive(5000);
        senderLeader.setTimeToLive(5000);
        
        if(this.leaderId == this.ID) {
        	receiverLeader = mySess.createConsumer(myQueueLeader);
        	receiverLeader.setMessageListener(this);
    	}
        
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
        if(receiverLeader != null) receiverLeader.close();
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
        
    private void updateSharedVariable(int number, int id) {
    	this.sharedVariable.setNumber(number);
    	this.sharedVariable.setId(id);
    }
    
    
    
    @Override
	public SharedVariable read() {
		MessageConsumer tempConsumer = null;
    	Queue tempQueue = null;
    	SharedVariable sv = null;
    	// request
    	try {
			tempQueue = mySess.createTemporaryQueue();
			synchronized(textMsg) {
				textMsg.clearProperties();
				textMsg.setBooleanProperty("REQUEST", true);
				textMsg.setJMSReplyTo(tempQueue);
				textMsg.setJMSCorrelationID(Long.toHexString(new Random(System.currentTimeMillis()).nextLong()));
				textMsg.setText("THIS IS A TEST");
				
				senderLeader.send(textMsg);
			}
			
			tempConsumer = mySess.createConsumer(tempQueue);		
			
		} catch (JMSException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    	
    	// reply to request
    	try {
			Message msg = tempConsumer.receive(5000); //timeout of two seconds
			sv = (SharedVariable)((ObjectMessage)msg).getObject();
			if(msg != null) {
				System.out.println("RECEIVED FROM LEADER - NUMBER: " + sv.getNumber() + " ID: " + sv.getId());
				tempConsumer.close();
			}
			else throw new JMSException("Error");
			
		} catch (JMSException e) {
			e.printStackTrace();
			System.out.println("LEADER IS NOT RESPONDING, INITIALIZATING ELECTION");
			sendElection(this.ID);
		} 
    	
    	return sv;
	}


	@Override
	public void write(int value) throws JMSException {
		updateSharedVariable(value, sharedVariable.getId() + 1 );
		synchronized(objectMsg) {
			objectMsg.clearProperties();
			objectMsg.setBooleanProperty("WRITE", true);
			objectMsg.setObject(sharedVariable);
			senderLeader.send(objectMsg);
		}
	}
	
	private void requestPermission() {
		try {
			synchronized(textMsg) {
				textMsg.clearProperties();
				textMsg.setBooleanProperty("PERMISSION", true);
				textMsg.setText(Integer.toString(this.ID));
				textMsg.setJMSCorrelationID(Long.toHexString(new Random(System.currentTimeMillis()).nextLong()));
				senderLeader.send(textMsg);
			}
		} catch (JMSException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
	
	private void releaseLock() {
		try {
			synchronized (textMsg) {
				textMsg.clearProperties();
				textMsg.setBooleanProperty("RELEASED", true);
				senderLeader.send(textMsg);
			}
		} catch(JMSException e) {
			e.printStackTrace();
			System.exit(1);
		}
		System.out.println("LOCK RELEASED");
	}
	
 
    
    // #################################################################################################
	public static void main(String[] args) throws Exception {
		Node node;
		
		if(args.length > 1) {
			// second or more nodes, they login to the previous node specified
        	int arg0 = Integer.parseInt(args[0]);
        	int arg1 = Integer.parseInt(args[1]);
			
			node = new Node(arg0, arg1, -1);
        	node.login(arg1);
        }
        else {
        	// first node and is leader automatically
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
		
//		Timer t = new Timer();
//        
//        t.schedule(new TimerTask() {
//        	@Override
//			public void run() {
//        		System.out.println();
//        		System.out.println("ID: " + node.ID);
//        		System.out.println("NextNode: " + node.nextNode );
//        		System.out.println("PreviousNode: " + node.previousID);
//        		System.out.println("LeaderId: " + node.leaderId);
//        		System.out.println("=============");
//        	}
//        }, 0, 5000);
        		       
		//node.receive();
		
		node.work();
        
	}
	
	private void work() {
    	int i = 0;
		while(i<1) {
    		Random random = new Random();
    		
    		try {
				Thread.sleep((long) (random.nextDouble()*10000));
				
				
				requestPermission();
				
				System.out.println("THREAD WAITING FOR CONFIRMATION");
				
				continueSignal.await();
				
				System.out.println("THREAD CONTINUING");
				
				// Compare local variable to the leader's one
				SharedVariable variable = read();
				
				if(variable.getId() > sharedVariable.getId() ) {
					updateSharedVariable(variable.getNumber(), variable.getId());
				}
				
//				if(random.nextBoolean()) {
					System.out.println("CHANGING VALUE");
					write(variable.getNumber() + 2);
//				}
				
				Thread.sleep(3000);	
					
				releaseLock();
				
				continueSignal = new CountDownLatch(1);
				
				i++;
			} catch (InterruptedException | JMSException e) {
				e.printStackTrace();
				System.exit(1);
			}
    		
    	}
    }


	

}

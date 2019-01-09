package jms;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.nio.file.Path;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.LinkedList;
import java.util.Map;
import java.util.Random;
import java.util.Scanner;
import java.util.Timer;
import java.util.TimerTask;
import java.util.TreeMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.logging.FileHandler;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

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

import org.w3c.dom.ls.LSInput;

import com.sun.messaging.ConnectionConfiguration;
import com.sun.messaging.ConnectionFactory;

public class Node implements NodeInterface, MessageListener {
	
	private String IPAdress = "localhost";
	private String queueName = "dsv";
	private String leaderQueueName = "dsvLeader";
	
	private Random random = new Random();
	
	// a message that will be sent to the queue
    private TextMessage textMsg;
    private ObjectMessage objectMsg;
    
    
    private int ID;
    
    private int nextNode;
    
    private int previousID;
    
    
    // Election and Leader
    private Boolean isParticipant = false;
    private int leaderId;
    
    private ArrayList<Integer> listAllNodes = new ArrayList<>();
    
    private ConnectionFactory myConnFactory;
    private Connection myConn;
    private Queue myQueue;
    private Queue myQueueLeader;
    private MessageConsumer receiver;
    private MessageProducer sender;
    private Session mySess;
	
    private MessageProducer senderLeader;
    private MessageConsumer receiverLeader;

    private Map<Integer, java.util.Queue<Integer>> criticalAccessFIFO = new TreeMap<>();
    private Map<Integer, Boolean> criticalAccessInUse = new TreeMap<>();
    
    
    private CountDownLatch continueSignal = new CountDownLatch(1);
    private CountDownLatch leaderContinueSignal = new CountDownLatch(0);
    
    // Example of Shared Memory
    // Shared Memory with 4 Integers being shared
    // Address int 1 = 439041089
    // Address int 2 = 439041090
    // Address int 3 = 439041091
    // Address int 4 = 439041092
    private SharedMemory sharedMemory= new SharedMemory();
    
    private final static int addr1 = 439041089;
    private final static int addr2 = 439041090;
    private final static int addr3 = 439041091;
    private final static int addr4 = 439041092;
    
    
    private Logger logger = Logger.getLogger("DSVLog");
    
    
	public Node(String IP, String id, String previousId, String leaderId) {
		this.IPAdress = IP;
		this.ID = Integer.parseInt(id);
		this.nextNode = Integer.parseInt(id);
		this.previousID = Integer.parseInt(previousId);
		this.leaderId = Integer.parseInt(leaderId);
		
		try {
			init();
		} catch (NamingException | JMSException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		FileHandler fh;
		Date date = new Date();
		SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH-mm-ss") ;
		try {
			Path path = new File("logs" + File.separator + id + "-" + dateFormat.format(date) + ".log").toPath();
			fh = new FileHandler(path.toString());
			logger.addHandler(fh);
			SimpleFormatter formatter = new SimpleFormatter();
			fh.setFormatter(formatter);
			logger.setUseParentHandlers(false);
			

		} catch (SecurityException | IOException e) {  
		    e.printStackTrace();  
		    System.exit(1);
		} 
		
		criticalAccessInUse.put(addr1, false);
		criticalAccessInUse.put(addr2, false);
		criticalAccessInUse.put(addr3, false);
		criticalAccessInUse.put(addr4, false);
		
		criticalAccessFIFO.put(addr1, new LinkedList<Integer>());
		criticalAccessFIFO.put(addr2, new LinkedList<Integer>());
		criticalAccessFIFO.put(addr3, new LinkedList<Integer>());
		criticalAccessFIFO.put(addr4, new LinkedList<Integer>());
		
		listAllNodes.add(this.ID);
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
                	logger.info("LOGIN REQUEST RECEIVED FROM: " + msgText);
                	
                	int nextNodeOld = this.nextNode;
                	
                	this.nextNode = Integer.parseInt(msgText);
                	
                	listAllNodes.add(this.nextNode);
                	
                	LoginDataResponse data = new LoginDataResponse(nextNodeOld, this.leaderId);
                	
                	// Update NEXTNODE info in the new node
                	synchronized(objectMsg) {
	                	objectMsg.clearProperties();
	                	objectMsg.setObjectProperty("ID", Integer.toString(this.nextNode));
	                	objectMsg.setBooleanProperty("NEXTNODE", true);
	                	objectMsg.setObject(data);
	                	sender.send(objectMsg);
                	}
                	
                	
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
                	logger.info("NEXTNODE UPDATED: " + this.nextNode);
                	synchronized(textMsg) {
                		textMsg.clearProperties();
	                	textMsg.setObjectProperty("ID", Integer.toString(this.nextNode));
	                	textMsg.setBooleanProperty("ASKFORUPDATENODE", true);
	                	sender.send(textMsg);
            		}
                
                }
                else if(msg.propertyExists("PREVIOUSNODE") && msg.getBooleanProperty("PREVIOUSNODE")) {
                	this.previousID = Integer.parseInt(msgText);
                }
                
                else if(msg.propertyExists("LOGOUT") && msg.getBooleanProperty("LOGOUT")) {
                	logger.info("LOGOUT MESSAGE RECEIVED");
                	int originId = msg.getIntProperty("ORIGINID");
                	this.previousID = Integer.parseInt(msgText);
                	
                	for(int i=0; i<listAllNodes.size(); i++) {
                		if(listAllNodes.get(i) == originId) {
                			listAllNodes.remove(i);
                		}
                	}
                	
                	if(originId == this.leaderId) {
                		this.leaderId = -1;
                		if(this.ID != this.nextNode) {
                			sendElection(this.ID);
                			logger.info("LEADER - LOGOUT, ELECTION STARTED");
                		}
                		else {
                			this.leaderId = this.ID;
                			//starts listening to the leaderQueue
                			receiverLeader = mySess.createConsumer(myQueueLeader);
                			receiverLeader.setMessageListener(this);
                			logger.info("I AM THE LEADER AND ONLY NODE");
                		}
                	}
                }
                
                // Election process
                else if(msg.propertyExists("ELECTION") && msg.getBooleanProperty("ELECTION")) {
                	logger.info("ELECTION MESSAGE RECEIVED");
                	int idMsg = Integer.parseInt(msgText);
                	logger.info("PROPOSED LEADER: " + idMsg);
                	if(idMsg > this.ID) {
                		logger.info("SENDING MESSAGE WITHOUT CHANGING ID");
                		sendElection(idMsg);
                		this.isParticipant = true;
                	}
                	else if(idMsg < this.ID && !this.isParticipant) {
                		logger.info("SENDING MY ID AS LEADER");
                		sendElection(this.ID);
                		this.isParticipant = true;
                	}
                	else if(idMsg < this.ID && this.isParticipant) {
                		logger.info("ELECTION MESSAGE DISCARDED!");
                	}
                	else {
                		System.out.println("IM THE LEADER");
                		logger.info("I AM THE LEADER");
                		this.leaderId = this.ID;
                		sendElectionFinished();
                		logger.info("ELECTION ENDED");
                	}
                }
                
                // propagating the leader's id
                else if(msg.propertyExists("LEADER") && msg.getBooleanProperty("LEADER")) {
                	int leader = Integer.parseInt(msgText);
                	System.out.println("NEW LEADER: " + leader); 
                	logger.info("NEW LEADER: " + leader); 
                	if(this.leaderId == leader) {					
            			
                		//starts listening to the leaderQueue
            			receiverLeader = mySess.createConsumer(myQueueLeader);
            			receiverLeader.setMessageListener(this);
                	}
                	else {
                		logger.info("ELECTION ENDED");
                		this.leaderId = Integer.parseInt(msgText);
	                	sendElectionFinished();
                	}
                	leaderContinueSignal.countDown();
                }
                
                // Leader is asked to return the value of the variable in the address given
                else if(msg.propertyExists("READ") && msg.getBooleanProperty("READ")) {
                	
    				logger.info("READ REQUEST: " + msgText);
    				
    				Destination replyQueue = msg.getJMSReplyTo();
    				
    				MessageProducer tempProducer = mySess.createProducer(replyQueue);
    				synchronized(objectMsg) {
    					objectMsg.clearProperties();
	    				objectMsg.setJMSDestination(replyQueue);
	    				objectMsg.setJMSCorrelationID(msg.getJMSCorrelationID());
	    				int address = Integer.parseInt(msgText);
	    				objectMsg.setObject(sharedMemory.getVariable(address));
    				
	    				//Thread.sleep(3000);
	    				
	    				tempProducer.send(objectMsg);
    				}
    				
    				tempProducer.close();
                }
                
                else if(msg.propertyExists("PERMISSION") && msg.getBooleanProperty("PERMISSION")) {
                	int addr = Integer.parseInt(msgText);
                	int id = 0;
                	if (msg.propertyExists("ID"))
                		id = msg.getIntProperty("ID");
                	logger.info("LEADER - LOCK REQUESTED BY ID " + id + " FOR " + addr);
                	if(criticalAccessInUse.get(addr)) {
                		criticalAccessFIFO.get(addr).add(id);
                		logger.info("LEADER - LOCK DENIED TO ID " + id + " FOR " + addr);
                	}
                	else {
                		criticalAccessInUse.put(addr, true);
                		sendPermissionGranted(id);
                		logger.info("LEADER - LOCK GRANTED TO ID " + id + " FOR " + addr);
                	}
                }
                
                else if(msg.propertyExists("RELEASED") && msg.getBooleanProperty("RELEASED")) {
                	int addr = Integer.parseInt(msgText);                	
                	if(!criticalAccessFIFO.get(addr).isEmpty()) {
                		int id = criticalAccessFIFO.get(addr).remove();
                		logger.info("LEADER - LOCK GRANTED TO ID " + id + " FOR " + addr);
                		sendPermissionGranted(id);
                	}
                	else {
                		criticalAccessInUse.put(addr, false);
                		logger.info("LOCK FOR " + addr + " NOT IN USE ANYMORE");
                	}
                }
                
//                Not implemented in time
//                List of all nodes, so that if one failed to prove that it is alive, the previous node could start
//                communicating with the next node
//                
//                else if(msg.propertyExists("NEWNODE") && msg.getBooleanProperty("NEWNODE")) {
//                	int id = msg.getIntProperty("NEWID");
//                	if(!(id == this.ID)) {
//	                	int before = Integer.parseInt(msgText);
//	                	
//	                	System.out.println("INSERTING ID " + id + "AFTER " + before);
//	                	
//	                	int index = listAllNodes.indexOf(before) + 1;
//	                	listAllNodes.add(index, id);
//	                	
//	                	String res = "";
//	            		for(Integer i : listAllNodes) {
//	            			res += i + " ";
//	            		}
//	            		System.out.println(res);
//	                	
//	                	synchronized(textMsg) {
//	                		textMsg.clearProperties();
//		                	textMsg.setObjectProperty("ID", Integer.toString(this.nextNode));
//		                	textMsg.setIntProperty("NEWID", id);
//		                	textMsg.setBooleanProperty("NEWNODE", true);
//		                	textMsg.setText(Integer.toString(before));
//		                	sender.send(textMsg);
//	            		}
//                	}
//                	
//                }
//                
//                else if(msg.propertyExists("ASKFORUPDATENODE") && msg.getBooleanProperty("ASKFORUPDATENODE")) {
//                	synchronized (objectMsg) {
//                		objectMsg.clearProperties();
//	                	objectMsg.setObjectProperty("ID", Integer.toString(this.previousID));
//	                	objectMsg.setBooleanProperty("ALLNODES", true);
//	                	int size = listAllNodes.size();
//	                	ArrayList<Integer> tempArray = new ArrayList<>(listAllNodes.subList(0, size));
//	                	objectMsg.setObject(tempArray);
//	                	sender.send(objectMsg);
//					}
//                }
                
                
            }
            
            // If a message is ObjectMessage
            else if(msg instanceof ObjectMessage) {
            	
            	if(msg.propertyExists("NEXTNODE") && msg.getBooleanProperty("NEXTNODE")) {
	            	
	            	LoginDataResponse data = (LoginDataResponse) ((ObjectMessage) msg).getObject();
	            	
	            	this.nextNode = data.getNextNode();
	            	this.leaderId = data.getLeaderId();
            	}
            	
            	else if(msg.propertyExists("WRITE") && msg.getBooleanProperty("WRITE")) {
            		
            		int address = 0; 
            		if(msg.propertyExists("ADDRESS")) 
            			address = msg.getIntProperty("ADDRESS");
            		
            		SharedVariable variable = (SharedVariable) ((ObjectMessage) msg).getObject();
            		
            		System.out.println("WRITE " + address + " - NUMBER: " + variable.getNumber() + " ID: "+variable.getId());
            		logger.info("WRITE " + address + " - NUMBER: " + variable.getNumber() + " ID: "+variable.getId());
            		SharedVariable localVariable = sharedMemory.getVariable(address);
            		
            		if(localVariable.getId() < variable.getId()) {
            			updateSharedMemory(address, variable.getNumber(), variable.getId());
            		}
            		
            		System.out.println("PROPAGATING CHANGES");
            		propagateWrite(address);
            		
            	}
            	
            	else if(msg.propertyExists("UPDATEMEMORY") && msg.getBooleanProperty("UPDATEMEMORY")) {
            		if(this.ID != this.leaderId) {
            			int address = 0;
            			if(msg.propertyExists("ADDRESS")) 
                			address = msg.getIntProperty("ADDRESS");
            			SharedVariable variable = (SharedVariable) ((ObjectMessage)msg).getObject();
            			logger.info("UPDATING SHARED MEMEORY ADDRESS " + address);

            			updateSharedMemory(address, variable.getNumber(), variable.getId());
            			propagateWrite(address);
//            			System.out.println(address + " NUMBER: " + variable.getNumber() + " ID: " + variable.getId());
             		}
            		else {
            			logger.info("PROPAGATING WRITE FINISHED");
            		}
            	}
            	
            	else if(msg.propertyExists("GRANTED") && msg.getBooleanProperty("GRANTED")) {
            		// Triggers thread waiting to continue
            		continueSignal.countDown();
            		logger.info("LOCK WAS GRANTED");
            	}
            	
//            	Would be used to implement a list containing all nodes
//            	
//            	else if(msg.propertyExists("ALLNODES") && msg.getBooleanProperty("ALLNODES")) {
//            		System.out.println("UPDATE LIST");
//            		ArrayList<Integer> array = (ArrayList<Integer>) ((ObjectMessage)msg).getObject();
//            		listAllNodes.addAll(array);
//            		String res = "";
//            		for(Integer i : listAllNodes) {
//            			res += i + " ";
//            		}
//            		System.out.println(res);
//            		
//            		synchronized(textMsg) {
//                		textMsg.clearProperties();
//	                	textMsg.setObjectProperty("ID", Integer.toString(nextNode));
//	                	textMsg.setIntProperty("NEWID", this.ID);
//	                	textMsg.setBooleanProperty("NEWNODE", true);
//	                	textMsg.setText(Integer.toString(this.previousID));
//	                	sender.send(textMsg);
//            		}
//            		
//            	}
            	
            }    
            
		} catch(Exception e) {
			e.printStackTrace();
			System.exit(1);
		}
                	
	}
	
	// Send permission granted to Node with ID id
	private void sendPermissionGranted(int id) {
		try {
			synchronized(objectMsg) {
				objectMsg.clearProperties();
				objectMsg.setBooleanProperty("GRANTED", true);
				objectMsg.setStringProperty("ID", Integer.toString(id));
				//objectMsg.setObject(sharedVariable);
				sender.send(objectMsg);
				logger.info("PERMISSION GRANTED SENT TO ID " + id);
			}
		} catch (JMSException e) {
			e.printStackTrace();
			System.exit(1);
		}
		
	}


	// Send modified address to every node
	private void propagateWrite(int address) {
		try {
			synchronized(objectMsg) {
				objectMsg.clearProperties();
				objectMsg.setBooleanProperty("UPDATEMEMORY", true);
				objectMsg.setStringProperty("ID", Integer.toString(this.nextNode));
				objectMsg.setIntProperty("ADDRESS", address);
				objectMsg.setObject(sharedMemory.getVariable(address));
				
				sender.send(objectMsg);
			}
			
		} catch (JMSException e) {
			e.printStackTrace();
			System.exit(1);
		}
		
		
	}

	// Send election has finished to all nodes
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
			
		} catch(JMSException e) {
			e.printStackTrace();
			System.exit(1);
		}
	}
	
	// Send election message containing the id of the leader
	public void sendElection(int idLeader) {
//		leaderContinueSignal = new CountDownLatch(1);
		System.out.println("NEXT ID " + this.nextNode);
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
	
//	Would be used to prove life to the next node
//	
//	private void sendHeartbeat() {
//		try {
//			synchronized(textMsg) {
//				textMsg.clearProperties();
//				textMsg.setObjectProperty("ID", Integer.toString(this.nextNode));
//				textMsg.setBooleanProperty("HEARTBEAT", true);
//				textMsg.setText(Integer.toString(this.ID));
//				sender.send(textMsg);
//			}
//		} catch(JMSException e) {
//			e.printStackTrace();
//			System.exit(1);
//		}
//	}

	
	@Override
	// Send login request to the next node
	public void login(int arg1) {
		try {
			synchronized(textMsg) {
				textMsg.clearProperties();
				textMsg.setObjectProperty("ID", Integer.toString(arg1));
				textMsg.setBooleanProperty("LOGIN", true);
				textMsg.setText(Integer.toString(this.ID));
				sender.send(textMsg);
			}
			System.out.println("LOGIN MESSAGE SENT TO " + arg1);
			logger.info("NODE - LOGIN MESSAGE SENT TO " + arg1);
		} catch (JMSException e) {
			e.printStackTrace();
			System.exit(1);
		}
	}

	@Override
	// Updating the next node and the previous node because this node is logging out
	public void logout() {
		if(this.ID != this.nextNode && this.ID != this.previousID) {
			try {
				synchronized(textMsg) {
					System.out.println("SENDING LOGOUT TO " + this.previousID + " (previousID) AND " + this.nextNode + " (nextNode)");
					textMsg.clearProperties();
					textMsg.setObjectProperty("ID", Integer.toString(this.previousID));
					textMsg.setBooleanProperty("NEXTNODE", true);
					textMsg.setIntProperty("ORIGINID", this.ID);
					textMsg.setText(Integer.toString(this.nextNode));
					sender.send(textMsg);
					
					textMsg.clearProperties();
					textMsg.setObjectProperty("ID", Integer.toString(this.nextNode));
					textMsg.setBooleanProperty("LOGOUT", true);
					textMsg.setIntProperty("ORIGINID", this.ID);
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
        myConnFactory.setProperty(ConnectionConfiguration.imqAddressList, this.IPAdress);
        myConn = myConnFactory.createConnection();
        mySess = myConn.createSession(false, Session.AUTO_ACKNOWLEDGE);
        myQueue = new com.sun.messaging.Queue(queueName);
        myQueueLeader = new com.sun.messaging.Queue(leaderQueueName);
        
        receiver = mySess.createConsumer(myQueue, "ID='" + this.ID + "'");
        receiver.setMessageListener(this);
        
        sender = mySess.createProducer(myQueue);
        senderLeader = mySess.createProducer(myQueueLeader);
        sender.setTimeToLive(20000);
        senderLeader.setTimeToLive(20000);
        
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
        
    // Update a memory address with a number and the id of writing
    private void updateSharedMemory(int address, int number, int id) {
    	SharedVariable local = sharedMemory.getVariable(address);
    	local.setNumber(number);
    	local.setId(id);
    	sharedMemory.setSharedVariable(address, number, id);
    }
    
    
    
    @Override
    // Reads an address from the leader's shared memory and updates it's own address
	public SharedVariable read(int address) {
		MessageConsumer tempConsumer = null;		
    	Queue tempQueue = null;
    	SharedVariable sv = null;
    	// request
    	try {
			tempQueue = mySess.createTemporaryQueue();
			synchronized(textMsg) {
				textMsg.clearProperties();
				textMsg.setBooleanProperty("READ", true);
				textMsg.setJMSReplyTo(tempQueue);
				textMsg.setJMSCorrelationID(Long.toHexString(new Random(System.currentTimeMillis()).nextLong()));
				textMsg.setText(Integer.toString(address));
				
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
    	
    	
    	SharedVariable local = sharedMemory.getVariable(address);
    	if(sv.getId() > local.getId()) {
    		updateSharedMemory(address, sv.getNumber(), sv.getId());
    	}
    	
    	return sv;
	}


	@Override
	// Updates the address in the leader's shared memory with the value specified
	public void write(int value, int address) throws JMSException {
		SharedVariable local = sharedMemory.getVariable(address);
		updateSharedMemory(address, value, local.getId() + 1);
		synchronized(objectMsg) {
			objectMsg.clearProperties();
			objectMsg.setBooleanProperty("WRITE", true);
			objectMsg.setIntProperty("ADDRESS", address);
			objectMsg.setObject(sharedMemory.getVariable(address));
			senderLeader.send(objectMsg);
		}
	}
	
	// Send a lock request for the address to the leader, so that the process can enter a critical zone safely
	private void requestLock(int address) {
		System.out.println("REQUEST PERMISSION FOR " + address);
		try {
			synchronized(textMsg) {
				textMsg.clearProperties();
				textMsg.setBooleanProperty("PERMISSION", true);
				textMsg.setIntProperty("ID", this.ID);
				textMsg.setJMSCorrelationID(Long.toHexString(new Random(System.currentTimeMillis()).nextLong()));
				textMsg.setText(Integer.toString(address));
				senderLeader.send(textMsg);
			}
		} catch (JMSException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}     
		
	}
	
	// Send info to leader so that the lock for the address may be released
	private void releaseLock(int address) {
		try {
			synchronized (textMsg) {
				textMsg.clearProperties();
				textMsg.setBooleanProperty("RELEASED", true);
				textMsg.setText(Integer.toString(address));
				senderLeader.send(textMsg);
			}
		} catch(JMSException e) {
			e.printStackTrace();
			System.exit(1);
		}
		System.out.println("LOCK RELEASED");
	}
	
 
    
    // ################################### Main ###################################################
	public static void main(String[] args) throws JMSException, InterruptedException {
		Node node;
		
		if(args.length > 2) {
			// second or more nodes, they login to the previous node specified
			node = new Node(args[0], args[1], args[2], "-1");
        	node.login(Integer.parseInt(args[2]));
        }
        else {
        	// first node and is leader automatically
        	node = new Node(args[0], args[1], args[1], args[1]);
        }
		
		Runtime.getRuntime().addShutdownHook(new Thread() 
	    { 
	      public void run() 
	      { 
	        System.out.println("SYSTEM EXITING!");
	        node.getLogger().info("SYSTEM EXITING!");
	        node.logout();
	        try {
				node.close();
			} catch (JMSException e) {
				e.printStackTrace();
			}
	        
	        
	      } 
	    }); 
		
//		Timer t = new Timer();
//        
//        t.schedule(new TimerTask() {
//        	@Override
//			public void run() {
//        		sendKeepALive();
//        	}
//        }, 0, 5000);
        		       

		Scanner scanner = new Scanner(System.in);
				
		while(true) {
			printMenu();
			int option = scanner.nextInt();	
			int cycles = 0;
			switch(option) {
			case 0:
				break;
			case 1:
				System.out.println("Insert number of cycles: ");
				cycles = scanner.nextInt();
				node.getLogger().info("EXECUTING BATCH WORK 1");
				node.batchWork1(cycles, addr1);
				break;
			case 2:
				System.out.println("Insert number of cycles: ");
				cycles = scanner.nextInt();
				node.getLogger().info("EXECUTING BATCH WORK 2");
				node.batchWork2(cycles);
				break;
			case 3:
				System.out.println("Insert number of cycles: ");
				cycles = scanner.nextInt();
				node.getLogger().info("EXECUTING BATCH WORK 3");
				node.batchWork3(cycles);
				break;
			case 4:
				System.out.println("Insert number of cycles: ");
				cycles = scanner.nextInt();
				node.getLogger().info("EXECUTING BATCH WORK 4");
				node.batchWork4(cycles);
				break;
			case 5:
				System.out.println("Insert number of cycles: ");
				cycles = scanner.nextInt();
				node.getLogger().info("EXECUTING BATCH WORK 5");
				node.batchWork5(cycles);
				break;
			default:
				System.out.println("Logging out!");
				System.exit(0);		
			
			}
			
			node.printVariables();
		}
		
	}
	
	// print the main menu
	private static void printMenu() {
		System.out.println("#### Main Menu ####");
		System.out.println("0 - Print Shared Memory");
		System.out.println("1 - Batch work 1");
		System.out.println("2 - Batch work 3");
		System.out.println("3 - Batch work 3");
		System.out.println("4 - Batch work 4");
		System.out.println("5 - Batch work 5");
		System.out.println("Any other number to logout");
		System.out.println("Please choose an option: ");
	}
	
	// print addresses of the shared variable
	public void printVariables() {
		System.out.println("1-"+sharedMemory.getVariable(addr1).getNumber());
		System.out.println("2-"+sharedMemory.getVariable(addr2).getNumber());
		System.out.println("3-"+sharedMemory.getVariable(addr3).getNumber());
		System.out.println("4-"+sharedMemory.getVariable(addr4).getNumber());
	}
		
	private Logger getLogger() {
		return logger;
	}
	
	// function that increments the value stored in address
	private void incrementVariableWork(int address) throws InterruptedException, JMSException {
		// wait for 2 lock confirmations
		if(continueSignal.getCount() > 0) {	
			System.out.println("WAITING FOR PERMISSION");
			continueSignal.await();
		}
		System.out.println("PERMISSION GRANTED");
		
		// Compare local variable to the leader's one
		SharedVariable variable = read(address);
		SharedVariable local = sharedMemory.getVariable(address);
		if(variable.getId() > local.getId() ) {
			updateSharedMemory(addr1, variable.getNumber(), variable.getId());
		}
		
		write(variable.getNumber() + 1, address);

		Thread.sleep(5000);	
			
	}
	
	// Batch work 1 - increments addr1
	private int batchWork1(int cycles, int address) {
		
		requestLock(address);
		
		while (cycles > 0) {
					
			try {
				
				System.out.println("BATCH 1 - REQUESTING PERMISSION FOR " + address);
				logger.info("BATCH 1 - REQUESTING PERMISSION FOR " + address);
				
				incrementVariableWork(address);
				
				cycles--;				
				
			} catch (InterruptedException e) {
				e.printStackTrace();
				releaseLock(address);
				return 1;
			} catch (JMSException e) {
				e.printStackTrace();
				releaseLock(address);
				return 1;
			}
				
		}
		
		releaseLock(address);
		
		logger.info("BATCH WORK FINISHED!");
		
		return 0;
	}
	
	// batch work 2 - adds addr2 and addr3 together and stores it in addr2
	private int batchWork2(int cycles) {
		requestLock(addr2);
		requestLock(addr3);
		
		continueSignal = new CountDownLatch(2);
		
		while(cycles > 0) {
			
						
			SharedVariable local1 = sharedMemory.getVariable(addr2);
			SharedVariable local2 = sharedMemory.getVariable(addr3);
			
			System.out.println("BATCH 2 - REQUESTING PERMISSION FOR ADDR2 AND ADDR3");
			logger.info("BATCH 2 - REQUESTING PERMISSION FOR ADDR2 AND ADDR3");
			
			// wait for 2 lock confirmations
			// should wait for answer from leader
			// if no answer, assume leader is dead and start election
			// NOT IMPLEMENTED CORRECTLY
			if(continueSignal.getCount() > 0) {
				try {
					continueSignal.await();
				} catch (InterruptedException e) {
					e.printStackTrace();
					releaseLock(addr2);
					releaseLock(addr3);
					return 1;
				}
			}
			
			
			
			SharedVariable variable1 = read(addr2);
			SharedVariable variable2 = read(addr3);
			
			int temp = variable1.getNumber() + variable2.getNumber();
						
			try {
				Thread.sleep(5000);
				
				write(temp, addr2);
			} catch (JMSException | InterruptedException e) {
				e.printStackTrace();
				releaseLock(addr2);
				releaseLock(addr3);
				return 1;
			}
			
			cycles--;
		}
		
		releaseLock(addr3);
		releaseLock(addr2);
		
		return 0;
	}
	
	// batch work 3 - adds addr2, addr3 and addr4 together, divides by 3 and stores it in addr4
	private int batchWork3(int cycles) {
		
		System.out.println("BATCH 3 - REQUESTING PERMISSION FOR ADDR1 AND ADDR3");
		requestLock(addr2);
		requestLock(addr3);
		requestLock(addr4);
		continueSignal = new CountDownLatch(2);
		
		while(cycles > 0) {
			// wait for 2 lock confirmations
			// should wait for answer from leader
			// if no answer, assume leader is dead and start election
			// NOT IMPLEMENTED CORRECTLY						
			if(continueSignal.getCount() > 0) {
				try {
					continueSignal.await();
				} catch (InterruptedException e) {
					e.printStackTrace();
					releaseLock(addr4);
					releaseLock(addr3);
					releaseLock(addr2);
					return 1;
				}
			}
			
			SharedVariable variable1 = read(addr2);
			SharedVariable variable2 = read(addr3);
			SharedVariable variable3 = read(addr4);
			
			int temp = variable1.getNumber() * variable2.getNumber() * variable3.getNumber() / 3;
						
			try {
				Thread.sleep(3000);
				
				write(temp, addr4);
			} catch (JMSException | InterruptedException e) {
				e.printStackTrace();
				releaseLock(addr4);
				releaseLock(addr3);
				releaseLock(addr2);
				return 1;
			}
			
			cycles--;
		
		}
	
		releaseLock(addr4);
		releaseLock(addr3);
		releaseLock(addr2);
		return 0;
	}

	// batch work 4 - subtracts addr3 to addr4 and compares it with addr1, if it's bigger stores in addr4, else stores it in addr3
	private int batchWork4(int cycles) {
		
		System.out.println("BATCH 4");
		logger.info("BATCH 4");
		
		requestLock(addr3);
		requestLock(addr4);
		
		continueSignal = new CountDownLatch(2);
		
		try {
			while(cycles > 0) {
				// wait for 2 lock confirmations
				// should wait for answer from leader
				// if no answer, assume leader is dead and start election
				// NOT IMPLEMENTED CORRECTLY					
				while(continueSignal.getCount() > 0) {
//					if(leaderContinueSignal.getCount() > 0) {
//						System.out.println("WAITING FOR LEADER ELECTION");
//						leaderContinueSignal.await();
//						requestLock(addr3);
//						requestLock(addr4);
//					}
//					if(!continueSignal.await(2, TimeUnit.SECONDS)) {
//						System.out.println("ELECTING LEADER");
//						logger.info("LEADER DIDN'T SEND ANT CONFIRMATION, ASSUMING IT IS DEAD");
//						sendElection(this.ID);
//					}
					continueSignal.await();
				}
				
				SharedVariable variable1 = read(addr3);
				SharedVariable variable2 = read(addr4);
				
				
				continueSignal = new CountDownLatch(1);
				
				requestLock(addr1);
				
				SharedVariable variable3 = read(addr1);
				
				releaseLock(addr1);
				
				int temp = variable2.getNumber() - variable1.getNumber();
				
				Thread.sleep(random.nextInt(6) * 1000);
				
				if(variable3.getNumber() < temp) {
					write(temp, addr3);
				}
				else {
					write(temp, addr4);
				}
				
				cycles--;
				
			}
			
			releaseLock(addr4);
			releaseLock(addr3);
			
			return 0;
		}
		catch(Exception e) {
			e.printStackTrace();
			releaseLock(addr4);
			releaseLock(addr3);
			return 1;
		}
	}
	
	// batch work 5 -  adds all shared addresses together and stores the result in addr1
	private int batchWork5(int cycles) {
		System.out.println("BATCH 4");
		logger.info("BATCH 4");
		
		requestLock(addr1);
		requestLock(addr2);
		requestLock(addr3);
		requestLock(addr4);
		
		continueSignal = new CountDownLatch(4);
		
		try {
			while(cycles > 0) {
				
				// wait for 4 lock confirmations
				// should wait for answer from leader
				// if no answer, assume leader is dead and start election
				// NOT IMPLEMENTED CORRECTLY					
				while(continueSignal.getCount() > 0) {
//					if(leaderContinueSignal.getCount() > 0) {
//						System.out.println("WAITING FOR LEADER ELECTION");
//						leaderContinueSignal.await();
//						requestLock(addr3);
//						requestLock(addr4);
//					}
//					if(!continueSignal.await(2, TimeUnit.SECONDS)) {
//						System.out.println("ELECTING LEADER");
//						logger.info("LEADER DIDN'T SEND ANT CONFIRMATION, ASSUMING IT IS DEAD");
//						sendElection(this.ID);
//					}
					
					continueSignal.await();
				}
				
				SharedVariable variable1 = read(addr1);
				SharedVariable variable2 = read(addr2);
				SharedVariable variable3 = read(addr3);
				SharedVariable variable4 = read(addr4);
				
				int temp = variable1.getNumber() + variable2.getNumber() + variable3.getNumber() + variable4.getNumber();
				
				Thread.sleep(random.nextInt(6) * 1000);
				
				write(temp, addr1);
				
				cycles--;
				
			}
			
			releaseLock(addr4);
			releaseLock(addr3);
			releaseLock(addr2);
			releaseLock(addr1);
			
			return 0;
		}
		catch(Exception e) {
			e.printStackTrace();
			releaseLock(addr4);
			releaseLock(addr3);
			releaseLock(addr2);
			releaseLock(addr1);
			return 1;
		}
	}
	

}

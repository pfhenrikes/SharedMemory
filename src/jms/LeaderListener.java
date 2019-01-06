package jms;

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

import com.sun.messaging.ConnectionConfiguration;
import com.sun.messaging.ConnectionFactory;

public class LeaderListener implements MessageListener {
	
	private ConnectionFactory myConnFactory;
    private Connection myConn;
    private Session mySess;
    private TextMessage textMsg;
    private ObjectMessage objectMsg;
	
	@Override
	public void onMessage(Message msg) {
		try {
			System.out.println("LEADER WAS ASKED SOMETHING");
			String msgText;
			if(msg instanceof TextMessage) {
				msgText = ((TextMessage) msg).getText();
				System.out.println("RECEIVED: " + msgText);
				
				Destination replyQueue = msg.getJMSReplyTo();
				
				MessageProducer tempProducer = mySess.createProducer(replyQueue);
				
				objectMsg.clearProperties();
				objectMsg.setJMSDestination(replyQueue);
				objectMsg.setJMSCorrelationID(msg.getJMSCorrelationID());
//				objectMsg.setObject();
				
				Thread.sleep(3000);
				
				tempProducer.send(textMsg);
				
				tempProducer.close();
				
			}
		}
		catch(JMSException | InterruptedException e) {
			e.printStackTrace();
			System.exit(1);
		}

	}
	
	
	
	public LeaderListener() {
		init();
	}
	
	private void init() {
        try {
        	myConnFactory = new ConnectionFactory();
            myConnFactory.setProperty(ConnectionConfiguration.imqAddressList, "192.168.56.3:7676");
            myConn = myConnFactory.createConnection();
			mySess = myConn.createSession(false, Session.AUTO_ACKNOWLEDGE);
			textMsg = mySess.createTextMessage();
			objectMsg = mySess.createObjectMessage();
		} catch (JMSException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}

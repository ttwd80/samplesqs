package com.github.ttwd80.samplesqs;

import static org.hamcrest.core.IsEqual.equalTo;
import static org.junit.Assert.assertThat;

import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.model.CreateQueueRequest;
import com.amazonaws.services.sqs.model.CreateQueueResult;
import com.amazonaws.services.sqs.model.DeleteQueueRequest;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.ReceiveMessageResult;
import com.amazonaws.services.sqs.model.SendMessageRequest;
import static org.hamcrest.Matchers.lessThan;

public class TestConsume {

	private final Regions region = Regions.AP_SOUTHEAST_2;
	private final int count = 18;
	private String queueUrl = null;
	private AmazonSQS sqs = null;

	@Before
	public void setUp() {
		sqs = new AmazonSQSClient();
		sqs.setRegion(Region.getRegion(region));
		long id = System.currentTimeMillis();
		String queueName = "test-queue-" + id;
		createQueue(queueName);
		sendMessages();
	}

	@After
	public void tearDown() {
		sqs.deleteQueue(new DeleteQueueRequest(queueUrl));
	}

	private void sendMessages() {
		for (int i = 0; i < count; i++) {
			String payload = "Item #" + Integer.toString(i);
			SendMessageRequest sendMessageRequest = new SendMessageRequest(queueUrl, payload);
			sqs.sendMessage(sendMessageRequest);
		}
	}

	private void createQueue(String queueName) {
		CreateQueueRequest createQueueRequest = new CreateQueueRequest(queueName);
		CreateQueueResult createQueueResult = sqs.createQueue(createQueueRequest);
		queueUrl = createQueueResult.getQueueUrl();
	}

	@Test
	public void test1() {
		System.out.println("Testing with default configuration");
		ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest(queueUrl);
		ReceiveMessageResult result = null;
		int receiveMessageCallCount = 0;
		do {
			result = sqs.receiveMessage(receiveMessageRequest);
			List<Message> list = result.getMessages();
			if (list.size() > 0) {
				receiveMessageCallCount++;
			}
			System.out.println("got " + list.size() + " items");
			for (Message message : list) {
				System.out.println("Process message: " + message.getMessageId());
			}
		} while (!result.getMessages().isEmpty());
		assertThat(receiveMessageCallCount, equalTo(18));
	}

	@Test
	public void test5() {
		System.out.println("Testing with max message 5");
		ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest(queueUrl);
		receiveMessageRequest.setMaxNumberOfMessages(5);
		ReceiveMessageResult result = null;
		int receiveMessageCallCount = 0;
		do {
			result = sqs.receiveMessage(receiveMessageRequest);
			List<Message> list = result.getMessages();
			if (list.size() > 0) {
				receiveMessageCallCount++;
			}
			System.out.println("got " + list.size() + " items");
			for (Message message : list) {
				System.out.println("Process message: " + message.getMessageId());
			}
		} while (!result.getMessages().isEmpty());
		assertThat(receiveMessageCallCount, lessThan(count));
	}

}

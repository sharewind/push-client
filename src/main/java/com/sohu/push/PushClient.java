package com.sohu.push;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.NameValuePair;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.ResponseHandler;
import org.apache.http.client.entity.EntityBuilder;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.util.EntityUtils;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.codehaus.jackson.annotate.JsonProperty;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;

import com.sohu.push.PushClient.RegisterResponse;
import com.sohu.push.PushClient.ResponseView;


//curl -d "hi2 sucess " http://10.10.79.134:8501/put\?channel_id\=12002\&device_type\=0
public class PushClient {

	private Log log = LogFactory.getLog(PushClient.class);

	public static final String registerUrl = "http://10.10.79.134:8501/registration";

//	public static final String registerUrl = "http://localhost:8501/registration";


	private static final int device_type_android = 3;

	TTransport transport;

	byte[] MagicV1 = "  V1".getBytes();

	String brokerAddress;

	long clientId;

	Thread heartbeatThread = null;

	private volatile boolean closing;

	long heartbeatInterval = TimeUnit.MINUTES.toSeconds(5);

	MessageHandler messageHandler = null;

	public class FrameType {

		// when successful
		public static final int FrameTypeResponse = 0;
		// when an error occurred
		public static final int FrameTypeError = 1;
		// when it's a serialized message
		public static final int FrameTypeMessage = 2;
		// when ack a put message success/failure
		public static final int FrameTypeAck = 3;

	}

	public RegisterResponse registerDevice(String serialNO, String deviceName) {

		try {
			List<NameValuePair> params = new ArrayList<NameValuePair>();
			params.add(new BasicNameValuePair("serial_no", serialNO));
			params.add(new BasicNameValuePair("device_name", deviceName));
			params.add(new BasicNameValuePair("device_type", String.valueOf(device_type_android)));

			EntityBuilder entityBuilder = EntityBuilder.create();
			entityBuilder.setParameters(params);
			HttpEntity entity = entityBuilder.build();

			HttpPost httpPost = new HttpPost(registerUrl);
			httpPost.setEntity(entity);
			log.info("registerDevice request " + httpPost.getRequestLine() + "\n entity:" + EntityUtils.toString(httpPost.getEntity()));

			// Create a custom response handler
			ResponseHandler<String> responseHandler = new ResponseHandler<String>() {

				public String handleResponse(final HttpResponse response) throws ClientProtocolException, IOException {
					int status = response.getStatusLine().getStatusCode();
					if (status >= 200 && status < 300) {
						HttpEntity entity = response.getEntity();
						return entity != null ? EntityUtils.toString(entity) : null;
					} else {
						throw new ClientProtocolException("Unexpected response status: " + status);
					}
				}

			};

			CloseableHttpClient httpClient = HttpClients.createDefault();
			String responseBody = httpClient.execute(httpPost, responseHandler);
			log.debug(String.format("registerDevice response: %s", responseBody));
			System.out.println(String.format("registerDevice response: %s", responseBody));
			if (responseBody == null || responseBody.length() == 0) {
				return null;
			}

			ObjectMapper mapper = new ObjectMapper();
			TypeReference<ResponseView<RegisterResponse>> ref = new TypeReference<ResponseView<RegisterResponse>>() {};
			ResponseView<RegisterResponse> resp = mapper.readValue(responseBody, ref);
			if (resp.getCode() != 0) {
				return null;
			}

			return resp.getData();
		} catch (Exception e) {
			log.error("registerDevice error", e);
			return null;
		}
	}

	public void connectBroker() throws TTransportException {
		String[] parts = this.brokerAddress.split(":");
		String host = parts[0];
		int port = Integer.parseInt(parts[1]);

		// SocketAddress socketAddress =
		// InetSocketAddress.createUnresolved(host, port);
		// Socket socket = new Socket();
		// socket.connect(endpoint);

		transport = new TSocket(host, port);
		transport.open();
		transport.write(MagicV1);
		transport.flush();
	}

	public void identify() throws Exception {

		Map<String, Object> identifyData = new HashMap<String, Object>();
		identifyData.put("heartbeatInterval", heartbeatInterval);
		identifyData.put("client_id", clientId);

		ObjectMapper objectMapper = new ObjectMapper();
		String json = objectMapper.writeValueAsString(identifyData);

		byte[] lenBytes = toBytes(json.length());

		String command = "IDENTIFY\n";

		ByteBuffer buffer = ByteBuffer.allocate(command.length() + 4 + json.length());
		buffer.put(command.getBytes());
		buffer.put(lenBytes);
		buffer.put(json.getBytes());
		byte[] data = buffer.array();

		System.out.println(new String(data));

		transport.write(data);
		transport.flush();
	}

	private void startHeartbeat() {
		final byte[] heartbeatBytes = "H\n".getBytes();

		heartbeatThread = new Thread(new Runnable() {
			@Override
			public void run() {
				try {
					for (;;) {
						if (closing) {
							System.out.println("heartbeat stop by closing flag");
							break;
						}

						transport.write(heartbeatBytes);
						transport.flush();
						long sleepMs = TimeUnit.SECONDS.toMillis(heartbeatInterval);
						Thread.currentThread().sleep(sleepMs/100);
					}
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		});
		heartbeatThread.start();
	}

	private void startReceive() {
		Thread receiveThread = new Thread(new Runnable() {
			@Override
			public void run() {
				try {

					for (;;) {
						byte[] buf = new byte[4];
						transport.readAll(buf, 0, 4);
						int size = fromBytes(buf);
						if (size < 0) {
							throw new TTransportException("Read a negative frame size (" + size + ")!");
						}
						System.out.println("receive data ..." + size);

						byte[] buf2 = new byte[size];
						transport.readAll(buf2, 0, size);

						ByteBuffer byteBuffer = ByteBuffer.wrap(buf2);
						int frameType = byteBuffer.getInt();

						switch (frameType) {
						case FrameType.FrameTypeMessage:
							System.out.println("recieve message ");
							System.out.println();
							Message.fromBytes(byteBuffer);
							break;
						case FrameType.FrameTypeResponse:
							System.out.println(new String(byteBuffer.array()));
							break;
						default:
							break;
						}
					}

				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		});
		receiveThread.start();
	}

	public void subChannel(String channelId, MessageHandler messageHandler) throws TTransportException, UnsupportedEncodingException {
		String command = String.format("SUB %s\n", channelId);
		System.out.println(command);
		transport.write(command.getBytes());
		transport.flush();

		this.messageHandler = messageHandler;
	}

	private byte[] toBytes(int len) {
		byte[] i32out = new byte[4];
		i32out[0] = (byte) (0xff & (len >> 24));
		i32out[1] = (byte) (0xff & (len >> 16));
		i32out[2] = (byte) (0xff & (len >> 8));
		i32out[3] = (byte) (0xff & (len));
		return i32out;
	}

	private int fromBytes(byte[] bytes) {
		int size = ((bytes[0] & 0xff) << 24) | ((bytes[1] & 0xff) << 16) | ((bytes[2] & 0xff) << 8) | ((bytes[3] & 0xff));
		return size;
	}

	public static void main(String[] args) throws Exception {

		final CountDownLatch countDownLatch = new CountDownLatch(100);

		PushClient pushClient = new PushClient();

		String serialNO = "SOHUXXX234242390";
		String deviceName = "Sohu Android test";
		RegisterResponse registerResponse = pushClient.registerDevice(serialNO, deviceName);
		if (registerResponse == null) {
			System.out.println("register failure!");
			return;
		}
		pushClient.brokerAddress = registerResponse.broker;
		pushClient.clientId = registerResponse.deviceId;

		String channelId = "12002";
		MessageHandler handler = new MessageHandler() {
			@Override
			public void handler(Message m) {
				System.out.println(m);
				countDownLatch.countDown();
			}
		};

		pushClient.connectBroker();
		pushClient.startHeartbeat();
		pushClient.startReceive();

		pushClient.identify();
//		Thread.sleep(TimeUnit.SECONDS.toMillis(10));
		pushClient.subChannel(channelId, handler);
		countDownLatch.await();
	}

	public interface MessageHandler {

		public void handler(Message m);

	}

	public static class Message {
		long messageId;
		byte[] body;
		long timestamp;
		int attemps;
		long expires;

		public Message() {
		}

		public Message(long messageId, long timestamp, int attemps, byte[] body) {
			this.messageId = messageId;
			this.timestamp = timestamp;
			this.attemps = attemps;
			this.body = body;
		}

		public static Message fromBytes(ByteBuffer byteBuffer) {
			try {
				byte[] ts = new byte[8];
				byteBuffer.get(ts, 0, 8);

				byte[] ats = new byte[2];
				byteBuffer.get(ats, 0, 2);

				byte[] byteID = new byte[16];
				byteBuffer.get(byteID, 0, 16);



				 ByteBuffer body = byteBuffer.allocate(1024);
				 while(byteBuffer.hasRemaining()){
					 body.put(byteBuffer.get());
				 }

				System.out.println(new String(body.array()));

				// return new Message(messageId, timestamp, attemps, body);
				return null;

			} catch (Exception e) {
				return null;
			}

		}

		public long getMessageId() {
			return messageId;
		}

		public void setMessageId(long messageId) {
			this.messageId = messageId;
		}

		public byte[] getBody() {
			return body;
		}

		public void setBody(byte[] body) {
			this.body = body;
		}

		public long getTimestamp() {
			return timestamp;
		}

		public void setTimestamp(long timestamp) {
			this.timestamp = timestamp;
		}

		public int getAttemps() {
			return attemps;
		}

		public void setAttemps(int attemps) {
			this.attemps = attemps;
		}

		public long getExpires() {
			return expires;
		}

		public void setExpires(long expires) {
			this.expires = expires;
		}

	}

	public static class ResponseView<T> {

		int code;

		String msg;

		T data;

		public int getCode() {
			return code;
		}

		public void setCode(int code) {
			this.code = code;
		}

		public String getMsg() {
			return msg;
		}

		public void setMsg(String msg) {
			this.msg = msg;
		}

		public T getData() {
			return data;
		}

		public void setData(T data) {
			this.data = data;
		}

	}

	public static class RegisterResponse {

		String broker;

		@JsonProperty("device_id")
		long deviceId;

		boolean success;
		boolean errorMessage;

		public String getBroker() {
			return broker;
		}

		public void setBroker(String broker) {
			this.broker = broker;
		}

		public long getDeviceId() {
			return deviceId;
		}

		public void setDeviceId(long deviceId) {
			this.deviceId = deviceId;
		}

	}

}

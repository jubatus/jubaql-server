import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;

public class DummySparkSubmit {
	public static void main(String[] args) throws Exception {
        String url = "";
        boolean isTimeout = false;
        boolean isReturn = false;
        boolean isAfter = false;
        boolean isAfterFailed = false;
        boolean isException = false;

		for (String arg : args) {
			System.out.println(arg);
            if(arg.startsWith("http://")) {
              url = arg;
            } else if (arg.contains("jubaql.checkpointdir")) {
            	if (arg.contains("timeout")) {
            		isTimeout = true;
            	} else if (arg.contains("return")) {
            		isReturn = true;
            	} else if (arg.contains("after")) {
            		isAfter = true;
            	} else if (arg.contains("afterFailed")) {
            		isAfterFailed = true;
            	}else if (arg.contains("exception")) {
            		isException = true;
            	}
            }
		}
		if (isTimeout) {
			long startTime = System.currentTimeMillis();
			while (System.currentTimeMillis() - startTime < 30000) {
				try {
					Thread.sleep(1000);
				} catch (Exception e) {
					e.printStackTrace();
				}
				System.out.println("state: ACCEPTED");
			}
		} else if (isReturn) {
			long startTime = System.currentTimeMillis();
			while (System.currentTimeMillis() - startTime < 5000) {
				try {
					Thread.sleep(1000);
				} catch (Exception e) {
					e.printStackTrace();
				}
				System.err.println("Standard Error Message");
				System.out.println("state: ACCEPTED");
			}
		} else if (isAfter) {
			Thread.sleep(1000);
			System.out.println("state: ACCEPTED");
			Thread.sleep(1000);
			System.out.println("state: RUNNING");
			sendRegistRequest(url);
			Thread.sleep(5000);
			System.out.println("[Error] Exception: xxxxxxxxxxxx");
		} else if (isAfterFailed) {
			Thread.sleep(1000);
			System.out.println("state: ACCEPTED");
			Thread.sleep(1000);
			System.out.println("state: RUNNING");
			sendRegistRequest(url);
			Thread.sleep(5000);
			System.out.println("[Error] Exception: xxxxxxxxxxxx");
			System.exit(10);
		} else if (isException) {
			Thread.sleep(1000);
			System.out.println("state: ACCEPTED");
			Thread.sleep(1000);
			System.out.println("state: RUNNING");
			sendRegistRequest(url);
			Thread.sleep(5000);
			throw new Exception("Runnning After Exception");
		} else {
			try {
				Thread.sleep(1000);
				System.out.println("state: ACCEPTED");
				Thread.sleep(1000);
				System.out.println("state: RUNNING");
				sendRegistRequest(url);
				Thread.sleep(5000);
				long startTime = System.currentTimeMillis();
				while (System.currentTimeMillis() - startTime < 30000) {
					Thread.sleep(1000);
					System.out.println("state: RUNNNING");
				}
			} catch (Exception e) {
				e.printStackTrace();
				throw e;
			}
		}
		System.exit(0);
	}

	private static void sendRegistRequest(String urlString) {
		HttpURLConnection connection = null;
		try {
			URL url = new URL(urlString);
			connection = (HttpURLConnection) url.openConnection();
			connection.setDoOutput(true);
			connection.setRequestMethod("POST");
			BufferedWriter writer = new BufferedWriter(
					new OutputStreamWriter(connection.getOutputStream(), StandardCharsets.UTF_8));
			writer.write("{\"action\": \"register\", \"ip\": \"localhost\",\"port\": 12345}");
			writer.flush();

			if (connection.getResponseCode() == HttpURLConnection.HTTP_OK) {
				try (InputStreamReader isr = new InputStreamReader(connection.getInputStream(), StandardCharsets.UTF_8);
						BufferedReader reader = new BufferedReader(isr)) {
					String line;
					while ((line = reader.readLine()) != null) {
						System.out.println(line);
					}
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if (connection != null) {
				connection.disconnect();
			}
		}

	}
}

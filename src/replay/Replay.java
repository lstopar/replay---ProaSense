package replay;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.json.simple.JSONObject;

import si.ijs.proasense.service.WebService;
import si.ijs.proasense.service.WebService.Method;


public class Replay {

//	private static final String FNAME = "/media/lstopar/1TB External HDD/data/Aker/joined1/setpoint/sorted/resampled/setpoint_all.csv";
	private static final String DIR_NAME = "/media/lstopar/hdd/data/Aker/cleaned";
	private static final int PORT = 8888;
	private static final String URL = "http://localhost:" + PORT + "/api/push";
	
	private static final int BATCH_SIZE = 1000;
	private static final long DELAY = 0;
	
	private List<String> stores;
	private Map<String, BufferedReader> inFileMap;
	private LinkedList<Instance> pending;
	private Instance[] currVals;
	
	private List<Instance> currentBatch;
	private long delay;
	
	private long maxSend;
	private long totalSent;
		
	public Replay(long delay, long maxSend) {
		this.delay = delay;
		this.maxSend = maxSend;
		
		totalSent = 0;
		currentBatch = new ArrayList<Replay.Instance>();
	}
	
	private Instance readInstance(String store) throws IOException {
		BufferedReader in = inFileMap.get(store);
		String line = in.readLine();
		
		if (line == null) return null;
		
		String[] split = line.split(",");
		return new Instance(store, Long.parseLong(split[0]), Double.parseDouble(split[1]));
	}
	
	private int findLowestValIdx() {
		int idx = -1;
		long lowestTime = Long.MAX_VALUE;
		
		for (int i = 0; i < currVals.length; i++) {
			if (currVals[i].timestamp < lowestTime) {
				lowestTime = currVals[i].timestamp;
				idx = i;
			}
		}
		
		return idx;
	}
	
	private boolean canInsert() {
		if (pending.isEmpty()) return false;

		Iterator<Instance> it = pending.iterator();
		
		Instance first = it.next();
		while (it.hasNext()) {
			Instance current = it.next();
			if (!current.store.equals(first.store) || current.timestamp != first.timestamp)
				return true;
		}
		
		return false;
	}
	
	private Instance extractVal() {
		if (pending.isEmpty()) return null;
		
		Iterator<Instance> it = pending.iterator();
		Instance first = it.next();
		int nEqual = 1;
		
		while (nEqual < pending.size()) {
			Instance curr = it.next();
			
			if (!curr.store.equals(first.store) || curr.timestamp != first.timestamp)
				break;
			
			nEqual++;
		}
		
		double sum = 0;
		for (int i = 0; i < nEqual; i++) {
			Instance val = pending.removeFirst();
			sum += val.value;
		}
		
		first.value = sum / nEqual;
		
		return first;
	}
	
	@SuppressWarnings("unchecked")
	private void send(Instance instance) {
		currentBatch.add(instance);
		
		if (currentBatch.size() >= BATCH_SIZE) {
			StringBuilder builder = new StringBuilder();
			
			builder.append("[");
			
			for (Iterator<Instance> it = currentBatch.iterator(); it.hasNext(); ) {
				Instance inst = it.next();
				
				JSONObject json = new JSONObject();
				json.put("store", inst.store);
				json.put("timestamp", inst.timestamp);
				json.put("value", inst.value);
				
				builder.append(json.toJSONString());
				
				if (it.hasNext()) builder.append(",");
			}
			
			builder.append("]");
			
			totalSent += currentBatch.size();
			currentBatch.clear();
			
			try {
				WebService.fetchUrl(URL, null, builder.toString(), null, "application/json", Method.POST);
			} catch (Throwable t) {
				System.out.println("Failed to send batch!");
			}
			
			if (delay > 0) {
				try {
					Thread.sleep(delay);
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
	}
	
	public void replay() {
		try {
			File dir1 = new File(DIR_NAME + "/gearbox/");
			File dir2 = new File(DIR_NAME + "/setpoint/");
			
			stores = new ArrayList<String>();
			inFileMap = new HashMap<String, BufferedReader>();
			pending = new LinkedList<Instance>();
			
			for (File file : dir1.listFiles()) {
				if (file.isDirectory()) continue;
				
				System.out.println(file.getName());
				
				String store = file.getName().split("\\.")[0];
				stores.add(store);
				inFileMap.put(store, new BufferedReader(new InputStreamReader(new FileInputStream(file))));
			}
			
			for (File file : dir2.listFiles()) {
				if (file.isDirectory() || file.getName().startsWith("hook_load")) continue;
				
				System.out.println(file.getName());
				
				String store = file.getName().split("\\.")[0];
				stores.add(store);
				inFileMap.put(store, new BufferedReader(new InputStreamReader(new FileInputStream(file))));
			}
			
			long[] counts = new long[stores.size()];
			
			currVals = new Instance[inFileMap.size()];
			
			// read initial line
			for (int i = 0; i < stores.size(); i++) {
				currVals[i] = readInstance(stores.get(i));
			}
			
//			long prevTime = -1;
			
			long i = 0;
			while (totalSent < maxSend) {
				int lowestValIdx = findLowestValIdx();
				
				if (lowestValIdx < 0) break;
				
				counts[lowestValIdx]++;
				Instance instance = currVals[lowestValIdx];			
				
				currVals[lowestValIdx] = readInstance(stores.get(lowestValIdx));
				pending.add(instance);
				
				if (i % 10000 == 0)
					System.out.println(String.format("%s: %s", new Date(instance.timestamp).toString(), Arrays.toString(counts)));
				
				while (canInsert()) {
					Instance insertInstance = extractVal();
					
					if (insertInstance == null) break;
					send(insertInstance);
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	private class Instance {
		
		private String store;
		private long timestamp;
		private double value;
		
		public Instance(String store, long timestamp, double value) {
			this.store = store;
			this.timestamp = timestamp;
			this.value = value;
		}
	}
	
	public static void main(String[] args) {
		new Replay(DELAY, 50000000).replay();
	}
}

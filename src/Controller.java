import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Timer;
import java.util.TimerTask;

public class Controller {
	
	private static int balance;
	private static List<Bank.InitBranch.Branch> branchList;
	private static int uniqueId;
	private static String name;
	
	private static synchronized int getUniqueId(){
		return uniqueId++;
	}
	
	public static void main(String arg[]) throws IOException, InterruptedException{
		if(arg.length != 2){
			System.err.println("Provide argumensts as <Money> <branchs_file>");
			System.exit(0);
		}
		if(Integer.parseInt(arg[0]) < 0) {
			System.err.println("Branches has to have non negative balance");
			System.exit(0);
		}
		name = "controller";
		try {
			balance = Integer.parseInt(arg[0]);
			BufferedReader br = new BufferedReader(new FileReader(new File(arg[1])));
			branchList = new ArrayList<Bank.InitBranch.Branch>();
			String line = br.readLine();
//		uniqueId = 5;
			while(line != null){
				String[] arr = line.split(" ");
				Bank.InitBranch.Branch.Builder branch = Bank.InitBranch.Branch.newBuilder();
				branch.setName(arr[0]);
				branch.setIp(arr[1]);
				branch.setPort(Integer.parseInt(arr[2]));
				branchList.add(branch.build());
				line = br.readLine();
			}
			br.close();
			sendInitMessageToAllBranch();
			Thread.sleep(10000);
			int i=0;
			while(true){
//			new Thread(new AutomatedInitSnapshotTread()).start();
				try {
					sendInitSnapshotMessage();
					
				}catch (Exception e) {
					// TODO: handle exception
					e.printStackTrace();
				}
//			Thread.sleep(5000);
				i++;
			}
			
		}catch (Exception e) {
			// TODO: handle exception
			System.out.println(e.getMessage());
		}
		
	}
	private static void sendInitMessageToAllBranch() throws UnknownHostException, IOException {
		// TODO Auto-generated method stub
		Double bal1 = balance/(double)branchList.size();
		if(bal1 % 1 != 0){
			System.err.println("Initial balance is not integer");
			System.exit(0);
		}
		int bal = bal1.intValue();
		for(Bank.InitBranch.Branch branch: branchList){
			System.out.println(branch.getIp()+"===="+branch.getPort());
			Socket socket = new Socket(branch.getIp(), branch.getPort());
			Bank.InitBranch.Builder initBranchMessage = Bank.InitBranch.newBuilder();
			initBranchMessage.setBalance(bal);
			initBranchMessage.addAllAllBranches(branchList);
			Bank.BranchMessage.Builder branchMsg = Bank.BranchMessage.newBuilder();
			branchMsg.setInitBranch(initBranchMessage.build());
			
//			socket.getOutputStream().write(name.length());
//			socket.getOutputStream().write(name.getBytes());
			
			branchMsg.build().writeDelimitedTo(socket.getOutputStream());
			socket.getOutputStream().close();
			socket.close();
		}
	}
	
	public static void sendInitSnapshotMessage() throws Exception{
//		System.out.println("Inside sendInitSnapshotMessage");
		int snapshotId = getUniqueId(); 
		int branchSelection = new Random().nextInt(branchList.size());
		System.out.println("Started Snapshot from="+branchList.get(branchSelection).getName());
		Socket socket = new Socket(branchList.get(branchSelection).getIp(), branchList.get(branchSelection).getPort());
		Bank.InitSnapshot.Builder initSnapshotMessage = Bank.InitSnapshot.newBuilder();
		initSnapshotMessage.setSnapshotId(snapshotId);
		Bank.BranchMessage.Builder branchMsg = Bank.BranchMessage.newBuilder();
		branchMsg.setInitSnapshot(initSnapshotMessage.build());
		
//		socket.getOutputStream().write(name.length());
//		socket.getOutputStream().write(name.getBytes());
		
		branchMsg.build().writeDelimitedTo(socket.getOutputStream());
		socket.getOutputStream().flush();
		socket.getOutputStream().close();
		socket.close();
		
//		System.out.println("Go to sleep="+System.currentTimeMillis());
		Thread.sleep(4000);
//		System.out.println("Retrive snapshot="+System.currentTimeMillis());
		
		retriveSnapshotMessage(snapshotId);
	}
	
	private static void retriveSnapshotMessage(int snapshotId) throws Exception{
		Map<String,Bank.ReturnSnapshot> returnedSnapshotMap = new HashMap<String,Bank.ReturnSnapshot>();
		for(Bank.InitBranch.Branch branch: branchList){
			Socket socket = new Socket(branch.getIp(), branch.getPort());
//			System.out.println("I am after retrive");
			Bank.RetrieveSnapshot.Builder retrieveSnapshotMsg = Bank.RetrieveSnapshot.newBuilder();
			retrieveSnapshotMsg.setSnapshotId(snapshotId);
			Bank.BranchMessage.Builder sendBranchMsg = Bank.BranchMessage.newBuilder();
			sendBranchMsg.setRetrieveSnapshot(retrieveSnapshotMsg.build());
			
//			socket.getOutputStream().write(name.length());
//			socket.getOutputStream().write(name.getBytes());
			sendBranchMsg.build().writeDelimitedTo(socket.getOutputStream());
			
//			System.out.println("I am before shutdownOutput");
//			socket.shutdownOutput();
//			System.out.println("I am after shutdownOutput");
			// Receive returnSnapshot Message.
			Bank.BranchMessage receivedBranchMsg = Bank.BranchMessage.parseDelimitedFrom(socket.getInputStream());
			if(receivedBranchMsg == null) {
				socket.close();
				throw new Exception("Snapshot with id "+snapshotId+" not ready yet..");
			}
			Bank.ReturnSnapshot returnedSnapshotMsg = receivedBranchMsg.getReturnSnapshot();
			returnedSnapshotMap.put(branch.getName(),returnedSnapshotMsg);
			socket.getInputStream().close();
			socket.close();
		}
		printBySnapshotId(snapshotId, returnedSnapshotMap);
	}

	private static  void printBySnapshotId(int snapshotId, Map<String, Bank.ReturnSnapshot> returnedSnapshotMap) {
		// TODO Auto-generated method stub
//		snapshot_id: 10
//		branch1: 1000, branch2->branch1: 10, branch3->branch1: 0
//		branch2: 1000, branch1->branch2: 0, branch3->branch2: 15
//		branch3: 960, branch->branch3: 15, branch2->branch3: 0
		System.out.println("snapshot_id: "+snapshotId);
		int sum = 0;
		for (String branchName:returnedSnapshotMap.keySet()) {
			StringBuilder sb = new StringBuilder();
			Bank.ReturnSnapshot returnSnapshot = returnedSnapshotMap.get(branchName);
			
			sb.append(branchName).append(": ").append(returnSnapshot.getLocalSnapshot().getBalance()).append(",");
			sum+=returnSnapshot.getLocalSnapshot().getBalance();
			List<Integer> channelList = returnSnapshot.getLocalSnapshot().getChannelStateList();
			for(int i=0;i<channelList.size();i++){
				if(branchList.get(i).getName().equals(branchName)) continue;
				sb.append(branchList.get(i).getName()).append("->").append(branchName).append(": ").append(channelList.get(i)).append(", ");
				sum+=channelList.get(i);
			}
			System.out.println(sb.toString());
			
			
		}
		System.out.println("Final sum="+sum);
	}
	
	
}

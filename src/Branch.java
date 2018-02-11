import java.io.IOException;
import java.io.InputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class Branch {
	private static String branchName;
	private static int PORT;
	private static int curBalance;
	private static int initialBalance;
	private static List<Bank.InitBranch.Branch> branchList;
	private static ServerSocket server;
	private static Map<Integer,Snapshot> idSnapshotMap;
	private static Map<Integer,Snapshot> finishedSnapshotMap;
	static Object lock = new Object();

	static class Snapshot{
		int id;
		int balance;
		Map<String,List<Integer>> recoringInChannel;
		Map<String,List<Integer>> recordedinChannel;
		
		public Snapshot(int id1, int bal) {
			// TODO Auto-generated constructor stub
			id = id1;
			balance = bal;
			recoringInChannel = new ConcurrentHashMap<String,List<Integer>>();
			recordedinChannel = new ConcurrentHashMap<String,List<Integer>>();
		}
		
		public Map<String,List<Integer>> getrecoringInChannel(){
			return recoringInChannel;
		}
		
		public Map<String,List<Integer>> getrecordedInChannel(){
			return recordedinChannel;
		}
	}
	
	
	public static String getBranchName() {
		return branchName;
	}
	
	public static void setBranchName(String branchName) {
		Branch.branchName = branchName;
	}

	public static int getPORT() {
		return PORT;
	}

	public static void setPORT(int pORT) {
		PORT = pORT;
	}

	public static int getInitialBalance() {
		return initialBalance;
	}

	public static void setInitialBalance(int initialBalance) {
		Branch.initialBalance = initialBalance;
	}

	public static List<Bank.InitBranch.Branch> getBranchList() {
		return branchList;
	}

	public static void setBranchList(List<Bank.InitBranch.Branch> branchList) {
		Branch.branchList = branchList;
	}

	public static ServerSocket getServer() {
		return server;
	}

	public static void setServer(ServerSocket server) {
		Branch.server = server;
	}

	public static synchronized int updateCurBalance(int amount,String from){
//		System.out.println("Inside updateCurBal curBalance=="+curBalance+",  from="+from+", amount="+amount);
		curBalance = curBalance + amount;
//		System.out.println("Finished Final curBalance=="+curBalance+", from="+from);
		return curBalance;
	}
	
	
	public static void main(String arg[]){
		if(arg.length != 2){
			System.err.println("Provide argumensts as <BRANCH_NAME> <PORT>");
			System.exit(0);
		}
		branchName = arg[0];
		PORT = Integer.parseInt(arg[1]);
		System.out.println("branchName="+branchName+", Port="+PORT);
		idSnapshotMap = new ConcurrentHashMap<>();
		finishedSnapshotMap = new ConcurrentHashMap<>();
		Socket client = null;
		try{
			server = new ServerSocket(PORT);
			System.out.println("Listening for connection on port...."+server.getLocalPort());
			while(true){
				client = server.accept();
				InputStream inputStream = client.getInputStream();
			
				Bank.BranchMessage branchMsg = Bank.BranchMessage.parseDelimitedFrom(inputStream);
				String inbranchName = "";
				if(!(branchMsg.hasInitBranch() || branchMsg.hasInitSnapshot() || branchMsg.hasRetrieveSnapshot())) {
					int read = inputStream.read();
					byte[] inbranchNameBytes = new byte[read];
					inputStream.read(inbranchNameBytes);
					inbranchName = new String(inbranchNameBytes);
				}
//				System.out.println("inbranchName=="+inbranchName);
//				String inbranchName = getBranchNameBy(client.getInetAddress().toString(),client.getLocalPort());
				if(branchMsg.hasInitBranch()){
//					System.out.println("Inside Init Branch Message barnch");
					Bank.InitBranch initBranchMsg = branchMsg.getInitBranch();
					if(initBranchMsg.getBalance() < 0){
						System.out.println("Branches has to have non negative balance");
						client.close();
						continue;
					}
					updateCurBalance(initBranchMsg.getBalance(),"initBranch");
					initialBalance = initBranchMsg.getBalance();
					branchList = initBranchMsg.getAllBranchesList();
//					makeIpPortBranchMap();
//					System.out.println("BranchName="+branchName+", PORT="+PORT+", Initial Balance="+initialBalance+", CurrentBalance="+curBalance+", branchList="+branchList);
					// Starting the scheduling of money transfer
					if(branchList.size() > 1) // If no other branches in system no money transfer
						new Thread(new MoneyTransfeThread()).start();
//					timerTask = new MoneyTransfeThread();
//			        Timer timer = new Timer(true);
//			        int rndNum = new Random().nextInt(5)+1;
//			        System.out.println("rndNum="+rndNum);
//			        timer.scheduleAtFixedRate(timerTask, 0, 1000*rndNum);
				}else if(branchMsg.hasTransfer()){
					Bank.Transfer transferMsg = branchMsg.getTransfer();
//					synchronized (lock) { 
//						System.out.println("Update cur balance=");
//					System.out.println("Transfer Message came with money="+transferMsg.getMoney());
						updateCurBalance(transferMsg.getMoney(),"transferReceived");
						for(Integer sId:idSnapshotMap.keySet()){
							Snapshot snapshot = idSnapshotMap.get(sId);
//							System.out.println("Here="+snapshot);
//							System.out.println("Here="+snapshot.getrecoringInChannel().get(inbranchName));
							if(snapshot.getrecoringInChannel().get(inbranchName) != null){
//								System.out.println("Inside Added transfer message to the recording channel="+transferMsg.getMoney());
								snapshot.getrecoringInChannel().get(inbranchName).add(transferMsg.getMoney());
							}
						}
//							System.out.println("Finished Transfer Message came with money="+transferMsg.getMoney());
//						}
				}else if(branchMsg.hasInitSnapshot()){
					synchronized (lock) {
//						System.out.println("Inside initsnapshot msg="+branchName);
						Bank.InitSnapshot initSnapshotMsg = branchMsg.getInitSnapshot();
						
						createSnapshotById(initSnapshotMsg.getSnapshotId());
						startRecordingOnIncomingChannelBySnapshotId(initSnapshotMsg.getSnapshotId(),"");
						sendingMarkerToOtherBranches(initSnapshotMsg.getSnapshotId());
//						System.out.println("Finished initsnapshot msg="+branchName);
						if(getBranchList().size() == 1) {
							finishedSnapshotMap.put(initSnapshotMsg.getSnapshotId(),idSnapshotMap.remove(initSnapshotMsg.getSnapshotId()));
						}
					}
				}else if(branchMsg.hasMarker()){
					try{
//						System.out.println("Sleeping now....");
						Thread.sleep(1000);
//						System.out.println("Waking up....");
					}catch(Exception e){}
					synchronized (lock) {
						Bank.Marker markerMsg = branchMsg.getMarker();
						int snapshotId = markerMsg.getSnapshotId();
					
						if(idSnapshotMap.get(snapshotId) == null){
//							System.out.println("First time seeing marker from="+inbranchName+"==SID="+snapshotId);
							// First time marker received get the snapshot of the branch state and set this incoming channel empty
								
								createSnapshotById(snapshotId); // records its own state first
								idSnapshotMap.get(snapshotId).getrecordedInChannel().put(inbranchName, new ArrayList<Integer>()); //Marks the state of channel Cji as “empty”
								startRecordingOnIncomingChannelBySnapshotId(snapshotId, inbranchName);
								sendingMarkerToOtherBranches(snapshotId);
									
						}else{
//							System.out.println("Second time seeing marker from="+inbranchName+"==SID="+snapshotId);
							// Second time it is seeing
							Snapshot snapshot = idSnapshotMap.get(snapshotId);
							
//							System.out.println("Size before="+snapshot.getrecoringInChannel().size());
//							finish the recording on that incoming channel
							snapshot.getrecordedInChannel().put(inbranchName,snapshot.getrecoringInChannel().remove(inbranchName));
//							System.out.println("Stoped recording from "+inbranchName+"->"+getBranchName()+",State="+snapshot.getrecordedInChannel().get(inbranchName));
//							System.out.println("Size After="+snapshot.getrecoringInChannel().size());
						}
						if(idSnapshotMap.get(snapshotId).getrecoringInChannel().isEmpty()){
							// Case where we have two branh and second brach will just have recive one marker
							finishedSnapshotMap.put(snapshotId,idSnapshotMap.remove(snapshotId));
//							System.out.println("Finished Snapshot with id="+snapshotId);
						}
//						System.out.println("Finished hasMarker Message");
					}
				}else if(branchMsg.hasRetrieveSnapshot()){
//					Bank.RetrieveSnapshot reteriveSnapshotMsg = branchMsg.getRetrieveSnapshot();
					
					int snapshotId = branchMsg.getRetrieveSnapshot().getSnapshotId();
					Snapshot snapshot = finishedSnapshotMap.get(snapshotId);
//					System.out.println("Inside retrive snapshot id="+snapshot);
					if(snapshot == null) {
						System.out.println("Snapshot with id="+snapshotId+" not finished yet..");
						client.close();
						continue;
					}
					Bank.ReturnSnapshot.Builder returnSnapshotMsg = Bank.ReturnSnapshot.newBuilder();
					Bank.ReturnSnapshot.LocalSnapshot.Builder localSnapshotMsg = Bank.ReturnSnapshot.LocalSnapshot.newBuilder();
					localSnapshotMsg.setBalance(snapshot.balance);
					localSnapshotMsg.setSnapshotId(snapshotId);
					for(Bank.InitBranch.Branch branch:getBranchList()){
						if(branch.getName().equals(getBranchName())){
							localSnapshotMsg.addChannelState(0);
							continue;
						}
						int sum = 0;
						for(int num:snapshot.getrecordedInChannel().get(branch.getName())){
							sum+=num;
						}
						localSnapshotMsg.addChannelState(sum);
					}
					returnSnapshotMsg.setLocalSnapshot(localSnapshotMsg.build());
					Bank.BranchMessage.Builder sendBranchMsg = Bank.BranchMessage.newBuilder();
					sendBranchMsg.setReturnSnapshot(returnSnapshotMsg.build());
					sendBranchMsg.build().writeDelimitedTo(client.getOutputStream());
//					client.shutdownOutput();
					
				}
//				client.getInputStream().close();
				client.close();
			}
		}catch(Exception e){
			System.out.println(e.getMessage());
		}
	}
	private static void startRecordingOnIncomingChannelBySnapshotId(int snapshotId, String inBranchName) {
		// TODO Auto-generated method stub
		Snapshot snapshot = idSnapshotMap.get(snapshotId);
		for(Bank.InitBranch.Branch branch: getBranchList()){
			if(branch.getName().equals(getBranchName()))
				continue;
			if(!branch.getName().equals(inBranchName)){
//				System.out.println("Started recording from "+branch.getName()+"->"+getBranchName());
				snapshot.getrecoringInChannel().put(branch.getName(), new ArrayList<Integer>());
			}else if(!inBranchName.equals("")){
//				System.out.println("Stopped recording from "+inBranchName+"->"+getBranchName());
				snapshot.getrecordedInChannel().put(inBranchName, new ArrayList<Integer>());
			}
		}
	}

	private static void sendingMarkerToOtherBranches(int snapshotId) throws UnknownHostException, IOException {
		// TODO Auto-generated method stub
		for(Bank.InitBranch.Branch branch: getBranchList()){
			if(branch.getName().equals(getBranchName()))
				continue;
//			System.out.println("Sending marker from "+getBranchName()+" to "+branch.getName());
			Socket socket = new Socket(branch.getIp(), branch.getPort());
			Bank.Marker.Builder initMarkerMsg = Bank.Marker.newBuilder();
			initMarkerMsg.setSnapshotId(snapshotId);
			Bank.BranchMessage.Builder branchMsg = Bank.BranchMessage.newBuilder();
			branchMsg.setMarker(initMarkerMsg.build());
			
			
			branchMsg.build().writeDelimitedTo(socket.getOutputStream());
			socket.getOutputStream().write(getBranchName().length());
			socket.getOutputStream().write(getBranchName().getBytes());
			socket.close();
		}
	}

	private static void createSnapshotById(int snapshotId){
//		System.out.println("Inside createSnapshotById balance="+updateCurBalance(0,"createSnapshotById"));
		Snapshot snapshot = new Snapshot(snapshotId, updateCurBalance(0,"createSnapshotById")); // because two thread trying to access the same variable.
		idSnapshotMap.put(snapshotId, snapshot);
	}
}

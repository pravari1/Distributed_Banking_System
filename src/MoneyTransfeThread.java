import java.io.IOException;
import java.net.Socket;
import java.util.List;
import java.util.Random;

public class MoneyTransfeThread implements Runnable {
	static boolean isSendMarkerDone;
	public MoneyTransfeThread() {
		// TODO Auto-generated constructor stub
		isSendMarkerDone = true;
	}
	@Override
	public void run() {
		// TODO Auto-generated methodt
//		System.out.println("I am in run");
		while(true) {
			try {
				int rndNum = (new Random().nextInt(6));
//				System.out.println(0);
				Thread.sleep(1000 * rndNum);
			} catch (InterruptedException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
			synchronized (Branch.lock) {
//				System.out.println("Inside synchronized in MoneyTransfer");
				if(Branch.updateCurBalance(0,"MoneyTransfeThread") == 0){
//					System.out.println("Inside return thread1");
					continue;
				}
	//			System.out.println("isSendMarkerDone="+isSendMarkerDone);
	//			System.out.println("Inside lock");
				//		System.out.println("i came out from wait state herrrrr in MoneyTransfeThread");
				int percent = new Random().nextInt(5)+1;
//				System.out.println("percent="+percent+", curBalance="+Branch.updateCurBalance(0)+"==="+(Branch.getInitialBalance() * percent));
				int transferAmount = (int) ((Branch.getInitialBalance() * percent) /100);
//				System.out.println("Transfer Amount="+transferAmount+", curBalance="+Branch.updateCurBalance(0));
				if(transferAmount <= 0 || (Branch.updateCurBalance(0,"MoneyTransfeThread")-transferAmount) < 0) {
//					System.out.println("Inside return thread2");
					continue;
				}	
				//		System.out.println("Transfer amount="+transferAmount+",from "+Branch.getBranchName()+", New balance="+Branch.updateCurBalance(-transferAmount));
				Branch.updateCurBalance(-transferAmount,"MoneyTransfeThread");
				List<Bank.InitBranch.Branch> branchList = Branch.getBranchList();
				int branchSelection = new Random().nextInt(branchList.size());
				while(branchList.get(branchSelection).getName().equals(Branch.getBranchName())){
					branchSelection = new Random().nextInt(branchList.size());
				}
				try {
//					System.out.println("Transferring to="+branchList.get(branchSelection).getName());
					Socket socket = new Socket(branchList.get(branchSelection).getIp(), branchList.get(branchSelection).getPort());
					Bank.Transfer.Builder transferMessage = Bank.Transfer.newBuilder();
					transferMessage.setMoney(transferAmount);
					Bank.BranchMessage.Builder branchMsg = Bank.BranchMessage.newBuilder();
					branchMsg.setTransfer(transferMessage.build());
					
					
					branchMsg.build().writeDelimitedTo(socket.getOutputStream());
					socket.getOutputStream().write(Branch.getBranchName().length());
					socket.getOutputStream().write(Branch.getBranchName().getBytes());
//					socket.getOutputStream().close();
					socket.close();
					//			System.out.println("Transferd to="+branchList.get(branchSelection).getName());
				} catch (IOException e) {
					// TODO Auto-generated catch block
					//			e.printStackTrace();
				}
				//		Branch.lock.notifyAll();
//				System.out.println("Inside END synchronized in MoneyTransfer");
			}//sync
		}
	}

}

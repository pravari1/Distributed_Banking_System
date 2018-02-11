import java.io.IOException;
import java.util.TimerTask;

public class AutomatedInitSnapshotTread implements Runnable {

	@Override
	public void run() {
		// TODO Auto-generated method stub
		System.out.println("Inside run controller");
		try {
			Controller.sendInitSnapshotMessage();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}

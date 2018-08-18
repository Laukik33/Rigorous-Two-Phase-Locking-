package TwoPhaseLocking;
//Lock class that stores attributes of lock table
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

public class Lock {
	
	public String item_name;
	public String lock_state = "UL";
	public List<String> tran_holding_lock = new ArrayList();
	public List<String> tran_waiting = new ArrayList();

}

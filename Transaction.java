package TwoPhaseLocking;
//Transaction class that stores the attributes of transaction table
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;


public class Transaction {
  
	public String tran_ID;
	public Integer tran_Timestamp;
	public String tran_State;
	public List<String> Locked_items = new ArrayList();
	public List<String> oper_Waiting = new ArrayList();

	
	
}
